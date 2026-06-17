// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use restate_memory::LocalMemoryPool;
use restate_types::identifiers::EntryIndex;
use restate_types::invocation::FencingToken;
use restate_types::retries;
use restate_types::schema::invocation_target::OnMaxAttempts;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::vqueues::VQueueId;
use restate_worker_api::resources::ReservedResources;

use crate::error::RequestedErrorBehavior;
use crate::quota::ConcurrencySlot;

use super::*;

/// Trait alias for types that can be used as retry timer keys.
/// This allows tests to use a simple mock key type instead of requiring a real `DelayQueue`.
pub(super) trait TimerKey: Copy + PartialEq + Eq + fmt::Debug + Send + 'static {}

// Blanket implementation for any type that satisfies the bounds
impl<T: Copy + PartialEq + Eq + fmt::Debug + Send + 'static> TimerKey for T {}

/// Component encapsulating the business logic of the invocation state machine.
///
/// The type parameter `K` represents the timer key type used for retry timers.
/// In production, this is `tokio_util::time::delay_queue::Key`.
/// In tests, this can be a simple mock type to avoid needing a real `DelayQueue`.
#[derive(derive_more::Debug)]
pub(super) struct InvocationStateMachine<K: TimerKey = tokio_util::time::delay_queue::Key> {
    #[allow(dead_code)]
    pub(super) qid: Option<VQueueId>,
    #[allow(dead_code)]
    #[debug(skip)]
    pub(super) _permit: ReservedResources,
    /// The invoker-task generation this state machine represents. Stamped onto
    /// every effect this ISM emits so the partition processor can fence stale
    /// effects from a previous attempt.
    pub(super) fencing_token: FencingToken,
    pub(super) invocation_target: InvocationTarget,
    pub(super) limit_key: LimitKey<ReString>,
    pub(super) idempotency_key: Option<ReString>,
    pub(super) last_transient_error_event: Option<TransientErrorEvent>,
    invocation_state: AttemptState<K>,
    retry_policy_state: RetryPolicyState,
    /// This retry count is passed in the StartMessage.
    /// For more details of when we bump it, see [`InvokerError::should_bump_start_message_retry_count_since_last_stored_entry`].
    pub(super) start_message_retry_count_since_last_stored_command: u32,
    pub(super) requested_pause: bool,
    _concurrency_slot: ConcurrencySlot,
    /// Per-invocation memory budget, preserved across retries to avoid
    /// re-acquiring from the global pool. `None` before the first task
    /// starts and after the ISM is finally cleaned up.
    #[debug(skip)]
    pub(super) budget: Option<LocalMemoryPool>,
}

/// This struct tracks which commands the invocation task generates,
/// and which ones have been already stored and acked by the partition processor.
/// This information is used to decide when it's safe to retry.
///
/// Every time the invocation task generates a new command, the index is notified to this struct with
/// [`JournalTracker::notify_command_sent_to_partition_processor`], and every time the invoker receives
/// [`InputCommand::StoredCommandAck`], the index is notified to this struct with [`JournalTracker::notify_acked_command_from_partition_processor`].
/// Similar story applies to notifications, because we need to wait for the self-proposed notifications from the SDK to be propagated to the
/// partition processor before retrying.
///
/// After the retry timer is fired, we can check whether we can retry immediately or not with [`JournalTracker::can_retry`].
///
/// **Note**: For Journal V1 Command == Entry
#[derive(Default, Debug, Clone)]
struct JournalTracker {
    last_acked_command_from_partition_processor: Option<CommandIndex>,
    last_command_sent_to_partition_processor: Option<CommandIndex>,
    last_acked_notifications_from_partition_processor: HashSet<NotificationId>,
    last_notifications_proposed_to_partition_processor: HashSet<NotificationId>,
}

impl JournalTracker {
    fn notify_acked_command_from_partition_processor(&mut self, idx: CommandIndex) {
        self.last_acked_command_from_partition_processor =
            cmp::max(Some(idx), self.last_acked_command_from_partition_processor)
    }

    fn notify_command_sent_to_partition_processor(&mut self, idx: CommandIndex) {
        self.last_command_sent_to_partition_processor =
            cmp::max(Some(idx), self.last_command_sent_to_partition_processor)
    }

    fn notify_acked_notification_from_partition_processor(&mut self, id: NotificationId) {
        self.last_acked_notifications_from_partition_processor
            .insert(id);
    }

    fn notify_notification_proposed_to_partition_processor(&mut self, id: NotificationId) {
        self.last_notifications_proposed_to_partition_processor
            .insert(id);
    }

    fn can_retry(&self) -> bool {
        let commands_condition = match (
            self.last_acked_command_from_partition_processor,
            self.last_command_sent_to_partition_processor,
        ) {
            (_, None) => {
                // The invocation task didn't generate new commands.
                // We're always good to retry in this case.
                true
            }
            (Some(last_acked_command), Some(last_proposed_command)) => {
                // Last acked must be higher than last sent,
                // otherwise we'll end up retrying when not all the commands have been stored.
                last_acked_command >= last_proposed_command
            }
            _ => false,
        };
        if !commands_condition {
            return false;
        }

        // If the notifications sent from PP to invoker contains all the ones sent from Invoker to PP, we're good to retry.
        let notifications_condition = self
            .last_notifications_proposed_to_partition_processor
            .is_subset(&self.last_acked_notifications_from_partition_processor);

        commands_condition && notifications_condition
    }
}

enum AttemptState<K: TimerKey> {
    New,

    InFlight {
        // If there is no completions_tx,
        // then the stream is open in request/response mode
        notifications_tx: Option<mpsc::UnboundedSender<Notification>>,
        journal_tracker: JournalTracker,
        abort_handle: AbortHandle,

        // Acks that should be propagated back to the SDK
        command_acks_to_propagate: HashSet<CommandIndex>,

        // Run completions the SDK proposed during this attempt. When the partition
        // processor echoes the stored notification back via [`Self::notify_entry`],
        // we swap [`Notification::Entry`] for [`Notification::ProposeRunCompletionAck`]
        // so the SDK gets the ack message on the wire (only protocol >= v7).
        //
        // The SDK will replace the ack message with the full notification, kept around locally.
        //
        // This mechanism is used only in PROCESSING, for run completions proposed during the current attempt,
        // and not when the invocation is REPLAYING.
        run_completion_proposals_to_ack: HashSet<CompletionId>,

        // Deployment being used during this attempt
        using_deployment: Option<PinnedDeployment>,
        // If true, we need to notify the deployment id to the partition processor
        should_notify_pinned_deployment: bool,
    },

    WaitingRetry {
        timer_fired: bool,
        journal_tracker: JournalTracker,
        // used to guard against orphaned retry timers
        retry_timer_key: K,
    },
}

impl<K: TimerKey> fmt::Debug for AttemptState<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttemptState::New => f.write_str("New"),
            AttemptState::InFlight {
                journal_tracker,
                abort_handle,
                notifications_tx,
                ..
            } => f
                .debug_struct("InFlight")
                .field("journal_tracker", journal_tracker)
                .field("abort_handle", abort_handle)
                .field(
                    "notifications_tx_open",
                    &notifications_tx
                        .as_ref()
                        .map(|s| !s.is_closed())
                        .unwrap_or(false),
                )
                .finish(),
            AttemptState::WaitingRetry {
                journal_tracker,
                timer_fired,
                retry_timer_key,
            } => f
                .debug_struct("WaitingRetry")
                .field("journal_tracker", journal_tracker)
                .field("timer_fired", timer_fired)
                .field("retry_timer_key", retry_timer_key)
                .finish(),
        }
    }
}

#[derive(Debug)]
struct RetryPolicyState {
    selected_from_deployment_id: Option<DeploymentId>,
    retry_iter: retries::RetryIter<'static>,
    on_max_attempts: OnMaxAttempts,
}

impl<K: TimerKey> InvocationStateMachine<K> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn create(
        qid: Option<VQueueId>,
        permit: ReservedResources,
        fencing_token: FencingToken,
        invocation_target: InvocationTarget,
        limit_key: LimitKey<ReString>,
        idempotency_key: Option<ReString>,
        retry_iter: retries::RetryIter<'static>,
        on_max_attempts: OnMaxAttempts,
        concurrency_slot: ConcurrencySlot,
    ) -> InvocationStateMachine<K> {
        let start_message_retry_count_since_last_stored_command =
            permit.metadata.retry_count_since_last_stored_command;

        Self {
            qid,
            _permit: permit,
            fencing_token,
            invocation_target,
            limit_key,
            idempotency_key,
            last_transient_error_event: None,
            invocation_state: AttemptState::New,
            retry_policy_state: RetryPolicyState {
                selected_from_deployment_id: None,
                retry_iter,
                on_max_attempts,
            },
            start_message_retry_count_since_last_stored_command,
            requested_pause: false,
            _concurrency_slot: concurrency_slot,
            budget: None,
        }
    }

    pub(super) fn start(
        &mut self,
        abort_handle: AbortHandle,
        notifications_tx: mpsc::UnboundedSender<Notification>,
    ) {
        debug_assert!(matches!(
            &self.invocation_state,
            AttemptState::New | AttemptState::WaitingRetry { .. }
        ));

        self.invocation_state = AttemptState::InFlight {
            notifications_tx: Some(notifications_tx),
            journal_tracker: Default::default(),
            abort_handle,
            command_acks_to_propagate: Default::default(),
            run_completion_proposals_to_ack: Default::default(),
            using_deployment: None,
            should_notify_pinned_deployment: false,
        };
    }

    /// The cumulative number of retry attempts this invocation has made so far
    pub(super) fn retry_attempts(&self) -> usize {
        self.retry_policy_state.retry_iter.attempts()
    }

    pub(super) fn abort(&mut self) {
        if let AttemptState::InFlight { abort_handle, .. } = &mut self.invocation_state {
            abort_handle.abort();
        }
    }

    pub(super) fn update_retry_policy_if_needed(
        &mut self,
        selected_deployment_id: DeploymentId,
        target_resolver: &impl InvocationTargetResolver,
    ) {
        if self
            .retry_policy_state
            .selected_from_deployment_id
            .is_some_and(|dp| dp == selected_deployment_id)
        {
            // No need to update, the retry is on the same deployment as before
            return;
        }

        let (mut retry_iter, on_max_attempts) = target_resolver.resolve_invocation_retry_policy(
            Some(&selected_deployment_id),
            self.invocation_target.service_name(),
            self.invocation_target.handler_name(),
        );
        // We advance the retry iterator to continue the same retry journey as previous
        // incarinations.
        retry_iter.fast_forward(self.retry_policy_state.retry_iter.attempts());
        self.retry_policy_state = RetryPolicyState {
            selected_from_deployment_id: Some(selected_deployment_id),
            retry_iter,
            on_max_attempts,
        }
    }

    pub(super) fn notify_pinned_deployment(
        &mut self,
        deployment: PinnedDeployment,
        has_changed: bool,
    ) {
        debug_assert!(matches!(
            &self.invocation_state,
            AttemptState::InFlight {
                using_deployment: None,
                ..
            }
        ));

        if let AttemptState::InFlight {
            using_deployment: pinned_deployment,
            should_notify_pinned_deployment,
            ..
        } = &mut self.invocation_state
        {
            *pinned_deployment = Some(deployment);
            // If the deployment has changed, we should notify the pinned deployment on the next entry produced.
            // See call sites of pinned_deployment_to_notify() for more details on when this happens.
            *should_notify_pinned_deployment = has_changed;
        }
    }

    pub(super) fn pinned_deployment_to_notify(&mut self) -> Option<PinnedDeployment> {
        debug_assert!(matches!(
            &self.invocation_state,
            AttemptState::InFlight { .. }
        ));

        if let AttemptState::InFlight {
            using_deployment: ref using_deployment_id,
            ref mut should_notify_pinned_deployment,
            ..
        } = self.invocation_state
        {
            if *should_notify_pinned_deployment && let Some(pinned_deployment) = using_deployment_id
            {
                // When notifying the pinned deployment, we also set the protocol version
                *should_notify_pinned_deployment = false;
                Some(pinned_deployment.clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(super) fn notify_new_command(&mut self, command_index: CommandIndex, requested_ack: bool) {
        debug_assert!(matches!(
            &self.invocation_state,
            AttemptState::InFlight { .. }
        ));

        // Invocation made some progress, reset the retry count and reset the last_transient_error_event
        self.start_message_retry_count_since_last_stored_command = 0;
        self.last_transient_error_event = None;

        if let AttemptState::InFlight {
            journal_tracker,
            command_acks_to_propagate: entries_to_ack,
            ..
        } = &mut self.invocation_state
        {
            if requested_ack {
                entries_to_ack.insert(command_index);
            }
            journal_tracker.notify_command_sent_to_partition_processor(command_index);
        }
    }

    pub(super) fn notify_new_notification_proposal(
        &mut self,
        notification_type: NotificationType,
        notification_id: NotificationId,
        requested_ack: bool,
    ) {
        debug_assert!(matches!(
            &self.invocation_state,
            AttemptState::InFlight { .. }
        ));

        // The only notification proposal currently defined in the protocol is
        // ProposeRunCompletionMessage. We assert the invariant explicitly so that
        // if/when new proposal-like messages are added, this code is forced to be
        // revisited rather than silently mistreating them as run completions.
        assert_eq!(
            notification_type,
            NotificationType::Completion(CompletionType::Run),
            "the only notification proposal currently defined in the protocol is ProposeRunCompletionMessage",
        );
        let completion_id = *notification_id
            .try_as_completion_id_ref()
            .expect("RunCompletion notification id must be a CompletionId");

        if let AttemptState::InFlight {
            journal_tracker,
            run_completion_proposals_to_ack,
            using_deployment,
            ..
        } = &mut self.invocation_state
        {
            // We track the run completion proposal to ack only if the SDK asked for
            // an ack (header flag) AND the negotiated protocol supports it (>= v7).
            // If either condition is missing, the proposal flows through the normal
            // `notify_entry` → `Notification::Entry` path and the SDK receives the
            // full `RunCompletionNotificationMessage` like on older protocols.
            if requested_ack
                && let Some(pinned_deployment) = using_deployment
                && pinned_deployment.service_protocol_version >= ServiceProtocolVersion::V7
            {
                run_completion_proposals_to_ack.insert(completion_id);
            }
            journal_tracker.notify_notification_proposed_to_partition_processor(notification_id);
        }
    }

    pub(super) fn notify_stored_ack(&mut self, command_index: CommandIndex) {
        match &mut self.invocation_state {
            AttemptState::InFlight {
                journal_tracker,
                command_acks_to_propagate,
                notifications_tx,
                ..
            } => {
                if command_acks_to_propagate.remove(&command_index) {
                    Self::try_send_notification(
                        notifications_tx,
                        Notification::CommandAck(command_index),
                    );
                }
                journal_tracker.notify_acked_command_from_partition_processor(command_index);
            }
            AttemptState::WaitingRetry {
                journal_tracker, ..
            } => {
                journal_tracker.notify_acked_command_from_partition_processor(command_index);
            }
            _ => {}
        }
    }

    pub(super) fn notify_completion(&mut self, entry_index: EntryIndex) {
        if let AttemptState::InFlight {
            notifications_tx, ..
        } = &mut self.invocation_state
        {
            Self::try_send_notification(notifications_tx, Notification::Completion(entry_index));
        }
    }

    pub(super) fn notify_entry(
        &mut self,
        entry_index: EntryIndex,
        notification_id: NotificationId,
    ) {
        match &mut self.invocation_state {
            AttemptState::InFlight {
                journal_tracker,
                notifications_tx,
                run_completion_proposals_to_ack,
                ..
            } => {
                let to_send = match &notification_id {
                    // We send RunCompletionAck only if we're tracking this specific run completion.
                    NotificationId::CompletionId(c)
                        if run_completion_proposals_to_ack.remove(c) =>
                    {
                        Notification::ProposeRunCompletionAck(*c)
                    }
                    _ => Notification::Entry(entry_index),
                };
                journal_tracker.notify_acked_notification_from_partition_processor(notification_id);
                Self::try_send_notification(notifications_tx, to_send);
            }
            AttemptState::WaitingRetry {
                journal_tracker, ..
            } => {
                journal_tracker.notify_acked_notification_from_partition_processor(notification_id);
            }
            _ => {}
        }
    }

    pub(super) fn try_send_notification(
        notifications_tx: &mut Option<mpsc::UnboundedSender<Notification>>,
        notification: Notification,
    ) {
        *notifications_tx = notifications_tx.take().and_then(move |sender| {
            if sender.send(notification).is_ok() {
                Some(sender)
            } else {
                None
            }
        });
    }

    /// Notifies that a retry timer has fired.
    /// Only reacts if the fired timer's key matches the key stored in WaitingRetry state.
    /// This provides defense-in-depth against stale timers.
    pub(super) fn notify_retry_timer_fired(&mut self, fired_key: K) {
        if let AttemptState::WaitingRetry {
            timer_fired,
            retry_timer_key,
            ..
        } = &mut self.invocation_state
        {
            // Only react if the fired timer matches our current timer
            if *retry_timer_key == fired_key {
                *timer_fired = true;
            }
            // Otherwise ignore - it's a stale timer from a previous state
        }
        // If not in WaitingRetry state, ignore the timer
    }

    /// Takes the retry timer key if in WaitingRetry state.
    /// Returns None if not in WaitingRetry state.
    pub(super) fn take_retry_timer_key(&self) -> Option<K> {
        match &self.invocation_state {
            AttemptState::WaitingRetry {
                retry_timer_key, ..
            } => Some(*retry_timer_key),
            _ => None,
        }
    }

    pub(super) fn handle_task_error(
        &mut self,
        error_is_transient: bool,
        requested_error_behavior: RequestedErrorBehavior,
        should_bump_start_message_retry_count_since_last_stored_command: bool,
        register_timer: impl FnOnce(Duration) -> K,
    ) -> OnTaskError {
        if self.requested_pause {
            // Shortcircuit to pause, as this is what the user asked for
            return OnTaskError::Pause;
        }

        let journal_tracker = match self.invocation_state {
            AttemptState::InFlight {
                ref journal_tracker,
                ..
            } => Some(journal_tracker),
            AttemptState::New => None,
            AttemptState::WaitingRetry {
                ref journal_tracker,
                timer_fired,
                ..
            } => {
                // TODO: https://github.com/restatedev/restate/issues/538
                assert!(
                    timer_fired,
                    "Restate does not support multiple retry timers yet. This would require \
                        deduplicating timers by some mean (e.g. fencing them off, overwriting \
                        old timers, not registering a new timer if an old timer has not fired yet, etc.)"
                );
                Some(journal_tracker)
            }
        };

        // The SDK can request a specific behavior, which takes precedence over the retry policy.
        let next_retry_interval_override = match requested_error_behavior {
            RequestedErrorBehavior::Pause => return OnTaskError::Pause,
            RequestedErrorBehavior::Fail => return OnTaskError::Fail,
            RequestedErrorBehavior::Retry => None,
            RequestedErrorBehavior::RetryWithIntervalOverride(interval) => Some(interval),
        };

        if error_is_transient
            && let Some(next_timer) =
                next_retry_interval_override.or_else(|| self.retry_policy_state.retry_iter.next())
        {
            if should_bump_start_message_retry_count_since_last_stored_command {
                self.start_message_retry_count_since_last_stored_command += 1;
            }

            // if Qid is present, vqueues are used so we switch into retrying via the scheduler
            // when the retry interval is greater > (threshold) second.
            if let Some(ref qid) = self.qid
                && next_timer
                    >= Configuration::pinned()
                        .invocation
                        .invocation_yield_threshold()
            {
                trace!(
                    vqueue = %qid,
                    "Invocation is using vqueues, switching to retrying via scheduler");
                return OnTaskError::RetryViaScheduler {
                    retry_after: next_timer,
                    retry_attempts: u32::try_from(self.retry_policy_state.retry_iter.attempts())
                        .unwrap_or(u32::MAX),
                    retry_count_since_last_stored_command: self
                        .start_message_retry_count_since_last_stored_command,
                };
            };
            let retry_timer_key = register_timer(next_timer);
            self.invocation_state = AttemptState::WaitingRetry {
                timer_fired: false,
                journal_tracker: journal_tracker.cloned().unwrap_or_default(),
                retry_timer_key,
            };
            OnTaskError::Retrying(next_timer)
        } else {
            match self.retry_policy_state.on_max_attempts {
                OnMaxAttempts::Pause => OnTaskError::Pause,
                OnMaxAttempts::Kill => OnTaskError::Fail,
            }
        }
    }

    pub(super) fn is_ready_to_retry(&self) -> bool {
        match &self.invocation_state {
            AttemptState::WaitingRetry {
                timer_fired,
                journal_tracker,
                ..
            } => *timer_fired && journal_tracker.can_retry(),
            _ => false,
        }
    }

    pub(super) fn attempt_deployment_id(&self) -> AttemptDeploymentId {
        AttemptDeploymentId(match &self.invocation_state {
            AttemptState::InFlight {
                using_deployment, ..
            } => using_deployment.as_ref().map(|pd| pd.deployment_id),
            _ => None,
        })
    }

    // If returns true, we should pause now, otherwise we should wait for that.
    pub(super) fn notify_pause(&mut self) -> bool {
        self.requested_pause = true;
        if let AttemptState::InFlight {
            notifications_tx, ..
        } = &mut self.invocation_state
        {
            // Close notifications_tx to trigger suspension
            *notifications_tx = None;

            // Invocation is still in-flight, pause will happen later on
            return false;
        }

        // Invocation is not in-flight, all good we can pause now
        true
    }

    pub(crate) fn should_emit_transient_error_event(
        &mut self,
        new_error_event: &TransientErrorEvent,
    ) -> bool {
        let Some(old_error_event) = &self.last_transient_error_event else {
            // We don't have last transient error, all good, emit it.
            self.last_transient_error_event = Some(new_error_event.clone());
            return true;
        };

        let should_emit = !(old_error_event.error_code == new_error_event.error_code
            && old_error_event.error_message == new_error_event.error_message
            && old_error_event.related_command_index == new_error_event.related_command_index
            && old_error_event.related_command_type == new_error_event.related_command_type
            && old_error_event.related_command_name == new_error_event.related_command_name);

        if should_emit {
            self.last_transient_error_event = Some(new_error_event.clone());
        }

        should_emit
    }

    #[inline]
    pub(super) fn invocation_state_debug(&self) -> impl fmt::Debug + '_ {
        &self.invocation_state
    }
}

#[derive(Debug)]
pub(super) enum OnTaskError {
    RetryViaScheduler {
        retry_after: Duration,
        /// For service-level retry configuration. This is the total number of retries
        /// we have performed throughout.
        retry_attempts: u32,
        /// For sdk-controlled retries. This defines the retry-count value that will be
        /// sent downstream to the SDK to be used for its ctx.run() retries on the next
        /// start message.
        retry_count_since_last_stored_command: u32,
    },
    Retrying(Duration),
    Pause,
    Fail,
}

pub(super) struct AttemptDeploymentId(Option<DeploymentId>);

impl fmt::Display for AttemptDeploymentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(dp) => fmt::Display::fmt(&dp, f),
            None => write!(f, "unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::matchers::{eq, some};
    use googletest::prelude::err;
    use googletest::{assert_that, pat};
    use std::time::Duration;
    use test_log::test;
    use tokio::sync::mpsc::error::TryRecvError;

    use restate_test_util::{assert, check, let_assert};
    use restate_types::retries::RetryPolicy;

    fn create_test_invocation_state_machine() -> InvocationStateMachine<u64> {
        InvocationStateMachine::create(
            None,
            ReservedResources::new_empty(),
            0,
            InvocationTarget::mock_virtual_object(),
            LimitKey::None,
            None,
            RetryPolicy::fixed_delay(Duration::from_secs(1), Some(10)).into_iter(),
            OnMaxAttempts::Kill,
            ConcurrencySlot::empty(),
        )
    }

    #[test]
    fn handle_error_when_waiting_for_retry() {
        let mut invocation_state_machine = create_test_invocation_state_machine();

        assert_that!(
            invocation_state_machine.handle_task_error(
                true,
                RequestedErrorBehavior::Retry,
                true,
                |_| 0
            ),
            pat!(OnTaskError::Retrying(_))
        );
        assert_that!(
            invocation_state_machine.invocation_state,
            pat!(AttemptState::WaitingRetry { .. })
        );

        invocation_state_machine.notify_retry_timer_fired(0);

        // We stay in `WaitingForRetry`
        assert_that!(
            invocation_state_machine.handle_task_error(
                true,
                RequestedErrorBehavior::Retry,
                true,
                |_| 1
            ),
            pat!(OnTaskError::Retrying(_))
        );
        assert_that!(
            invocation_state_machine.invocation_state,
            pat!(AttemptState::WaitingRetry { .. })
        );
    }

    #[test]
    fn handle_error_with_pause_behavior_pauses() {
        let mut invocation_state_machine = create_test_invocation_state_machine();

        // Even though the error is transient and the retry policy allows more retries,
        // the SDK-requested Pause behavior takes precedence.
        assert_that!(
            invocation_state_machine.handle_task_error(
                true,
                RequestedErrorBehavior::Pause,
                true,
                |_| 0
            ),
            pat!(OnTaskError::Pause)
        );
    }

    #[test]
    fn handle_error_with_fail_behavior_kills() {
        let mut invocation_state_machine = create_test_invocation_state_machine();

        // Even though the error is transient and the retry policy allows more retries,
        // the SDK-requested Fail behavior takes precedence and fails without retrying.
        assert_that!(
            invocation_state_machine.handle_task_error(
                true,
                RequestedErrorBehavior::Fail,
                true,
                |_| 0
            ),
            pat!(OnTaskError::Fail)
        );
    }

    #[test(tokio::test)]
    async fn handle_requested_ack() {
        let mut invocation_state_machine = create_test_invocation_state_machine();

        let abort_handle = tokio::spawn(async {}).abort_handle();
        let (tx, mut rx) = mpsc::unbounded_channel();

        invocation_state_machine.start(abort_handle, tx);
        invocation_state_machine.notify_new_command(1, true);
        invocation_state_machine.notify_new_command(2, false);
        invocation_state_machine.notify_new_command(3, true);

        invocation_state_machine.notify_stored_ack(1);
        invocation_state_machine.notify_stored_ack(2);
        invocation_state_machine.notify_stored_ack(3);

        // Check notification was sent for ack 1 and 3
        let notification = rx.recv().await;
        assert_that!(notification, some(pat!(Notification::CommandAck(eq(1)))));
        let notification = rx.recv().await;
        assert_that!(notification, some(pat!(Notification::CommandAck(eq(3)))));

        // Channel should be empty
        let try_recv = rx.try_recv();
        assert_that!(try_recv, err(eq(TryRecvError::Empty)));
    }

    fn start_with_protocol(
        ism: &mut InvocationStateMachine<u64>,
        version: ServiceProtocolVersion,
    ) -> mpsc::UnboundedReceiver<Notification> {
        let abort_handle = tokio::spawn(async {}).abort_handle();
        let (tx, rx) = mpsc::unbounded_channel();
        ism.start(abort_handle, tx);
        ism.notify_pinned_deployment(
            PinnedDeployment::new(DeploymentId::default(), version),
            true,
        );
        rx
    }

    #[test(tokio::test)]
    async fn notify_entry_swaps_proposed_run_completion_to_ack_on_v7() {
        let mut ism = create_test_invocation_state_machine();
        let mut rx = start_with_protocol(&mut ism, ServiceProtocolVersion::V7);

        // Track a proposal for CompletionId 5 with requested_ack=true
        ism.notify_new_notification_proposal(
            NotificationType::Completion(CompletionType::Run),
            NotificationId::CompletionId(5),
            true,
        );

        // PP echoes back the stored notification — the ISM must swap to the ack
        ism.notify_entry(7, NotificationId::CompletionId(5));
        assert_that!(
            rx.recv().await,
            some(pat!(Notification::ProposeRunCompletionAck(eq(5))))
        );

        // The proposal was consumed: a second notify_entry for the same id falls
        // back to the regular Entry path.
        ism.notify_entry(7, NotificationId::CompletionId(5));
        assert_that!(rx.recv().await, some(pat!(Notification::Entry(eq(7)))));
    }

    #[test(tokio::test)]
    async fn notify_entry_does_not_swap_on_v6_old_path_preserved() {
        let mut ism = create_test_invocation_state_machine();
        let mut rx = start_with_protocol(&mut ism, ServiceProtocolVersion::V6);

        // Proposal is recorded in the journal tracker (for retry safety) but NOT
        // tracked for swapping, because the deployment is on protocol v6 — even
        // if the SDK had set requested_ack, the runtime caps the behaviour at v7.
        ism.notify_new_notification_proposal(
            NotificationType::Completion(CompletionType::Run),
            NotificationId::CompletionId(5),
            true,
        );

        // PP echoes back the stored notification — the ISM forwards the full Entry,
        // exactly like before protocol v7 existed.
        ism.notify_entry(7, NotificationId::CompletionId(5));
        assert_that!(rx.recv().await, some(pat!(Notification::Entry(eq(7)))));
    }

    #[test(tokio::test)]
    async fn notify_entry_on_v7_without_requested_ack_falls_through_to_entry() {
        let mut ism = create_test_invocation_state_machine();
        let mut rx = start_with_protocol(&mut ism, ServiceProtocolVersion::V7);

        // V7 deployment but SDK did NOT set the requested_ack header flag — the
        // proposal is tracked in the journal tracker for retry safety, but no
        // swap happens, and the SDK gets the full notification back.
        ism.notify_new_notification_proposal(
            NotificationType::Completion(CompletionType::Run),
            NotificationId::CompletionId(5),
            false,
        );

        ism.notify_entry(7, NotificationId::CompletionId(5));
        assert_that!(rx.recv().await, some(pat!(Notification::Entry(eq(7)))));
    }

    #[test(tokio::test)]
    async fn notify_entry_on_v7_without_proposal_falls_through_to_entry() {
        let mut ism = create_test_invocation_state_machine();
        let mut rx = start_with_protocol(&mut ism, ServiceProtocolVersion::V7);

        // No prior proposal: a completion notification flows through as Entry.
        ism.notify_entry(3, NotificationId::CompletionId(5));
        assert_that!(rx.recv().await, some(pat!(Notification::Entry(eq(3)))));

        // Signals are also never swapped — only completion ids tracked at propose time qualify.
        ism.notify_entry(4, NotificationId::SignalIndex(17));
        assert_that!(rx.recv().await, some(pat!(Notification::Entry(eq(4)))));
    }

    #[test(tokio::test)]
    async fn handle_error_counts_attempts_on_same_entry() {
        let mut invocation_state_machine = create_test_invocation_state_machine();

        // Start invocation
        invocation_state_machine.start(
            tokio::spawn(async {}).abort_handle(),
            mpsc::unbounded_channel().0,
        );

        // Notify error
        let_assert!(
            OnTaskError::Retrying(_) = invocation_state_machine.handle_task_error(
                true,
                RequestedErrorBehavior::Retry,
                true,
                |_| 0
            )
        );
        assert_eq!(
            invocation_state_machine.start_message_retry_count_since_last_stored_command,
            1
        );

        // Try to start again
        invocation_state_machine.start(
            tokio::spawn(async {}).abort_handle(),
            mpsc::unbounded_channel().0,
        );

        // Get error again
        let_assert!(
            OnTaskError::Retrying(_) = invocation_state_machine.handle_task_error(
                true,
                RequestedErrorBehavior::Retry,
                true,
                |_| 1
            )
        );
        assert_eq!(
            invocation_state_machine.start_message_retry_count_since_last_stored_command,
            2
        );

        // Try to start again
        invocation_state_machine.start(
            tokio::spawn(async {}).abort_handle(),
            mpsc::unbounded_channel().0,
        );
        assert_eq!(
            invocation_state_machine.start_message_retry_count_since_last_stored_command,
            2
        );

        // Now complete the entry
        invocation_state_machine.notify_new_command(1, false);
        assert_eq!(
            invocation_state_machine.start_message_retry_count_since_last_stored_command,
            0
        );
    }

    #[test(tokio::test)]
    async fn journal_tracker_correctly_tracks_commands() {
        let mut invocation_state_machine = create_test_invocation_state_machine();

        let abort_handle = tokio::spawn(async {}).abort_handle();
        let (tx, _rx) = mpsc::unbounded_channel();

        invocation_state_machine.start(abort_handle, tx);

        // Invoker generates entry 1
        invocation_state_machine.notify_new_command(1, false);
        let_assert!(
            OnTaskError::Retrying(_) = invocation_state_machine.handle_task_error(
                true,
                RequestedErrorBehavior::Retry,
                true,
                |_| 0
            )
        );

        // PP sends ack for command 1
        invocation_state_machine.notify_stored_ack(1);

        // Still waiting retry timer fired
        assert!(!invocation_state_machine.is_ready_to_retry());
        assert!(let AttemptState::WaitingRetry { .. } = invocation_state_machine.invocation_state);

        // After the retry timer fires, we're ready to retry
        invocation_state_machine.notify_retry_timer_fired(0);
        assert!(invocation_state_machine.is_ready_to_retry());
    }

    #[test(tokio::test)]
    async fn journal_tracker_correctly_tracks_notification_proposals() {
        let mut invocation_state_machine = create_test_invocation_state_machine();

        let abort_handle = tokio::spawn(async {}).abort_handle();
        let (tx, _rx) = mpsc::unbounded_channel();

        invocation_state_machine.start(abort_handle, tx);
        // Only RunCompletion notifications are valid proposals today; the ISM asserts this.
        // requested_ack=false because this test is about journal-tracker accounting for
        // retry safety, not about the v7 ack swap.
        invocation_state_machine.notify_new_notification_proposal(
            NotificationType::Completion(CompletionType::Run),
            NotificationId::CompletionId(18),
            false,
        );
        invocation_state_machine.notify_new_notification_proposal(
            NotificationType::Completion(CompletionType::Run),
            NotificationId::CompletionId(1),
            false,
        );
        let_assert!(
            OnTaskError::Retrying(_) = invocation_state_machine.handle_task_error(
                true,
                RequestedErrorBehavior::Retry,
                true,
                |_| 0
            )
        );

        // Waiting notifications acks and retry timer fired
        assert!(!invocation_state_machine.is_ready_to_retry());
        assert!(let AttemptState::WaitingRetry { .. } = invocation_state_machine.invocation_state);

        // Got completion 18
        invocation_state_machine.notify_entry(0, NotificationId::CompletionId(18));

        // Retry timer fired
        invocation_state_machine.notify_retry_timer_fired(0);

        // Waiting notifications acks
        assert!(!invocation_state_machine.is_ready_to_retry());
        assert!(let AttemptState::WaitingRetry { .. } = invocation_state_machine.invocation_state);

        // For whatever reason notification index 2
        invocation_state_machine.notify_entry(1, NotificationId::CompletionId(2));

        // Still waiting completion id 1
        assert!(!invocation_state_machine.is_ready_to_retry());
        assert!(let AttemptState::WaitingRetry { .. } = invocation_state_machine.invocation_state);

        // Send notification index 1
        invocation_state_machine.notify_entry(2, NotificationId::CompletionId(1));

        // Ready to retry
        assert!(invocation_state_machine.is_ready_to_retry());
    }

    #[test]
    fn stale_timer_key_is_ignored() {
        let mut invocation_state_machine = create_test_invocation_state_machine();

        // Put the ISM in WaitingRetry state with timer key 0
        let_assert!(
            OnTaskError::Retrying(_) = invocation_state_machine.handle_task_error(
                true,
                RequestedErrorBehavior::Retry,
                true,
                |_| 0
            )
        );
        check!(let AttemptState::WaitingRetry { .. } = invocation_state_machine.invocation_state);

        // Fire with a WRONG timer key (1 instead of 0) - should be ignored
        invocation_state_machine.notify_retry_timer_fired(1);

        // Should NOT be ready to retry because the wrong key was used
        assert!(
            !invocation_state_machine.is_ready_to_retry(),
            "stale timer key should be ignored"
        );

        // Now fire with the CORRECT timer key (0)
        invocation_state_machine.notify_retry_timer_fired(0);

        // Now it should be ready to retry
        assert!(
            invocation_state_machine.is_ready_to_retry(),
            "correct timer key should mark timer as fired"
        );
    }
}
