use super::*;

use restate_common::journal::Completion;
use restate_common::retry_policy;
use std::fmt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

/// Component encapsulating the business logic of the invocation state machine
#[derive(Debug)]
pub(super) struct InvocationStateMachine {
    invocation_state: InvocationState,
    retry_iter: retry_policy::Iter,
}

/// This struct tracks which entries the invocation task generates,
/// and which ones have been already stored and acked by the partition processor.
/// This information is used to decide when it's safe to retry.
///
/// Every time the invocation task generates a new entry, the index is notified to this struct with
/// [`JournalTracker::notify_entry_sent_to_partition_processor`], and every time the invoker receives
/// [`InputCommand::StoredEntryAck`], the index is notified to this struct with [`JournalTracker::notify_acked_entry_from_partition_processor`].
///
/// After the retry timer is fired, we can check whether we can retry immediately or not with [`JournalTracker::can_retry`].
#[derive(Default, Debug, Copy, Clone)]
struct JournalTracker {
    last_acked_entry_from_partition_processor: Option<EntryIndex>,
    last_entry_sent_to_partition_processor: Option<EntryIndex>,
}

impl JournalTracker {
    fn notify_acked_entry_from_partition_processor(&mut self, idx: EntryIndex) {
        self.last_acked_entry_from_partition_processor =
            cmp::max(Some(idx), self.last_acked_entry_from_partition_processor)
    }

    fn notify_entry_sent_to_partition_processor(&mut self, idx: EntryIndex) {
        self.last_entry_sent_to_partition_processor =
            cmp::max(Some(idx), self.last_entry_sent_to_partition_processor)
    }

    fn can_retry(&self) -> bool {
        match (
            self.last_acked_entry_from_partition_processor,
            self.last_entry_sent_to_partition_processor,
        ) {
            (_, None) => {
                // The invocation task didn't generated new entries.
                // We're always good to retry in this case.
                true
            }
            (Some(last_acked), Some(last_sent)) => {
                // Last acked must be higher than last sent,
                // otherwise we'll end up retrying when not all the entries have been stored.
                last_acked >= last_sent
            }
            _ => false,
        }
    }
}

enum InvocationState {
    New,

    InFlight {
        // If there is no completions_tx,
        // then the stream is open in request/response mode
        completions_tx: Option<mpsc::UnboundedSender<Completion>>,
        journal_tracker: JournalTracker,
        abort_handle: AbortHandle,
    },

    WaitingRetry {
        timer_fired: bool,
        journal_tracker: JournalTracker,
    },
}

impl fmt::Debug for InvocationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvocationState::New => f.write_str("New"),
            InvocationState::InFlight {
                journal_tracker,
                abort_handle,
                completions_tx,
            } => f
                .debug_struct("InFlight")
                .field("journal_tracker", journal_tracker)
                .field("abort_handle", abort_handle)
                .field(
                    "completions_tx_open",
                    &completions_tx
                        .as_ref()
                        .map(|s| !s.is_closed())
                        .unwrap_or(false),
                )
                .finish(),
            InvocationState::WaitingRetry {
                journal_tracker,
                timer_fired,
            } => f
                .debug_struct("WaitingRetry")
                .field("journal_tracker", journal_tracker)
                .field("timer_fired", timer_fired)
                .finish(),
        }
    }
}

impl InvocationStateMachine {
    pub(super) fn create(retry_policy: RetryPolicy) -> InvocationStateMachine {
        Self {
            invocation_state: InvocationState::New,
            retry_iter: retry_policy.into_iter(),
        }
    }

    pub(super) fn start(
        &mut self,
        abort_handle: AbortHandle,
        completions_tx: Option<mpsc::UnboundedSender<Completion>>,
    ) {
        debug_assert!(matches!(
            &self.invocation_state,
            InvocationState::New | InvocationState::WaitingRetry { .. }
        ));

        self.invocation_state = InvocationState::InFlight {
            completions_tx,
            journal_tracker: Default::default(),
            abort_handle,
        };
    }

    pub(super) fn abort(&mut self) {
        if let InvocationState::InFlight { abort_handle, .. } = &mut self.invocation_state {
            abort_handle.abort();
        }
    }

    pub(super) fn notify_new_entry(&mut self, entry_index: EntryIndex) {
        debug_assert!(matches!(
            &self.invocation_state,
            InvocationState::InFlight { .. }
        ));

        if let InvocationState::InFlight {
            journal_tracker, ..
        } = &mut self.invocation_state
        {
            journal_tracker.notify_entry_sent_to_partition_processor(entry_index);
        }
    }

    pub(super) fn notify_stored_ack(&mut self, entry_index: EntryIndex) {
        match &mut self.invocation_state {
            InvocationState::InFlight {
                journal_tracker, ..
            } => {
                journal_tracker.notify_acked_entry_from_partition_processor(entry_index);
            }
            InvocationState::WaitingRetry {
                journal_tracker, ..
            } => {
                journal_tracker.notify_acked_entry_from_partition_processor(entry_index);
            }
            _ => {}
        }
    }

    pub(super) fn notify_completion(&mut self, completion: Completion) {
        if let InvocationState::InFlight {
            completions_tx: Some(sender),
            ..
        } = &mut self.invocation_state
        {
            let _ = sender.send(completion);
        }
    }

    pub(super) fn notify_retry_timer_fired(&mut self) {
        debug_assert!(matches!(
            &self.invocation_state,
            InvocationState::WaitingRetry { .. }
        ));

        if let InvocationState::WaitingRetry { timer_fired, .. } = &mut self.invocation_state {
            *timer_fired = true;
        }
    }

    /// Returns Some() with the timer for the next retry, otherwise None if retry limit exhausted
    pub(super) fn handle_task_error(&mut self) -> Option<Duration> {
        let journal_tracker = match &self.invocation_state {
            InvocationState::InFlight {
                journal_tracker, ..
            } => *journal_tracker,
            InvocationState::New => JournalTracker::default(),
            InvocationState::WaitingRetry {
                journal_tracker,
                timer_fired,
            } => {
                // TODO: https://github.com/restatedev/restate/issues/538
                assert!(timer_fired,
                        "Restate does not support multiple retry timers yet. This would require \
                        deduplicating timers by some mean (e.g. fencing them off, overwriting \
                        old timers, not registering a new timer if an old timer has not fired yet, etc.)");
                *journal_tracker
            }
        };
        let next_timer = self.retry_iter.next();

        if next_timer.is_some() {
            self.invocation_state = InvocationState::WaitingRetry {
                timer_fired: false,
                journal_tracker,
            };
            next_timer
        } else {
            None
        }
    }

    pub(super) fn is_ready_to_retry(&self) -> bool {
        match self.invocation_state {
            InvocationState::WaitingRetry {
                timer_fired,
                journal_tracker,
            } => timer_fired && journal_tracker.can_retry(),
            _ => false,
        }
    }

    #[inline]
    pub(super) fn invocation_state_debug(&self) -> impl fmt::Debug + '_ {
        &self.invocation_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use restate_common::retry_policy::RetryPolicy;
    use restate_test_util::{check, test};
    use std::time::Duration;

    #[test]
    fn handle_error_when_waiting_for_retry() {
        let mut invocation_state_machine =
            InvocationStateMachine::create(RetryPolicy::fixed_delay(Duration::from_secs(1), 10));

        assert!(invocation_state_machine.handle_task_error().is_some());
        check!(let InvocationState::WaitingRetry { .. } = invocation_state_machine.invocation_state);

        invocation_state_machine.notify_retry_timer_fired();

        // We stay in `WaitingForRetry`
        assert!(invocation_state_machine.handle_task_error().is_some());
        check!(let InvocationState::WaitingRetry { .. } = invocation_state_machine.invocation_state);
    }
}
