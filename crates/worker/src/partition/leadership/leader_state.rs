// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::future;
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::{Duration, SystemTime};

use futures::future::OptionFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, stream};
use metrics::counter;
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tracing::{debug, trace};

use restate_bifrost::CommitToken;
use restate_core::network::{Oneshot, Reciprocal};
use restate_core::{Metadata, MetadataKind, TaskCenter, TaskHandle, TaskId};
use restate_invoker_api::capacity::InvokerToken;
use restate_partition_store::{PartitionDb, PartitionStore};
use restate_storage_api::vqueue_table::EntryCard;
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::{
    InvocationId, LeaderEpoch, PartitionId, PartitionKey, PartitionProcessorRpcRequestId,
    WithPartitionKey,
};
use restate_types::invocation::client::{InvocationOutput, SubmittedInvocationNotification};
use restate_types::logs::Keys;
use restate_types::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcResponse,
};
use restate_types::time::MillisSinceEpoch;
use restate_types::{SemanticRestateVersion, Version, Versioned};
use restate_vqueues::VQueueEvent;
use restate_vqueues::{SchedulerService, VQueuesMeta, scheduler};
use restate_wal_protocol::Command;
use restate_wal_protocol::control::UpsertSchema;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::vqueues::Cards;

use crate::metric_definitions::{PARTITION_HANDLE_LEADER_ACTIONS, USAGE_LEADER_ACTION_COUNT};
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::leadership::self_proposer::SelfProposer;
use crate::partition::leadership::{ActionEffect, Error, InvokerStream, TimerService};
use crate::partition::shuffle;
use crate::partition::shuffle::HintSender;
use crate::partition::state_machine::{Action, StateMachine};

use super::durability_tracker::DurabilityTracker;

const BATCH_READY_UP_TO: usize = 10;

type RpcReciprocal =
    Reciprocal<Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>>;

pub struct LeaderState {
    pub(crate) partition_id: PartitionId,
    pub leader_epoch: LeaderEpoch,
    // only needed for proposing TruncateOutbox to ourselves
    partition_key_range: RangeInclusive<PartitionKey>,

    pub shuffle_hint_tx: HintSender,
    // It's illegal to await the shuffle task handle once it has
    // been resolved. Hence run() should never be called again if it
    // returns a [`Error:TaskFailed`] error.
    shuffle_task_handle: Option<TaskHandle<anyhow::Result<()>>>,
    pub timer_service: Pin<Box<TimerService>>,
    scheduler: SchedulerService<PartitionDb, InvokerToken>,
    self_proposer: SelfProposer,

    awaiting_rpc_actions: HashMap<PartitionProcessorRpcRequestId, RpcReciprocal>,
    awaiting_rpc_self_propose: FuturesUnordered<SelfAppendFuture>,

    invoker_stream: InvokerStream,
    shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
    schema_stream: WatchStream<Version>,
    pub pending_cleanup_timers_to_schedule: VecDeque<(InvocationId, Duration)>,
    cleaner_task_id: TaskId,
    trimmer_task_id: TaskId,
    durability_tracker: DurabilityTracker,
}

impl LeaderState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: PartitionId,
        leader_epoch: LeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        shuffle_task_handle: TaskHandle<anyhow::Result<()>>,
        cleaner_task_id: TaskId,
        trimmer_task_id: TaskId,
        shuffle_hint_tx: HintSender,
        timer_service: TimerService,
        scheduler: SchedulerService<PartitionDb, InvokerToken>,
        self_proposer: SelfProposer,
        invoker_rx: InvokerStream,
        shuffle_rx: tokio::sync::mpsc::Receiver<shuffle::OutboxTruncation>,
        durability_tracker: DurabilityTracker,
    ) -> Self {
        LeaderState {
            partition_id,
            leader_epoch,
            partition_key_range,
            shuffle_task_handle: Some(shuffle_task_handle),
            cleaner_task_id,
            trimmer_task_id,
            shuffle_hint_tx,
            schema_stream: Metadata::with_current(|m| {
                WatchStream::new(m.watch(MetadataKind::Schema))
            }),
            timer_service: Box::pin(timer_service),
            scheduler,
            self_proposer,
            awaiting_rpc_actions: Default::default(),
            awaiting_rpc_self_propose: Default::default(),
            invoker_stream: invoker_rx,
            shuffle_stream: ReceiverStream::new(shuffle_rx),
            pending_cleanup_timers_to_schedule: Default::default(),
            durability_tracker,
        }
    }

    /// Runs the leader specific task which is the awaiting of action effects and the monitoring
    /// of unmanaged tasks.
    ///
    /// Important: The future needs to be cancellation safe since it is polled as a tokio::select
    /// arm!
    pub async fn run(
        &mut self,
        state_machine: &StateMachine,
        vqueues: VQueuesMeta<'_>,
    ) -> Result<Vec<ActionEffect>, Error> {
        let timer_stream = std::pin::pin!(stream::unfold(
            &mut self.timer_service,
            |timer_service| async {
                let timer_value = timer_service.as_mut().next_timer().await;
                Some((ActionEffect::Timer(timer_value), timer_service))
            }
        ));

        // todo(asoli): consider adding the scheduler pick_next() directly to the tokio::select!
        // if we have problems with latency
        let scheduler_stream =
            std::pin::pin!(stream::unfold(&mut self.scheduler, |scheduler| async {
                let assignment = scheduler.schedule_next(vqueues).await;
                Some((ActionEffect::Scheduler(assignment), scheduler))
            }));

        let schema_stream = (&mut self.schema_stream).filter_map(|_| {
            // only upsert schema iff version is newer than current version
            let current_version = state_machine
                .schema
                .as_ref()
                .map(|schema| schema.version())
                .unwrap_or_else(Version::invalid);

            std::future::ready(
                Some(Metadata::with_current(|m| m.schema()))
                    .filter(|schema| schema.version() > current_version)
                    .map(|schema| ActionEffect::UpsertSchema(schema.clone())),
            )
        });

        let invoker_stream = (&mut self.invoker_stream).map(ActionEffect::Invoker);
        let shuffle_stream = (&mut self.shuffle_stream).map(ActionEffect::Shuffle);
        let dur_tracker_stream =
            (&mut self.durability_tracker).map(ActionEffect::PartitionMaintenance);

        let action_effects_stream = stream::unfold(
            &mut self.pending_cleanup_timers_to_schedule,
            |pending_cleanup_timers_to_schedule| {
                let result = pending_cleanup_timers_to_schedule.pop_front();
                future::ready(result.map(|(invocation_id, duration)| {
                    (
                        ActionEffect::ScheduleCleanupTimer(invocation_id, duration),
                        pending_cleanup_timers_to_schedule,
                    )
                }))
            },
        )
        .fuse();
        let awaiting_rpc_self_propose_stream =
            (&mut self.awaiting_rpc_self_propose).map(|_| ActionEffect::AwaitingRpcSelfProposeDone);

        let all_streams = futures::stream_select!(
            scheduler_stream,
            invoker_stream,
            shuffle_stream,
            timer_stream,
            action_effects_stream,
            awaiting_rpc_self_propose_stream,
            dur_tracker_stream,
            schema_stream
        );
        let mut all_streams = all_streams.ready_chunks(BATCH_READY_UP_TO);

        let shuffle_task_handle = self.shuffle_task_handle.as_mut().expect("is set");
        tokio::select! {
            // watch the shuffle task in case it crashed
            result = shuffle_task_handle => {
                // it's not possible to await the shuffler handle
                // if it returns an error. Hence we take it here.
                // run() should then never be called again.
                self.shuffle_task_handle.take();
                match result {
                    Ok(Ok(_)) => Err(Error::task_terminated_unexpectedly("shuffle")),
                    Ok(Err(err)) => Err(Error::task_failed("shuffle", err)),
                    Err(shutdown_error) => Err(Error::Shutdown(shutdown_error))
                }
            }
            Some(action_effects) = all_streams.next() => {
                Ok(action_effects)
            },
            result = self.self_proposer.join_on_err() => {
                Err(result.expect_err("never should never be returned"))
            }
        }
    }

    /// Stops all leader relevant tasks.
    pub async fn stop(
        mut self,
        invoker_handle: &mut impl restate_invoker_api::InvokerHandle<
            InvokerStorageReader<PartitionStore>,
        >,
    ) {
        let shuffle_handle = match self.shuffle_task_handle {
            None => OptionFuture::from(None),
            Some(shuffle_handle) => {
                shuffle_handle.cancel();
                OptionFuture::from(Some(shuffle_handle))
            }
        };

        // Completely unnecessary but left here for being defensive against any potential future
        // re-use of the self proposer
        self.self_proposer.mark_as_non_leader().await;

        let cleaner_handle = OptionFuture::from(TaskCenter::cancel_task(self.cleaner_task_id));

        // We don't really care about waiting for the trimmer to finish cancelling
        TaskCenter::cancel_task(self.trimmer_task_id);

        // It's ok to not check the abort_result because either it succeeded or the invoker
        // is not running. If the invoker is not running, and we are not shutting down, then
        // we will fail the next time we try to invoke.
        let _ = invoker_handle.abort_all_partition((self.partition_id, self.leader_epoch));
        let (shuffle_result, cleaner_result) = tokio::join!(shuffle_handle, cleaner_handle);

        if let Some(shuffle_result) = shuffle_result {
            let _ = shuffle_result.expect("graceful termination of shuffle task");
        }
        if let Some(cleaner_result) = cleaner_result {
            cleaner_result.expect("graceful termination of cleaner task");
        }

        // Reply to all RPCs with not a leader
        for (request_id, reciprocal) in self.awaiting_rpc_actions.drain() {
            trace!(
                %request_id,
                "Failing rpc because I lost leadership",
            );
            reciprocal.send(Err(PartitionProcessorRpcError::LostLeadership(
                self.partition_id,
            )))
        }
        for fut in self.awaiting_rpc_self_propose.iter_mut() {
            fut.fail_with_lost_leadership(self.partition_id);
        }
    }

    pub async fn handle_action_effects(
        &mut self,
        action_effects: impl IntoIterator<Item = ActionEffect>,
        // invoker_tx: &mut impl restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>>,
    ) -> Result<(), Error> {
        for effect in action_effects {
            match effect {
                ActionEffect::Scheduler(decisions) => {
                    let commands: Vec<_> = decisions?.into_iter().flat_map(|(qid, decision)| {
                        decision.into_iter_per_action().map(move |(action, items)| {
                            let cards = Cards::new(&qid, items);
                            match action {
                                scheduler::Action::MoveToRunning => {
                                    (qid.partition_key, Command::VQWaitingToRunning(cards))
                                }
                                scheduler::Action::Yield => {
                                    (qid.partition_key, Command::VQYieldRunning(cards))
                                }
                                scheduler::Action::ResumeAlreadyRunning => {
                                        todo!(
                                            "Unsupported: We don't support directly resuming at the moment"
                                        )
                                        // invoker_tx
                                        //     .run(
                                        //         self.partition_id,
                                        //         self.leader_epoch,
                                        //         inv_id,
                                        //         // todo: fix/remove
                                        //         SharedString::from_borrowed(""),
                                        //         SharedString::from_borrowed(""),
                                        //     )
                                        //     .map_err(Error::Invoker)?;
                                    }
                                }
                            })
                    }).collect();
                    self.self_proposer
                        .propose_many(commands.into_iter())
                        .await?;
                }
                ActionEffect::PartitionMaintenance(partition_durability) => {
                    // based on configuration, whether to consider partition-local durability in
                    // the replica-set as a sufficient source of durability, or only snapshots.
                    self.self_proposer
                        .propose(
                            *self.partition_key_range.start(),
                            Command::UpdatePartitionDurability(partition_durability),
                        )
                        .await?;
                }
                ActionEffect::Invoker(invoker_effect) => {
                    self.self_proposer
                        .propose(
                            invoker_effect.invocation_id.partition_key(),
                            Command::InvokerEffect(invoker_effect),
                        )
                        .await?;
                }
                ActionEffect::Shuffle(outbox_truncation) => {
                    // todo: Until we support partition splits we need to get rid of outboxes or introduce partition
                    //  specific destination messages that are identified by a partition_id
                    self.self_proposer
                        .propose(
                            *self.partition_key_range.start(),
                            Command::TruncateOutbox(outbox_truncation.index()),
                        )
                        .await?;
                }
                ActionEffect::Timer(timer) => {
                    self.self_proposer
                        .propose(timer.invocation_id().partition_key(), Command::Timer(timer))
                        .await?;
                }
                ActionEffect::ScheduleCleanupTimer(invocation_id, duration) => {
                    self.self_proposer
                        .propose(
                            invocation_id.partition_key(),
                            Command::ScheduleTimer(TimerKeyValue::clean_invocation_status(
                                MillisSinceEpoch::from(SystemTime::now() + duration),
                                invocation_id,
                            )),
                        )
                        .await?;
                }
                ActionEffect::UpsertSchema(schema) => {
                    const GATE_VERSION: SemanticRestateVersion =
                        SemanticRestateVersion::new(1, 7, 0);

                    if SemanticRestateVersion::current().is_equal_or_newer_than(&GATE_VERSION) {
                        self.self_proposer
                            .propose(
                                *self.partition_key_range.start(),
                                Command::UpsertSchema(UpsertSchema {
                                    partition_key_range: Keys::RangeInclusive(
                                        self.partition_key_range.clone(),
                                    ),
                                    schema,
                                }),
                            )
                            .await?;
                    }
                }
                ActionEffect::AwaitingRpcSelfProposeDone => {
                    // Nothing to do here
                }
            }
        }

        Ok(())
    }

    pub async fn handle_rpc_proposal_command(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        reciprocal: Reciprocal<
            Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        >,
        partition_key: PartitionKey,
        cmd: Command,
    ) {
        match self.awaiting_rpc_actions.entry(request_id) {
            Entry::Occupied(mut o) => {
                // In this case, someone already proposed this command,
                // let's just replace the reciprocal and fail the old one to avoid keeping it dangling
                let old_reciprocal = o.insert(reciprocal);
                trace!(%request_id, "Replacing rpc with newer request");
                old_reciprocal.send(Err(PartitionProcessorRpcError::Internal(
                    "retried".to_string(),
                )));
            }
            Entry::Vacant(v) => {
                // In this case, no one proposed this command yet, let's try to propose it
                if let Err(e) = self.self_proposer.propose(partition_key, cmd).await {
                    reciprocal.send(Err(PartitionProcessorRpcError::Internal(e.to_string())));
                } else {
                    v.insert(reciprocal);
                }
            }
        }
    }

    pub async fn self_propose_and_respond_asynchronously(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        reciprocal: RpcReciprocal,
        success_response: PartitionProcessorRpcResponse,
    ) {
        match self
            .self_proposer
            .propose_with_notification(partition_key, cmd)
            .await
        {
            Ok(commit_token) => {
                self.awaiting_rpc_self_propose.push(SelfAppendFuture::new(
                    commit_token,
                    success_response,
                    reciprocal,
                ));
            }
            Err(e) => reciprocal.send(Err(PartitionProcessorRpcError::Internal(e.to_string()))),
        }
    }

    pub fn handle_actions(
        &mut self,
        invoker_tx: &mut impl restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>>,
        actions: impl Iterator<Item = Action>,
        vqueues: VQueuesMeta<'_>,
    ) -> Result<(), Error> {
        for action in actions {
            let action_name = action.name();

            trace!(?action, "Apply action");
            counter!(PARTITION_HANDLE_LEADER_ACTIONS, "action" => action_name).increment(1);

            counter!(
                USAGE_LEADER_ACTION_COUNT,
                "partition" => self.partition_id.to_string(),
                "action" => action_name,
            )
            .increment(1);

            self.handle_action(action, invoker_tx, vqueues)?;
        }

        Ok(())
    }

    fn handle_action(
        &mut self,
        action: Action,
        invoker_tx: &mut impl restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>>,
        vqueues: VQueuesMeta<'_>,
    ) -> Result<(), Error> {
        // todo(asoli): update our hlc timestamp
        let now = UniqueTimestamp::from_unix_millis(MillisSinceEpoch::now()).unwrap();

        let partition_leader_epoch = (self.partition_id, self.leader_epoch);
        match action {
            Action::Invoke {
                invocation_id,
                invocation_epoch,
                invocation_target,
                invoke_input_journal,
            } => invoker_tx
                .invoke(
                    partition_leader_epoch,
                    invocation_id,
                    invocation_epoch,
                    invocation_target,
                    invoke_input_journal,
                )
                .map_err(Error::Invoker)?,
            Action::NewOutboxMessage {
                seq_number,
                message,
            } => self
                .shuffle_hint_tx
                .send(shuffle::NewOutboxMessage::new(seq_number, message)),
            Action::RegisterTimer { timer_value } => {
                self.timer_service.as_mut().add_timer(timer_value)
            }
            Action::DeleteTimer { timer_key } => {
                self.timer_service.as_mut().remove_timer(timer_key)
            }
            Action::AckStoredCommand {
                invocation_id,
                invocation_epoch,
                command_index,
            } => {
                invoker_tx
                    .notify_stored_command_ack(
                        partition_leader_epoch,
                        invocation_id,
                        invocation_epoch,
                        command_index,
                    )
                    .map_err(Error::Invoker)?;
            }
            Action::ForwardCompletion {
                invocation_id,
                completion,
            } => invoker_tx
                .notify_completion(partition_leader_epoch, invocation_id, completion)
                .map_err(Error::Invoker)?,
            Action::AbortInvocation {
                invocation_id,
                invocation_epoch,
            } => invoker_tx
                .abort_invocation(partition_leader_epoch, invocation_id, invocation_epoch)
                .map_err(Error::Invoker)?,
            Action::IngressResponse {
                request_id,
                invocation_id,
                response,
                completion_expiry_time,
                ..
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    response_tx.send(Ok(PartitionProcessorRpcResponse::Output(
                        InvocationOutput {
                            request_id,
                            invocation_id,
                            completion_expiry_time,
                            response,
                        },
                    )));
                } else {
                    debug!(%request_id, "Ignoring sending ingress response because there is no awaiting rpc");
                }
            }
            Action::IngressSubmitNotification {
                request_id,
                execution_time,
                is_new_invocation,
                ..
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    response_tx.send(Ok(PartitionProcessorRpcResponse::Submitted(
                        SubmittedInvocationNotification {
                            request_id,
                            execution_time,
                            is_new_invocation,
                        },
                    )));
                }
            }
            Action::ScheduleInvocationStatusCleanup {
                invocation_id,
                retention,
            } => {
                self.pending_cleanup_timers_to_schedule
                    .push_back((invocation_id, retention));
            }
            Action::ForwardNotification {
                invocation_id,
                invocation_epoch,
                notification,
            } => {
                invoker_tx
                    .notify_notification(
                        partition_leader_epoch,
                        invocation_id,
                        invocation_epoch,
                        notification,
                    )
                    .map_err(Error::Invoker)?;
            }
            Action::ForwardKillResponse {
                request_id,
                response,
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    response_tx.send(Ok(PartitionProcessorRpcResponse::KillInvocation(
                        response.into(),
                    )));
                }
            }
            Action::ForwardCancelResponse {
                request_id,
                response,
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    response_tx.send(Ok(PartitionProcessorRpcResponse::CancelInvocation(
                        response.into(),
                    )));
                }
            }
            Action::ForwardPurgeInvocationResponse {
                request_id,
                response,
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    response_tx.send(Ok(PartitionProcessorRpcResponse::PurgeInvocation(
                        response.into(),
                    )));
                }
            }
            Action::ForwardPurgeJournalResponse {
                request_id,
                response,
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    response_tx.send(Ok(PartitionProcessorRpcResponse::PurgeJournal(
                        response.into(),
                    )));
                }
            }
            Action::ForwardResumeInvocationResponse {
                request_id,
                response,
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    response_tx.send(Ok(PartitionProcessorRpcResponse::ResumeInvocation(
                        response.into(),
                    )));
                }
            }
            Action::ForwardRestartAsNewInvocationResponse {
                request_id,
                response,
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    response_tx.send(Ok(PartitionProcessorRpcResponse::RestartAsNewInvocation(
                        response.into(),
                    )));
                }
            }
            Action::VQEvent(inbox_event) => {
                self.handle_vqueue_inbox_event(now, inbox_event, vqueues)?;
            }
            Action::VQInvoke {
                qid,
                item_hash,
                invocation_id,
                invocation_target,
                invoke_input_journal,
            } => {
                let permit = self.scheduler.confirm_assignment(&qid, item_hash);
                let permit = permit.expect("invoke invocations assigned by scheduler");
                invoker_tx
                    .vqueue_invoke(
                        partition_leader_epoch,
                        qid,
                        permit,
                        invocation_id,
                        invocation_target,
                        invoke_input_journal,
                    )
                    .map_err(Error::Invoker)?
            }
        }

        Ok(())
    }

    fn handle_vqueue_inbox_event(
        &mut self,
        now: UniqueTimestamp,
        event: VQueueEvent<EntryCard>,
        vqueues: VQueuesMeta<'_>,
    ) -> Result<(), Error> {
        self.scheduler
            .on_inbox_event(now, vqueues, &event)
            .map_err(Error::Storage)?;

        Ok(())
    }
}

struct SelfAppendFuture {
    commit_token: CommitToken,
    response: Option<(PartitionProcessorRpcResponse, RpcReciprocal)>,
}

impl SelfAppendFuture {
    fn new(
        commit_token: CommitToken,
        success_response: PartitionProcessorRpcResponse,
        response_reciprocal: RpcReciprocal,
    ) -> Self {
        Self {
            commit_token,
            response: Some((success_response, response_reciprocal)),
        }
    }

    fn fail_with_internal(&mut self) {
        if let Some((_, reciprocal)) = self.response.take() {
            reciprocal.send(Err(PartitionProcessorRpcError::Internal(
                "error when proposing to bifrost".to_string(),
            )));
        }
    }

    fn fail_with_lost_leadership(&mut self, this_partition_id: PartitionId) {
        if let Some((_, reciprocal)) = self.response.take() {
            reciprocal.send(Err(PartitionProcessorRpcError::LostLeadership(
                this_partition_id,
            )));
        }
    }

    fn succeed_with_appended(&mut self) {
        if let Some((success_response, reciprocal)) = self.response.take() {
            reciprocal.send(Ok(success_response));
        }
    }
}

impl Future for SelfAppendFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let append_result = ready!(self.commit_token.poll_unpin(cx));

        if append_result.is_err() {
            self.fail_with_internal();
            return Poll::Ready(());
        }
        self.succeed_with_appended();
        Poll::Ready(())
    }
}
