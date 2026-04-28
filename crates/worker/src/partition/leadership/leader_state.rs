// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::BytesMut;
use futures::future::OptionFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, stream};
use itertools::Itertools;
use metrics::counter;
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tracing::{debug, error, trace};

use restate_bifrost::CommitToken;
use restate_core::network::{Oneshot, Reciprocal};
use restate_core::{Metadata, MetadataKind, TaskCenter, TaskHandle, TaskId};
use restate_invoker_impl::InvokerHandle as InvokerChannelServiceHandle;
use restate_partition_store::PartitionDb;
use restate_storage_api::vqueue_table::scheduler::SchedulerDecisions;
use restate_types::identifiers::{
    LeaderEpoch, PartitionId, PartitionKey, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use restate_types::invocation::PurgeInvocationRequest;
use restate_types::invocation::client::{InvocationOutput, SubmittedInvocationNotification};
use restate_types::logs::Keys;
use restate_types::net::ingest::IngestRecord;
use restate_types::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcResponse,
};
use restate_types::sharding::KeyRange;
use restate_types::{RESTATE_VERSION_1_7_0, SemanticRestateVersion, Version, Versioned, vqueues};
use restate_vqueues::SchedulerService;
use restate_vqueues::VQueueEvent;
use restate_vqueues::scheduler::Decisions;
use restate_wal_protocol::Command;
use restate_wal_protocol::control::UpsertSchema;
use restate_worker_api::SchedulerStatusEntry;
use restate_worker_api::invoker::InvokerHandle;

use crate::metric_definitions::{PARTITION_HANDLE_LEADER_ACTIONS, USAGE_LEADER_ACTION_COUNT};
use crate::partition::cleaner::{CleanerEffect, CleanerHandle};
use crate::partition::leadership::self_proposer::SelfProposer;
use crate::partition::leadership::{ActionEffect, Error, InvokerStream, TimerService};
use crate::partition::shuffle;
use crate::partition::shuffle::HintSender;
use crate::partition::state_machine::{Action, StateMachine};
use crate::partition_processor_manager::LeaderQueryGuard;

use super::durability_tracker::DurabilityTracker;

const BATCH_READY_UP_TO: usize = 10;

type RpcReciprocal =
    Reciprocal<Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>>;

pub struct LeaderState {
    pub(crate) partition_id: PartitionId,
    pub leader_epoch: LeaderEpoch,
    // only needed for proposing TruncateOutbox to ourselves
    partition_key_range: KeyRange,

    pub shuffle_hint_tx: HintSender,
    // It's illegal to await the shuffle task handle once it has
    // been resolved. Hence run() should never be called again if it
    // returns a [`Error:TaskFailed`] error.
    shuffle_task_handle: Option<TaskHandle<anyhow::Result<()>>>,
    pub timer_service: Pin<Box<TimerService>>,
    scheduler: SchedulerService<PartitionDb>,
    invoker_handle: InvokerChannelServiceHandle,
    invoker_task_handle: Option<TaskHandle<()>>,
    self_proposer: SelfProposer,

    awaiting_rpc_actions: HashMap<PartitionProcessorRpcRequestId, RpcReciprocal>,
    awaiting_rpc_self_propose: FuturesUnordered<SelfAppendFuture>,

    invoker_stream: InvokerStream,
    shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
    schema_stream: WatchStream<Version>,
    cleaner_handle: CleanerHandle,
    trimmer_task_id: TaskId,
    durability_tracker: DurabilityTracker,
    // Unregisters the leader-query registry entry on drop. Must live as long as
    // the partition processor's select! is willing to serve scheduler queries.
    _leader_query_guard: LeaderQueryGuard,
}

impl LeaderState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: PartitionId,
        leader_epoch: LeaderEpoch,
        partition_key_range: KeyRange,
        shuffle_task_handle: TaskHandle<anyhow::Result<()>>,
        cleaner_handle: CleanerHandle,
        trimmer_task_id: TaskId,
        shuffle_hint_tx: HintSender,
        timer_service: TimerService,
        scheduler: SchedulerService<PartitionDb>,
        invoker_handle: InvokerChannelServiceHandle,
        invoker_task_handle: TaskHandle<()>,
        self_proposer: SelfProposer,
        invoker_rx: InvokerStream,
        shuffle_rx: tokio::sync::mpsc::Receiver<shuffle::OutboxTruncation>,
        durability_tracker: DurabilityTracker,
        leader_query_guard: LeaderQueryGuard,
    ) -> Self {
        LeaderState {
            partition_id,
            leader_epoch,
            partition_key_range,
            shuffle_task_handle: Some(shuffle_task_handle),
            cleaner_handle,
            trimmer_task_id,
            shuffle_hint_tx,
            schema_stream: Metadata::with_current(|m| {
                WatchStream::new(m.watch(MetadataKind::Schema))
            }),
            timer_service: Box::pin(timer_service),
            scheduler,
            invoker_handle,
            invoker_task_handle: Some(invoker_task_handle),
            self_proposer,
            awaiting_rpc_actions: Default::default(),
            awaiting_rpc_self_propose: Default::default(),
            invoker_stream: invoker_rx,
            shuffle_stream: ReceiverStream::new(shuffle_rx),
            durability_tracker,
            _leader_query_guard: leader_query_guard,
        }
    }

    pub fn read_scheduler_status(&self, keys: KeyRange) -> Vec<SchedulerStatusEntry> {
        self.scheduler
            .iter_status()
            .into_iter()
            .flatten()
            .filter(|(qid, _)| keys.contains(&qid.partition_key()))
            .collect()
    }

    /// Runs the leader specific task which is the awaiting of action effects and the monitoring
    /// of unmanaged tasks.
    ///
    /// Important: The future needs to be cancellation safe since it is polled as a tokio::select
    /// arm!
    pub async fn run(&mut self, state_machine: &StateMachine) -> Result<Vec<ActionEffect>, Error> {
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
                match scheduler.schedule_next().await {
                    Ok(decisions) => Some((ActionEffect::Scheduler(decisions), scheduler)),
                    Err(e) => {
                        error!("Fatal error when polling scheduler: {e}");
                        None
                    }
                }
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
        let cleaner_stream = self.cleaner_handle.effects().map(ActionEffect::Cleaner);

        let dur_tracker_stream =
            (&mut self.durability_tracker).map(ActionEffect::PartitionMaintenance);

        let awaiting_rpc_self_propose_stream =
            (&mut self.awaiting_rpc_self_propose).map(|_| ActionEffect::AwaitingRpcSelfProposeDone);

        let all_streams = futures::stream_select!(
            scheduler_stream,
            invoker_stream,
            shuffle_stream,
            timer_stream,
            cleaner_stream,
            awaiting_rpc_self_propose_stream,
            dur_tracker_stream,
            schema_stream
        );
        let mut all_streams = all_streams.ready_chunks(BATCH_READY_UP_TO);

        let shuffle_task_handle = self.shuffle_task_handle.as_mut().expect("is set");
        let invoker_task_handle = self.invoker_task_handle.as_mut().expect("is set");
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
            result = invoker_task_handle => {
                self.invoker_task_handle.take();
                match result {
                    Ok(()) => Err(Error::task_terminated_unexpectedly("invoker")),
                    Err(shutdown_error) => Err(Error::Shutdown(shutdown_error)),
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
    pub async fn stop(mut self) {
        let shuffle_handle = match self.shuffle_task_handle {
            None => OptionFuture::from(None),
            Some(shuffle_handle) => {
                shuffle_handle.cancel();
                OptionFuture::from(Some(shuffle_handle))
            }
        };

        let invoker_task_handle = match self.invoker_task_handle {
            None => OptionFuture::from(None),
            Some(invoker_task_handle) => {
                invoker_task_handle.cancel();
                OptionFuture::from(Some(invoker_task_handle))
            }
        };

        // Completely unnecessary but left here for being defensive against any potential future
        // re-use of the self proposer
        self.self_proposer.mark_as_non_leader();

        let cleaner_handle = OptionFuture::from(self.cleaner_handle.stop());

        // We don't really care about waiting for the trimmer to finish cancelling
        TaskCenter::cancel_task(self.trimmer_task_id);

        // It's ok to not check the abort_result because either it succeeded or the invoker
        // is not running. If the invoker is not running, and we are not shutting down, then
        // we will fail the next time we try to invoke.
        let _ = self.invoker_handle.abort_all();
        let (shuffle_result, cleaner_result, invoker_result) =
            tokio::join!(shuffle_handle, cleaner_handle, invoker_task_handle);

        if let Some(shuffle_result) = shuffle_result {
            let _ = shuffle_result.expect("graceful termination of shuffle task");
        }
        if let Some(cleaner_result) = cleaner_result {
            cleaner_result.expect("graceful termination of cleaner task");
        }
        if let Some(invoker_result) = invoker_result {
            let _ = invoker_result;
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
    ) -> Result<(), Error> {
        let mut arena = BytesMut::with_capacity(128 * 1024);
        for effect in action_effects {
            match effect {
                ActionEffect::Scheduler(decisions) => {
                    let Decisions {
                        qids,
                        num_run,
                        num_yield,
                    } = decisions;
                    trace!(
                        "Scheduler decided to run {num_run} entries and yield {num_yield} entries across {} vqueues",
                        qids.len()
                    );

                    let commands: Vec<_> = qids
                        .into_iter()
                        .chunk_by(|(id, _)| id.partition_key())
                        .into_iter()
                        // one command per partition key
                        .map(|(partition_key, group)| {
                            let decisions = SchedulerDecisions {
                                qids: group.collect(),
                            };

                            arena.reserve(decisions.encoded_len());
                            // safe to unwrap because we reserved enough space
                            decisions.bilrost_encode(&mut arena).unwrap();

                            (
                                partition_key,
                                Command::VQSchedulerDecisions(arena.split().freeze()),
                            )
                            // an action goes into command, and resources are popped
                        })
                        // Unfortunately chunk_by cannot generate an ExactSizeIterator.
                        // I'm hoping that this is a temporary measure until SelfProposer is redesigned.
                        .collect();
                    self.self_proposer
                        .self_propose_many(commands.into_iter())
                        .await?;
                }
                ActionEffect::PartitionMaintenance(partition_durability) => {
                    // based on configuration, whether to consider partition-local durability in
                    // the replica-set as a sufficient source of durability, or only snapshots.
                    self.self_proposer
                        .self_propose(
                            self.partition_key_range.start(),
                            Command::UpdatePartitionDurability(partition_durability),
                        )
                        .await?;
                }
                ActionEffect::Invoker(invoker_effect) => {
                    self.self_proposer
                        .self_propose(
                            invoker_effect.invocation_id.partition_key(),
                            Command::InvokerEffect(invoker_effect),
                        )
                        .await?;
                }
                ActionEffect::Shuffle(outbox_truncation) => {
                    // todo: Until we support partition splits we need to get rid of outboxes or introduce partition
                    //  specific destination messages that are identified by a partition_id
                    self.self_proposer
                        .self_propose(
                            self.partition_key_range.start(),
                            Command::TruncateOutbox(outbox_truncation.index()),
                        )
                        .await?;
                }
                ActionEffect::Timer(timer) => {
                    self.self_proposer
                        .self_propose(timer.invocation_id().partition_key(), Command::Timer(timer))
                        .await?;
                }
                ActionEffect::Cleaner(effect) => {
                    let (invocation_id, cmd) = match effect {
                        CleanerEffect::PurgeJournal(invocation_id) => (
                            invocation_id,
                            Command::PurgeJournal(PurgeInvocationRequest {
                                invocation_id,
                                response_sink: None,
                            }),
                        ),
                        CleanerEffect::PurgeInvocation(invocation_id) => (
                            invocation_id,
                            Command::PurgeInvocation(PurgeInvocationRequest {
                                invocation_id,
                                response_sink: None,
                            }),
                        ),
                    };

                    self.self_proposer
                        .self_propose(invocation_id.partition_key(), cmd)
                        .await?;
                }
                ActionEffect::UpsertSchema(schema) => {
                    if SemanticRestateVersion::current()
                        .is_equal_or_newer_than(&RESTATE_VERSION_1_7_0)
                    {
                        self.self_proposer
                            .self_propose(
                                self.partition_key_range.start(),
                                Command::UpsertSchema(UpsertSchema {
                                    partition_key_range: Keys::RangeInclusive(
                                        self.partition_key_range.into(),
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
                if let Err(e) = self.self_proposer.self_propose(partition_key, cmd).await {
                    reciprocal.send(Err(PartitionProcessorRpcError::Internal(e.to_string())));
                } else {
                    v.insert(reciprocal);
                }
            }
        }
    }

    /// Append a command to Bifrost **without** dedup information and respond on Bifrost commit.
    ///
    /// Records appended this way are never filtered by the dedup mechanism during leadership
    /// transitions, making this safe for fire-and-forget ingress commands (signals, invocation
    /// responses).
    pub async fn append_and_respond_asynchronously(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        reciprocal: RpcReciprocal,
        success_response: PartitionProcessorRpcResponse,
    ) {
        match self
            .self_proposer
            .append_with_notification(partition_key, cmd)
            .await
        {
            Ok(commit_token) => {
                self.awaiting_rpc_self_propose.push(SelfAppendFuture::new(
                    commit_token,
                    |result: Result<(), PartitionProcessorRpcError>| {
                        reciprocal.send(result.map(|_| success_response));
                    },
                ));
            }
            Err(e) => reciprocal.send(Err(PartitionProcessorRpcError::Internal(e.to_string()))),
        }
    }

    pub async fn forward_many_with_callback<F>(
        &mut self,
        records: impl ExactSizeIterator<Item = IngestRecord>,
        callback: F,
    ) where
        F: FnOnce(Result<(), PartitionProcessorRpcError>) + Send + Sync + 'static,
    {
        match self
            .self_proposer
            .forward_many_with_notification(records)
            .await
        {
            Ok(commit_token) => {
                self.awaiting_rpc_self_propose
                    .push(SelfAppendFuture::new(commit_token, callback));
            }
            Err(e) => callback(Err(PartitionProcessorRpcError::Internal(e.to_string()))),
        }
    }

    pub fn handle_actions(&mut self, actions: impl Iterator<Item = Action>) -> Result<(), Error> {
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

            self.handle_action(action)?;
        }

        Ok(())
    }

    fn handle_action(&mut self, action: Action) -> Result<(), Error> {
        match action {
            Action::Invoke {
                invocation_id,
                invocation_target,
            } => self
                .invoker_handle
                .invoke(invocation_id, invocation_target)
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
                command_index,
            } => {
                self.invoker_handle
                    .notify_stored_command_ack(invocation_id, command_index)
                    .map_err(Error::Invoker)?;
            }
            Action::ForwardCompletion {
                invocation_id,
                entry_index,
            } => self
                .invoker_handle
                .notify_completion(invocation_id, entry_index)
                .map_err(Error::Invoker)?,
            Action::AbortInvocation { invocation_id } => self
                .invoker_handle
                .abort_invocation(invocation_id)
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
            Action::ForwardNotification {
                invocation_id,
                entry_index,
                notification_id,
            } => {
                self.invoker_handle
                    .notify_notification(invocation_id, entry_index, notification_id)
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
                self.handle_vqueue_inbox_event(inbox_event);
            }
            Action::VQInvoke {
                qid,
                key,
                invocation_target,
            } => {
                // state mutations should not create Invoke actions. At least for now.
                assert!(matches!(key.kind(), vqueues::EntryKind::Invocation));
                let invocation_id = key
                    .entry_id()
                    .to_invocation_id(qid.partition_key())
                    .unwrap();

                let mut run_permit = self.scheduler.confirm_run_attempt(&qid, &key).unwrap_or_else(|| {
                    tracing::error!(
                        vqueue = %qid,
                        restate.invocation.id = %invocation_id,
                        "Cannot find a permit for entry key {key:?} in scheduler. Will not respect the invoker limit for this invocation"
                    );
                    unimplemented!()
                    // todo: RunPermit::new_empty()
                });
                // todo: This is temporary until we wrap the returned permit into an InvokePermit
                // that invoker permit will carry the inner permit as opaque type.
                let (permit, memory_lease) = run_permit.take_invoker_permit();
                self.invoker_handle
                    .vqueue_invoke(qid, permit, invocation_id, invocation_target, memory_lease)
                    .map_err(Error::Invoker)?
            }
        }

        Ok(())
    }

    pub fn invoker_handle(&mut self) -> &mut InvokerChannelServiceHandle {
        &mut self.invoker_handle
    }

    fn handle_vqueue_inbox_event(&mut self, event: VQueueEvent) {
        self.scheduler.on_inbox_event(event);
    }
}

trait CallbackInner: Send + Sync + 'static {
    fn call(self: Box<Self>, result: Result<(), PartitionProcessorRpcError>);
}

impl<F> CallbackInner for F
where
    F: FnOnce(Result<(), PartitionProcessorRpcError>) + Send + Sync + 'static,
{
    fn call(self: Box<Self>, result: Result<(), PartitionProcessorRpcError>) {
        self(result)
    }
}

struct Callback {
    inner: Box<dyn CallbackInner>,
}

impl Callback {
    fn call(self, result: Result<(), PartitionProcessorRpcError>) {
        self.inner.call(result);
    }
}

impl<I> From<I> for Callback
where
    I: CallbackInner,
{
    fn from(value: I) -> Self {
        Self {
            inner: Box::new(value),
        }
    }
}

struct SelfAppendFuture {
    commit_token: CommitToken,
    callback: Option<Callback>,
}

impl SelfAppendFuture {
    fn new(commit_token: CommitToken, callback: impl Into<Callback>) -> Self {
        Self {
            commit_token,
            callback: Some(callback.into()),
        }
    }

    fn fail_with_internal(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback.call(Err(PartitionProcessorRpcError::Internal(
                "error when proposing to bifrost".to_string(),
            )));
        }
    }

    fn fail_with_lost_leadership(&mut self, this_partition_id: PartitionId) {
        if let Some(callback) = self.callback.take() {
            callback.call(Err(PartitionProcessorRpcError::LostLeadership(
                this_partition_id,
            )));
        }
    }

    fn succeed_with_appended(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback.call(Ok(()))
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
