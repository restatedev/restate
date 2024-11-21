// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::future;
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::{Duration, SystemTime};

use futures::future::OptionFuture;
use futures::stream::FuturesUnordered;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use metrics::counter;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, instrument, trace, warn};

use restate_bifrost::{Bifrost, CommitToken};
use restate_core::network::Reciprocal;
use restate_core::{
    metadata, task_center, Metadata, ShutdownError, TaskCenter, TaskHandle, TaskId, TaskKind,
};
use restate_errors::NotRunningError;
use restate_invoker_api::InvokeInputJournal;
use restate_partition_store::PartitionStore;
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};
use restate_storage_api::invocation_status_table::ReadOnlyInvocationStatusTable;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::timer_table::{TimerKey, TimerTable};
use restate_timer::TokioClock;
use restate_types::identifiers::{
    InvocationId, PartitionKey, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionLeaderEpoch};
use restate_types::logs::LogId;
use restate_types::message::MessageIndex;
use restate_types::net::partition_processor::{
    InvocationOutput, PartitionProcessorRpcError, PartitionProcessorRpcResponse,
    SubmittedInvocationNotification,
};
use restate_types::storage::StorageEncodeError;
use restate_types::time::MillisSinceEpoch;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

use crate::metric_definitions::PARTITION_HANDLE_LEADER_ACTIONS;
use crate::partition::cleaner::Cleaner;
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::shuffle::{HintSender, OutboxReaderError, Shuffle, ShuffleMetadata};
use crate::partition::state_machine::Action;
use crate::partition::{respond_to_rpc, shuffle};

const BATCH_READY_UP_TO: usize = 10;

type TimerService = restate_timer::TimerService<TimerKeyValue, TokioClock, TimerReader>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invoker is unreachable. This indicates a bug or the system is shutting down: {0}")]
    Invoker(NotRunningError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
    #[error("failed writing to bifrost: {0}")]
    Bifrost(#[from] restate_bifrost::Error),
    #[error("failed serializing payload: {0}")]
    Codec(#[from] StorageEncodeError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("error when self proposing")]
    SelfProposer,
}

#[derive(Debug)]
pub(crate) enum ActionEffect {
    Invoker(restate_invoker_api::Effect),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerKeyValue),
    ScheduleCleanupTimer(InvocationId, Duration),
    AwaitingRpcSelfProposeDone,
}

pub(crate) struct LeaderState {
    leader_epoch: LeaderEpoch,
    shuffle_hint_tx: HintSender,
    shuffle_task_id: TaskId,
    timer_service: Pin<Box<TimerService>>,
    self_proposer: SelfProposer,

    awaiting_rpc_actions: HashMap<
        PartitionProcessorRpcRequestId,
        Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
    >,
    awaiting_rpc_self_propose: FuturesUnordered<SelfAppendFuture>,

    invoker_stream: ReceiverStream<restate_invoker_api::Effect>,
    shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
    pending_cleanup_timers_to_schedule: VecDeque<(InvocationId, Duration)>,
    cleaner_task_id: TaskId,
}

pub enum State {
    Follower,
    Candidate {
        leader_epoch: LeaderEpoch,
        appender_task: TaskHandle<Result<(), ShutdownError>>,
    },
    Leader(LeaderState),
}

impl State {
    fn leader_epoch(&self) -> Option<LeaderEpoch> {
        match self {
            State::Follower => None,
            State::Candidate { leader_epoch, .. } => Some(*leader_epoch),
            State::Leader(leader_state) => Some(leader_state.leader_epoch),
        }
    }
}

pub struct PartitionProcessorMetadata {
    node_id: GenerationalNodeId,
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
}

impl PartitionProcessorMetadata {
    pub const fn new(
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> Self {
        Self {
            node_id,
            partition_id,
            partition_key_range,
        }
    }
}

pub(crate) struct LeadershipState<I> {
    state: State,
    last_seen_leader_epoch: Option<LeaderEpoch>,

    partition_processor_metadata: PartitionProcessorMetadata,
    num_timers_in_memory_limit: Option<usize>,
    cleanup_interval: Duration,
    channel_size: usize,
    invoker_tx: I,
    bifrost: Bifrost,
    task_center: TaskCenter,
}

impl<I> LeadershipState<I>
where
    I: restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>>,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        task_center: TaskCenter,
        partition_processor_metadata: PartitionProcessorMetadata,
        num_timers_in_memory_limit: Option<usize>,
        cleanup_interval: Duration,
        channel_size: usize,
        invoker_tx: I,
        bifrost: Bifrost,
        last_seen_leader_epoch: Option<LeaderEpoch>,
    ) -> Self {
        Self {
            task_center,
            state: State::Follower,
            partition_processor_metadata,
            num_timers_in_memory_limit,
            cleanup_interval,
            channel_size,
            invoker_tx,
            bifrost,
            last_seen_leader_epoch,
        }
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self.state, State::Leader(_))
    }

    fn is_new_leader_epoch(&self, leader_epoch: LeaderEpoch) -> bool {
        if let Some(max_leader_epoch) = self.state.leader_epoch().or(self.last_seen_leader_epoch) {
            max_leader_epoch < leader_epoch
        } else {
            true
        }
    }

    #[instrument(level = "debug", skip_all, fields(leader_epoch = %leader_epoch))]
    pub async fn run_for_leader(&mut self, leader_epoch: LeaderEpoch) -> Result<(), Error> {
        if self.is_new_leader_epoch(leader_epoch) {
            self.become_follower().await;
            self.announce_leadership(leader_epoch).await?;
            debug!("Running for leadership.");
        } else {
            debug!("Asked to run for leadership with an outdated leader epoch. Ignoring, since futile.")
        }

        Ok(())
    }

    async fn announce_leadership(
        &mut self,
        leader_epoch: LeaderEpoch,
    ) -> Result<(), ShutdownError> {
        let header = Header {
            dest: Destination::Processor {
                partition_key: *self
                    .partition_processor_metadata
                    .partition_key_range
                    .start(),
                dedup: Some(DedupInformation::self_proposal(EpochSequenceNumber::new(
                    leader_epoch,
                ))),
            },
            source: Source::Processor {
                partition_id: self.partition_processor_metadata.partition_id,
                partition_key: Some(
                    *self
                        .partition_processor_metadata
                        .partition_key_range
                        .start(),
                ),
                leader_epoch,
                // Kept for backward compatibility.
                node_id: self.partition_processor_metadata.node_id.as_plain(),
                generational_node_id: Some(self.partition_processor_metadata.node_id),
            },
        };

        let envelope = Envelope::new(
            header,
            Command::AnnounceLeader(AnnounceLeader {
                // todo: Still need to write generational id for supporting rolling back, can be removed
                //  with the next release.
                node_id: Some(self.partition_processor_metadata.node_id),
                leader_epoch,
                partition_key_range: Some(
                    self.partition_processor_metadata
                        .partition_key_range
                        .clone(),
                ),
            }),
        );

        let envelope = Arc::new(envelope);
        let log_id = LogId::from(self.partition_processor_metadata.partition_id);
        let bifrost = self.bifrost.clone();

        // todo replace with background appender and allowing PP to gracefully fail w/o stopping the process
        let appender_task = task_center().spawn_unmanaged(TaskKind::Background, "announce-leadership", Some(self.partition_processor_metadata.partition_id), async move {
            loop {
                // further instructions/commands from PP manager.
                match bifrost.append(log_id, Arc::clone(&envelope)).await {
                    // only stop on shutdown
                    Err(restate_bifrost::Error::Shutdown(_)) => return Err(ShutdownError),
                    Err(e) => {
                        info!(
                        %log_id,
                        %leader_epoch,
                        ?e,
                        "Failed to write the announce leadership message to bifrost. Retrying."
                    );
                        // todo: retry with backoff. At the moment, this is very aggressive (intentionally)
                        // to avoid blocking for too long.
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    Ok(lsn) => {
                        debug!(
                        %log_id,
                        %leader_epoch,
                        %lsn,
                        "Written announce leadership message to bifrost."
                    );
                        return Ok(());
                    }
                }
            }
        })?;

        self.state = State::Candidate {
            leader_epoch,
            appender_task,
        };

        Ok(())
    }

    pub async fn step_down(&mut self) {
        debug!("Stepping down. Being a role model for Joe.");
        self.become_follower().await
    }

    #[instrument(level = "debug", skip_all, fields(leader_epoch = %announce_leader.leader_epoch))]
    pub async fn on_announce_leader(
        &mut self,
        announce_leader: AnnounceLeader,
        partition_store: &mut PartitionStore,
    ) -> Result<bool, Error> {
        self.last_seen_leader_epoch = Some(announce_leader.leader_epoch);

        match &self.state {
            State::Follower => {
                debug!("Observed new leader. Staying an obedient follower.");
            }
            State::Candidate { leader_epoch, .. } => {
                match leader_epoch.cmp(&announce_leader.leader_epoch) {
                    Ordering::Less => {
                        debug!("Lost leadership campaign. Becoming an obedient follower.");
                        self.become_follower().await;
                    }
                    Ordering::Equal => {
                        debug!("Won the leadership campaign. Becoming the strong leader now.");
                        self.become_leader(partition_store).await?
                    }
                    Ordering::Greater => {
                        debug!("Observed an intermittent leader. Still believing to win the leadership campaign.");
                    }
                }
            }
            State::Leader(leader_state) => {
                match leader_state.leader_epoch.cmp(&announce_leader.leader_epoch) {
                    Ordering::Less => {
                        debug!(
                            my_leadership_epoch = %leader_state.leader_epoch,
                            new_leader_epoch = %announce_leader.leader_epoch,
                            "Every reign must end. Stepping down and becoming an obedient follower."
                        );
                        self.become_follower().await;
                    }
                    Ordering::Equal => {
                        warn!("Observed another leadership announcement for my own leadership. This should never happen and indicates a bug!");
                    }
                    Ordering::Greater => {
                        warn!("Observed a leadership announcement for an outdated epoch. This should never happen and indicates a bug!");
                    }
                }
            }
        }

        Ok(self.is_leader())
    }

    async fn become_leader(&mut self, partition_store: &mut PartitionStore) -> Result<(), Error> {
        if let State::Candidate { leader_epoch, .. } = self.state {
            let invoker_rx = Self::resume_invoked_invocations(
                &mut self.invoker_tx,
                (self.partition_processor_metadata.partition_id, leader_epoch),
                self.partition_processor_metadata
                    .partition_key_range
                    .clone(),
                partition_store,
                self.channel_size,
            )
            .await?;

            let timer_service = Box::pin(TimerService::new(
                TokioClock,
                self.num_timers_in_memory_limit,
                TimerReader::from(partition_store.clone()),
            ));

            let (shuffle_tx, shuffle_rx) = mpsc::channel(self.channel_size);

            let shuffle = Shuffle::new(
                ShuffleMetadata::new(
                    self.partition_processor_metadata.partition_id,
                    leader_epoch,
                    self.partition_processor_metadata.node_id,
                ),
                OutboxReader::from(partition_store.clone()),
                shuffle_tx,
                self.channel_size,
                self.bifrost.clone(),
            );

            let shuffle_hint_tx = shuffle.create_hint_sender();

            let shuffle_task_id = task_center().spawn_child(
                TaskKind::Shuffle,
                "shuffle",
                Some(self.partition_processor_metadata.partition_id),
                shuffle.run(),
            )?;

            let self_proposer = SelfProposer::new(
                self.partition_processor_metadata.partition_id,
                EpochSequenceNumber::new(leader_epoch),
                &self.bifrost,
                metadata(),
            )?;

            let cleaner = Cleaner::new(
                self.partition_processor_metadata.partition_id,
                leader_epoch,
                self.partition_processor_metadata.node_id,
                partition_store.clone(),
                self.bifrost.clone(),
                self.partition_processor_metadata
                    .partition_key_range
                    .clone(),
                self.cleanup_interval,
            );

            let cleaner_task_id = task_center().spawn_child(
                TaskKind::Cleaner,
                "cleaner",
                Some(self.partition_processor_metadata.partition_id),
                cleaner.run(),
            )?;

            self.state = State::Leader(LeaderState {
                leader_epoch,
                shuffle_task_id,
                cleaner_task_id,
                shuffle_hint_tx,
                timer_service,
                self_proposer,
                awaiting_rpc_actions: Default::default(),
                awaiting_rpc_self_propose: Default::default(),
                invoker_stream: ReceiverStream::new(invoker_rx),
                shuffle_stream: ReceiverStream::new(shuffle_rx),
                pending_cleanup_timers_to_schedule: Default::default(),
            });

            Ok(())
        } else {
            unreachable!("Can only become the leader if I was the candidate before!");
        }
    }

    async fn resume_invoked_invocations(
        invoker_handle: &mut I,
        partition_leader_epoch: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        partition_store: &mut PartitionStore,
        channel_size: usize,
    ) -> Result<mpsc::Receiver<restate_invoker_api::Effect>, Error> {
        let (invoker_tx, invoker_rx) = mpsc::channel(channel_size);

        invoker_handle
            .register_partition(
                partition_leader_epoch,
                partition_key_range,
                InvokerStorageReader::new(partition_store.clone()),
                invoker_tx,
            )
            .await
            .map_err(Error::Invoker)?;

        {
            let invoked_invocations = partition_store.all_invoked_invocations();
            tokio::pin!(invoked_invocations);

            let mut count = 0;
            while let Some(invocation_id_and_target) = invoked_invocations.next().await {
                let (invocation_id, invocation_target) = invocation_id_and_target?;
                invoker_handle
                    .invoke(
                        partition_leader_epoch,
                        invocation_id,
                        invocation_target,
                        InvokeInputJournal::NoCachedJournal,
                    )
                    .await
                    .map_err(Error::Invoker)?;
                count += 1;
            }
            debug!("Leader partition resumed {} invocations", count);
        }

        Ok(invoker_rx)
    }

    async fn become_follower(&mut self) {
        match &mut self.state {
            State::Follower => {
                // nothing to do :-)
            }
            State::Candidate { appender_task, .. } => {
                appender_task.abort();
            }
            State::Leader(LeaderState {
                leader_epoch,
                shuffle_task_id,
                cleaner_task_id,
                ref mut awaiting_rpc_actions,
                awaiting_rpc_self_propose: ref mut awaiting_rpc_self_appends,
                ..
            }) => {
                let shuffle_handle =
                    OptionFuture::from(task_center().cancel_task(*shuffle_task_id));
                let cleaner_handle =
                    OptionFuture::from(task_center().cancel_task(*cleaner_task_id));

                // It's ok to not check the abort_result because either it succeeded or the invoker
                // is not running. If the invoker is not running, and we are not shutting down, then
                // we will fail the next time we try to invoke.
                let (shuffle_result, cleaner_result, _abort_result) = tokio::join!(
                    shuffle_handle,
                    cleaner_handle,
                    self.invoker_tx.abort_all_partition((
                        self.partition_processor_metadata.partition_id,
                        *leader_epoch
                    )),
                );

                if let Some(shuffle_result) = shuffle_result {
                    shuffle_result.expect("graceful termination of shuffle task");
                }
                if let Some(cleaner_result) = cleaner_result {
                    cleaner_result.expect("graceful termination of cleaner task");
                }

                // Reply to all RPCs with not a leader
                for (request_id, reciprocal) in awaiting_rpc_actions.drain() {
                    trace!(
                        %request_id,
                        "Failing rpc because I lost leadership",
                    );
                    respond_to_rpc(
                        &self.task_center,
                        reciprocal.prepare(Err(PartitionProcessorRpcError::LostLeadership(
                            self.partition_processor_metadata.partition_id,
                        ))),
                    );
                }
                for fut in awaiting_rpc_self_appends.iter_mut() {
                    fut.fail_with_lost_leadership(self.partition_processor_metadata.partition_id);
                }
                awaiting_rpc_self_appends.clear();
            }
        }

        self.state = State::Follower;
    }

    pub async fn handle_actions(
        &mut self,
        actions: impl Iterator<Item = Action>,
    ) -> Result<(), Error> {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                for action in actions {
                    trace!(?action, "Apply action");
                    counter!(PARTITION_HANDLE_LEADER_ACTIONS, "action" =>
                        action.name())
                    .increment(1);
                    Self::handle_action(
                        &self.task_center,
                        action,
                        (
                            self.partition_processor_metadata.partition_id,
                            leader_state.leader_epoch,
                        ),
                        &mut self.invoker_tx,
                        &leader_state.shuffle_hint_tx,
                        leader_state.timer_service.as_mut(),
                        &mut leader_state.pending_cleanup_timers_to_schedule,
                        &mut leader_state.awaiting_rpc_actions,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_action(
        task_center: &TaskCenter,
        action: Action,
        partition_leader_epoch: PartitionLeaderEpoch,
        invoker_tx: &mut I,
        shuffle_hint_tx: &HintSender,
        mut timer_service: Pin<&mut TimerService>,
        actions_effects: &mut VecDeque<(InvocationId, Duration)>,
        awaiting_rpcs: &mut HashMap<
            PartitionProcessorRpcRequestId,
            Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        >,
    ) -> Result<(), Error> {
        match action {
            Action::Invoke {
                invocation_id,
                invocation_target,
                invoke_input_journal,
            } => invoker_tx
                .invoke(
                    partition_leader_epoch,
                    invocation_id,
                    invocation_target,
                    invoke_input_journal,
                )
                .await
                .map_err(Error::Invoker)?,
            Action::NewOutboxMessage {
                seq_number,
                message,
            } => shuffle_hint_tx.send(shuffle::NewOutboxMessage::new(seq_number, message)),
            Action::RegisterTimer { timer_value } => timer_service.as_mut().add_timer(timer_value),
            Action::DeleteTimer { timer_key } => timer_service.as_mut().remove_timer(timer_key),
            Action::AckStoredEntry {
                invocation_id,
                entry_index,
            } => {
                invoker_tx
                    .notify_stored_entry_ack(partition_leader_epoch, invocation_id, entry_index)
                    .await
                    .map_err(Error::Invoker)?;
            }
            Action::ForwardCompletion {
                invocation_id,
                completion,
            } => invoker_tx
                .notify_completion(partition_leader_epoch, invocation_id, completion)
                .await
                .map_err(Error::Invoker)?,
            Action::AbortInvocation(invocation_id) => invoker_tx
                .abort_invocation(partition_leader_epoch, invocation_id)
                .await
                .map_err(Error::Invoker)?,
            Action::IngressResponse {
                request_id,
                invocation_id,
                response,
                completion_expiry_time,
                ..
            } => {
                if let Some(response_tx) = awaiting_rpcs.remove(&request_id) {
                    respond_to_rpc(
                        task_center,
                        response_tx.prepare(Ok(PartitionProcessorRpcResponse::Output(
                            InvocationOutput {
                                request_id,
                                invocation_id,
                                completion_expiry_time,
                                response,
                            },
                        ))),
                    );
                } else {
                    debug!(%request_id, "Ignoring sending ingress response because there is no awaiting rpc");
                }
            }
            Action::IngressSubmitNotification {
                request_id,
                is_new_invocation,
                ..
            } => {
                if let Some(response_tx) = awaiting_rpcs.remove(&request_id) {
                    respond_to_rpc(
                        task_center,
                        response_tx.prepare(Ok(PartitionProcessorRpcResponse::Submitted(
                            SubmittedInvocationNotification {
                                request_id,
                                is_new_invocation,
                            },
                        ))),
                    );
                }
            }
            Action::ScheduleInvocationStatusCleanup {
                invocation_id,
                retention,
            } => {
                actions_effects.push_back((invocation_id, retention));
            }
        }

        Ok(())
    }

    pub async fn next_action_effects(&mut self) -> Option<Vec<ActionEffect>> {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => None,
            State::Leader(leader_state) => {
                let timer_stream = std::pin::pin!(stream::unfold(
                    &mut leader_state.timer_service,
                    |timer_service| async {
                        let timer_value = timer_service.as_mut().next_timer().await;
                        Some((ActionEffect::Timer(timer_value), timer_service))
                    }
                ));

                let invoker_stream = (&mut leader_state.invoker_stream).map(ActionEffect::Invoker);
                let shuffle_stream = (&mut leader_state.shuffle_stream).map(ActionEffect::Shuffle);
                let action_effects_stream = stream::unfold(
                    &mut leader_state.pending_cleanup_timers_to_schedule,
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
                let awaiting_rpc_self_propose_stream = (&mut leader_state
                    .awaiting_rpc_self_propose)
                    .map(|_| ActionEffect::AwaitingRpcSelfProposeDone);

                let all_streams = futures::stream_select!(
                    invoker_stream,
                    shuffle_stream,
                    timer_stream,
                    action_effects_stream,
                    awaiting_rpc_self_propose_stream
                );
                let mut all_streams = all_streams.ready_chunks(BATCH_READY_UP_TO);
                all_streams.next().await
            }
        }
    }

    pub async fn handle_action_effect(
        &mut self,
        action_effects: impl IntoIterator<Item = ActionEffect>,
    ) -> anyhow::Result<()> {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                for effect in action_effects {
                    match effect {
                        ActionEffect::Invoker(invoker_effect) => {
                            leader_state
                                .self_proposer
                                .propose(
                                    invoker_effect.invocation_id.partition_key(),
                                    Command::InvokerEffect(invoker_effect),
                                )
                                .await?;
                        }
                        ActionEffect::Shuffle(outbox_truncation) => {
                            // todo: Until we support partition splits we need to get rid of outboxes or introduce partition
                            //  specific destination messages that are identified by a partition_id
                            leader_state
                                .self_proposer
                                .propose(
                                    *self
                                        .partition_processor_metadata
                                        .partition_key_range
                                        .start(),
                                    Command::TruncateOutbox(outbox_truncation.index()),
                                )
                                .await?;
                        }
                        ActionEffect::Timer(timer) => {
                            leader_state
                                .self_proposer
                                .propose(
                                    timer.invocation_id().partition_key(),
                                    Command::Timer(timer),
                                )
                                .await?;
                        }
                        ActionEffect::ScheduleCleanupTimer(invocation_id, duration) => {
                            leader_state
                                .self_proposer
                                .propose(
                                    invocation_id.partition_key(),
                                    Command::ScheduleTimer(TimerKeyValue::clean_invocation_status(
                                        MillisSinceEpoch::from(SystemTime::now() + duration),
                                        invocation_id,
                                    )),
                                )
                                .await?;
                        }
                        ActionEffect::AwaitingRpcSelfProposeDone => {
                            // Nothing to do here
                        }
                    }
                }
            }
        };

        Ok(())
    }

    pub async fn handle_rpc_proposal_command(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        reciprocal: Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        partition_key: PartitionKey,
        cmd: Command,
    ) {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => {
                // Just fail the rpc
                respond_to_rpc(
                    &self.task_center,
                    reciprocal.prepare(Err(PartitionProcessorRpcError::NotLeader(
                        self.partition_processor_metadata.partition_id,
                    ))),
                );
            }
            State::Leader(leader_state) => {
                match leader_state.awaiting_rpc_actions.entry(request_id) {
                    Entry::Occupied(o) => {
                        // In this case, someone already proposed this command,
                        // let's just replace the reciprocal and fail the old one to avoid keeping it dangling
                        let old_reciprocal = o.remove();
                        trace!(%request_id, "Replacing rpc with newer request");
                        respond_to_rpc(
                            &self.task_center,
                            old_reciprocal.prepare(Err(PartitionProcessorRpcError::Internal(
                                "retried".to_string(),
                            ))),
                        );
                        leader_state
                            .awaiting_rpc_actions
                            .insert(request_id, reciprocal);
                    }
                    Entry::Vacant(v) => {
                        // In this case, no one proposed this command yet, let's try to propose it
                        if let Err(e) = leader_state.self_proposer.propose(partition_key, cmd).await
                        {
                            respond_to_rpc(
                                &self.task_center,
                                reciprocal.prepare(Err(PartitionProcessorRpcError::Internal(
                                    e.to_string(),
                                ))),
                            );
                        } else {
                            v.insert(reciprocal);
                        }
                    }
                }
            }
        }
    }

    /// Self propose to this partition, and register the reciprocal to respond asynchronously.
    pub async fn self_propose_and_respond_asynchronously(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        reciprocal: Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
    ) {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => respond_to_rpc(
                &self.task_center,
                reciprocal.prepare(Err(PartitionProcessorRpcError::NotLeader(
                    self.partition_processor_metadata.partition_id,
                ))),
            ),
            State::Leader(leader_state) => {
                match leader_state
                    .self_proposer
                    .propose_with_notification(partition_key, cmd)
                    .await
                {
                    Ok(commit_token) => {
                        leader_state
                            .awaiting_rpc_self_propose
                            .push(SelfAppendFuture::new(
                                self.task_center.clone(),
                                commit_token,
                                reciprocal,
                            ));
                    }
                    Err(e) => {
                        respond_to_rpc(
                            &self.task_center,
                            reciprocal
                                .prepare(Err(PartitionProcessorRpcError::Internal(e.to_string()))),
                        );
                    }
                }
            }
        }
    }
}

struct SelfAppendFuture {
    task_center: TaskCenter,
    commit_token: CommitToken,
    response: Option<Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>>,
}

impl SelfAppendFuture {
    fn new(
        task_center: TaskCenter,
        commit_token: CommitToken,
        response: Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
    ) -> Self {
        Self {
            task_center,
            commit_token,
            response: Some(response),
        }
    }

    fn fail_with_internal(&mut self) {
        if let Some(reciprocal) = self.response.take() {
            respond_to_rpc(
                &self.task_center,
                reciprocal.prepare(Err(PartitionProcessorRpcError::Internal(
                    "error when proposing to bifrost".to_string(),
                ))),
            );
        }
    }

    fn fail_with_lost_leadership(&mut self, this_partition_id: PartitionId) {
        if let Some(reciprocal) = self.response.take() {
            respond_to_rpc(
                &self.task_center,
                reciprocal.prepare(Err(PartitionProcessorRpcError::LostLeadership(
                    this_partition_id,
                ))),
            );
        }
    }

    fn succeed_with_appended(&mut self) {
        if let Some(reciprocal) = self.response.take() {
            respond_to_rpc(
                &self.task_center,
                reciprocal.prepare(Ok(PartitionProcessorRpcResponse::Appended)),
            );
        }
    }
}

impl Future for SelfAppendFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let append_result = ready!(self.commit_token.poll_unpin(cx));

        if append_result.is_err() {
            self.get_mut().fail_with_internal();
            return Poll::Ready(());
        }
        self.succeed_with_appended();
        Poll::Ready(())
    }
}

// Constants since it's very unlikely that we can derive a meaningful configuration
// that the user can reason about.
//
// The queue size is small to reduce the tail latency. This comes at the cost of throughput but
// this runs within a single processor and the expected throughput is bound by the overall
// throughput of the processor itself.
const BIFROST_QUEUE_SIZE: usize = 20;
const MAX_BIFROST_APPEND_BATCH: usize = 5000;

struct SelfProposer {
    partition_id: PartitionId,
    epoch_sequence_number: EpochSequenceNumber,
    bifrost_appender: restate_bifrost::AppenderHandle<Envelope>,
    metadata: Metadata,
}

impl SelfProposer {
    fn new(
        partition_id: PartitionId,
        epoch_sequence_number: EpochSequenceNumber,
        bifrost: &Bifrost,
        metadata: Metadata,
    ) -> Result<Self, Error> {
        let bifrost_appender = bifrost
            .create_background_appender(
                LogId::from(partition_id),
                BIFROST_QUEUE_SIZE,
                MAX_BIFROST_APPEND_BATCH,
            )?
            .start(task_center(), "self-appender", Some(partition_id))?;

        Ok(Self {
            partition_id,
            epoch_sequence_number,
            bifrost_appender,
            metadata,
        })
    }

    async fn propose(&mut self, partition_key: PartitionKey, cmd: Command) -> Result<(), Error> {
        let envelope = Envelope::new(self.create_header(partition_key), cmd);

        // Only blocks if background append is pushing back (queue full)
        self.bifrost_appender
            .sender()
            .enqueue(Arc::new(envelope))
            .await
            .map_err(|_| Error::SelfProposer)?;

        Ok(())
    }

    async fn propose_with_notification(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
    ) -> Result<CommitToken, Error> {
        let envelope = Envelope::new(self.create_header(partition_key), cmd);

        let commit_token = self
            .bifrost_appender
            .sender()
            .enqueue_with_notification(Arc::new(envelope))
            .await
            .map_err(|_| Error::SelfProposer)?;

        Ok(commit_token)
    }

    fn create_header(&mut self, partition_key: PartitionKey) -> Header {
        let esn = self.epoch_sequence_number.next();
        self.epoch_sequence_number = esn;

        let my_node_id = self.metadata.my_node_id();
        Header {
            dest: Destination::Processor {
                partition_key,
                dedup: Some(DedupInformation::self_proposal(esn)),
            },
            source: Source::Processor {
                partition_id: self.partition_id,
                partition_key: Some(partition_key),
                leader_epoch: self.epoch_sequence_number.leader_epoch,
                node_id: my_node_id.as_plain(),
                generational_node_id: Some(my_node_id),
            },
        }
    }
}

#[derive(Debug, derive_more::From)]
struct TimerReader(PartitionStore);

impl restate_timer::TimerReader<TimerKeyValue> for TimerReader {
    async fn get_timers(
        &mut self,
        num_timers: usize,
        previous_timer_key: Option<TimerKey>,
    ) -> Vec<TimerKeyValue> {
        self.0
            .next_timers_greater_than(previous_timer_key.as_ref(), num_timers)
            .map(|result| result.map(|(timer_key, timer)| TimerKeyValue::new(timer_key, timer)))
            // TODO: Update timer service to maintain transaction while reading the timer stream: See https://github.com/restatedev/restate/issues/273
            // have to collect the stream because it depends on the local transaction
            .try_collect::<Vec<_>>()
            .await
            // TODO: Extend TimerReader to return errors: See https://github.com/restatedev/restate/issues/274
            .expect("timer deserialization should not fail")
    }
}

#[derive(Debug, derive_more::From)]
struct OutboxReader(PartitionStore);

impl shuffle::OutboxReader for OutboxReader {
    async fn get_next_message(
        &mut self,
        next_sequence_number: MessageIndex,
    ) -> Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError> {
        let result = if let Some((message_index, outbox_message)) =
            self.0.get_next_outbox_message(next_sequence_number).await?
        {
            Some((message_index, outbox_message))
        } else {
            None
        };

        Ok(result)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskError {
    #[error(transparent)]
    Error(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use crate::partition::leadership::{LeadershipState, PartitionProcessorMetadata, State};
    use assert2::let_assert;
    use restate_bifrost::Bifrost;
    use restate_core::{task_center, TestCoreEnv};
    use restate_invoker_api::test_util::MockInvokerHandle;
    use restate_partition_store::{OpenMode, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::{CommonOptions, RocksDbOptions, StorageOptions};
    use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
    use restate_types::live::Constant;
    use restate_types::logs::{KeyFilter, Lsn, SequenceNumber};
    use restate_types::GenerationalNodeId;
    use restate_wal_protocol::control::AnnounceLeader;
    use restate_wal_protocol::{Command, Envelope};
    use std::ops::RangeInclusive;
    use std::time::Duration;
    use test_log::test;
    use tokio_stream::StreamExt;

    const PARTITION_ID: PartitionId = PartitionId::MIN;
    const NODE_ID: GenerationalNodeId = GenerationalNodeId::new(0, 0);
    const PARTITION_KEY_RANGE: RangeInclusive<PartitionKey> = PartitionKey::MIN..=PartitionKey::MAX;
    const PARTITION_PROCESSOR_METADATA: PartitionProcessorMetadata =
        PartitionProcessorMetadata::new(NODE_ID, PARTITION_ID, PARTITION_KEY_RANGE);

    #[test(tokio::test)]
    async fn become_leader_then_step_down() -> googletest::Result<()> {
        let env = TestCoreEnv::create_with_single_node(0, 0).await;
        let tc = env.tc.clone();
        let storage_options = StorageOptions::default();
        let rocksdb_options = RocksDbOptions::default();

        tc.run_in_scope_sync("db-manager-init", None, || {
            RocksDbManager::init(Constant::new(CommonOptions::default()))
        });

        let bifrost = tc
            .run_in_scope(
                "init bifrost",
                None,
                Bifrost::init_in_memory(env.metadata.clone()),
            )
            .await;

        tc.run_in_scope("test", None, async {
            let partition_store_manager = PartitionStoreManager::create(
                Constant::new(storage_options.clone()).boxed(),
                Constant::new(rocksdb_options.clone()).boxed(),
                &[(PARTITION_ID, PARTITION_KEY_RANGE)],
            )
            .await?;

            let invoker_tx = MockInvokerHandle::default();
            let mut state = LeadershipState::new(
                task_center(),
                PARTITION_PROCESSOR_METADATA,
                None,
                Duration::from_secs(60 * 60),
                42,
                invoker_tx,
                bifrost.clone(),
                None,
            );

            assert!(matches!(state.state, State::Follower));

            let leader_epoch = LeaderEpoch::from(1);
            state.run_for_leader(leader_epoch).await?;

            assert!(matches!(state.state, State::Candidate { .. }));

            let record = bifrost
                .create_reader(PARTITION_ID.into(), KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)
                .expect("valid reader")
                .next()
                .await
                .unwrap()?;

            let envelope = record.try_decode::<Envelope>().unwrap()?;

            let_assert!(Command::AnnounceLeader(announce_leader) = envelope.command);
            assert_eq!(
                announce_leader,
                AnnounceLeader {
                    node_id: Some(NODE_ID),
                    leader_epoch,
                    partition_key_range: Some(PARTITION_KEY_RANGE),
                }
            );

            let mut partition_store = partition_store_manager
                .open_partition_store(
                    PARTITION_ID,
                    PARTITION_KEY_RANGE,
                    OpenMode::CreateIfMissing,
                    &rocksdb_options,
                )
                .await?;
            state
                .on_announce_leader(announce_leader, &mut partition_store)
                .await?;

            assert!(matches!(state.state, State::Leader(_)));

            state.step_down().await;

            assert!(matches!(state.state, State::Follower));

            googletest::Result::Ok(())
        })
        .await?;

        tc.shutdown_node("test_completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
