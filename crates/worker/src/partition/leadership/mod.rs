// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod durability_tracker;
mod leader_state;
mod self_proposer;
pub mod trim_queue;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, instrument, warn};

use restate_core::network::{Oneshot, Reciprocal, TransportConnect};
use restate_core::{Metadata, ShutdownError, TaskCenter, TaskKind};
use restate_errors::NotRunningError;
use restate_ingestion_client::IngestionClient;
use restate_invoker_impl::{
    InvokerHandle as InvokerChannelServiceHandle, Service as InvokerService,
};
use restate_partition_store::PartitionStore;
use restate_platform::hash::HashMap;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::StorageError;
use restate_storage_api::deduplication_table::EpochSequenceNumber;
use restate_storage_api::invocation_status_table::{
    InvokedInvocationStatusLite, ScanInvocationStatusTable,
};
use restate_storage_api::outbox_table::{OutboxMessage, ReadOutboxTable};
use restate_storage_api::timer_table::{ReadTimerTable, TimerKey};
use restate_timer::TokioClock;
use restate_types::GenerationalNodeId;
use restate_types::cluster::cluster_state::RunMode;
use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::identifiers::{InvocationId, LeaderEpoch, PartitionId};
use restate_types::identifiers::{PartitionKey, PartitionProcessorRpcRequestId};
use restate_types::invocation::FencingToken;
use restate_types::live::LiveLoadExt;
use restate_types::logs::Keys;
use restate_types::message::MessageIndex;
use restate_types::net::ingest::IngestRecord;
use restate_types::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcResponse,
};
use restate_types::partitions::PartitionFeatureChange;
use restate_types::protobuf::cluster::DetailedRunMode;
use restate_types::schema::Schema;
use restate_types::storage::{StorageDecodeError, StorageEncodeError};
use restate_util_time::DurationExt;
use restate_vqueues::context::{HasVQueues, HasVQueuesMut};
use restate_vqueues::scheduler::{self};
use restate_vqueues::{ResourceManager, SchedulerService, VQueuesMeta};
use restate_wal_protocol::control::{
    AnnounceLeaderCommand, UpdatePartitionDurabilityCommand, VersionBarrierCommand,
};
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::{Command, Envelope};
use restate_worker_api::invoker::InvokerHandle;
use restate_worker_api::{
    LeaderQueryCommand, LeaderQueryRequest, LeaderQueryResponse, LeaderQuerySender,
};

use self::durability_tracker::DurabilityTracker;
use self::trim_queue::{HasTrimQueue, LogTrimmer};
use crate::invoker_integration::EntryEnricher;
use crate::partition::LeadershipInfo;
use crate::partition::cleaner::{self, Cleaner};
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::leadership::leader_state::LeaderState;
use crate::partition::leadership::self_proposer::SelfProposer;
use crate::partition::processor::FsmAccess;
use crate::partition::shuffle;
use crate::partition::shuffle::{OutboxReaderError, Shuffle, ShuffleMetadata};
use crate::partition::state_machine::Action;
use crate::partition::types::InvokerEffect;

use super::node::NodeContext;
use super::processor::*;

type TimerService = restate_timer::TimerService<TimerKeyValue, TokioClock, TimerReader>;
type InvokerStream = ReceiverStream<InvokerEffect>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invoker is unreachable. This indicates a bug or the system is shutting down: {0}")]
    Invoker(NotRunningError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("failed writing to bifrost: {0}")]
    Bifrost(#[from] restate_bifrost::Error),
    #[error("failed serializing payload: {0}")]
    Encode(#[from] StorageEncodeError),
    #[error("failed deserializing payload: {0}")]
    Decode(#[from] StorageDecodeError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    InvokerBuild(#[from] restate_invoker_impl::BuildError),
    #[error("error when self proposing: {0}")]
    SelfProposer(String),
    #[error("task '{name}' failed: {cause}")]
    TaskFailed {
        name: &'static str,
        cause: TaskTermination,
    },
}

impl Error {
    fn task_terminated_unexpectedly(name: &'static str) -> Self {
        Error::TaskFailed {
            name,
            cause: TaskTermination::Unexpected,
        }
    }

    fn task_failed(name: &'static str, err: impl Into<GenericError>) -> Self {
        Error::TaskFailed {
            name,
            cause: TaskTermination::Failure(err.into()),
        }
    }
}

#[derive(Debug, derive_more::Display)]
pub(crate) enum TaskTermination {
    #[display("unexpected termination")]
    Unexpected,
    #[display("{}", _0)]
    Failure(GenericError),
}

#[derive(Debug)]
pub(crate) enum ActionEffect {
    Scheduler(scheduler::Decisions),
    Invoker(InvokerEffect),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerKeyValue),
    Cleaner(cleaner::CleanerEffect),
    PartitionMaintenance(UpdatePartitionDurabilityCommand),
    UpsertSchema(Schema),
    UpsertRuleBook(Arc<restate_limiter::RuleBook>),
    AwaitingRpcSelfProposeDone,
}

enum State {
    Follower,
    Candidate {
        at: Instant,
        leader_epoch: LeaderEpoch,
        // to be able to move out of it
        self_proposer: Option<SelfProposer>,
    },
    /// A leader that's performing migrations or other tasks before it can operate as a full leader.
    /// From the perspective of other nodes, it's the effective leader of the partition.
    BecomingLeader {
        at: Instant,
        leader_epoch: LeaderEpoch,
        // to be able to move out of it
        self_proposer: Option<SelfProposer>,
    },
    Leader(Box<LeaderState>),
}

impl State {
    fn leader_epoch(&self) -> Option<LeaderEpoch> {
        match self {
            State::Follower => None,
            State::Candidate { leader_epoch, .. } => Some(*leader_epoch),
            State::BecomingLeader { leader_epoch, .. } => Some(*leader_epoch),
            State::Leader(leader_state) => Some(leader_state.leader_epoch),
        }
    }
}

pub(crate) struct LeadershipState<T> {
    state: State,

    partition_id: PartitionId,
    ingestion_client: IngestionClient<T, Envelope>,
    leader_query_tx: LeaderQuerySender,
}

impl<T> LeadershipState<T>
where
    T: TransportConnect,
{
    pub(crate) fn new(
        partition_id: PartitionId,
        ingestion_client: IngestionClient<T, Envelope>,
        leader_query_tx: LeaderQuerySender,
    ) -> Self {
        Self {
            state: State::Follower,
            partition_id,
            ingestion_client,
            leader_query_tx,
        }
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self.state, State::Leader(_) | State::BecomingLeader { .. })
    }

    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    pub fn detailed_effective_mode(&self) -> DetailedRunMode {
        match self.state {
            State::Follower => DetailedRunMode::Follower,
            State::Candidate { .. } => DetailedRunMode::Candidate,
            State::BecomingLeader { .. } => DetailedRunMode::BecomingLeader,
            State::Leader(_) => DetailedRunMode::Leader,
        }
    }

    pub(super) fn should_process_rpc(&self) -> bool {
        // In case of BecomingLeader we prefer to park RPC requests
        // until we transition out of the current state.
        matches!(
            self.state,
            State::Leader(_) | State::Follower | State::Candidate { .. }
        )
    }

    #[instrument(level = "debug", skip_all, fields(leader_epoch = %leadership_info.leader_epoch))]
    pub async fn run_for_leader(
        &mut self,
        mut ctx: impl Processor + HasStatusMut,
        node_ctx: &NodeContext,
        leadership_info: Box<LeadershipInfo>,
    ) -> Result<(), Error> {
        let max_leader_epoch = self
            .state
            .leader_epoch()
            .unwrap_or_else(|| ctx.current_leader_epoch());

        if max_leader_epoch < leadership_info.leader_epoch {
            ctx.status_mut().set_planned_run_mode(RunMode::Leader);
            self.become_follower().await;
            self.announce_leadership(leadership_info, ctx, node_ctx)
                .await?;
            debug!("Running for leadership.");
        } else {
            debug!(
                "Asked to run for leadership with an outdated leader epoch. Ignoring, since futile."
            )
        }

        Ok(())
    }

    async fn announce_leadership(
        &mut self,
        leadership_info: Box<LeadershipInfo>,
        ctx: impl Processor,
        node_ctx: &NodeContext,
    ) -> Result<(), Error> {
        let leader_epoch = leadership_info.leader_epoch;

        let announce_leader = Command::AnnounceLeader(Box::new(AnnounceLeaderCommand {
            node_id: node_ctx.my_node_id(),
            leader_epoch,
            epoch_version: Some(leadership_info.version),
            partition_key_range: ctx.key_range(),
            current_config: Some(leadership_info.current_config),
            next_config: leadership_info.next_config,
        }));

        let mut self_proposer = SelfProposer::new(
            ctx.log_id(),
            EpochSequenceNumber::new(leader_epoch),
            &node_ctx.bifrost,
        )?;

        self_proposer
            .self_propose(ctx.key_range().start(), announce_leader)
            .await?;

        self.state = State::Candidate {
            at: Instant::now(),
            leader_epoch,
            self_proposer: Some(self_proposer),
        };

        Ok(())
    }

    pub async fn step_down(&mut self) {
        debug!("Stepping down. Being a role model for Joe.");
        self.become_follower().await
    }

    pub async fn maybe_step_down(
        &mut self,
        mut ctx: impl Processor + HasStatusMut,
        new_leader_epoch: LeaderEpoch,
        new_leader_node: GenerationalNodeId,
    ) {
        if ctx.status().last_observed_leader_epoch() < new_leader_epoch {
            ctx.status_mut()
                .set_last_observed_leader_epoch(new_leader_epoch);
            if new_leader_node.is_valid() {
                ctx.status_mut()
                    .set_last_observed_leader_node(new_leader_node);
            }
        }

        let planned_mode = match &self.state {
            State::Follower => RunMode::Follower,
            State::Candidate { leader_epoch, .. } => {
                match leader_epoch.cmp(&new_leader_epoch) {
                    Ordering::Less => {
                        debug!(
                            "Lost leadership campaign. Conceding to {} at epoch {}",
                            new_leader_node, new_leader_epoch
                        );
                        self.become_follower().await;
                        RunMode::Follower
                    }
                    _ => {
                        /* nothing do to, we are still candidates for leadership */
                        RunMode::Leader
                    }
                }
            }
            State::BecomingLeader { leader_epoch, .. } => {
                match leader_epoch.cmp(&new_leader_epoch) {
                    Ordering::Less => {
                        debug!(
                            my_leadership_epoch = %leader_epoch,
                            %new_leader_epoch,
                            "Every reign must end. Stepping down and becoming an conceding to {} at epoch {}",
                            new_leader_node, new_leader_epoch
                        );
                        self.become_follower().await;
                        RunMode::Follower
                    }
                    _ => {
                        /* nothing do to, we are still leaders */
                        RunMode::Leader
                    }
                }
            }
            State::Leader(leader_state) => match leader_state.leader_epoch.cmp(&new_leader_epoch) {
                Ordering::Less => {
                    debug!(
                        my_leadership_epoch = %leader_state.leader_epoch,
                        %new_leader_epoch,
                        "Every reign must end. Stepping down and becoming an conceding to {} at epoch {}",
                        new_leader_node, new_leader_epoch
                    );
                    self.become_follower().await;
                    RunMode::Follower
                }
                _ => RunMode::Leader,
            },
        };

        ctx.status_mut().set_planned_run_mode(planned_mode);
    }

    #[instrument(level = "debug", skip_all, fields(leader_epoch = %leader_epoch))]
    pub async fn on_announce_leader(
        &mut self,
        node_ctx: &mut NodeContext,
        ctx: impl Processor + HasVQueuesMut + HasTrimQueue,
        partition_store: &mut PartitionStore,
        leader_epoch: LeaderEpoch,
    ) -> Result<(), Error> {
        match &self.state {
            State::Follower => {
                debug!("Observed new leader. Staying an obedient follower.");
            }
            State::Candidate {
                leader_epoch: my_leader_epoch,
                ..
            }
            | State::BecomingLeader {
                leader_epoch: my_leader_epoch,
                ..
            } => match my_leader_epoch.cmp(&leader_epoch) {
                Ordering::Less => {
                    debug!("Lost leadership campaign. Becoming an obedient follower.");
                    self.become_follower().await;
                }
                Ordering::Equal => {
                    debug!("Won the leadership campaign. Becoming the strong leader now.");
                    self.become_leader(node_ctx, ctx, partition_store).await?
                }
                Ordering::Greater => {
                    debug!(
                        "Observed an intermittent leader. Still believing to win the leadership campaign."
                    );
                }
            },
            State::Leader(leader_state) => match leader_state.leader_epoch.cmp(&leader_epoch) {
                Ordering::Less => {
                    debug!(
                        my_leadership_epoch = %leader_state.leader_epoch,
                        new_leader_epoch = %leader_epoch,
                        "Every reign must end. Stepping down and becoming an obedient follower."
                    );
                    self.become_follower().await;
                }
                Ordering::Equal => {
                    warn!(
                        "Observed another leadership announcement for my own leadership. This should never happen and indicates a bug!"
                    );
                }
                Ordering::Greater => {
                    warn!(
                        "Observed a leadership announcement for an outdated epoch. This should never happen and indicates a bug!"
                    );
                }
            },
        }
        Ok(())
    }

    pub async fn finish_becoming_leader(
        &mut self,
        node_ctx: &mut NodeContext,
        processor: impl Processor + HasTrimQueue + HasVQueuesMut,
        partition_store: &mut PartitionStore,
    ) -> Result<(), Error> {
        if !matches!(
            self.detailed_effective_mode(),
            DetailedRunMode::BecomingLeader
        ) {
            return Ok(());
        }

        // finish up becoming a leader.
        self.become_leader(node_ctx, processor, partition_store)
            .await
    }

    async fn become_leader(
        &mut self,
        node_ctx: &mut NodeContext,
        mut processor: impl Processor + HasTrimQueue + HasVQueuesMut,
        partition_store: &mut PartitionStore,
    ) -> Result<(), Error> {
        let prev_mode = self.detailed_effective_mode();

        if let State::Candidate {
            at,
            leader_epoch,
            self_proposer,
        }
        | State::BecomingLeader {
            at,
            leader_epoch,
            self_proposer,
        } = &mut self.state
        {
            let live_config = &mut node_ctx.config;
            let config = live_config.live_load();

            let mut self_proposer = self_proposer.take().expect("must be present");
            self_proposer.mark_as_leader();

            // Collect feature changes to apply as a single VersionBarrierCommand.
            //
            // RESTATE_INTERNAL_STATE_MACHINE_FEATURES is a comma-separated list of
            // PartitionFeatureChange variant names (case-insensitive) used by internal
            // testing to enable specific features explicitly.
            let mut feature_changes: Vec<PartitionFeatureChange> =
                if let Ok(raw) = std::env::var("RESTATE_INTERNAL_STATE_MACHINE_FEATURES") {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .filter_map(|name| match PartitionFeatureChange::from_str(name) {
                            Ok(change) => Some(change),
                            Err(_) => {
                                warn!(
                                    "Ignoring unknown state-machine feature \
                                     '{name}' from RESTATE_INTERNAL_STATE_MACHINE_FEATURES"
                                );
                                None
                            }
                        })
                        .filter(|change| !processor.fsm().features().has_feature(*change))
                        .collect()
                } else {
                    Vec::new()
                };

            // Since v1.7.0 we enable by default writing to the journal v2.
            if !processor.fsm().features().use_journal_v2_as_default() {
                feature_changes.push(PartitionFeatureChange::EnableJournalV2);
            }

            // Opt this partition in to vqueues if the operator has flipped the experimental config
            // flag on and the FSM hasn't already recorded the opt-in. The FSM update itself
            // happens via `OnVersionBarrierCommand` once this proposed barrier is applied; we do
            // not touch the local FSM mirror here.
            if config.common.experimental.is_vqueues_enabled()
                && !processor.fsm().features().is_vqueues_enabled()
            {
                feature_changes.push(PartitionFeatureChange::EnableVqueues);
            }

            // Persist a unique random seed on new invocations. Needs to be opted-in because
            // it was only introduced with v1.7.0
            if config.common.experimental.is_unique_random_seeds_enabled()
                && !processor.fsm().features().is_unique_random_seeds_enabled()
            {
                feature_changes.push(PartitionFeatureChange::EnableUniqueRandomSeeds);
            }

            if !feature_changes.is_empty() {
                // Smallest version that supports every listed feature, but never below
                // the partition's current min_restate_version.
                let barrier_version = feature_changes
                    .iter()
                    .map(|c| c.min_required_version())
                    .max()
                    .expect("non-empty")
                    .max(processor.fsm().min_restate_version())
                    .clone();

                debug!(
                    "Proposing VersionBarrier command to enable state-machine features {}",
                    feature_changes
                        .iter()
                        .map(|c| {
                            let name: &'static str = c.into();
                            name
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                self_proposer
                    .self_propose(
                        processor.key_range().start(),
                        Command::VersionBarrier(VersionBarrierCommand {
                            version: barrier_version,
                            partition_key_range: Keys::RangeInclusive(processor.key_range().into()),
                            human_reason: Some("Apply state-machine feature changes".to_owned()),
                            feature_changes: feature_changes.into_iter().map(|c| c.id()).collect(),
                        }),
                    )
                    .await?;

                // Switch to BecomingLeader state until we finish any pending tasks to enable the
                // new features. We will transition us to an effective leader when the state
                // machine applies the VersionBarrier command and perform any necessary migrations.
                //
                // This will happen by calling become_leader() again.
                //
                // Note that between us self proposing and transitioning to a full leader, we may
                // get preempted by a newer leader. Therefore, we don't perform any migration until
                // we actually observe the VersionBarrier command back from the log. But we also
                // don't expect any other commands other than AnnounceLeader (from preemptions) or
                // VersionBarrier (our own self-proposal) to be next in the log.
                debug!(
                    "Transitioning from {prev_mode} -> {}. Spent {} in {prev_mode} mode.",
                    DetailedRunMode::BecomingLeader,
                    at.elapsed().friendly(),
                );

                self.state = State::BecomingLeader {
                    at: Instant::now(),
                    leader_epoch: *leader_epoch,
                    self_proposer: Some(self_proposer),
                };

                return Ok(());
            }

            let schema = Metadata::with_current(|m| m.updateable_schema());

            let (invoker_tx, invoker_rx) = mpsc::channel(config.worker.internal_queue_length());
            let invoker_rx = ReceiverStream::new(invoker_rx);

            let invoker: InvokerService<
                InvokerStorageReader<PartitionStore>,
                EntryEnricher<Schema, ProtobufRawEntryCodec>,
                Schema,
            > = InvokerService::from_options(
                processor.partition_id(),
                processor.key_range(),
                InvokerStorageReader::new(partition_store.clone()),
                invoker_tx,
                &config.worker.invoker.service_client,
                &config.worker.invoker,
                EntryEnricher::new(schema.clone()),
                schema,
                node_ctx.invoker_capacity.invocation_token_bucket.clone(),
                node_ctx.invoker_capacity.action_token_bucket.clone(),
                node_ctx.invoker_capacity.memory_pool.clone(),
            )?;

            let mut invoker_handle = invoker.handle();

            // Register the direct invoker-status handle so DataFusion reads bypass
            // the partition processor's main select! loop. The guard is moved into
            // the invoker task's future below, binding the entry's lifetime to the
            // invoker task: cancel or panic drops the future, drops the guard, and
            // removes the entry.
            let invoker_status_guard = node_ctx
                .leader_handles_registry
                .register_invoker_status(processor.key_range(), invoker.status_reader());

            // Register the leader-query channel separately so scheduler status (and
            // future user-limit counters) can be routed through the partition
            // processor's select! while we're leader. The guard is stored in
            // LeaderState so it drops exactly when we step down — independent of
            // the invoker task's lifetime. When the scheduler becomes its own task,
            // it will register its own status handle and own its own guard.
            let leader_query_guard = node_ctx
                .leader_handles_registry
                .register_leader_query(processor.key_range(), self.leader_query_tx.clone());

            let invoker_name = Arc::from(format!("invoker-{}", processor.partition_id()));
            let invoker_config = Configuration::live().map(|c| &c.worker.invoker);
            let invoker_task_guard =
                TaskCenter::spawn_unmanaged(TaskKind::SystemService, invoker_name, async move {
                    let _invoker_status_guard = invoker_status_guard;
                    invoker.run(invoker_config).await
                })?
                .into_guard();

            let scheduler_service = SchedulerService::create(
                ResourceManager::create(
                    partition_store.partition_db().clone(),
                    node_ctx.invoker_capacity.concurrency.clone(),
                    node_ctx.invoker_capacity.invocation_token_bucket.clone(),
                    node_ctx.invoker_capacity.memory_pool.clone(),
                    node_ctx.invoker_capacity.initial_invocation_memory,
                )
                .await?,
                partition_store.partition_db().clone(),
                processor.vqueues_mut(),
            )
            .await?;

            // Seed the scheduler's UserLimiter with whatever rules
            // have already been applied to this partition.
            let initial_diff = processor.fsm().rule_book().diff_from_empty();
            if !initial_diff.is_empty() {
                scheduler_service.on_rules_updated(initial_diff);
            }

            let (fencing_tokens, next_fencing_token) =
                Self::resume_invoked_invocations(&mut invoker_handle, partition_store).await?;

            let timer_service = TimerService::new(
                TokioClock,
                config.worker.num_timers_in_memory_limit(),
                TimerReader::from(partition_store.clone()),
            );

            let (shuffle_tx, shuffle_rx) = mpsc::channel(config.worker.internal_queue_length());

            let shuffle = Shuffle::new(
                ShuffleMetadata::new(processor.partition_id(), *leader_epoch),
                OutboxReader::from(partition_store.clone()),
                shuffle_tx,
                config.worker.internal_queue_length(),
                self.ingestion_client.clone(),
            );

            let shuffle_hint_tx = shuffle.create_hint_sender();

            let shuffle_task_handle =
                TaskCenter::spawn_unmanaged(TaskKind::Shuffle, "shuffle", shuffle.run())?;

            let cleaner = Cleaner::new(
                partition_store.clone(),
                processor.partition_id(),
                config.worker.cleanup_interval(),
            );

            let cleaner_handle = cleaner.start()?;

            let trimmer_task_id = LogTrimmer::spawn(
                node_ctx.bifrost.clone(),
                processor.log_id(),
                processor.trim_queue().clone(),
            )?;

            let last_reported_durable_lsn =
                processor.fsm().durable_point().map(|d| d.durable_point);

            let durability_tracker = DurabilityTracker::new(
                processor.partition_id(),
                last_reported_durable_lsn,
                node_ctx.replica_set_states.clone(),
                partition_store.partition_db().watch_archived_lsn(),
                Duration::from_secs(5).add_jitter(0.5),
            );

            debug!(
                "Transitioning from {prev_mode} -> {}. Spent {} in {prev_mode} mode.",
                DetailedRunMode::Leader,
                at.elapsed().friendly(),
            );

            self.state = State::Leader(Box::new(LeaderState::new(
                processor.partition_id(),
                *leader_epoch,
                processor.key_range(),
                shuffle_task_handle,
                cleaner_handle,
                trimmer_task_id,
                shuffle_hint_tx,
                timer_service,
                scheduler_service,
                invoker_handle,
                invoker_task_guard.into_handle(),
                self_proposer,
                invoker_rx,
                fencing_tokens,
                next_fencing_token,
                shuffle_rx,
                durability_tracker,
                leader_query_guard,
                node_ctx.rule_book_cache.subscribe(),
            )));

            Ok(())
        } else {
            unreachable!("Can only become the leader if I was the candidate before!");
        }
    }

    async fn resume_invoked_invocations(
        invoker_handle: &mut InvokerChannelServiceHandle,
        partition_store: &mut PartitionStore,
    ) -> Result<(HashMap<InvocationId, FencingToken>, FencingToken), Error> {
        // todo(asoli): If we are asked to migrate to vqueues (or vqueues are enabled).
        // we must migrate all invoked invocations here (through a wal command).
        // (blocker to v1.7.0)

        let mut invoked_invocations = std::pin::pin!(
            partition_store
                .scan_legacy_invoked_invocations()
                .map_err(Error::Storage)?
        );

        let start = tokio::time::Instant::now();
        // Seed a fresh fencing token per resumed invocation so the leader accepts its effects and
        // can later fence stragglers from a re-invoke. On a fresh term there are no in-flight
        // stragglers yet, so the starting tokens only need to be distinct from the ones
        // `LeaderState::mint_fencing_token` will mint later, which continues from
        // `next_fencing_token`.
        let mut fencing_tokens = HashMap::default();
        let mut next_fencing_token: FencingToken = 0;
        while let Some(invoked_invocation) = invoked_invocations.next().await {
            let InvokedInvocationStatusLite {
                invocation_id,
                invocation_target,
            } = invoked_invocation?;
            let fencing_token = next_fencing_token;
            next_fencing_token = next_fencing_token.wrapping_add(1);
            fencing_tokens.insert(invocation_id, fencing_token);
            invoker_handle
                .invoke(invocation_id, fencing_token, invocation_target)
                .map_err(Error::Invoker)?;
        }
        debug!(
            "Leader partition resumed {} invocations in {:?}",
            fencing_tokens.len(),
            start.elapsed(),
        );

        Ok((fencing_tokens, next_fencing_token))
    }

    async fn become_follower(&mut self) {
        let old_state = mem::replace(&mut self.state, State::Follower);

        match old_state {
            State::Follower | State::Candidate { .. } | State::BecomingLeader { .. } => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                // Registry entries are unregistered via RAII: the invoker-status
                // entry drops with the invoker task's future (after stop cancels it),
                // and the leader-query entry drops with LeaderState (via its
                // `_leader_query_guard` field).
                leader_state.stop().await;
            }
        }
    }

    pub fn handle_actions(
        &mut self,
        processor: impl Processor + HasVQueues,
        actions: impl Iterator<Item = Action>,
    ) -> Result<(), Error> {
        match &mut self.state {
            State::Follower | State::Candidate { .. } | State::BecomingLeader { .. } => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                leader_state.handle_actions(processor, actions)?;
            }
        }

        Ok(())
    }

    /// Runs the leadership state tasks. This depends on the current state value:
    ///
    /// * Follower: Nothing to do
    /// * Candidate: Monitor appender task
    /// * Leader: Await action effects and monitor appender task
    pub async fn run(
        &mut self,
        ctx: impl Processor + HasVQueues,
    ) -> Result<Vec<ActionEffect>, Error> {
        match &mut self.state {
            State::Follower => Ok(futures::future::pending::<Vec<_>>().await),
            State::Candidate { self_proposer, .. }
            | State::BecomingLeader { self_proposer, .. } => Err(self_proposer
                .as_mut()
                .expect("must be present")
                .join_on_err()
                .await
                .expect_err("never should never be returned")),
            State::Leader(leader_state) => leader_state.run(ctx).await,
        }
    }

    pub async fn handle_action_effects(
        &mut self,
        action_effects: impl IntoIterator<Item = ActionEffect>,
    ) -> Result<(), Error> {
        match &mut self.state {
            State::Follower | State::Candidate { .. } | State::BecomingLeader { .. } => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                leader_state.handle_action_effects(action_effects).await?
            }
        }

        Ok(())
    }

    // This is returned only if we're leaders (otherwise there's no messages to be sent to the invoker)
    pub fn invoker_handle(&mut self) -> Option<&mut InvokerChannelServiceHandle> {
        match &mut self.state {
            State::Leader(leader_state) => Some(leader_state.invoker_handle()),
            _ => None,
        }
    }
}

impl<T> LeadershipState<T> {
    pub fn handle_leader_query(
        &self,
        metas: VQueuesMeta<'_>,
        leader_query_cmd: LeaderQueryCommand,
    ) {
        let (request, response_tx) = leader_query_cmd.into_inner();

        match (&self.state, request) {
            (State::Leader(leader_state), LeaderQueryRequest::SchedulerStatus { keys }) => {
                let _ = response_tx.send(LeaderQueryResponse::SchedulerStatus(
                    leader_state.read_scheduler_status(metas, keys),
                ));
            }
            (State::Leader(leader_state), LeaderQueryRequest::UserLimitCounters { keys }) => {
                let _ = response_tx.send(LeaderQueryResponse::UserLimitCounters(
                    leader_state.read_user_limit_counters(keys),
                ));
            }
            (_, request) => {
                let _ = response_tx.send(LeaderQueryResponse::NotLeader(request.kind()));
            }
        }
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
        match &mut self.state {
            State::Follower | State::Candidate { .. } | State::BecomingLeader { .. } => {
                // Just fail the rpc
                reciprocal.send(Err(PartitionProcessorRpcError::NotLeader(
                    self.partition_id,
                )))
            }
            State::Leader(leader_state) => {
                leader_state
                    .handle_rpc_proposal_command(request_id, reciprocal, partition_key, cmd)
                    .await;
            }
        }
    }

    pub async fn propose_pause_and_fence(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        reciprocal: Reciprocal<
            Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        >,
        invocation_id: InvocationId,
        cmd: Command,
    ) {
        match &mut self.state {
            State::Follower | State::Candidate { .. } | State::BecomingLeader { .. } => {
                // Just fail the rpc
                reciprocal.send(Err(PartitionProcessorRpcError::NotLeader(
                    self.partition_id,
                )))
            }
            State::Leader(leader_state) => {
                leader_state
                    .propose_pause_and_fence(request_id, reciprocal, invocation_id, cmd)
                    .await;
            }
        }
    }

    /// Append a command to Bifrost without dedup information, responding on Bifrost commit.
    pub async fn append_and_respond_asynchronously(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        reciprocal: Reciprocal<
            Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        >,
        success_response: PartitionProcessorRpcResponse,
    ) {
        match &mut self.state {
            State::Follower | State::Candidate { .. } | State::BecomingLeader { .. } => reciprocal
                .send(Err(PartitionProcessorRpcError::NotLeader(
                    self.partition_id,
                ))),
            State::Leader(leader_state) => {
                leader_state
                    .append_and_respond_asynchronously(
                        partition_key,
                        cmd,
                        reciprocal,
                        success_response,
                    )
                    .await;
            }
        }
    }

    /// Forward externally-created records to this partition.
    pub async fn forward_many_with_callback<F>(
        &mut self,
        records: impl ExactSizeIterator<Item = IngestRecord>,
        callback: F,
    ) where
        F: FnOnce(Result<(), PartitionProcessorRpcError>) + Send + Sync + 'static,
    {
        match &mut self.state {
            State::Follower | State::Candidate { .. } | State::BecomingLeader { .. } => callback(
                Err(PartitionProcessorRpcError::NotLeader(self.partition_id)),
            ),
            State::Leader(leader_state) => {
                leader_state
                    .forward_many_with_callback(records, callback)
                    .await;
            }
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
            .expect("timers should be read from storage successfully")
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use test_log::test;
    use tokio_stream::StreamExt;

    use assert2::let_assert;
    use restate_bifrost::Bifrost;
    use restate_core::partitions::PartitionRouting;
    use restate_core::{TaskCenter, TestCoreEnv};
    use restate_ingestion_client::{IngestionClient, SessionOptions};
    use restate_partition_store::PartitionStoreManager;
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::Configuration;
    use restate_types::identifiers::{LeaderEpoch, PartitionId};
    use restate_types::logs::{KeyFilter, Lsn, SequenceNumber};
    use restate_types::partitions::state::PartitionReplicaSetStates;
    use restate_types::partitions::{
        Partition, PartitionConfiguration, PartitionFeatureChange, PersistedFeatures,
    };
    use restate_types::sharding::KeyRange;
    use restate_types::{GenerationalNodeId, Version};
    use restate_wal_protocol::Command;
    use restate_wal_protocol::Envelope;
    use restate_worker_api::invoker::capacity::InvokerCapacity;

    use crate::partition::leadership::{LeadershipState, State};
    use crate::partition::processor::ProcessorRawContext;
    use crate::partition::{LeadershipInfo, NodeContext};
    use crate::partition_processor_manager::PartitionLeaderHandlesRegistry;
    use crate::rule_book_cache::RuleBookCacheHandle;

    const PARTITION_ID: PartitionId = PartitionId::MIN;
    const NODE_ID: GenerationalNodeId = GenerationalNodeId::new(0, 0);
    const PARTITION_KEY_RANGE: KeyRange = KeyRange::FULL;
    const PARTITION: Partition = Partition::new(PARTITION_ID, PARTITION_KEY_RANGE);

    #[test(restate_core::test)]
    async fn become_leader_then_step_down() -> googletest::Result<()> {
        let env = TestCoreEnv::create_with_single_node(0, 0).await;

        RocksDbManager::init();
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;
        let replica_set_states = PartitionReplicaSetStates::default();

        let partition_store_manager = PartitionStoreManager::create(true).await?;

        let ingress = IngestionClient::new(
            env.networking.clone(),
            env.metadata.updateable_partition_table(),
            PartitionRouting::new(replica_set_states.clone(), TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(),
            SessionOptions::default(),
        );

        // The per-partition state that used to be passed to `LeadershipState` and
        // its methods now lives in `NodeContext` (node-wide handles) and the
        // `Processor` context (per-partition caches).
        let mut node_ctx = NodeContext::new(
            NODE_ID,
            Configuration::live(),
            replica_set_states.clone(),
            RuleBookCacheHandle::detached(),
            bifrost.clone(),
            InvokerCapacity::new_unlimited(),
            PartitionLeaderHandlesRegistry::default(),
        );

        let mut partition_store = partition_store_manager.open(&PARTITION, None).await?;
        // Drive leadership through an in-memory processor context; `become_leader`
        // still uses the real `partition_store` for the invoker/scheduler/cleaner.
        let mut ctx = ProcessorRawContext::new(Arc::new(PARTITION), PersistedFeatures::default());

        let (leader_query_tx, _leader_query_rx) = restate_worker_api::channel();
        let mut state = LeadershipState::new(PARTITION_ID, ingress, leader_query_tx);

        assert!(matches!(state.state, State::Follower));

        let leader_epoch = LeaderEpoch::from(1);
        let current_config = PartitionConfiguration::default();
        let leadership_info = LeadershipInfo {
            version: Version::MIN,
            leader_epoch,
            current_config: current_config.into(),
            next_config: None,
        };
        state
            .run_for_leader(&mut ctx, &node_ctx, Box::new(leadership_info))
            .await?;

        assert!(matches!(state.state, State::Candidate { .. }));

        let mut reader = bifrost
            .create_reader(PARTITION_ID.into(), KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)
            .expect("valid reader");

        let record = reader.next().await.unwrap()?;
        let envelope = record.try_decode::<Envelope>().unwrap()?;

        let_assert!(Command::AnnounceLeader(announce_leader) = envelope.command);
        assert_eq!(announce_leader.node_id, NODE_ID);
        assert_eq!(announce_leader.leader_epoch, leader_epoch);
        assert_eq!(announce_leader.partition_key_range, PARTITION_KEY_RANGE);
        assert!(announce_leader.current_config.is_some());
        assert!(announce_leader.next_config.is_none());

        state
            .on_announce_leader(
                &mut node_ctx,
                &mut ctx,
                &mut partition_store,
                announce_leader.leader_epoch,
            )
            .await?;

        // Since v1.7.0, winning the campaign first proposes a VersionBarrier to enable
        // the journal-v2 default; the processor stays `BecomingLeader` until that barrier
        // is applied.
        assert!(matches!(state.state, State::BecomingLeader { .. }));

        let record = reader.next().await.unwrap()?;
        let envelope = record.try_decode::<Envelope>().unwrap()?;
        let_assert!(Command::VersionBarrier(barrier) = envelope.command);
        assert!(
            barrier
                .feature_changes
                .contains(&PartitionFeatureChange::EnableJournalV2.id())
        );

        // Simulate the barrier being applied to the FSM, then complete the transition
        // into a full leader (no further feature changes remain to be proposed).
        ctx.set_enabled_features_in_memory(PersistedFeatures {
            journal_v2: true,
            ..PersistedFeatures::default()
        });
        state
            .finish_becoming_leader(&mut node_ctx, &mut ctx, &mut partition_store)
            .await?;

        assert!(matches!(state.state, State::Leader(_)));

        state.step_down().await;

        assert!(matches!(state.state, State::Follower));

        TaskCenter::current()
            .shutdown_node("test_completed", 0)
            .await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
