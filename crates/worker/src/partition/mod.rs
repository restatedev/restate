// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// When expose-internals makes this module `pub`, some internal types referenced
// by `PartitionProcessor` and `ProcessorError` become visible but remain
// `pub(crate)`. This is expected -- benchmarks only use `state_machine`.
#![cfg_attr(
    feature = "expose-internals",
    allow(private_interfaces, private_bounds)
)]

mod cleaner;
pub mod invoker_storage_reader;
mod leadership;
pub mod node;
mod processor;
mod rpc;
pub mod shuffle;
#[cfg(feature = "expose-internals")]
pub mod state_machine;
#[cfg(not(feature = "expose-internals"))]
mod state_machine;
pub mod types;

pub use self::node::NodeContext;
// Re-exported so external drivers (e.g. pp-bench) can build a context to drive `StateMachine::apply`.
#[cfg(feature = "expose-internals")]
pub use self::processor::ProcessorRawContext;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::{FutureExt, StreamExt};
use metrics::histogram;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, error, instrument, trace, warn};

use restate_bifrost::loglet::FindTailOptions;
use restate_bifrost::{DataRecord, DataRecordError, LogEntry};
use restate_core::network::{
    Incoming, Oneshot, Reciprocal, Rpc, ServiceMessage, ServiceStream, TransportConnect, Verdict,
};
use restate_core::{Metadata, ShutdownError, TaskCenter, TaskKind, cancellation_token};
use restate_ingestion_client::IngestionClient;
use restate_partition_store::{
    MigrationError, PartitionDb, PartitionStore, PartitionStoreTransaction,
};
use restate_platform::memory::EstimatedMemorySize;
use restate_storage_api::deduplication_table::{
    DedupSequenceNumber, ProducerId, ReadDeduplicationTable,
};
use restate_storage_api::{StorageError, Transaction};
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::LeaderEpoch;
use restate_types::logs::{self, Lsn, RecordDecodeError, SequenceNumber};
use restate_types::net::ingest::{
    DedupSequenceNrQueryRequest, DedupSequenceNrQueryResponse, ReceivedIngestRequest,
    ResponseStatus,
};
use restate_types::net::partition_processor::{
    PartitionLeaderService, PartitionProcessorRpcError, PartitionProcessorRpcRequest,
    PartitionProcessorRpcResponse,
};
use restate_types::net::{RpcRequest, ingest};
use restate_types::partitions::PartitionFeatureChange;
use restate_types::retries::RetryPolicy;
use restate_types::schema::Schema;
use restate_types::storage::{
    PolyBytes, StorageCodec, StorageDecode, StorageDecodeError, StorageEncode,
};
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, SemanticRestateVersion, Version};
use restate_util_time::DurationExt;
use restate_vqueues::context::HasVQueues;
use restate_wal_protocol::control::{CurrentReplicaSetConfiguration, NextReplicaSetConfiguration};
use restate_wal_protocol::v2::CommandScope;
use restate_wal_protocol::{Envelope, v2};
use restate_worker_api::{LeaderQueryCommand, LeaderQueryReceiver};

use self::processor::commands::{
    AnnounceLeaderContext, ApplyPartitionCommand, NextStep, TruncateOutboxContext,
    UpdateDurabilityContext, UpsertRuleBookContext, UpsertSchemaContext, VersionBarrierContext,
};
use self::processor::*;
use self::state_machine::StateMachine;
use crate::metric_definitions::{
    LEADER_LABEL, LEADER_LABEL_LEADER, PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS,
};
use crate::partition::leadership::LeadershipState;
use crate::partition::processor::leadership::LeadershipContext;
use crate::partition::state_machine::ActionCollector;

/// Information needed to run as leader, including the epoch and partition configurations.
#[derive(Clone, Debug)]
pub struct LeadershipInfo {
    pub version: Version,
    pub leader_epoch: LeaderEpoch,
    pub current_config: CurrentReplicaSetConfiguration,
    pub next_config: Option<NextReplicaSetConfiguration>,
}

impl From<EpochMetadata> for LeadershipInfo {
    fn from(value: EpochMetadata) -> Self {
        let (version, leader_epoch, current, next, _) = value.into_inner();

        Self {
            version,
            leader_epoch,
            current_config: current.into(),
            next_config: next.map(|c| c.into()),
        }
    }
}

/// Target leader state of the partition processor.
#[derive(Clone, Debug, Default)]
pub enum TargetLeaderState {
    Leader(Box<LeadershipInfo>),
    #[default]
    Follower,
}

pub(super) struct PartitionProcessorBuilder {
    target_leader_state_rx: watch::Receiver<TargetLeaderState>,
    network_svc_rx: ServiceStream<PartitionLeaderService>,
    status_watch_tx: watch::Sender<PartitionProcessorStatus>,
    node_ctx: NodeContext,
}

impl PartitionProcessorBuilder {
    pub(super) fn new(
        target_leader_state_rx: watch::Receiver<TargetLeaderState>,
        network_svc_rx: ServiceStream<PartitionLeaderService>,
        status_watch_tx: watch::Sender<PartitionProcessorStatus>,
        node_ctx: NodeContext,
    ) -> Self {
        Self {
            target_leader_state_rx,
            network_svc_rx,
            status_watch_tx,
            node_ctx,
        }
    }

    pub async fn build<T>(
        self,
        ingestion_client: IngestionClient<T, Envelope>,
        partition_db: PartitionDb,
    ) -> Result<PartitionProcessor<T>, ProcessorError>
    where
        T: TransportConnect,
    {
        let PartitionProcessorBuilder {
            target_leader_state_rx,
            network_svc_rx: rpc_rx,
            status_watch_tx,
            node_ctx,
        } = self;

        let mut partition_store = PartitionStore::from(partition_db);

        let ctx =
            ProcessorRawContext::create(SemanticRestateVersion::current(), &mut partition_store)
                .await?;

        // Seed the cache with whatever we just loaded from the FSM
        // table, so a freshly-restarted PP doesn't briefly serve the
        // empty default to subscribers between PP boot and the first
        // metadata-store poll.
        node_ctx
            .rule_book_cache
            .notify_observed(ctx.fsm().rule_book());

        node_ctx.replica_set_states.note_observed_leader(
            ctx.partition_id(),
            restate_types::partitions::state::LeadershipState {
                current_leader_epoch: ctx.current_leader_epoch(),
                // if we don't know the old leader node-id, another node will announce it
                current_leader: GenerationalNodeId::INVALID,
            },
        );

        let (leader_query_tx, leader_query_rx) = restate_worker_api::channel();

        let leadership_state =
            LeadershipState::new(ctx.partition_id(), ingestion_client, leader_query_tx);

        Ok(PartitionProcessor {
            partition_store,
            ctx,
            node_ctx,
            leadership_state,
            target_leader_state_rx,
            network_leader_svc_rx: rpc_rx,
            status_watch_tx,
            leader_query_rx,
        })
    }
}

pub struct PartitionProcessor<T> {
    partition_store: PartitionStore,
    ctx: ProcessorRawContext,
    node_ctx: NodeContext,
    leadership_state: LeadershipState<T>,
    target_leader_state_rx: watch::Receiver<TargetLeaderState>,
    network_leader_svc_rx: ServiceStream<PartitionLeaderService>,
    status_watch_tx: watch::Sender<PartitionProcessorStatus>,
    leader_query_rx: LeaderQueryReceiver,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessorError {
    /// Indicates that the processor encountered a trim gap in the log.
    /// This is a signal to the PartitionProcessorManager to attempt to restart
    /// the processor for this partition. This might occur after the first startup
    /// of a worker that's been down while a log trim occurred, and recoverable
    /// as long as we can find a snapshot with a min LSN of trim_gap_end or later.
    #[error("[{read_pointer}..{trim_gap_end}]")]
    TrimGapEncountered {
        read_pointer: Lsn,
        trim_gap_end: Lsn,
    },
    #[error("[{read_pointer}..{data_loss_gap_end}]")]
    DataLossGapEncountered {
        read_pointer: Lsn,
        data_loss_gap_end: Lsn,
    },
    #[error(
        "partition appears to be ahead of the log, \
    this indicates data-loss in the log or that partition mismatches its backing log. partition_applied_lsn: {partition_applied_lsn}, log_tail_lsn: {log_tail_lsn}"
    )]
    PartitionAheadOfLog {
        partition_applied_lsn: Lsn,
        log_tail_lsn: Lsn,
    },
    #[error(
        "partition is blocked; requires an upgrade to restate-server version \
        {required_min_version} or higher; reason='{barrier_reason}'; feature changes={feature_changes:?}"
    )]
    VersionBarrier {
        required_min_version: SemanticRestateVersion,
        barrier_reason: String,
        feature_changes: Vec<u16>,
    },
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Decode(#[from] StorageDecodeError),
    #[error(transparent)]
    Bifrost(#[from] restate_bifrost::Error),
    #[error(transparent)]
    StoreOpen(#[from] restate_partition_store::OpenError),
    #[error(transparent)]
    StateMachine(#[from] state_machine::Error),
    /// *Since v1.7.0*
    #[error(
        "partition is blocked; restate-server does not recognize feature change IDs {unknown_ids:?}; reason='{barrier_reason}'"
    )]
    UnknownFeatureFlags {
        unknown_ids: Vec<u16>,
        required_min_version: SemanticRestateVersion,
        barrier_reason: String,
    },
    /// *Since v1.7.0*
    #[allow(unused)]
    #[error(
        "partition is blocked; pre-existing in-flight data must be migrated before applying \
         feature changes {features:?}; consult the Restate documentation for the server version \
         that supports this migration"
    )]
    MigrationRequired {
        features: Vec<PartitionFeatureChange>,
    },
    #[error(
        "partition is blocked; restate-server cannot perform data migration. reason='{reason}'"
    )]
    MigrationBarrier { reason: String },
    #[error(transparent)]
    ActionEffect(#[from] leadership::Error),
    #[error(transparent)]
    ShutdownError(#[from] ShutdownError),
    #[error("log read stream has terminated")]
    LogReadStreamTerminated,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// OrderedOperations are scheduled operations that
/// will only get executed once the partition read up to
/// the bifrost tail that was found once the operation
/// was submitted.
enum OrderedOp {
    QueryLegacyDedupSn {
        request: Incoming<Rpc<DedupSequenceNrQueryRequest>>,
    },
}

impl<T> PartitionProcessor<T>
where
    T: TransportConnect,
{
    #[instrument(
        level = "error", skip_all,
        fields(partition_id = %self.ctx.partition_id())
    )]
    pub async fn run(mut self) -> Result<(), ProcessorError> {
        debug!("Starting the partition processor.");

        let cancel = cancellation_token();
        // Run migrations first.
        //
        // Important to note: This only runs the migration for the given partition store. In a setup,
        // where not every node runs every partition, it can happen that partition data remains
        // untouched when going from one version to the next.
        // todo https://github.com/restatedev/restate/issues/4175.
        //
        // This will fail with StorageError::MigrationCancelled if the current task was cancelled
        // during the migration phase.
        match self
            .partition_store
            .verify_and_run_migrations(cancel.clone(), self.node_ctx.config.live_load())
            .await
        {
            Ok(()) => {}
            Err(MigrationError::MigrationCancelled) => {
                debug!(
                    "Shutting partition processor during data migration because it was cancelled."
                );
                return Ok(());
            }
            Err(MigrationError::MigrationBarrier(reason)) => {
                return Err(ProcessorError::MigrationBarrier { reason });
            }
            Err(MigrationError::StorageError(err)) => {
                warn!(
                    "Shutting partition processor during data migration down because of error: {err}"
                );
                return Err(err.into());
            }
        }

        // Note: Boxing because it's a rather large future (>35KiB)
        let res = match cancel.run_until_cancelled(Box::pin(self.run_inner())).await {
            Some(res) => res,
            None => {
                debug!("Shutting partition processor down because it was cancelled.");
                Ok(())
            }
        };

        // clean up pending rpcs and stop child tasks
        self.leadership_state.step_down().await;

        // Drain leader network service
        self.network_leader_svc_rx.close();
        while let Some(msg) = self.network_leader_svc_rx.next().await {
            // signals that we are not the leader anymore
            msg.fail(Verdict::SortCodeNotFound);
        }

        res
    }

    /// Decode record tries to decode the record first as v2 Envelope, if it failed,
    /// it decodes as v1 Envelope then converts into v2.
    fn decode_record(
        record: DataRecord<PolyBytes>,
    ) -> Result<DataRecord<v2::Envelope<v2::Raw>>, StorageDecodeError> {
        fn decode_payload<T: StorageDecode + StorageEncode + Clone>(
            payload: PolyBytes,
        ) -> Result<T, RecordDecodeError> {
            match payload {
                PolyBytes::Bytes(slice) => {
                    let mut buf = std::io::Cursor::new(slice);
                    Ok(StorageCodec::decode(&mut buf)?)
                }
                PolyBytes::Typed(value) | PolyBytes::Both(value, _) => {
                    let cached = value
                        .downcast_arc()
                        .map_err(RecordDecodeError::TypedValueMismatch)?;
                    Ok(Arc::unwrap_or_clone(cached))
                }
            }
        }

        record.try_map(|payload| {
           match decode_payload::<v2::Envelope<v2::Raw>>(payload) {
                Ok(envelope) => Ok(envelope),
            Err(RecordDecodeError::TypedValueMismatch(v1_envelope)) => {
                let v1_envelope: Arc<Envelope> = v1_envelope
                    .downcast_arc()
                    .map_err(|_| StorageDecodeError::DecodeValue("Type mismatch. Record value in PolyBytes::Typed does not match requested type".into()))?;

                let v1_envelope = Arc::unwrap_or_clone(v1_envelope);

                let envelope: v2::Envelope<v2::Raw> = v1_envelope
                    .try_into()
                    .map_err(|err: anyhow::Error| StorageDecodeError::DecodeValue(err.into()))?;
                Ok(envelope)
            }
            Err(RecordDecodeError::StorageDecodeError(e)) => Err(e),
           }
        })
    }

    async fn run_inner(&mut self) -> Result<(), ProcessorError> {
        let last_applied_lsn_watch = self.ctx.subscribe_to_last_applied_lsn();

        let log_id = self.ctx.log_id();
        let partition_id = self.ctx.partition_id();
        let my_node = self.node_ctx.my_node_id().as_plain();

        let mut durable_lsn_watch = self.partition_store.get_durable_lsn().await?;
        let durable_lsn = durable_lsn_watch
            .borrow_and_update()
            .unwrap_or(Lsn::INVALID);

        self.node_ctx
            .replica_set_states
            .note_durable_lsn(partition_id, my_node, durable_lsn);

        // If the underlying log is not provisioned, now is the time to provision it.
        // We'll retry a few times before giving back control to PPM
        //
        // The primary reason for retries is the initial cluster provision case where nodes might
        // still be starting up and we don't have enough nodes to form legal nodesets.
        let mut retries = RetryPolicy::exponential(
            Duration::from_secs(1),
            1.5,
            Some(3),
            Some(Duration::from_secs(5)),
        )
        .into_iter();
        while let Err(e) = self
            .node_ctx
            .bifrost
            .admin()
            .ensure_log_exists(log_id)
            .await
        {
            // We cannot provision the log for this partition
            if let Some(dur) = retries.next() {
                debug!(
                    "Cannot create a bifrost log for partition {}, will retry in {:?}; reason={}",
                    partition_id, dur, e
                );
                tokio::time::sleep(dur).await;
            } else {
                return Err(e.into());
            }
        }

        debug!("Finding tail for partition",);
        // propagate errors and let the PPM handle error retries
        let current_tail = self
            .node_ctx
            .bifrost
            .find_tail(log_id, FindTailOptions::ConsistentRead)
            .await?;

        // If our `last_applied_lsn` is at or beyond the tail, this is a strong indicator
        // that the log has reverted backwards.
        if self.ctx.fsm().last_applied_lsn() >= current_tail.offset() {
            return Err(ProcessorError::PartitionAheadOfLog {
                partition_applied_lsn: self.ctx.fsm().last_applied_lsn(),
                log_tail_lsn: current_tail.offset(),
            });
        }

        self.ctx.status_mut().set_started_at(Instant::now());
        self.ctx.set_catchup_lsn(current_tail.offset());
        debug!(
            last_applied_lsn = %self.ctx.fsm().last_applied_lsn(),
            current_log_tail = %current_tail,
            "Partition creating log reader",
        );

        let mut live_schemas = Metadata::with_current(|m| m.updateable_schema());

        // Telemetry setup
        // Note: we didn't remove the leader label to avoid breaking existing dashboards. This can
        // be removed in the future if deemed necessary.
        let leader_record_write_to_read_latency = histogram!(PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS, LEADER_LABEL => LEADER_LABEL_LEADER);

        // Start reading after the last applied lsn
        let mut record_stream = std::pin::pin!(
            self.node_ctx
                .bifrost
                .create_reader(
                    log_id,
                    logs::KeyFilter::Within(self.ctx.key_range().into()),
                    self.ctx.fsm().last_applied_lsn().next(),
                    Lsn::MAX,
                )?
                .peekable()
        );

        // avoid synchronized timers.
        let mut status_update_timer =
            tokio::time::interval(Duration::from_millis(500).add_jitter(0.5));
        status_update_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut action_collector = ActionCollector::default();

        let mut watch_leader_changes = self
            .node_ctx
            .replica_set_states
            .watch_leadership_state(partition_id);
        watch_leader_changes.mark_changed();

        let mut cloned_partition_store = self.partition_store.clone();
        let mut txn = cloned_partition_store.transaction();

        loop {
            let config = self.node_ctx.config.live_load();
            let max_batching_size = config.worker.max_command_batch_size();
            let bytes_limit = config.worker.max_command_batch_bytes.as_usize();

            tokio::select! {
                _ = self.target_leader_state_rx.changed() => {
                    let target_leader_state = self.target_leader_state_rx.borrow_and_update().clone();
                    self.on_target_leader_state(target_leader_state).await.context("failed handling target leader state change")?;
                    self.refresh_status(&mut durable_lsn_watch)?;
                }
                Ok(()) = watch_leader_changes.changed() => {
                    // cloning to avoid holding the underlying RwLock.
                    let new_state = *watch_leader_changes.borrow_and_update();
                    self.leadership_state.maybe_step_down(&mut self.ctx, new_state.current_leader_epoch, new_state.current_leader).await;
                    self.refresh_status(&mut durable_lsn_watch)?;
                }
                Some(msg) = self.network_leader_svc_rx.next(), if self.leadership_state.should_process_rpc() => {
                    // todo: replace the live schema with the leader's consistent schema
                    self.on_rpc(msg, live_schemas.live_load(), &last_applied_lsn_watch).await;
                }
                _ = status_update_timer.tick() => {
                    // Update the status to ensure changes are observable
                    self.refresh_status(&mut durable_lsn_watch)?;
                }
                // Awaiting the first record is the only stream `.await` and is cancellation-safe:
                // if this branch is dropped before a record is ready, nothing has been consumed.
                // Subsequent records are drained synchronously below (`now_or_never`), so the
                // applied-but-uncommitted records can never be lost to select cancellation.
                maybe_first = record_stream.next() => {
                    let Some(first) = maybe_first else {
                        return Err(ProcessorError::LogReadStreamTerminated);
                    };
                    let first = first?;

                    txn.clear();
                    // clear buffers used when applying the next record
                    action_collector.clear();

                    // Apply the batch one record at a time, seeded with the record we just awaited.
                    // The first record is always applied, which guarantees forward progress even
                    // when a single record is larger than `bytes_limit`. Further records are pulled
                    // only while immediately available and within the record-count and byte limits.
                    let mut accumulated_bytes = 0;
                    let mut count = 0usize;
                    let mut next_entry = Some(first);
                    while let Some(entry) = next_entry.take() {
                        accumulated_bytes += entry.estimated_memory_size();
                        count += 1;
                        match self.apply_log_entry(
                            entry,
                            &mut txn,
                            &mut action_collector,
                            &leader_record_write_to_read_latency,
                        )
                        .await? {
                            NextStep::AdvanceLastAppliedLsn { lsn, ref dedup, scope } => {
                                self.ctx.dedup_mut().store_dedup_information(&mut txn, dedup)?;
                                self.ctx.update_last_applied_lsn(&mut txn, lsn)?;
                                if matches!(scope, CommandScope::PartitionScoped) {
                                    // Update the status to ensure changes are observable
                                    self.refresh_status(&mut durable_lsn_watch)?;
                                }
                            },
                            NextStep::SkipUntil(lsn) => {
                                self.ctx.update_last_applied_lsn(&mut txn, lsn)?;
                            }
                        }

                        if count >= max_batching_size {
                            break;
                        }

                        // Peek primes `Peekable`'s slot without consuming, so we can decide against
                        // pulling the next record before committing to it.
                        let next_size = match record_stream.as_mut().peek().now_or_never() {
                            Some(Some(Ok(peeked))) => peeked.estimated_memory_size(),
                            // Not immediately available, stream terminated, or an error: stop the
                            // batch. Termination/errors resurface on the next `next().await`.
                            _ => break,
                        };
                        if accumulated_bytes + next_size > bytes_limit {
                            // Leave the peeked record in the slot; it leads the next batch.
                            break;
                        }

                        // The slot was primed by the peek above: immediately ready and `Ok`.
                        next_entry = Some(
                            record_stream
                                .next()
                                .now_or_never()
                                .expect("peeked record is buffered")
                                .expect("stream cannot terminate after a successful peek")?,
                        );
                    }

                    // Commit our changes and notify actuators about actions if we are the leader
                    txn.commit().await?;
                    self.ctx.release_applied_lsn();
                    self.leadership_state.handle_actions(&mut self.ctx, action_collector.drain(..))?;
                },
                result = self.leadership_state.run(&mut self.ctx) => {
                    let action_effects = result?;
                    // We process the action_effects not directly in the run future because it
                    // requires the run future to be cancellation safe. In the future this could be
                    // implemented.
                    self.leadership_state.handle_action_effects(action_effects).await?;
                }
                Some(leader_query_cmd) = self.leader_query_rx.recv() => {
                    self.on_leader_query(leader_query_cmd);
                }
            }
            // Allow other tasks on this thread to run, but only if we have exhausted the coop
            // budget.
            tokio::task::consume_budget().await;
        }
    }

    async fn on_target_leader_state(
        &mut self,
        target_leader_state: TargetLeaderState,
    ) -> anyhow::Result<()> {
        match target_leader_state {
            TargetLeaderState::Leader(leadership_info) => {
                self.leadership_state
                    .run_for_leader(&mut self.ctx, &self.node_ctx, leadership_info)
                    .await
                    .context("failed handling RunForLeader command")?;
            }
            TargetLeaderState::Follower => {
                self.ctx
                    .status_mut()
                    .set_planned_run_mode(RunMode::Follower);
                self.leadership_state.step_down().await;
            }
        }

        Ok(())
    }

    fn on_leader_query(&mut self, leader_query_cmd: LeaderQueryCommand) {
        self.leadership_state
            .handle_leader_query(self.ctx.vqueues(), leader_query_cmd);
    }

    async fn on_pp_rpc_request(
        &mut self,
        response_tx: Reciprocal<
            Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        >,
        body: PartitionProcessorRpcRequest,
        schemas: &Schema,
    ) {
        let _ = rpc::RpcHandler::handle(
            rpc::RpcContext::new(
                &mut self.leadership_state,
                schemas,
                &mut self.partition_store,
            ),
            body,
            rpc::Replier::new(response_tx),
        )
        .await;
    }

    fn refresh_status(
        &mut self,
        durable_lsn_watch: &mut watch::Receiver<Option<Lsn>>,
    ) -> Result<(), ProcessorError> {
        let durable_lsn = if durable_lsn_watch
            .has_changed()
            .map_err(|e| ProcessorError::Other(e.into()))?
        {
            let durable_lsn = durable_lsn_watch
                .borrow_and_update()
                .unwrap_or(Lsn::INVALID);
            self.node_ctx.replica_set_states.note_durable_lsn(
                self.ctx.partition_id(),
                self.node_ctx.my_node_id().as_plain(),
                durable_lsn,
            );
            durable_lsn
        } else {
            durable_lsn_watch.borrow().unwrap_or(Lsn::INVALID)
        };
        self.status_watch_tx.send_modify(|old| {
            self.ctx.merge_with_status(old);
            old.durable_lsn = Some(durable_lsn);
            old.storage_version = Some(self.partition_store.storage_version());
            old.effective_mode = self.leadership_state.detailed_effective_mode().into();
            old.detailed_effective_mode = self.leadership_state.detailed_effective_mode();
            old.updated_at = MillisSinceEpoch::now();
        });

        Ok(())
    }

    async fn on_rpc(
        &mut self,
        msg: ServiceMessage<PartitionLeaderService>,
        schemas: &Schema,
        last_applied_lsn_watch: &watch::Receiver<Lsn>,
    ) {
        match msg {
            ServiceMessage::Rpc(msg) if msg.msg_type() == PartitionProcessorRpcRequest::TYPE => {
                let msg = msg.into_typed::<PartitionProcessorRpcRequest>();
                // note: split() decodes the payload
                let (response_tx, body) = msg.split();
                self.on_pp_rpc_request(response_tx, body, schemas).await;
            }
            ServiceMessage::Rpc(msg) if msg.msg_type() == ReceivedIngestRequest::TYPE => {
                self.on_pp_ingest_request(msg.into_typed()).await;
            }
            ServiceMessage::Rpc(msg) if msg.msg_type() == DedupSequenceNrQueryRequest::TYPE => {
                self.wait_for_tail_then(
                    last_applied_lsn_watch,
                    OrderedOp::QueryLegacyDedupSn {
                        request: msg.into_typed(),
                    },
                );
            }
            msg => {
                msg.fail(Verdict::MessageUnrecognized);
            }
        }
    }

    async fn on_ordered_op(partition_store: &mut PartitionStore, op: OrderedOp) {
        match op {
            OrderedOp::QueryLegacyDedupSn { request } => {
                Self::on_dedup_sn_query(partition_store, request).await;
            }
        }
    }

    fn wait_for_tail_then(
        &self,
        last_applied_lsn_watch: &watch::Receiver<Lsn>,
        ordered_op: OrderedOp,
    ) {
        let bifrost = self.node_ctx.bifrost.clone();
        let log_id = self.ctx.log_id();
        let mut last_applied_lsn_watch = last_applied_lsn_watch.clone();
        let mut partition_store = self.partition_store.clone();

        _ = TaskCenter::current().spawn_child(
            TaskKind::Disposable,
            "ordered-operation",
            async move {
                let tail = bifrost
                    .find_tail(log_id, FindTailOptions::ConsistentRead)
                    .await?;
                let wait_for = tail.offset().prev();
                last_applied_lsn_watch.wait_for(|v| v >= &wait_for).await?;
                Self::on_ordered_op(&mut partition_store, ordered_op).await;
                Ok(())
            },
        );
    }

    /// Used mainly by kafka-ingress to query old style dedup information
    /// during the migration to the new u128 based producer id introduced with v1.6.
    async fn on_dedup_sn_query(
        partition_store: &mut PartitionStore,
        msg: Incoming<Rpc<DedupSequenceNrQueryRequest>>,
    ) {
        let (tx, body) = msg.split();
        let producer_id = match body.producer_id {
            ingest::ProducerId::Unknown => {
                tx.send(DedupSequenceNrQueryResponse {
                    status: ResponseStatus::Internal {
                        msg: "missing producer id".into(),
                    },
                    sequence_number: None,
                });
                return;
            }
            ingest::ProducerId::String(v) => ProducerId::Other(v.into()),
            ingest::ProducerId::Numeric(v) => ProducerId::Producer(v.into()),
        };

        match partition_store
            .get_dedup_sequence_number(&producer_id)
            .await
        {
            Ok(result) => {
                let sequence_number = result.and_then(|v| {
                    if let DedupSequenceNumber::Sn(sn) = v {
                        Some(sn)
                    } else {
                        None
                    }
                });

                tx.send(DedupSequenceNrQueryResponse {
                    status: ResponseStatus::Ack,
                    sequence_number,
                });
            }
            Err(err) => {
                tx.send(DedupSequenceNrQueryResponse {
                    status: ResponseStatus::Internal {
                        msg: err.to_string(),
                    },
                    sequence_number: None,
                });
            }
        }
    }

    async fn on_pp_ingest_request(&mut self, msg: Incoming<Rpc<ReceivedIngestRequest>>) {
        let (reciprocal, request) = msg.split();

        self.leadership_state
            .forward_many_with_callback(
                request.records.into_iter(),
                move |result: Result<(), PartitionProcessorRpcError>| match result {
                    Ok(_) => reciprocal.send(ResponseStatus::Ack.into()),
                    Err(err) => match err {
                        PartitionProcessorRpcError::NotLeader(id)
                        | PartitionProcessorRpcError::LostLeadership(id) => {
                            reciprocal.send(ResponseStatus::NotLeader { of: id }.into())
                        }
                        PartitionProcessorRpcError::Internal(msg) => {
                            reciprocal.send(ResponseStatus::Internal { msg }.into())
                        }
                    },
                },
            )
            .await;
    }

    // --- Apply new commands/records

    /// Applies a single log entry to the in-flight `transaction`, advancing the FSM and, when the
    /// entry announces a new leader, committing the batch so far and reacting to the leadership
    /// change. Filtered gaps only advance the applied LSN and return without applying a record.
    async fn apply_log_entry(
        &mut self,
        entry: LogEntry,
        txn: &mut PartitionStoreTransaction<'_>,
        action_collector: &mut ActionCollector,
        leader_record_write_to_read_latency: &metrics::Histogram,
    ) -> Result<NextStep, ProcessorError> {
        trace!(
            "Processing {} record at lsn {}",
            entry.kind(),
            entry.sequence_number()
        );

        let record = match DataRecord::try_from(entry) {
            Ok(record) => record,
            Err(DataRecordError::Trimmed { from, to }) => {
                return Err(ProcessorError::TrimGapEncountered {
                    trim_gap_end: to,
                    read_pointer: from,
                });
            }
            Err(DataRecordError::Filtered { to, .. }) => {
                // We advance our applied lsn to the end of the filtered gap
                // Update replay status
                return Ok(NextStep::SkipUntil(to));
            }
            Err(DataRecordError::DataLoss { from, to }) => {
                let log_id = self.ctx.log_id();
                error!(%log_id, "Encountered a data-loss gap in the log: [{from}..{to}]");
                return Err(ProcessorError::DataLossGapEncountered {
                    data_loss_gap_end: to,
                    read_pointer: from,
                });
            }
        };

        if self.leadership_state.is_leader() {
            // todo: move to leadership state
            leader_record_write_to_read_latency.record(record.created_at().elapsed());
        }

        let lsn = record.seq();
        let envelope = Self::decode_record(record)?;
        trace!(lsn = %lsn, "Processing bifrost record for '{}': {:?}", envelope.as_ref().kind(), envelope.as_ref().header());

        // if this is a duplicate record, skip and move on.
        if self
            .ctx
            .dedup()
            .is_duplicate(envelope.as_ref().dedup(), txn)
            .await?
        {
            debug!(
                lsn = %lsn,
                "Ignoring outdated or duplicate message: {:?}",
                envelope.as_ref().header()
            );
            return Ok(NextStep::SkipUntil(lsn));
        }

        let scope = envelope.as_ref().scope();
        match scope {
            CommandScope::PartitionScoped => {
                self.apply_partition_command(envelope, txn, action_collector)
                    .await
            }
            CommandScope::KeyScoped => {
                let dedup = envelope.as_ref().dedup().clone();
                StateMachine::apply(
                    &mut self.ctx,
                    txn,
                    envelope,
                    action_collector,
                    self.leadership_state.is_leader(),
                )
                .await?;
                Ok(NextStep::AdvanceLastAppliedLsn { lsn, dedup, scope })
            }
        }
    }

    async fn apply_partition_command<'a, 'b>(
        &'a mut self,
        record: DataRecord<v2::Envelope<v2::Raw>>,
        txn: &'a mut PartitionStoreTransaction<'b>,
        action_collector: &'a mut ActionCollector,
    ) -> Result<NextStep, ProcessorError> {
        match record.as_ref().kind() {
            v2::CommandKind::AnnounceLeader => {
                AnnounceLeaderContext {
                    txn,
                    node_ctx: &mut self.node_ctx,
                    processor: &mut self.ctx,
                    partition_store: &mut self.partition_store,
                    action_collector,
                    leadership: &mut self.leadership_state,
                }
                .apply(record.map(v2::Envelope::into_typed))
                .await
            }
            v2::CommandKind::UpdatePartitionDurability => {
                UpdateDurabilityContext {
                    txn,
                    processor: &mut self.ctx,
                }
                .apply(record.map(v2::Envelope::into_typed))
                .await
            }
            v2::CommandKind::UpsertRuleBook => {
                UpsertRuleBookContext {
                    txn,
                    node_ctx: &self.node_ctx,
                    processor: &mut self.ctx,
                    leadership: &mut self.leadership_state,
                    action_collector,
                }
                .apply(record.map(v2::Envelope::into_typed))
                .await
            }
            v2::CommandKind::VersionBarrier => {
                let partition_db = self.partition_store.partition_db().clone();
                let mut leadership = LeadershipContext {
                    partition_store: &mut self.partition_store,
                    leadership: &mut self.leadership_state,
                };
                VersionBarrierContext {
                    txn,
                    node_ctx: &mut self.node_ctx,
                    partition_db,
                    processor: &mut self.ctx,
                    leadership: &mut leadership,
                }
                .apply(record.map(v2::Envelope::into_typed))
                .await
            }
            v2::CommandKind::UpsertSchema => {
                UpsertSchemaContext {
                    txn,
                    processor: &mut self.ctx,
                }
                .apply(record.map(v2::Envelope::into_typed))
                .await
            }
            v2::CommandKind::TruncateOutbox => {
                TruncateOutboxContext {
                    txn,
                    processor: &mut self.ctx,
                }
                .apply(record.map(v2::Envelope::into_typed))
                .await
            }
            e => {
                error!("Unsupported command kind {:?}", e);
                Err(ProcessorError::StateMachine(
                    state_machine::Error::UnknownCommandKind,
                ))
            }
        }
    }
}
