// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod introspection;
mod processor_state;
mod reconciliation;
mod spawn_processor_task;

pub use introspection::{LeaderQueryGuard, PartitionLeaderHandlesRegistry};

use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::ops::Add;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ahash::{HashMap, HashSet};
use anyhow::{Context, bail};
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::{counter, gauge};
use rand::RngExt;
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, field, info, info_span, instrument, trace, warn};

use restate_bifrost::Bifrost;
use restate_bifrost::loglet::FindTailOptions;
use restate_core::network::{
    BackPressureMode, ControlServiceShards, Incoming, MessageRouterBuilder, Rpc, ServiceMessage,
    ServiceReceiver, ShardControlMessage, ShardRegistrationDecision, Sharded, TransportConnect,
    Verdict,
};
use restate_core::{
    Metadata, MetadataWriter, TaskCenterFutureExt, TaskHandle, TaskKind, cancellation_watcher,
    my_node_id,
};
use restate_core::{RuntimeTaskHandle, TaskCenter};
use restate_ingestion_client::IngestionClient;
use restate_memory::ByteCount;
use restate_metadata_server::{MetadataStoreClient, ReadModifyWriteError};
use restate_metadata_store::{ReadWriteError, RetryError, retry_on_retryable_error};
use restate_partition_store::snapshots::{
    PartitionSnapshotStatus, SnapshotPartitionTask, SnapshotRepository, remove_leftover_exports,
};
use restate_partition_store::{DroppedPartitionStore, PartitionStoreManager};
use restate_partition_store::{SnapshotError, SnapshotErrorKind};
use restate_types::cluster::cluster_state::ReplayStatus;
use restate_types::cluster::cluster_state::{BrokenReason, PartitionProcessorStatus, RunMode};
use restate_types::config::{AutomaticSnapshots, Configuration};
use restate_types::epoch::EpochMetadata;
use restate_types::health::HealthStatus;
use restate_types::identifiers::PartitionId;
use restate_types::identifiers::SnapshotId;
use restate_types::live::Live;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::metadata::MetadataKind;
use restate_types::net::partition_processor::PartitionLeaderService;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, CreateSnapshotRequest, CreateSnapshotResponse,
    DropPartitionStoreError, DropPartitionStoreOutcome, DropPartitionStoreRequest,
    DropPartitionStoreResponse, PartitionManagerService, ProcessorCommand, Snapshot,
    SnapshotError as NetSnapshotError,
};
use restate_types::net::{RpcRequest as _, UnaryMessage};
use restate_types::nodes_config::{NodesConfigError, NodesConfiguration, WorkerState};
use restate_types::partition_table::PartitionTable;
use restate_types::partitions::Partition;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::protobuf::common::WorkerStatus;
use restate_types::{GenerationalNodeId, Version};
use restate_util_string::format_restring;
use restate_util_time::DurationExt;
use restate_wal_protocol::v2::{Envelope, Raw};
use restate_worker_api::invoker::capacity::InvokerCapacity;
use restate_worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};

use crate::metric_definitions::{
    ERROR_STOP, FLARE_REASON_AHEAD_OF_LOG, FLARE_REASON_MIGRATION_BARRIER,
    FLARE_REASON_SNAPSHOT_UNAVAILABLE, FLARE_REASON_VERSION_BARRIER, GAP_STOP, NORMAL_STOP,
    NUM_ACTIVE_PARTITION_LEADERS, NUM_ACTIVE_PARTITIONS, NUM_PARTITIONS, PARTITION_APPLIED_LSN_LAG,
    PARTITION_BLOCKED_FLARE, PARTITION_LABEL, PARTITION_NUM_UNKNOWN_APPLIED_LSN_LAG,
    PARTITION_START, PARTITION_STOP, PARTITION_TIME_SINCE_LAST_STATUS_UPDATE, REASON_LABEL,
    SNAPSHOT_AGE, STARTUP_ERROR_STOP, TYPE_LABEL,
};
use crate::partition::{LeadershipInfo, NodeContext, ProcessorError};
use crate::partition_processor_manager::processor_state::{
    LeaderEpochToken, ProcessorState, StartedProcessor,
};
use crate::partition_processor_manager::reconciliation::{ReconciliationPlan, StopDisposition};
use crate::partition_processor_manager::spawn_processor_task::SpawnPartitionProcessorTask;
use crate::rule_book_cache::{RuleBookCache, RuleBookCacheHandle};

pub struct PartitionProcessorManager<T> {
    health_status: HealthStatus<WorkerStatus>,
    updateable_config: Live<Configuration>,
    processor_states: BTreeMap<PartitionId, ProcessorState>,

    metadata_writer: MetadataWriter,
    partition_store_manager: Arc<PartitionStoreManager>,
    ppm_svc_rx: ServiceReceiver<PartitionManagerService>,
    pp_rpc_svc: Sharded<PartitionLeaderService>,
    pp_rpc_shards: Option<ControlServiceShards<PartitionLeaderService>>,
    bifrost: Bifrost,
    rx: mpsc::Receiver<ProcessorsManagerCommand>,
    tx: mpsc::Sender<ProcessorsManagerCommand>,

    replica_set_states: PartitionReplicaSetStates,
    target_tail_lsns: HashMap<PartitionId, Lsn>,
    leader_handles_registry: PartitionLeaderHandlesRegistry,

    asynchronous_operations: JoinSet<AsynchronousEvent>,

    pending_drops: HashMap<PartitionId, PendingDrop>,
    pending_snapshots: HashMap<PartitionId, PendingSnapshotTask>,
    latest_snapshots: HashMap<PartitionId, PartitionSnapshotStatus>,
    pending_snapshot_status_refreshes: HashSet<PartitionId>,
    snapshot_export_tasks: FuturesUnordered<TaskHandle<SnapshotResultInternal>>,
    snapshot_repository: Option<SnapshotRepository>,
    fast_forward_on_startup: HashMap<PartitionId, Lsn>,

    partition_table: Live<PartitionTable>,
    wait_for_partition_table_update: bool,

    invoker_capacity: InvokerCapacity,

    ingestion_client: IngestionClient<T, Envelope<Raw>>,

    /// Built in `new`; the polling task is spawned at the start of `run`.
    rule_book_cache_task: Option<RuleBookCache>,
    rule_book_cache: RuleBookCacheHandle,
}

type SnapshotResult = Result<PartitionSnapshotStatus, SnapshotError>;
type SnapshotResultInternal = Result<(PartitionId, PartitionSnapshotStatus), SnapshotError>;

struct PendingSnapshotTask {
    snapshot_id: SnapshotId,
    sender: Option<oneshot::Sender<SnapshotResult>>,
}

type DropResult = Result<DropPartitionStoreOutcome, DropPartitionStoreError>;

/// How long a drop request without `force` waits for this node to learn about the partition
/// configuration that the requester saw.
const DROP_PARTITION_STORE_CONFIGURATION_WAIT: Duration = Duration::from_secs(5);

/// An operator's request to drop this node's local copy of a partition, see
/// [`DropPartitionStoreRequest`].
struct DropRequest {
    force: bool,
    /// Only report what would be dropped, without changing anything.
    dry_run: bool,
    min_partition_configuration_version: Version,
    reason: String,
    sender: oneshot::Sender<DropResult>,
}

/// An in-flight request to delete this node's local copy of a partition.
///
/// While one of these exists for a partition, the manager will not start a processor for it,
/// so that we never race the drop against a fresh `open()`.
struct PendingDrop {
    /// Set while we are waiting for a running processor to terminate before we can drop.
    awaiting_stop: bool,
    reason: String,
    sender: oneshot::Sender<DropResult>,
}

/// What to do with a partition once its processor's runtime task has terminated.
enum PostStopAction {
    Restart(RestartDelay),
    /// Give up on the partition: retrying cannot fix the failure, so we park the processor
    /// in [`ProcessorState::Broken`] until an operator intervenes.
    Park(BrokenReason),
}

enum RestartDelay {
    Immediate,
    Fixed,
    Exponential {
        start_time: Instant,
        last_delay: Option<Duration>,
    },
    MaxBackoff,
}

impl RestartDelay {
    pub fn next_delay(&self) -> Option<Duration> {
        const DELAY_BASE: Duration = Duration::from_secs(1);
        const DELAY_MAX: Duration = Duration::from_secs(30);
        const RESET_RUNNING_TIME: Duration = Duration::from_secs(60);

        match self {
            RestartDelay::Immediate => None,
            RestartDelay::Fixed => Some(DELAY_BASE),
            RestartDelay::Exponential {
                start_time,
                last_delay,
            } => {
                if start_time.elapsed() > RESET_RUNNING_TIME {
                    // if we have been running for a while, reset back to the base delay
                    Some(DELAY_BASE)
                } else {
                    Some(last_delay.unwrap_or(DELAY_BASE).mul_f64(2.0).min(DELAY_MAX))
                }
            }
            RestartDelay::MaxBackoff => Some(DELAY_MAX),
        }
    }
}

impl std::fmt::Display for RestartDelay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RestartDelay::Immediate => write!(f, "will retry immediately"),
            _ => write!(
                f,
                "will retry after {}",
                self.next_delay().unwrap().friendly()
            ),
        }
    }
}

impl<T> PartitionProcessorManager<T>
where
    T: TransportConnect,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        health_status: HealthStatus<WorkerStatus>,
        updateable_config: Live<Configuration>,
        metadata_writer: MetadataWriter,
        partition_store_manager: Arc<PartitionStoreManager>,
        replica_set_states: PartitionReplicaSetStates,
        router_builder: &mut MessageRouterBuilder,
        bifrost: Bifrost,
        snapshot_repository: Option<SnapshotRepository>,
        ingestion_client: IngestionClient<T, Envelope<Raw>>,
    ) -> Self {
        let config = updateable_config.pinned();
        let ppm_svc_rx = router_builder.register_service(BackPressureMode::Lossy);

        // NOTE: this is a shared pool for RPC requests from ingress and ingestion
        // clients across all partitions.
        let pp_rpc_pool = TaskCenter::with_current(|tc| {
            tc.memory_controller()
                .create_pool("partition-leader-rpc", || {
                    Configuration::pinned().worker.data_service_memory_limit
                })
        });
        let pp_rpc_svc = router_builder
            .register_sharded_service_with_pool(pp_rpc_pool, BackPressureMode::PushBack);

        let invoker_memory_pool = TaskCenter::with_current(|tc| {
            tc.memory_controller().create_pool("invoker", || {
                Configuration::pinned().worker.invoker.memory_limit
            })
        });

        let invoker_capacity = InvokerCapacity::new(
            config.worker.invoker.concurrent_invocations_limit(),
            config.worker.invoker.invocation_throttling.as_ref(),
            config.worker.invoker.action_throttling.as_ref(),
            invoker_memory_pool,
            config.worker.invoker.per_invocation_initial_memory,
        );

        let (tx, rx) = mpsc::channel(updateable_config.pinned().worker.internal_queue_length());

        let rule_book_poll_interval = config.worker.rule_book_poll_interval.into();
        let (rule_book_cache_task, rule_book_cache) = RuleBookCache::create(
            metadata_writer.raw_metadata_store_client().clone(),
            rule_book_poll_interval,
        );

        Self {
            health_status,
            updateable_config,
            processor_states: BTreeMap::default(),
            metadata_writer,
            partition_store_manager,
            ppm_svc_rx,
            pp_rpc_svc,
            pp_rpc_shards: None,
            bifrost,
            rx,
            tx,
            replica_set_states,
            target_tail_lsns: HashMap::default(),
            leader_handles_registry: PartitionLeaderHandlesRegistry::default(),
            asynchronous_operations: JoinSet::default(),
            pending_drops: HashMap::default(),
            pending_snapshots: HashMap::default(),
            latest_snapshots: HashMap::default(),
            pending_snapshot_status_refreshes: HashSet::default(),
            snapshot_export_tasks: FuturesUnordered::default(),
            snapshot_repository,
            fast_forward_on_startup: HashMap::default(),
            partition_table: Metadata::with_current(|m| m.updateable_partition_table()),
            wait_for_partition_table_update: false,
            invoker_capacity,
            ingestion_client,
            rule_book_cache_task: Some(rule_book_cache_task),
            rule_book_cache,
        }
    }

    pub fn handle(&self) -> ProcessorsManagerHandle {
        ProcessorsManagerHandle::new(
            self.tx.clone(),
            Arc::new(self.rule_book_cache.clone()),
            Arc::new(self.leader_handles_registry.clone()),
        )
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());

        // Remove snapshot exports left over from a previous run, e.g. one that crashed while
        // exporting. Exports only start from the loop below, so none can be in progress yet.
        let snapshots_options = Configuration::pinned().worker.snapshots.clone();
        let _ =
            tokio::task::spawn_blocking(move || remove_leftover_exports(&snapshots_options)).await;

        if let Some(cache) = self.rule_book_cache_task.take() {
            TaskCenter::spawn_child(
                TaskKind::MetadataBackgroundSync,
                "rule-book-cache",
                cache.run().map(|()| Ok(())),
            )?;
        }

        let metadata = Metadata::current();

        let mut partition_table_version_watcher = metadata.watch(MetadataKind::PartitionTable);
        gauge!(NUM_PARTITIONS).set(self.partition_table.live_load().len() as f64);

        let mut snapshot_check_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(rand::rng().random_range(30..60)), // delay scheduled snapshots on startup
            Duration::from_secs(1).add_jitter(0.1),
        );
        snapshot_check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut update_target_tail_lsns = tokio::time::interval(Duration::from_secs(1));
        update_target_tail_lsns.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut ppm_svc_rx = self.ppm_svc_rx.take().start();
        let (mut pp_rpc_control, pp_rpc_shards) = self.pp_rpc_svc.take().start();
        self.pp_rpc_shards = Some(pp_rpc_shards);
        self.health_status.update(WorkerStatus::Ready);

        provision_worker(&self.metadata_writer).await?;

        // need an extra clone to work around the borrow checker which would otherwise borrow self
        // in the pin! expression below.
        let replica_set_states = self.replica_set_states.clone();
        let mut replica_set_states_changed = std::pin::pin!(replica_set_states.changed());

        self.on_replica_set_state_changes(&replica_set_states);

        loop {
            tokio::select! {
                Some(command) = self.rx.recv() => {
                    self.on_command(command);
                }
                _ = snapshot_check_interval.tick(), if self.snapshot_repository.is_some() => {
                    self.trigger_periodic_partition_snapshots();
                }
                _ = update_target_tail_lsns.tick() => {
                    self.update_target_tail_lsns();
                }
                Some(op) = ppm_svc_rx.next() => {
                    self.handle_ppm_service_op(op);
                }
                _ = partition_table_version_watcher.changed() => {
                    gauge!(NUM_PARTITIONS).set(self.partition_table.live_load().len() as f64);
                    if self.wait_for_partition_table_update {
                        self.wait_for_partition_table_update = false;
                        // we might have not started some followers because of missing partition table
                        // information
                        self.on_replica_set_state_changes(&replica_set_states);
                    }
                }
                Some(event) = self.asynchronous_operations.join_next() => {
                    self.on_asynchronous_event(event.context("asynchronous operations must not panic")?);
                }
                Some(ShardControlMessage::RegisterSortCode { sort_code, decision }) = pp_rpc_control.next() => {
                    self.on_register_pp_rpc_shard(sort_code, decision);
                }
                Some(result) = self.snapshot_export_tasks.next() => {
                    match result {
                        Ok(snapshot_result) => self.on_create_snapshot_task_completed(snapshot_result),
                        Err(join_error) => {
                            debug!("Create snapshot task panicked: {}", join_error);
                        }
                    }
                }
                () = &mut replica_set_states_changed => {
                    // register for the next replica set states updates to not miss any
                    replica_set_states_changed.set(replica_set_states.changed());
                    self.on_replica_set_state_changes(&replica_set_states);
                }
                _ = &mut shutdown => {
                    drop(pp_rpc_control);
                    break
                }
            }
        }

        self.shutdown().await;
        Ok(())
    }

    async fn shutdown(&mut self) {
        debug!("Shutting down partition processor manager.");
        self.rx.close();

        self.health_status.update(WorkerStatus::Unknown);
        // These gauges are only refreshed by get_state(), which no longer runs once the command
        // channel is closed. Reset them so that a node that is shutting down does not keep
        // exporting its last leader count.
        gauge!(NUM_ACTIVE_PARTITIONS).set(0);
        gauge!(NUM_ACTIVE_PARTITION_LEADERS).set(0);

        for task in self.snapshot_export_tasks.iter() {
            task.cancel();
        }

        // abandon in-flight drop requests for partitions still awaiting stop rather than deleting data
        // on the way down; the requester sees its RPC fail and can retry against the restarted node.
        //
        // Leave the ones that already have an in-flight task dropping them so that the caller gets
        // its result back (this is best-effort though as we don't wait for the pending_drops on shutdown).
        self.pending_drops.retain(|_, val| !val.awaiting_stop);

        // broken processors have no runtime task left to await, so drop them upfront to keep
        // `await_processors_termination` from waiting on an event that can never arrive
        self.processor_states.retain(|_, state| !state.is_broken());

        // stop all running processors
        for processor_state in self.processor_states.values_mut() {
            processor_state.stop();
        }

        // await that all running processors terminate
        self.await_processors_termination().await;
    }

    async fn await_processors_termination(&mut self) {
        while let Some(event) = self.asynchronous_operations.join_next().await {
            let event = event.expect("asynchronous operations must not panic");
            self.on_asynchronous_event(event);

            if self.processor_states.is_empty() {
                // all processors have terminated :-)
                break;
            }
        }
    }

    fn handle_ppm_service_op(&mut self, msg: ServiceMessage<PartitionManagerService>) {
        match msg {
            ServiceMessage::Unary(msg) if msg.msg_type() == ControlProcessors::TYPE => {
                let msg = msg.into_typed::<ControlProcessors>();
                let peer = msg.peer();
                let control_processors = msg.into_body();

                info_span!("on_control_processors", from_cluster_controller = %peer).in_scope(
                    || {
                        for control_processor in control_processors.commands {
                            self.on_control_processor(control_processor);
                        }
                    },
                );
            }
            ServiceMessage::Rpc(msg) if msg.msg_type() == CreateSnapshotRequest::TYPE => {
                let request = msg.into_typed::<CreateSnapshotRequest>();
                self.handle_create_snapshot_request(request);
            }
            ServiceMessage::Rpc(msg) if msg.msg_type() == DropPartitionStoreRequest::TYPE => {
                let request = msg.into_typed::<DropPartitionStoreRequest>();
                self.handle_drop_partition_store_request(request);
            }
            msg => {
                msg.fail(Verdict::MessageUnrecognized);
            }
        }
    }

    fn on_register_pp_rpc_shard(
        &self,
        sort_code: u64,
        decision: ShardRegistrationDecision<PartitionLeaderService>,
    ) {
        let Ok(partition_id) = u16::try_from(sort_code) else {
            error!(%sort_code, "Invalid partition id in RPC request. This indicates a protocol bug!");
            decision.fail(Verdict::MessageUnrecognized);
            return;
        };

        let partition_id = PartitionId::from(partition_id);
        match self.processor_states.get(&partition_id) {
            Some(ProcessorState::Started { processor, .. }) => {
                // The PP is running but its shard wasn't pre-registered (race between
                // start and first message). Register it now.
                let pp = processor.as_ref().expect("must be some");
                decision.accept(pp.rpc_shard_sender());
            }
            _ => {
                // Not started (or starting/stopping) — reject.
                //
                // todo(asoli): consider other strategies here:
                //   1. If the partition is still starting up, latch the registration
                //      token onto it and resolve when read, or drop if it fails.
                //   2. If partition is starting, up still maybe we can add a "sort-code not ready"
                //   error to make it clear that the shard is still starting up?
                decision.fail(Verdict::SortCodeNotFound);
            }
        }
    }

    fn unregister_pp_rpc_shard(&self, partition_id: PartitionId) {
        self.pp_rpc_shards
            .as_ref()
            .unwrap()
            .force_unregister_sort_code(u64::from(u16::from(partition_id)));
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(partition_id = %event.partition_id, event = %<&'static str as From<&EventKind>>::from(&event.inner))
    )]
    fn on_asynchronous_event(&mut self, event: AsynchronousEvent) {
        let AsynchronousEvent {
            partition_id,
            inner,
        } = event;

        match inner {
            EventKind::Started(result) => {
                match result {
                    Ok((started_processor, runtime_handle)) => {
                        if let Some(processor_state) = self.processor_states.get_mut(&partition_id)
                        {
                            match processor_state {
                                ProcessorState::Starting {
                                    target_run_mode,
                                    start_time,
                                    delay,
                                } => {
                                    debug!(%target_run_mode, "Partition processor was successfully created.");
                                    // Note: leader-side handles are registered by the partition
                                    // processor itself on leadership transition — see
                                    // `LeadershipState::become_leader`.

                                    // Pre-register the shard so messages route
                                    // directly to this PP without going through
                                    // the control stream.
                                    self.pp_rpc_shards
                                        .as_ref()
                                        .unwrap()
                                        .force_register_sort_code(
                                            u64::from(u16::from(partition_id)),
                                            started_processor.rpc_shard_sender(),
                                        );

                                    let mut new_state = ProcessorState::started(
                                        started_processor,
                                        *start_time,
                                        *delay,
                                    );
                                    // check whether we need to obtain a new leader epoch
                                    if *target_run_mode == RunMode::Leader
                                        && let Some(leader_epoch_token) = new_state.run_as_leader()
                                    {
                                        Self::obtain_new_leader_epoch(
                                            partition_id,
                                            leader_epoch_token,
                                            self.metadata_writer
                                                .raw_metadata_store_client()
                                                .clone(),
                                            &mut self.asynchronous_operations,
                                        );
                                    }

                                    *processor_state = new_state;

                                    self.await_runtime_task_result(partition_id, runtime_handle);
                                }
                                ProcessorState::Started { .. } => {
                                    panic!(
                                        "Started two processors for the same partition '{partition_id}'"
                                    );
                                }
                                ProcessorState::Stopping { processor, .. } => {
                                    assert!(
                                        processor.is_none(),
                                        "Started two processor for the same partition '{partition_id}'"
                                    );

                                    debug!(
                                        "Started partition processor is no longer needed. Stopping it."
                                    );
                                    *processor = Some(started_processor);

                                    runtime_handle.cancel();
                                    self.await_runtime_task_result(partition_id, runtime_handle);
                                }
                                ProcessorState::Broken { .. } => {
                                    // Unreachable in practice: we only park a processor as
                                    // broken after its runtime task reported termination.
                                    // Stop the new one and stay broken.
                                    debug!(
                                        "Started partition processor for a partition we gave up on. Stopping it."
                                    );
                                    runtime_handle.cancel();
                                    self.await_runtime_task_result(partition_id, runtime_handle);
                                }
                            }
                        } else {
                            debug!("Started partition processor is no longer needed. Stopping it.");
                            self.processor_states
                                .insert(partition_id, ProcessorState::stopping(started_processor));
                            runtime_handle.cancel();
                            self.await_runtime_task_result(partition_id, runtime_handle);
                        }
                    }
                    Err(err) => {
                        info!(%partition_id, %err, "Partition processor failed to start");
                        self.processor_states.remove(&partition_id);
                        self.restart_partition_processor_if_replica(
                            partition_id,
                            RestartDelay::Fixed,
                        );
                        // no runtime task was created, so no `Stopped` event will follow: a drop
                        // waiting for this processor to go away has to be resumed from here
                        if self.take_pending_drop_stop_wait(partition_id) {
                            self.spawn_drop_partition_store_task(partition_id);
                        }
                    }
                }
            }
            EventKind::Stopped(result) => {
                self.unregister_pp_rpc_shard(partition_id);
                let action = match self.processor_states.remove(&partition_id) {
                    None => {
                        debug!("Stopped partition processor which is no longer running.");
                        // immediately try to restart if we are still part of the partition's membership
                        counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => NORMAL_STOP).increment(1);
                        PostStopAction::Restart(RestartDelay::Immediate)
                    }
                    Some(processor_state) => match processor_state {
                        ProcessorState::Starting { .. } => {
                            counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => STARTUP_ERROR_STOP).increment(1);
                            warn!(%partition_id, "Partition processor failed to start: {result:?}");
                            PostStopAction::Restart(RestartDelay::Fixed)
                        }
                        ProcessorState::Started {
                            processor,
                            start_time,
                            delay,
                            ..
                        } => {
                            // Defensive: normally the processor unregisters itself during
                            // step_down before run() returns, but this guards against panics
                            // or abnormal exits that skip that path.
                            self.leader_handles_registry.unregister_all(
                                processor.as_ref().expect("must be some").key_range(),
                            );

                            match &result {
                                Err(e @ ProcessorError::VersionBarrier { .. }) => {
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => ERROR_STOP).increment(1);
                                    gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL => partition_id.to_string(), REASON_LABEL => FLARE_REASON_VERSION_BARRIER).set(1);
                                    error!(%partition_id, "Partition processor start error: {e}");
                                    PostStopAction::Restart(RestartDelay::MaxBackoff)
                                }
                                Err(e @ ProcessorError::MigrationBarrier { .. }) => {
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => ERROR_STOP).increment(1);
                                    gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL => partition_id.to_string(), REASON_LABEL => FLARE_REASON_MIGRATION_BARRIER).set(1);
                                    error!(%partition_id, "Partition processor start error: {e}");
                                    PostStopAction::Restart(RestartDelay::MaxBackoff)
                                }
                                Err(e @ ProcessorError::PartitionAheadOfLog { .. }) => {
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => ERROR_STOP).increment(1);
                                    gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL => partition_id.to_string(), REASON_LABEL => FLARE_REASON_AHEAD_OF_LOG).set(1);
                                    error!(
                                        %partition_id,
                                        "Partition processor start error: {e}. The local partition store \
                                        is sealed; this node will not run this partition again until the \
                                        store has been dropped and replaced by a safe snapshot",
                                    );
                                    PostStopAction::Park(BrokenReason::AheadOfLog)
                                }
                                Err(ProcessorError::TrimGapEncountered {
                                    read_pointer: sequence_number,
                                    trim_gap_end: to_lsn,
                                })
                                | Err(ProcessorError::DataLossGapEncountered {
                                    read_pointer: sequence_number,
                                    data_loss_gap_end: to_lsn,
                                }) => {
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => GAP_STOP).increment(1);
                                    if self.partition_store_manager.is_repository_configured() {
                                        info!(
                                            %partition_id,
                                            "Partition processor stopped due to a log gap [{sequence_number}..{to_lsn}], \
                                                will attempt to fast-forward on restart",
                                        );
                                        self.fast_forward_on_startup.insert(partition_id, *to_lsn);

                                        PostStopAction::Restart(RestartDelay::Immediate)
                                    } else {
                                        error!(
                                            %partition_id,
                                            "Partition processor stopped due to a log gap [{sequence_number}..{to_lsn}], and no snapshot repository is configured. \
                                                Cannot recover without a partition snapshot!",
                                        );
                                        gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL => partition_id.to_string(), REASON_LABEL => FLARE_REASON_SNAPSHOT_UNAVAILABLE).set(1);
                                        // configuration problem; until we have peer-to-peer state exchange we can only wait
                                        PostStopAction::Restart(RestartDelay::MaxBackoff)
                                    }
                                }
                                Err(err) => {
                                    let next_delay = RestartDelay::Exponential {
                                        start_time,
                                        last_delay: delay,
                                    };
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => ERROR_STOP).increment(1);
                                    error!(%partition_id, %err, "Partition processor exited unexpectedly, {}", next_delay);
                                    PostStopAction::Restart(next_delay)
                                }
                                Ok(_) => {
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => NORMAL_STOP).increment(1);
                                    info!(%partition_id, "Partition processor stopped");
                                    PostStopAction::Restart(RestartDelay::Immediate)
                                }
                            }
                        }
                        ProcessorState::Stopping { processor, .. } => {
                            if let Some(processor) = processor {
                                self.leader_handles_registry
                                    .unregister_all(processor.key_range());
                            }
                            counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => NORMAL_STOP).increment(1);
                            PostStopAction::Restart(RestartDelay::Immediate)
                        }
                        // Unreachable in practice: a broken processor has no runtime task that
                        // could report back. Stay broken rather than silently restarting.
                        ProcessorState::Broken { reason, .. } => PostStopAction::Park(reason),
                    },
                };

                // A pending drop was waiting for this processor to release the store. Note that
                // `should_run_processor` reports false while a drop is pending, so neither branch
                // below revives the processor we just stopped.
                let resume_drop = self.take_pending_drop_stop_wait(partition_id);

                match action {
                    PostStopAction::Restart(delay) => {
                        if !self.restart_partition_processor_if_replica(partition_id, delay) {
                            debug!("Partition processor stopped: {result:?}");
                        }
                    }
                    PostStopAction::Park(reason) => {
                        if self.should_run_processor(partition_id) {
                            self.processor_states
                                .insert(partition_id, ProcessorState::broken(reason));
                        } else {
                            debug!("Partition processor stopped: {result:?}");
                        }
                    }
                }
                if resume_drop {
                    self.spawn_drop_partition_store_task(partition_id);
                }
            }
            EventKind::NewLeaderEpoch {
                leader_epoch_token,
                result,
            } => {
                if let Some(processor_state) = self.processor_states.get_mut(&partition_id) {
                    match result {
                        Ok(leadership_info) => {
                            processor_state
                                .on_leader_epoch_obtained(leadership_info, leader_epoch_token);
                        }
                        Err(err) => {
                            if processor_state.is_valid_leader_epoch_token(leader_epoch_token) {
                                info!(%partition_id, %err, "Failed obtaining new leader epoch. Continue running as follower");
                                processor_state.run_as_follower();
                            } else {
                                debug!("Received outdated new leader epoch. Ignoring it.");
                            }
                        }
                    }
                } else {
                    debug!(
                        "Partition processor is no longer running. Ignoring new leader epoch result."
                    );
                }
            }
            EventKind::NewTargetTail { tail } => {
                let Some(tail_lsn) = tail else {
                    self.target_tail_lsns.remove(&partition_id);
                    return;
                };

                match self.target_tail_lsns.entry(partition_id) {
                    Entry::Occupied(mut o) => {
                        if *o.get() < tail_lsn {
                            o.insert(tail_lsn);
                        }
                    }
                    Entry::Vacant(v) => {
                        v.insert(tail_lsn);
                    }
                }
            }
            EventKind::SnapshotStatusUpdated { snapshot_status } => {
                self.pending_snapshot_status_refreshes.remove(&partition_id);
                self.update_snapshot_status(partition_id, snapshot_status);
            }
            EventKind::SnapshotStatusUpdateSkipped => {
                self.pending_snapshot_status_refreshes.remove(&partition_id);
                // No-op: no snapshot repository, partition no longer in the partition table,
                // or error fetching status (logged upstream)
            }
            EventKind::DropRequestReady(request) => {
                self.on_drop_request_ready(partition_id, request);
            }
            EventKind::PartitionStoreDropped(result) => {
                self.on_partition_store_dropped(partition_id, result);
            }
        }
    }

    fn await_runtime_task_result(
        &mut self,
        partition_id: PartitionId,
        runtime_task_handle: RuntimeTaskHandle<Result<(), ProcessorError>>,
    ) {
        let psm = self.partition_store_manager.clone();
        self.asynchronous_operations
            .build_task()
            .name(&format!("runtime-result-{partition_id}"))
            .spawn(
                async move {
                    let result = runtime_task_handle.await;
                    // make sure we tell partition store manager to mark the partition db as closed
                    psm.close(partition_id).await;

                    AsynchronousEvent {
                        partition_id,
                        inner: EventKind::Stopped(result),
                    }
                }
                .in_current_tc(),
            )
            .expect("to spawn await runtime task result");
    }

    /// A lightweight tail watcher that leverages the loglet watch tail implementation
    /// to retrieve the most recently observed tail for the writable segment.
    /// This ensures that the tail remains close to the actual value,
    /// regardless of which segment is currently being processed by the partition processor.
    fn update_target_tail_lsns(&mut self) {
        for partition_id in self.processor_states.keys().cloned() {
            let bifrost = self.bifrost.clone();

            self.asynchronous_operations.spawn(
                async move {
                    let log_id = Metadata::with_current(|m| {
                        m.partition_table_ref()
                            .get(&partition_id)
                            .map(Partition::log_id)
                    })
                    .expect("partition is in partition table");

                    let tail = bifrost
                        .find_tail(log_id, FindTailOptions::Fast)
                        .await
                        .map(|tail| tail.offset())
                        .ok();

                    AsynchronousEvent {
                        partition_id,
                        inner: EventKind::NewTargetTail { tail },
                    }
                }
                .in_current_tc(),
            );
        }
    }

    fn obtain_new_leader_epoch(
        partition_id: PartitionId,
        leader_epoch_token: LeaderEpochToken,
        metadata_store_client: MetadataStoreClient,
        asynchronous_operations: &mut JoinSet<AsynchronousEvent>,
    ) {
        asynchronous_operations
            .build_task()
            .name(&format!("obtain-leader-epoch-{partition_id}"))
            .spawn(
                Self::obtain_new_leader_epoch_task(
                    leader_epoch_token,
                    partition_id,
                    metadata_store_client,
                    my_node_id(),
                )
                .in_current_tc(),
            )
            .expect("spawn obtain leader epoch task");
    }

    /// Collect enriched processor status from all running partitions
    fn get_state(&self) -> BTreeMap<PartitionId, PartitionProcessorStatus> {
        let mut num_active_leaders: u32 = 0;
        let mut num_unknown_applied_lsn_lag: u32 = 0;
        let mut last_updated_samples = Vec::with_capacity(self.processor_states.len());
        let mut applied_lsn_lag_samples = Vec::with_capacity(self.processor_states.len());
        let mut snapshot_age_samples = Vec::with_capacity(self.processor_states.len());

        let statuses = self
            .processor_states
            .iter()
            .filter_map(|(partition_id, processor_state)| {
                let mut status = processor_state.partition_processor_status()?;

                last_updated_samples.push(status.updated_at.elapsed().as_secs());
                num_active_leaders += u32::from(status.is_effective_leader());

                // todo: PartitionProcessorStatus struct is shared across PP and PPM, consider splitting it
                let latest_snapshot = self.latest_snapshots.get(partition_id);
                if let Some(snapshot) = latest_snapshot {
                    snapshot_age_samples
                        .push(snapshot.latest_snapshot_created_at.elapsed().as_secs());
                }

                status.last_archived_log_lsn = latest_snapshot.map(|s| s.archived_lsn);
                status.last_archived_log_byte_offset =
                    latest_snapshot.and_then(|s| s.archived_log_byte_offset);
                status.last_archived_byte_offsets_base_lsn =
                    latest_snapshot.and_then(|s| s.archived_byte_offsets_base_lsn);

                let current_tail_lsn = self.target_tail_lsns.get(partition_id).cloned();
                let target_tail_lsn = if current_tail_lsn > status.target_tail_lsn {
                    current_tail_lsn
                } else {
                    status.target_tail_lsn
                };

                match target_tail_lsn {
                    None => {
                        num_unknown_applied_lsn_lag += 1;
                    }
                    Some(target_tail_lsn) => {
                        status.target_tail_lsn = Some(target_tail_lsn);

                        // tail lsn always points to the next "free" lsn slot. Therefor the lag is calculate as `lsn-1`
                        // hence we do target_tail_lsn.prev() below
                        applied_lsn_lag_samples.push(
                            target_tail_lsn.prev().as_u64().saturating_sub(
                                status.last_applied_log_lsn.unwrap_or(Lsn::OLDEST).as_u64(),
                            ),
                        );
                    }
                }

                Some((*partition_id, status))
            })
            .collect();
        report_quantile_gauges(
            PARTITION_TIME_SINCE_LAST_STATUS_UPDATE,
            &mut last_updated_samples,
        );
        report_quantile_gauges(PARTITION_APPLIED_LSN_LAG, &mut applied_lsn_lag_samples);
        report_quantile_gauges(SNAPSHOT_AGE, &mut snapshot_age_samples);
        gauge!(PARTITION_NUM_UNKNOWN_APPLIED_LSN_LAG).set(num_unknown_applied_lsn_lag as f64);
        gauge!(NUM_ACTIVE_PARTITIONS).set(self.processor_states.len() as f64);
        gauge!(NUM_ACTIVE_PARTITION_LEADERS).set(num_active_leaders as f64);
        statuses
    }

    fn on_command(&mut self, command: ProcessorsManagerCommand) {
        match command {
            ProcessorsManagerCommand::GetState(sender) => {
                let _ = sender.send(self.get_state());
            }
        }
    }

    #[instrument(level = "info", skip_all, fields(partition_id = %control_processor.partition_id))]
    fn on_control_processor(&mut self, control_processor: ControlProcessor) {
        let partition_id = control_processor.partition_id;

        if control_processor.current_version
            < self
                .replica_set_states
                .membership_state(partition_id)
                .observed_current_membership
                .version
        {
            debug!("Ignoring control processor command because it is outdated");
            return;
        }

        match control_processor.command {
            ProcessorCommand::Leader => {
                if let Some(processor_state) = self.processor_states.get_mut(&partition_id) {
                    if let Some(leader_epoch_token) = processor_state.run_as_leader() {
                        debug!(
                            "Asked to run as leader by cluster controller. Obtaining required leader epoch"
                        );
                        Self::obtain_new_leader_epoch(
                            partition_id,
                            leader_epoch_token,
                            self.metadata_writer.raw_metadata_store_client().clone(),
                            &mut self.asynchronous_operations,
                        );
                    }
                } else {
                    // todo handle leader messages that arrive from the "future" (before we have observed
                    //  the corresponding membership state.
                    debug!(
                        "Unknown partition id. Ignoring {} command.",
                        control_processor.command
                    );
                }
            }
            ProcessorCommand::Follower | ProcessorCommand::Stop => {
                trace!("Ignoring {} command.", control_processor.command);
            }
        }
    }

    fn on_create_snapshot(
        &mut self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        sender: oneshot::Sender<SnapshotResult>,
    ) {
        let processor_state = match self.processor_states.get(&partition_id) {
            Some(state) => state,
            None => {
                let _ = sender.send(Err(SnapshotError {
                    partition_id,
                    kind: SnapshotErrorKind::PartitionNotFound,
                }));
                return;
            }
        };

        let snapshot_repository = self.snapshot_repository.clone();
        let Some(snapshot_repository) = snapshot_repository else {
            let _ = sender.send(Err(SnapshotError {
                partition_id,
                kind: SnapshotErrorKind::RepositoryNotConfigured,
            }));
            return;
        };

        if !processor_state.should_publish_snapshots() {
            let _ = sender.send(Err(SnapshotError {
                partition_id,
                kind: SnapshotErrorKind::InvalidState,
            }));
            return;
        }

        self.spawn_create_snapshot_task(
            partition_id,
            min_target_lsn,
            snapshot_repository,
            Some(sender),
        );
    }

    fn on_create_snapshot_task_completed(&mut self, result: SnapshotResultInternal) {
        let (partition_id, response) = match result {
            Ok((partition_id, status)) => {
                self.update_snapshot_status(partition_id, status.clone());
                (partition_id, Ok(status))
            }
            Err(snapshot_error) => (snapshot_error.partition_id, Err(snapshot_error)),
        };

        if let Some(pending_task) = self.pending_snapshots.remove(&partition_id) {
            if let Some(sender) = pending_task.sender {
                let _ = sender.send(response);
            }
        } else {
            info!(
                result = ?response,
                "Snapshot task result received without a pending task!",
            )
        }
    }

    fn update_snapshot_status(
        &mut self,
        partition_id: PartitionId,
        snapshot_status: PartitionSnapshotStatus,
    ) {
        match self.latest_snapshots.entry(partition_id) {
            Entry::Occupied(mut e) => {
                if snapshot_status.archived_lsn >= e.get().archived_lsn {
                    e.insert(snapshot_status);
                } else {
                    // TODO: This shouldn't ever happen; if it does that means that the CAS update against the repository is broken
                    warn!(
                        %partition_id,
                        published = ?snapshot_status,
                        known = ?e.get(),
                        "Received latest snapshot status update with lower LSN than the known one!"
                    );
                }
            }
            Entry::Vacant(v) => {
                v.insert(snapshot_status);
            }
        }
    }

    fn trigger_periodic_partition_snapshots(&mut self) {
        let Some(snapshot_repository) = self.snapshot_repository.clone() else {
            return;
        };

        let snapshots_options = &self.updateable_config.live_load().worker.snapshots;
        let Some(triggers) = snapshots_options.automatic_snapshots() else {
            return;
        };
        let concurrency_limit = snapshots_options.automatic_snapshot_concurrency_limit() as usize;

        // Partitions with a status suitable for taking a snapshot without a running snapshot task
        let candidate_partitions = self
            .processor_states
            .iter()
            .filter(|(partition_id, _)| !self.pending_snapshots.contains_key(partition_id))
            .filter_map(|(partition_id, state)| {
                state
                    .partition_processor_status()
                    .filter(|status| {
                        status.effective_mode == RunMode::Leader
                            && status.replay_status == ReplayStatus::Active
                    })
                    .map(|status| (*partition_id, status))
            });

        // Limit the number of snapshots in progress, requested ones included
        let limit = concurrency_limit.saturating_sub(self.pending_snapshots.len());

        let mut candidate_partitions: Vec<_> = candidate_partitions.collect();
        candidate_partitions.shuffle(&mut rand::rng());

        let mut unknown_latest_snapshot = Vec::new();
        let mut due_partitions = Vec::new();
        for (partition_id, status) in &candidate_partitions {
            let Some(latest_snapshot) = self.latest_snapshots.get(partition_id) else {
                unknown_latest_snapshot.push(*partition_id);
                continue;
            };
            // Stop evaluating once `limit` partitions are due
            if due_partitions.len() < limit
                && is_snapshot_due(*partition_id, triggers, latest_snapshot, status)
            {
                due_partitions.push(*partition_id);
            }
        }

        for partition_id in unknown_latest_snapshot {
            self.spawn_update_latest_snapshot(partition_id);
        }

        for partition_id in due_partitions {
            self.spawn_create_snapshot_task(partition_id, None, snapshot_repository.clone(), None);
        }
    }

    /// Spawn a task to create a snapshot of the given partition. Optionally, a sender will be
    /// notified of the result on completion. If the minimum requested snapshot LSN is already
    /// met by the last known snapshot, it will be immediately returned to the sender instead of
    /// creating a new snapshot.
    fn spawn_create_snapshot_task(
        &mut self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        snapshot_repository: SnapshotRepository,
        sender: Option<oneshot::Sender<SnapshotResult>>,
    ) {
        if let Some(snapshot) = self.latest_snapshots.get(&partition_id)
            && min_target_lsn.is_some_and(|target_lsn| snapshot.archived_lsn >= target_lsn)
            && let Some(sender) = sender
        {
            sender
                        .send(Ok(snapshot.clone()))
                        .inspect_err(|err| {
                            debug!(
                                ?min_target_lsn,
                                snapshot_id = ?snapshot.latest_snapshot_id,
                                archived_lsn = ?snapshot.archived_lsn,
                                "New snapshot was not created because the target LSN was already covered by existing snapshot. \
                                However, we failed to notify the request sender: {:?}",
                                err
                            )
                        })
                        .ok();

            return;
        }

        match self.pending_snapshots.entry(partition_id) {
            Entry::Vacant(entry) => {
                let config = self.updateable_config.live_load();

                let snapshot_base_path = config.worker.snapshots.snapshots_dir(partition_id);
                let snapshot_id = SnapshotId::new();
                let (node_name, node_id, cluster_name, cluster_fingerprint) =
                    Metadata::with_current(|m| {
                        let nodes_config = m.nodes_config_ref();
                        let node_id = m.my_node_id();
                        let node_name = nodes_config
                            .find_node_by_id(node_id)
                            .expect("my node must be present")
                            .name
                            .clone();
                        (
                            node_name,
                            node_id,
                            nodes_config.cluster_name().to_owned(),
                            nodes_config.cluster_fingerprint(),
                        )
                    });

                let create_snapshot_task = SnapshotPartitionTask {
                    snapshot_id,
                    partition_id,
                    commit_fence_tx: self
                        .processor_states
                        .get(&partition_id)
                        .and_then(ProcessorState::commit_fence_sender),
                    min_target_lsn,
                    snapshot_base_path,
                    partition_store_manager: self.partition_store_manager.clone(),
                    cluster_name,
                    cluster_fingerprint,
                    node_name,
                    node_id,
                    snapshot_repository,
                };

                let jitter = if sender.is_some() {
                    Duration::ZERO
                } else {
                    Duration::from_millis(rand::rng().random_range(0..10_000))
                };
                let spawn_task_result = TaskCenter::spawn_unmanaged_child(
                    TaskKind::PartitionSnapshotProducer,
                    "create-snapshot",
                    async move {
                        tokio::time::sleep(jitter).await;
                        create_snapshot_task.run().await
                    },
                );

                match spawn_task_result {
                    Ok(handle) => {
                        self.snapshot_export_tasks.push(handle);
                        entry.insert(PendingSnapshotTask {
                            snapshot_id,
                            sender,
                        });
                    }
                    Err(_shutdown) => {
                        if let Some(sender) = sender {
                            let _ = sender.send(Err(SnapshotError {
                                partition_id,
                                kind: SnapshotErrorKind::InvalidState,
                            }));
                        }
                    }
                }
            }
            Entry::Occupied(pending) => {
                debug!(
                    %partition_id,
                    snapshot_id = %pending.get().snapshot_id,
                    "A snapshot export is already in progress, refusing to start a new export"
                );
                if let Some(sender) = sender {
                    let _ = sender.send(Err(SnapshotError {
                        partition_id,
                        kind: SnapshotErrorKind::SnapshotInProgress,
                    }));
                }
            }
        }
    }

    fn spawn_update_latest_snapshot(&mut self, partition_id: PartitionId) {
        if !self.pending_snapshot_status_refreshes.insert(partition_id) {
            return;
        }

        let psm = self.partition_store_manager.clone();
        self.asynchronous_operations
            .build_task()
            .name(&format!("update-latest-snapshot-{partition_id}"))
            .spawn(
                async move {
                    let Some(partition) = Metadata::with_current(|m| {
                        m.partition_table_ref().get(&partition_id).cloned()
                    }) else {
                        // The partition is no longer in the partition table.
                        return AsynchronousEvent {
                            partition_id,
                            inner: EventKind::SnapshotStatusUpdateSkipped,
                        };
                    };
                    match psm
                        .refresh_latest_partition_snapshot_status(&partition)
                        .await
                    {
                        Ok(Some(snapshot_status)) => AsynchronousEvent {
                            partition_id,
                            inner: EventKind::SnapshotStatusUpdated { snapshot_status },
                        },
                        Ok(None) => {
                            // No snapshot repository is configured
                            AsynchronousEvent {
                                partition_id,
                                inner: EventKind::SnapshotStatusUpdateSkipped,
                            }
                        }
                        Err(err) => {
                            warn!(
                                %partition_id,
                                %err,
                                "Failed to refresh latest archived LSN from snapshot repository"
                            );
                            AsynchronousEvent {
                                partition_id,
                                inner: EventKind::SnapshotStatusUpdateSkipped,
                            }
                        }
                    }
                }
                .in_current_tc(),
            )
            .expect("to spawn update latest snapshot task");
    }

    async fn obtain_new_leader_epoch_task(
        leader_epoch_token: LeaderEpochToken,
        partition_id: PartitionId,
        metadata_store_client: MetadataStoreClient,
        node_id: GenerationalNodeId,
    ) -> AsynchronousEvent {
        AsynchronousEvent {
            partition_id,
            inner: EventKind::NewLeaderEpoch {
                leader_epoch_token,
                result: Self::obtain_next_epoch(metadata_store_client, partition_id, node_id)
                    .await
                    .map_err(Into::into),
            },
        }
    }

    async fn obtain_next_epoch(
        metadata_store_client: MetadataStoreClient,
        partition_id: PartitionId,
        node_id: GenerationalNodeId,
    ) -> Result<Box<LeadershipInfo>, ReadModifyWriteError> {
        let epoch: EpochMetadata = metadata_store_client
            .read_modify_write(partition_processor_epoch_key(partition_id), |epoch| {
                let next_epoch = epoch
                    .map(|epoch: EpochMetadata| epoch.claim_leadership(node_id, partition_id))
                    .ok_or_else(|| "missing epoch metadata".to_owned())?;

                Ok(next_epoch)
            })
            .await?;

        Ok(Box::new(epoch.into()))
    }

    /// Handles an operator's request to delete this node's local copy of a partition.
    ///
    /// Unless the request is forced, we first make sure that we know the partition's replica set
    /// at least as well as the requester did. With an older one, this node could drop its store
    /// although the requester saw that it was added back to the replica set, or refuse because it
    /// doesn't know yet that it left. So we wait for a newer partition configuration, for a few
    /// seconds at most, without blocking the partition's processor in the meantime.
    fn on_drop_partition_store(&mut self, partition_id: PartitionId, request: DropRequest) {
        let required = request.min_partition_configuration_version;
        let observed = self.latest_configuration_version(partition_id);
        if request.force || observed >= required {
            self.try_drop_partition_store(partition_id, request);
            return;
        }

        debug!(
            %partition_id,
            %observed,
            %required,
            "{}Waiting to learn about the partition configuration before dropping the partition \
            store",
            dry_run_prefix(request.dry_run)
        );
        let replica_set_states = self.replica_set_states.clone();
        let cancellation = restate_core::cancellation_token();
        self.asynchronous_operations
            .build_task()
            .name(&format!("drop-partition-store-wait-{partition_id}"))
            .spawn(
                async move {
                    let configuration_observed = async {
                        loop {
                            // register before checking, so we don't miss an update in between
                            let changed = replica_set_states.membership_changed();
                            if replica_set_states
                                .membership_state(partition_id)
                                .latest_configuration_version()
                                >= required
                            {
                                return;
                            }
                            changed.await;
                        }
                    };
                    tokio::select! {
                        _ = tokio::time::timeout(
                            DROP_PARTITION_STORE_CONFIGURATION_WAIT,
                            configuration_observed,
                        ) => {}
                        _ = cancellation.cancelled() => {}
                    }
                    AsynchronousEvent {
                        partition_id,
                        inner: EventKind::DropRequestReady(request),
                    }
                }
                .in_current_tc(),
            )
            .expect("to spawn drop partition store wait task");
    }

    fn on_drop_request_ready(&mut self, partition_id: PartitionId, request: DropRequest) {
        if is_shutting_down() {
            // don't start deleting data on the way down, including while the node shuts down
            // other roles before the worker; dropping the request fails the requester's RPC, so
            // it can retry against the restarted node
            return;
        }

        let required = request.min_partition_configuration_version;
        let observed = self.latest_configuration_version(partition_id);
        if observed < required {
            refuse_drop(
                partition_id,
                request,
                DropPartitionStoreError::PartitionConfigurationNotObserved { required, observed },
            );
            return;
        }
        self.try_drop_partition_store(partition_id, request);
    }

    fn latest_configuration_version(&self, partition_id: PartitionId) -> Version {
        self.replica_set_states
            .membership_state(partition_id)
            .latest_configuration_version()
    }

    /// Deletes this node's local copy of a partition if that is safe. A dry run makes the same
    /// checks and reports what would be deleted, without changing anything.
    ///
    /// We only touch the store once we are sure that no processor is using it: either this node
    /// already gave up on the partition, or it is neither running the partition nor part of its
    /// replica set. With `force`, we stop a running processor and wait for it to terminate first,
    /// and drop the store of a replica too. [`Self::pending_drops`] keeps the manager from
    /// starting a new processor in the meantime. Since this runs on the manager's event loop, no
    /// processor can start between these checks and recording the pending drop.
    fn try_drop_partition_store(&mut self, partition_id: PartitionId, request: DropRequest) {
        if self.pending_drops.contains_key(&partition_id) {
            refuse_drop(
                partition_id,
                request,
                DropPartitionStoreError::DropInProgress,
            );
            return;
        }

        if !self.partition_table.live_load().contains(&partition_id) {
            // without the partition's metadata we don't know which column family to drop
            refuse_drop(
                partition_id,
                request,
                DropPartitionStoreError::UnknownPartition,
            );
            return;
        }

        // `Some(true)` if this node gave up on the partition, `Some(false)` if it runs it
        let processor_broken = self
            .processor_states
            .get(&partition_id)
            .map(|state| state.is_broken());
        let refusal = match processor_broken {
            Some(false) if !request.force => Some(DropPartitionStoreError::ProcessorRunning),
            // e.g. a replica whose processor is waiting to be restarted
            None if !request.force
                && self
                    .replica_set_states
                    .membership_state(partition_id)
                    .contains(Metadata::with_current(|m| m.my_node_id().as_plain())) =>
            {
                Some(DropPartitionStoreError::ReplicaSetMember)
            }
            _ => None,
        };
        if let Some(error) = refusal {
            refuse_drop(partition_id, request, error);
            return;
        }

        if request.dry_run {
            self.spawn_drop_preview(partition_id, request);
            return;
        }

        let awaiting_stop = match processor_broken {
            Some(true) => {
                // no runtime task to await, the processor is already gone
                self.processor_states.remove(&partition_id);
                false
            }
            Some(false) => {
                warn!(
                    %partition_id,
                    reason = request.reason,
                    "Stopping a running partition processor to drop its local store"
                );
                self.processor_states
                    .get_mut(&partition_id)
                    .expect("the processor is running")
                    .stop();
                true
            }
            // this node isn't running the partition; nothing can race us
            None => false,
        };

        self.pending_drops.insert(
            partition_id,
            PendingDrop {
                awaiting_stop,
                reason: request.reason,
                sender: request.sender,
            },
        );

        if !awaiting_stop {
            self.spawn_drop_partition_store_task(partition_id);
        }
    }

    /// Clears the "waiting for the processor to terminate" flag of a pending drop. Returns true
    /// if a drop was waiting, and is now ready to run.
    fn take_pending_drop_stop_wait(&mut self, partition_id: PartitionId) -> bool {
        self.pending_drops
            .get_mut(&partition_id)
            .is_some_and(|pending| std::mem::take(&mut pending.awaiting_stop))
    }

    /// Answers a dry run with what dropping the partition's local store would delete, without
    /// touching the processor or the store.
    fn spawn_drop_preview(&mut self, partition_id: PartitionId, request: DropRequest) {
        let partition = self.partition_table.live_load().get(&partition_id).cloned();
        let psm = self.partition_store_manager.clone();
        tokio::spawn(
            async move {
                let result = match partition {
                    None => Err(DropPartitionStoreError::UnknownPartition),
                    Some(partition) => {
                        drop_result(psm.preview_drop_partition_store(&partition).await, true)
                    }
                };
                let reason = request.reason.as_str();
                match &result {
                    Ok(DropPartitionStoreOutcome::Dropped { deleted_bytes, .. }) => info!(
                        %partition_id,
                        reason,
                        deleted_bytes =
                            deleted_bytes.map(|bytes| field::display(ByteCount::from(bytes))),
                        "[DRY RUN] Would drop local partition store on request"
                    ),
                    Ok(DropPartitionStoreOutcome::NoStoreFound { .. }) => {
                        debug!(%partition_id, reason, "[DRY RUN] No local partition store to drop");
                    }
                    Err(err) => {
                        warn!(%partition_id, reason, "[DRY RUN] Drop partition store request would fail: {err}");
                    }
                }
                let _ = request.sender.send(result);
            }
            .in_current_tc(),
        );
    }

    fn spawn_drop_partition_store_task(&mut self, partition_id: PartitionId) {
        let partition = self.partition_table.live_load().get(&partition_id).cloned();
        let psm = self.partition_store_manager.clone();

        self.asynchronous_operations
            .build_task()
            .name(&format!("drop-partition-store-{partition_id}"))
            .spawn(
                async move {
                    let result = match partition {
                        None => Err(DropPartitionStoreError::UnknownPartition),
                        Some(partition) => {
                            drop_result(psm.drop_partition_store(&partition).await, false)
                        }
                    };

                    AsynchronousEvent {
                        partition_id,
                        inner: EventKind::PartitionStoreDropped(result),
                    }
                }
                .in_current_tc(),
            )
            .expect("to spawn drop partition store task");
    }

    fn on_partition_store_dropped(&mut self, partition_id: PartitionId, result: DropResult) {
        let pending = self.pending_drops.remove(&partition_id);
        let reason = pending
            .as_ref()
            .map_or("", |pending| pending.reason.as_str());
        match &result {
            Ok(DropPartitionStoreOutcome::Dropped { deleted_bytes, .. }) => {
                info!(
                    %partition_id,
                    reason,
                    deleted_bytes =
                        deleted_bytes.map(|bytes| field::display(ByteCount::from(bytes))),
                    "Dropped local partition store on request"
                );
            }
            Ok(DropPartitionStoreOutcome::NoStoreFound { .. }) => {
                debug!(%partition_id, reason, "No local partition store to drop");
            }
            Err(err) => {
                warn!(%partition_id, reason, "Drop partition store request failed: {err}");
            }
        }
        if result.is_ok() {
            // the partition can only have been blocked on data we just deleted
            gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL => partition_id.to_string(), REASON_LABEL => FLARE_REASON_AHEAD_OF_LOG).set(0);
        }

        if let Some(pending) = pending {
            let _ = pending.sender.send(result);
        }

        // now that the drop is no longer pending, resume normal operation: if we are still a
        // replica of this partition the processor starts over from a snapshot or an empty store
        self.restart_partition_processor_if_replica(partition_id, RestartDelay::Immediate);
        gauge!(NUM_ACTIVE_PARTITIONS).set(self.processor_states.len() as f64);
    }

    fn handle_drop_partition_store_request(
        &mut self,
        request: Incoming<Rpc<DropPartitionStoreRequest>>,
    ) {
        let (sender, rx) = oneshot::channel();
        let (reciprocal, body) = request.split();
        self.on_drop_partition_store(
            body.partition_id,
            DropRequest {
                force: body.force,
                dry_run: body.dry_run,
                min_partition_configuration_version: body.min_partition_configuration_version,
                reason: body.reason,
                sender,
            },
        );
        tokio::spawn(async move {
            let Ok(result) = rx.await else {
                // dropping the reciprocal will notify the sender that the request will not
                // complete.
                return;
            };
            reciprocal.send(DropPartitionStoreResponse::from(result));
        });
    }

    fn handle_create_snapshot_request(&mut self, request: Incoming<Rpc<CreateSnapshotRequest>>) {
        let (sender, rx) = oneshot::channel();
        let (reciprocal, body) = request.split();
        self.on_create_snapshot(body.partition_id, body.min_target_lsn, sender);
        tokio::spawn(async move {
            let Ok(result) = rx.await else {
                // dropping the reciprocal will notify the sender that the request will not
                // complete.
                return;
            };
            match result {
                Ok(snapshot) => reciprocal.send(CreateSnapshotResponse {
                    result: Ok(Snapshot {
                        snapshot_id: snapshot.latest_snapshot_id,
                        log_id: snapshot.log_id,
                        min_applied_lsn: snapshot.archived_lsn,
                    }),
                }),
                Err(err) => reciprocal.send(CreateSnapshotResponse {
                    result: Err(NetSnapshotError::SnapshotCreationFailed(err.to_string())),
                }),
            };
        });
    }

    fn on_replica_set_state_changes(&mut self, replica_set_states: &PartitionReplicaSetStates) {
        let my_node_id = Metadata::with_current(|m| m.my_node_id().as_plain());
        let plan =
            ReconciliationPlan::build(&self.processor_states, replica_set_states, my_node_id);

        if !plan.is_empty() {
            info!("Reconciling partition processors: {plan}");
        }

        for partition_id in plan.starts.keys().copied() {
            self.start_partition_processor(partition_id, None);
        }

        for (&partition_id, planned_stop) in &plan.stops {
            match planned_stop.disposition() {
                StopDisposition::RequestStop => {
                    let Some(processor) = self.processor_states.get_mut(&partition_id) else {
                        continue;
                    };
                    processor.stop();

                    if self.pending_snapshots.contains_key(&partition_id) {
                        info!(%partition_id,
                            "Partition processor stop requested with snapshot task result outstanding"
                        );
                    }
                }
                StopDisposition::ForgetBroken => {
                    self.processor_states.remove(&partition_id);
                }
            }
            self.latest_snapshots.remove(&partition_id);
        }
    }

    /// Whether this node should be running a processor for the given partition, i.e. we are
    /// part of its replica set, its local store is not being dropped, and neither the node nor
    /// the manager is shutting down.
    fn should_run_processor(&self, partition_id: PartitionId) -> bool {
        !is_shutting_down()
            && !self.pending_drops.contains_key(&partition_id)
            && self
                .replica_set_states
                .membership_state(partition_id)
                .contains(Metadata::with_current(|m| m.my_node_id().as_plain()))
    }

    /// Requests a restart if this node should still run the partition processor.
    /// Returns false if the processor is no longer eligible to run.
    fn restart_partition_processor_if_replica(
        &mut self,
        partition_id: PartitionId,
        delay: RestartDelay,
    ) -> bool {
        if is_shutting_down() {
            debug!(%partition_id, "Not restarting partition processor because shutdown is in progress");
            return false;
        }
        if !self.should_run_processor(partition_id) {
            return false;
        }

        self.start_partition_processor(partition_id, delay.next_delay().map(|d| d.add_jitter(0.3)));
        true
    }

    #[instrument(level = "info", skip_all, fields(partition_id = %partition_id))]
    fn start_partition_processor(&mut self, partition_id: PartitionId, delay: Option<Duration>) {
        // Node shutdown starts before the worker's task is cancelled. Reconciliation can still
        // request starts in that window, but task-center no longer accepts new runtimes.
        if is_shutting_down() {
            debug!("Not starting partition processor because shutdown is in progress");
            return;
        }

        if self.pending_drops.contains_key(&partition_id) {
            // dropping the column family under a starting processor would corrupt it
            debug!("Not starting partition processor while its local store is being dropped");
            return;
        }

        let Some(partition) = self.partition_table.live_load().get(&partition_id).cloned() else {
            debug!(
                "Cannot start partition processor because it is not contained in the partition table. Waiting for a partition table update."
            );
            self.wait_for_partition_table_update = true;
            return;
        };

        debug!("Starting new partition processor",);

        let node_ctx = NodeContext::new(
            my_node_id(),
            self.updateable_config.clone(),
            self.replica_set_states.clone(),
            self.rule_book_cache.clone(),
            self.bifrost.clone(),
            self.invoker_capacity.clone(),
            self.leader_handles_registry.clone(),
        );

        let starting_task = SpawnPartitionProcessorTask::new(
            node_ctx,
            format_restring!("pp-{partition_id}"),
            partition,
            self.partition_store_manager.clone(),
            self.fast_forward_on_startup.remove(&partition_id),
            self.ingestion_client.clone(),
        );

        self.asynchronous_operations
            .build_task()
            .name(&format_restring!("start-pp-{partition_id}"))
            .spawn(
                async move {
                    counter!(PARTITION_START, PARTITION_LABEL => partition_id.to_string())
                        .increment(1);
                    let result = starting_task.run(delay);
                    AsynchronousEvent {
                        partition_id,
                        inner: EventKind::Started(result),
                    }
                }
                .in_current_tc(),
            )
            .expect("to spawn starting pp task");

        self.processor_states.insert(
            partition_id,
            ProcessorState::starting(RunMode::Follower, delay),
        );
    }
}

/// Provisions the worker. This entails updating the [`WorkerState`] from provisioning to
/// active in this node's [`NodeConfig`]. Any other [`WorkerState`] will be kept.
async fn provision_worker(metadata_writer: &MetadataWriter) -> anyhow::Result<()> {
    let (my_node_id, nodes_config) =
        Metadata::with_current(|m| (m.my_node_id(), m.nodes_config_ref()));

    let my_node_config = nodes_config.find_node_by_id(my_node_id).context("A newer version of myself must have been started somewhere else or I was removed in the meantime")?;

    match my_node_config.worker_config.worker_state {
        WorkerState::Provisioning => {
            let retry_policy = Configuration::pinned()
                .common
                .network_error_retry_policy
                .clone();

            // We need to use an atomic bool here only because the retry_on_retryable_error closure
            // returns an async block which captures a reference to this variable, and therefore it would
            // escape the closure body. With an atomic bool, we can pass in a simple borrow which can
            // escape the closure body. This can be changed once AsyncFnMut allows us to define Send bounds.
            let first_attempt = AtomicBool::new(true);

            let metadata_client = metadata_writer.global_metadata();

            if let Err(err) = retry_on_retryable_error(retry_policy, || {
                metadata_client.read_modify_write(
                    |nodes_config: Option<Arc<NodesConfiguration>>| {
                        let nodes_config = nodes_config.expect(
                            "nodes config must be present if the node starts the worker role",
                        );

                        let node_config = nodes_config
                            .find_node_by_id(my_node_id)
                            .map_err(ProvisionWorkerError::NewerGenerationOrRemoved)?;

                        if node_config.worker_config.worker_state != WorkerState::Provisioning {
                            return if first_attempt.load(Ordering::Relaxed) {
                                Err(ProvisionWorkerError::NotProvisioning(
                                    node_config.worker_config.worker_state,
                                ))
                            } else {
                                Err(ProvisionWorkerError::PreviousAttemptSucceeded)
                            };
                        }

                        let mut my_node_config = node_config.clone();
                        my_node_config.worker_config.worker_state = WorkerState::Active;

                        let mut new_nodes_config = nodes_config.as_ref().clone();
                        new_nodes_config.upsert_node(my_node_config);
                        new_nodes_config.increment_version();

                        first_attempt.store(false, Ordering::Relaxed);

                        Ok(new_nodes_config)
                    },
                )
            })
            .await
            .map_err(|err| err.map(|err| err.transpose()))
            {
                match err {
                    RetryError::RetriesExhausted(
                        ProvisionWorkerError::PreviousAttemptSucceeded,
                    )
                    | RetryError::NotRetryable(ProvisionWorkerError::PreviousAttemptSucceeded) => {}
                    err => {
                        bail!("failed to update worker state: {}", err);
                    }
                }
            }

            debug_assert!(
                !first_attempt.load(Ordering::Relaxed),
                "Should have tried to set the worker-state at least once"
            );
        }
        WorkerState::Active | WorkerState::Draining | WorkerState::Disabled => {
            // We are also starting the worker if it has been disabled. In this case, no
            // partitions should be placed on this node so that the worker will be idle.
            // However, it is possible to change the state back to active to place partitions on
            // this node again.
        }
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum ProvisionWorkerError {
    #[error(
        "could not find my node config; this indicates a newer version of myself was started somewhere else or I was removed in the meantime: {0}"
    )]
    NewerGenerationOrRemoved(NodesConfigError),
    #[error("worker state is not provisioning but {0}")]
    NotProvisioning(WorkerState),
    #[error("previous attempt succeeded")]
    PreviousAttemptSucceeded,
    #[error(transparent)]
    MetadataClient(#[from] ReadWriteError),
}

/// Whether the node is shutting down or this manager is being cancelled. Processors must not
/// be started then: the task center no longer accepts new runtimes.
fn is_shutting_down() -> bool {
    TaskCenter::is_shutdown_requested() || restate_core::is_cancellation_requested()
}

/// Answers a drop request that this node won't carry out.
fn refuse_drop(partition_id: PartitionId, request: DropRequest, error: DropPartitionStoreError) {
    warn!(
        %partition_id,
        force = request.force,
        reason = request.reason,
        "{}Refusing to drop the local partition store: {error}",
        dry_run_prefix(request.dry_run)
    );
    let _ = request.sender.send(Err(error));
}

/// Marks log messages about dry runs, so they aren't mistaken for actual drops.
fn dry_run_prefix(dry_run: bool) -> &'static str {
    if dry_run { "[DRY RUN] " } else { "" }
}

/// What a drop request returns for the store that the partition store manager dropped, or
/// would drop in a dry run.
fn drop_result<E: std::fmt::Display>(
    result: Result<Option<DroppedPartitionStore>, E>,
    dry_run: bool,
) -> DropResult {
    match result {
        Ok(Some(store)) => Ok(DropPartitionStoreOutcome::Dropped {
            deleted_bytes: store.disk_usage,
            dry_run,
        }),
        Ok(None) => Ok(DropPartitionStoreOutcome::NoStoreFound { dry_run }),
        Err(err) => Err(DropPartitionStoreError::Internal(err.to_string())),
    }
}

struct AsynchronousEvent {
    partition_id: PartitionId,
    inner: EventKind,
}

#[derive(strum::IntoStaticStr)]
enum EventKind {
    Started(
        anyhow::Result<(
            StartedProcessor,
            RuntimeTaskHandle<Result<(), ProcessorError>>,
        )>,
    ),
    Stopped(Result<(), ProcessorError>),
    NewLeaderEpoch {
        leader_epoch_token: LeaderEpochToken,
        result: anyhow::Result<Box<LeadershipInfo>>,
    },
    NewTargetTail {
        tail: Option<Lsn>,
    },
    SnapshotStatusUpdated {
        snapshot_status: PartitionSnapshotStatus,
    },
    SnapshotStatusUpdateSkipped,
    /// A drop request is done waiting for this node to learn about a newer partition
    /// configuration, or gave up waiting.
    DropRequestReady(DropRequest),
    PartitionStoreDropped(DropResult),
}

/// Whether the configured triggers make the partition due for a new snapshot. When both
/// triggers are set, both must be met.
///
/// Expects at least one trigger to be set, as `SnapshotsOptions::automatic_snapshots`
/// guarantees. With neither set, every partition is due.
fn is_snapshot_due(
    partition_id: PartitionId,
    triggers: AutomaticSnapshots,
    latest_snapshot: &PartitionSnapshotStatus,
    processor_status: &PartitionProcessorStatus,
) -> bool {
    let applied_lsn = processor_status
        .last_applied_log_lsn
        .unwrap_or(Lsn::INVALID);
    let next_snapshot_target_lsn = triggers.min_records.map(|target_delta| {
        latest_snapshot
            .latest_snapshot_lsn
            .add(Lsn::from(target_delta.get()))
    });
    let latest_snapshot_age = latest_snapshot.latest_snapshot_created_at.elapsed();

    let due = next_snapshot_target_lsn.is_none_or(|target| applied_lsn >= target)
        && triggers
            .interval
            .is_none_or(|interval| latest_snapshot_age > interval.add_jitter(0.1));
    if !due {
        trace!(
            ?partition_id,
            ?applied_lsn,
            latest_snapshot_lsn = ?latest_snapshot.latest_snapshot_lsn,
            ?latest_snapshot_age,
            ?next_snapshot_target_lsn,
            "Not snapshotting partition yet"
        );
    }
    due
}

fn report_quantile_gauges(metric_name: &'static str, samples: &mut [u64]) {
    const PARTITION_METRIC_QUANTILES: [(&str, f64); 4] =
        [("0.5", 0.5), ("0.9", 0.9), ("0.99", 0.99), ("1.0", 1.0)];

    samples.sort_unstable();

    for (quantile_label, quantile) in PARTITION_METRIC_QUANTILES {
        let value = if samples.is_empty() {
            f64::NAN
        } else {
            let rank = (quantile * samples.len() as f64).ceil() as usize;
            samples[rank.saturating_sub(1)] as f64
        };
        gauge!(metric_name, "quantile" => quantile_label).set(value);
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use googletest::IntoTestResult;
    use test_log::test;
    use tracing::info;

    use restate_bifrost::BifrostService;
    use restate_bifrost::providers::memory_loglet;
    use restate_clock::UniqueTimestamp;
    use restate_core::partitions::PartitionRouting;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_ingestion_client::{IngestionClient, SessionOptions};
    use restate_partition_store::faults::{FaultPoint, Faults};
    use restate_partition_store::snapshots::PartitionSnapshotStatus;
    use restate_partition_store::{PartitionStoreManager, ReadSnapshot};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::fsm_table::{FsmPosition, ReadFsmTable};
    use restate_types::cluster::cluster_state::PartitionProcessorStatus;
    use restate_types::config::{AutomaticSnapshots, Configuration, ThrottlingOptions};
    use restate_types::health::HealthStatus;
    use restate_types::identifiers::PartitionId;
    use restate_types::live::Live;
    use restate_types::logs::builder::LogsBuilder;
    use restate_types::logs::metadata::{LogsConfiguration, ProviderConfiguration};
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::net::address::AdvertisedAddress;
    use restate_types::net::metadata::MetadataKind;
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
    use restate_types::partitions::state::{
        MemberState, PartitionReplicaSetStates, ReplicaSetState,
    };
    use restate_types::rate::Rate;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, RestateVersion, Version};
    use restate_worker_api::ProcessorsManagerHandle;

    use crate::partition_processor_manager::spawn_processor_task;
    use crate::partition_processor_manager::{PartitionProcessorManager, is_snapshot_due};

    #[test]
    fn snapshot_due_triggers() {
        let min_records = NonZeroU64::new(100);
        let interval = Some(Duration::from_secs(3600));
        // Snapshot at LSN 1000; `old` is well past the interval plus its up-to-10% jitter
        let snapshot = |age: Duration| PartitionSnapshotStatus {
            latest_snapshot_lsn: Lsn::new(1000),
            latest_snapshot_created_at: MillisSinceEpoch::now() - age,
            ..PartitionSnapshotStatus::none(LogId::new(0))
        };
        let (fresh, old) = (
            snapshot(Duration::ZERO),
            snapshot(Duration::from_secs(7200)),
        );
        let due = |interval, min_records, latest: &PartitionSnapshotStatus, applied_lsn| {
            let status = PartitionProcessorStatus {
                last_applied_log_lsn: Some(Lsn::new(applied_lsn)),
                ..Default::default()
            };
            let triggers = AutomaticSnapshots {
                interval,
                min_records,
            };
            is_snapshot_due(PartitionId::MIN, triggers, latest, &status)
        };

        // Records only: due once `min_records` were applied since the snapshot, whatever its age
        assert!(!due(None, min_records, &old, 1099));
        assert!(due(None, min_records, &fresh, 1100));
        // Interval only: due once the snapshot is older than the interval, whatever was applied
        assert!(!due(interval, None, &fresh, 5000));
        assert!(due(interval, None, &old, 1000));
        // Both: neither alone is enough
        assert!(!due(interval, min_records, &fresh, 1100));
        assert!(!due(interval, min_records, &old, 1099));
        assert!(due(interval, min_records, &old, 1100));
    }

    #[test(restate_core::test)]
    async fn processor_starts_are_suppressed_before_worker_cancellation() -> anyhow::Result<()> {
        use tokio::sync::oneshot;

        use restate_core::cancellation_watcher;
        use restate_types::net::metadata::MetadataKind;

        use super::RestartDelay;

        let node_id = GenerationalNodeId::new(42, 42);
        let mut env_builder = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_my_node_id(node_id)
            .add_mock_nodes_config();
        RocksDbManager::init();
        let bifrost_svc = BifrostService::new(env_builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let replica_set_states = PartitionReplicaSetStates::default();
        let partition_id = PartitionId::MIN;
        replica_set_states.note_observed_membership(
            partition_id,
            Default::default(),
            &ReplicaSetState {
                version: Version::MIN,
                members: vec![MemberState {
                    node_id: node_id.as_plain(),
                    durable_lsn: Lsn::INVALID,
                }],
            },
            &None,
        );
        let ingestion_client = IngestionClient::new(
            env_builder.networking.clone(),
            env_builder.metadata.updateable_partition_table(),
            PartitionRouting::new(replica_set_states.clone(), TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(),
            SessionOptions::default(),
        );
        let mut manager = PartitionProcessorManager::new(
            HealthStatus::default(),
            Live::from_value(Configuration::default()),
            env_builder.metadata_writer.clone(),
            PartitionStoreManager::create(false).await?,
            replica_set_states,
            &mut env_builder.router_builder,
            bifrost_svc.handle(),
            None,
            ingestion_client,
        );
        let env = env_builder.build().await;
        env.metadata
            .wait_for_version(MetadataKind::PartitionTable, Version::MIN)
            .await?;
        assert!(manager.partition_table.live_load().contains(&partition_id));
        assert!(manager.should_run_processor(partition_id));

        // Hold shutdown before it cancels WorkerRole, with the node-wide flag already set.
        let (reached, waiting) = oneshot::channel();
        let (resume, resumed) = oneshot::channel();
        TaskCenter::spawn(TaskKind::HttpIngressRole, "hold-shutdown", async move {
            cancellation_watcher().await;
            reached.send(()).unwrap();
            resumed.await?;
            Ok(())
        })?;
        let tc = TaskCenter::current();
        let shutdown = tokio::spawn(async move { tc.shutdown_node("test completed", 0).await });
        waiting.await?;
        assert!(TaskCenter::is_shutdown_requested());
        assert!(!restate_core::is_cancellation_requested());

        assert!(!manager.should_run_processor(partition_id));
        assert!(
            !manager.restart_partition_processor_if_replica(partition_id, RestartDelay::Immediate)
        );
        manager.start_partition_processor(partition_id, None);
        assert!(manager.processor_states.is_empty());
        assert!(manager.asynchronous_operations.is_empty());

        resume.send(()).unwrap();
        shutdown.await?;
        Ok(())
    }

    #[test(restate_core::test)]
    async fn drop_partition_store_only_drops_stores_of_former_replicas() -> anyhow::Result<()> {
        use tokio::sync::oneshot;

        use restate_core::network::TransportConnect;
        use restate_types::net::metadata::MetadataKind;
        use restate_types::net::partition_processor_manager::{
            DropPartitionStoreError, DropPartitionStoreOutcome,
        };

        use super::{DropRequest, DropResult};

        fn request_drop<T: TransportConnect>(
            manager: &mut PartitionProcessorManager<T>,
            partition_id: PartitionId,
            min_version: u32,
            dry_run: bool,
        ) -> oneshot::Receiver<DropResult> {
            let (sender, rx) = oneshot::channel();
            manager.on_drop_partition_store(
                partition_id,
                DropRequest {
                    force: false,
                    dry_run,
                    min_partition_configuration_version: Version::from(min_version),
                    reason: "test".to_owned(),
                    sender,
                },
            );
            rx
        }

        async fn answer<T: TransportConnect>(
            manager: &mut PartitionProcessorManager<T>,
            mut rx: oneshot::Receiver<DropResult>,
        ) -> anyhow::Result<DropResult> {
            // dry runs answer from a task of their own, other requests through the manager's
            // asynchronous operations
            loop {
                tokio::select! {
                    result = &mut rx => return Ok(result?),
                    Some(event) = manager.asynchronous_operations.join_next() => {
                        manager.on_asynchronous_event(event?);
                    }
                }
            }
        }

        let node_id = GenerationalNodeId::new(42, 42);
        let mut env_builder = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_my_node_id(node_id)
            .add_mock_nodes_config();
        RocksDbManager::init();
        let bifrost_svc = BifrostService::new(env_builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let replica_set_states = PartitionReplicaSetStates::default();
        let partition_id = PartitionId::MIN;
        let note_replica_set = |version: u32, member: GenerationalNodeId| {
            replica_set_states.note_observed_membership(
                partition_id,
                Default::default(),
                &ReplicaSetState {
                    version: Version::from(version),
                    members: vec![MemberState {
                        node_id: member.as_plain(),
                        durable_lsn: Lsn::INVALID,
                    }],
                },
                &None,
            );
        };
        note_replica_set(1, node_id);
        let partition_store_manager = PartitionStoreManager::create(false).await?;
        let ingestion_client = IngestionClient::new(
            env_builder.networking.clone(),
            env_builder.metadata.updateable_partition_table(),
            PartitionRouting::new(replica_set_states.clone(), TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(),
            SessionOptions::default(),
        );
        let mut manager = PartitionProcessorManager::new(
            HealthStatus::default(),
            Live::from_value(Configuration::default()),
            env_builder.metadata_writer.clone(),
            partition_store_manager.clone(),
            replica_set_states.clone(),
            &mut env_builder.router_builder,
            bifrost_svc.handle(),
            None,
            ingestion_client,
        );
        let env = env_builder.build().await;
        env.metadata
            .wait_for_version(MetadataKind::PartitionTable, Version::MIN)
            .await?;
        let partition = manager
            .partition_table
            .live_load()
            .get(&partition_id)
            .cloned()
            .expect("partition is in the partition table");
        drop(partition_store_manager.open(&partition, None).await?);

        // A replica keeps its store even while it isn't running a processor. A dry run makes
        // the same checks.
        for dry_run in [false, true] {
            let rx = request_drop(&mut manager, partition_id, 1, dry_run);
            assert!(matches!(
                answer(&mut manager, rx).await?,
                Err(DropPartitionStoreError::ReplicaSetMember)
            ));
        }

        // The request waits until this node learns that it left the replica set. A dry run
        // reports the store it would drop, and leaves it in place for the actual drop.
        let rx = request_drop(&mut manager, partition_id, 2, true);
        tokio::task::yield_now().await;
        assert!(
            manager.pending_drops.is_empty(),
            "doesn't block the processor"
        );
        note_replica_set(2, GenerationalNodeId::new(7, 1));
        let Ok(DropPartitionStoreOutcome::Dropped {
            deleted_bytes: Some(would_delete),
            dry_run: true,
        }) = answer(&mut manager, rx).await?
        else {
            panic!("the dry run finds the store");
        };
        let rx = request_drop(&mut manager, partition_id, 2, false);
        assert!(matches!(
            answer(&mut manager, rx).await?,
            Ok(DropPartitionStoreOutcome::Dropped {
                deleted_bytes: Some(deleted),
                dry_run: false,
            }) if deleted == would_delete
        ));
        assert!(manager.processor_states.is_empty());

        let rx = request_drop(&mut manager, partition_id, 2, false);
        assert!(matches!(
            answer(&mut manager, rx).await?,
            Ok(DropPartitionStoreOutcome::NoStoreFound { dry_run: false })
        ));

        // Without the newer configuration, the request gives up after a while.
        tokio::time::pause();
        let rx = request_drop(&mut manager, partition_id, 3, false);
        assert!(matches!(
            answer(&mut manager, rx).await?,
            Err(DropPartitionStoreError::PartitionConfigurationNotObserved { required, observed })
                if required == Version::from(3) && observed == Version::from(2)
        ));
        Ok(())
    }

    #[test(restate_core::test)]
    async fn proper_partition_processor_lifecycle_single_db() -> googletest::Result<()> {
        proper_partition_processor_lifecycle(false).await
    }

    #[test(restate_core::test)]
    async fn proper_partition_processor_lifecycle_multi_db() -> googletest::Result<()> {
        proper_partition_processor_lifecycle(true).await
    }

    /// A running partition processor manager of one node, with in-memory logs. Its
    /// processors start once their partition's membership names the node.
    struct Processors {
        handle: ProcessorsManagerHandle,
        replica_set_states: PartitionReplicaSetStates,
        node_id: GenerationalNodeId,
    }

    async fn start_processors(
        config: Configuration,
        partition_store_manager: Arc<PartitionStoreManager>,
    ) -> googletest::Result<Processors> {
        let mut nodes_config = NodesConfiguration::new_for_testing();
        let node_id = GenerationalNodeId::new(42, 42);
        let node_config = NodeConfig::builder()
            .name("42".to_owned())
            .current_generation(node_id)
            .address(AdvertisedAddress::default())
            .roles(Role::Worker | Role::Admin)
            .binary_version(RestateVersion::current())
            .build();
        nodes_config.upsert_node(node_config);

        let mut env_builder = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_my_node_id(node_id)
            .set_nodes_config(nodes_config);
        let health_status = HealthStatus::default();

        let bifrost_svc = BifrostService::new(env_builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let replica_set_states = PartitionReplicaSetStates::default();

        let ingestion_client = IngestionClient::new(
            env_builder.networking.clone(),
            env_builder.metadata.updateable_partition_table(),
            PartitionRouting::new(replica_set_states.clone(), TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(),
            SessionOptions::default(),
        );

        let partition_processor_manager = PartitionProcessorManager::new(
            health_status,
            Live::from_value(config),
            env_builder.metadata_writer.clone(),
            partition_store_manager,
            replica_set_states.clone(),
            &mut env_builder.router_builder,
            bifrost,
            None,
            ingestion_client,
        );

        // only needed for setting up the metadata
        let env = env_builder.build().await;
        // Match the log default to this fixture's enabled in-memory provider, so
        // that processors can read their logs.
        env.metadata
            .wait_for_version(MetadataKind::Logs, Version::MIN)
            .await?;
        let mut logs = LogsBuilder::try_from(env.metadata.logs_snapshot().as_ref().clone())?;
        logs.set_configuration(LogsConfiguration {
            default_provider: ProviderConfiguration::InMemory,
        });
        env.metadata_writer.update(Arc::new(logs.build())).await?;
        let handle = partition_processor_manager.handle();

        bifrost_svc.start().await.into_test_result()?;
        TaskCenter::spawn(
            TaskKind::SystemService,
            "partition-processor-manager",
            partition_processor_manager.run(),
        )?;
        Ok(Processors {
            handle,
            replica_set_states,
            node_id,
        })
    }

    impl Processors {
        /// Names this node the only member of `partition`, or no member at all.
        fn set_membership(&self, partition: PartitionId, version: Version, member: bool) {
            let members = if member {
                vec![MemberState {
                    node_id: self.node_id.as_plain(),
                    durable_lsn: Lsn::INVALID,
                }]
            } else {
                Vec::new()
            };
            self.replica_set_states.note_observed_membership(
                partition,
                Default::default(),
                &ReplicaSetState { version, members },
                &None,
            );
        }
    }

    /// This test ensures that the lifecycle of partition processors is properly managed by the
    /// [`PartitionProcessorManager`]. See https://github.com/restatedev/restate/issues/2258 for
    /// more details.
    async fn proper_partition_processor_lifecycle(multi_db: bool) -> googletest::Result<()> {
        RocksDbManager::init();
        let Processors {
            handle: processors_manager_handle,
            replica_set_states,
            node_id,
        } = start_processors(
            Configuration::default(),
            PartitionStoreManager::create(multi_db).await?,
        )
        .await?;

        let mut current_replica_set_node = ReplicaSetState {
            version: Version::MIN,
            members: vec![MemberState {
                node_id: node_id.as_plain(),
                durable_lsn: Lsn::INVALID,
            }],
        };

        let mut current_replica_set_empty = ReplicaSetState {
            version: Version::MIN,
            members: vec![],
        };

        let mut version = Version::MIN;

        // let's check whether we can start and stop the partition processor multiple times
        for i in 0..=10 {
            let has_node = i % 2 == 0;
            let current_replica_set = if has_node {
                info!("Starting partition processor");
                current_replica_set_node.version = version;
                &current_replica_set_node
            } else {
                info!("Stopping partition processor");
                current_replica_set_empty.version = version;
                &current_replica_set_empty
            };
            replica_set_states.note_observed_membership(
                PartitionId::MIN,
                Default::default(),
                current_replica_set,
                &None,
            );

            loop {
                let current_state = processors_manager_handle.get_state().await?;

                if current_state.contains_key(&PartitionId::MIN) == has_node {
                    break;
                } else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }

            version = version.next();
        }

        TaskCenter::shutdown_node("test completed", 0).await;
        Ok(())
    }

    /// Writes `n` finished entries to `partition` while no index or statistic is
    /// maintained, so a backfill has to walk them, then closes the store.
    async fn seed_finished_entries(
        stores: &PartitionStoreManager,
        partition: &restate_types::partitions::Partition,
        n: u64,
        position: Option<FsmPosition>,
    ) {
        use restate_clock::RoughTimestamp;
        use restate_storage_api::Transaction;
        use restate_storage_api::invocation_status_table::{
            CompletedInvocation, InvocationStatus, WriteInvocationStatusTable,
        };
        use restate_storage_api::vqueue_table::stats::EntryStatistics;
        use restate_storage_api::vqueue_table::{
            EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
        };
        use restate_types::identifiers::{InvocationId, InvocationUuid};
        use restate_types::vqueues::{EntryId, EntryTargetExt};

        let mut store = stores.open(partition, None).await.unwrap();
        if position.is_some() {
            // A store with a position is not empty: initialize it first, so that its
            // later opens find a storage version to migrate from.
            store
                .verify_and_run_migrations(Default::default(), &Configuration::default())
                .await
                .unwrap();
        }
        let mut tx = store.transaction();
        // With a position, the commit persists it with the batch's bytes counted.
        if let Some(position) = position {
            tx.set_fsm_position(position);
        }
        for i in 0..n {
            let partition_key = partition.key_range.start() + i;
            let mut uuid = [0; 16];
            uuid[..8].copy_from_slice(&i.to_be_bytes());
            let id = InvocationId::from_parts(partition_key, InvocationUuid::from_bytes(uuid));
            let status = CompletedInvocation::mock_neo_with_vqueue(partition_key);
            let qid = status.vqueue_id.clone().unwrap();
            let target = status.invocation_target.clone();
            tx.put_invocation_status(&id, &InvocationStatus::Completed(status))
                .unwrap();
            let key = EntryKey::new(false, RoughTimestamp::new(10), i, EntryId::from(id));
            let stats = EntryStatistics::new(
                UniqueTimestamp::try_from_parts(100, 1).unwrap(),
                key.run_at(),
            );
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target.entry_target_ref(),
                },
                EntryStateRef {
                    stage: Stage::Finished,
                    status: Status::Succeeded,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &stats,
                },
            );
        }
        tx.commit().await.unwrap();
        drop(tx);
        drop(store);
        stores.close(partition.partition_id).await;
    }

    /// Partition 0 of the fixture's partition table, seeded with `entries` finished
    /// entries: a configuration that maintains the status gauges and the entry
    /// indexes, so opening registers them and starts their backfill, and the
    /// partition's faults.
    async fn seeded_partition(
        stores: &PartitionStoreManager,
        entries: u64,
    ) -> (Configuration, Faults) {
        seeded_partition_at(stores, entries, None).await
    }

    /// [`seeded_partition`] with the partition at `position`, so that it tracks
    /// byte offsets.
    async fn seeded_partition_at(
        stores: &PartitionStoreManager,
        entries: u64,
        position: Option<FsmPosition>,
    ) -> (Configuration, Faults) {
        let table = restate_types::partition_table::PartitionTable::with_equally_sized_partitions(
            Version::MIN,
            10,
        );
        let partition = restate_types::partitions::Partition::new(
            PartitionId::MIN,
            table.get(&PartitionId::MIN).unwrap().key_range,
        );
        seed_finished_entries(stores, &partition, entries, position).await;
        let mut config = Configuration::default();
        config.common.experimental.set_track_status_gauges(true);
        (config, stores.faults(&partition))
    }

    /// Background throttling that caps every slice at `keys` entries, at a rate so
    /// high that the cap, not the rate, sizes the slices.
    fn slices_of(keys: u32) -> Option<ThrottlingOptions> {
        Some(ThrottlingOptions {
            rate: Rate::Second(NonZeroU32::MAX),
            capacity: NonZeroU32::new(keys),
        })
    }

    /// Lets the background step held at its pause point go ahead, and waits until
    /// the next one is due and held.
    async fn next_step(faults: &Faults, resume: &mut tokio::sync::oneshot::Sender<()>) {
        let (reached, next) = faults.pause(FaultPoint::WalkStep);
        drop(std::mem::replace(resume, next));
        reached.await.unwrap();
    }

    /// Whether the service gauges are Ready in a new snapshot of `store`.
    fn ready(store: &restate_partition_store::PartitionStore) -> bool {
        use restate_partition_store::registry::IndexKind;

        let snapshot = store.query_read(Default::default()).unwrap().0;
        snapshot
            .ready_index("service_stats/1", IndexKind::Aggregate)
            .is_ok()
    }

    /// The service-gauge total of a snapshot read: entries counted once each.
    fn service_total(snapshot: &ReadSnapshot) -> u64 {
        use std::ops::ControlFlow;

        use restate_partition_store::keys::selection::KeySelection;
        use restate_partition_store::stats::StatValueCodec;
        use restate_partition_store::stats::aggregated::{ServiceStats, StageStatusCounts};
        use restate_storage_query_api::scan::BlockingScan;

        let mut total = 0;
        <ReadSnapshot as BlockingScan<ServiceStats>>::scan_blocking(
            snapshot,
            Arc::new(KeySelection::all()),
            |(_, payload)| {
                let counts = StageStatusCounts::deserialize_from(payload).unwrap();
                total += counts.iter().map(|(_, count)| count).sum::<u64>();
                ControlFlow::Continue(())
            },
        )
        .unwrap();
        total
    }

    /// Statistics backfill in a real processor: opening registers the
    /// gauges and starts their walk, and the processor's backfill arm
    /// runs it step by step while the processor serves. This fixture's processor
    /// applies no record and serves no RPC, so every step counts as quiet. The
    /// test holds every step at its pause point, so nothing depends on timing.
    #[test(restate_core::test(flavor = "multi_thread", worker_threads = 4))]
    async fn background_backfill_progresses_step_by_step_and_publishes_ready()
    -> googletest::Result<()> {
        RocksDbManager::init();
        let stores = PartitionStoreManager::create(true).await?;
        let (mut config, faults) = seeded_partition(&stores, 300).await;
        config.worker.storage.background_migrations_throttling = slices_of(16);
        let (reached, mut resume) = faults.pause(FaultPoint::WalkStep);
        let processors = start_processors(config, Arc::clone(&stores)).await?;
        processors.set_membership(PartitionId::MIN, Version::MIN, true);
        reached.await.unwrap();
        // The observer reads the store directly, not through the processor.
        let store = stores.get_partition_store(PartitionId::MIN).await.unwrap();
        assert!(!ready(&store));
        assert_eq!(
            store.walk_stats(),
            restate_partition_store::walk::WalkStatsSnapshot::default()
        );

        // Each step takes one slice of the throttling's size.
        for step in 1..=3 {
            next_step(&faults, &mut resume).await;
            let stats = store.walk_stats();
            assert_eq!(
                (stats.quiet_steps, stats.busy_steps, stats.examined),
                (step, 0, 16 * step),
                "{stats:?}"
            );
        }
        assert!(!ready(&store));

        // Unheld, the backfill runs to its end: Ready, each entry counted once.
        let (idle, _) = faults.pause(FaultPoint::WalksIdle);
        drop(resume);
        idle.await.unwrap();
        assert!(ready(&store));
        let snapshot = store.query_read(Default::default()).unwrap().0;
        assert_eq!(service_total(&snapshot), 300);
        let done = store.walk_stats();
        assert_eq!(
            (done.examined, done.busy_steps, done.failures),
            (300, 0, 0),
            "{done:?}"
        );
        drop((snapshot, store));
        TaskCenter::shutdown_node("test completed", 0).await;
        Ok(())
    }

    /// The status a processor reports once its partition store byte offset is
    /// `offset`: the processor refreshes its position after a commit and
    /// publishes the status on its timer.
    async fn status_at_store_offset(
        handle: &ProcessorsManagerHandle,
        offset: u64,
    ) -> PartitionProcessorStatus {
        loop {
            if let Some(status) = handle.get_state().await.unwrap().get(&PartitionId::MIN)
                && status.partition_store_byte_offset == Some(offset)
            {
                return status.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A background commit alone advances the store byte offset the processor's
    /// status reports, to the persisted value, while the applied LSN and the log
    /// byte offset stay: the processor refreshes its position after the step.
    #[test(restate_core::test(flavor = "multi_thread", worker_threads = 4))]
    async fn background_backfill_commits_advance_the_reported_store_offset()
    -> googletest::Result<()> {
        RocksDbManager::init();
        let stores = PartitionStoreManager::create(true).await?;
        // A partition that tracks byte offsets from its first record, none applied
        // yet: its log is empty, so nothing is ahead of it.
        let position = FsmPosition {
            applied_lsn: Lsn::INVALID,
            byte_offsets_base_lsn: Lsn::OLDEST,
            log_byte_offset: 0,
            partition_store_byte_offset: 0,
        };
        let (mut config, faults) = seeded_partition_at(&stores, 300, Some(position)).await;
        config.worker.storage.background_migrations_throttling = slices_of(16);
        let (reached, mut resume) = faults.pause(FaultPoint::WalkStep);
        let processors = start_processors(config, Arc::clone(&stores)).await?;
        processors.set_membership(PartitionId::MIN, Version::MIN, true);
        reached.await.unwrap();
        let mut store = stores.get_partition_store(PartitionId::MIN).await.unwrap();
        let before = store.get_fsm_position().await?.unwrap();
        assert!(before.partition_store_byte_offset > 0, "{before:?}");
        let reported =
            status_at_store_offset(&processors.handle, before.partition_store_byte_offset).await;
        assert_eq!(reported.byte_offsets_base_lsn, Some(Lsn::OLDEST));

        // One step commits a slice through the processor's transaction.
        next_step(&faults, &mut resume).await;
        let after = store.get_fsm_position().await?.unwrap();
        assert!(
            after.partition_store_byte_offset > before.partition_store_byte_offset,
            "{before:?} {after:?}"
        );
        assert_eq!(
            (
                after.applied_lsn,
                after.byte_offsets_base_lsn,
                after.log_byte_offset
            ),
            (
                before.applied_lsn,
                before.byte_offsets_base_lsn,
                before.log_byte_offset
            )
        );
        let reported =
            status_at_store_offset(&processors.handle, after.partition_store_byte_offset).await;
        assert_eq!(
            (reported.byte_offsets_base_lsn, reported.log_byte_offset),
            (Some(Lsn::OLDEST), Some(0)),
            "{reported:?}"
        );
        drop((resume, store));
        TaskCenter::shutdown_node("test completed", 0).await;
        Ok(())
    }

    /// A snapshot export of a partition whose processor is backfilling. While the
    /// export holds the processor's commit fence, each due backfill step commits
    /// nothing and only schedules a recheck; once the export released the fence,
    /// the backfill resumes and completes. The test holds every step at its pause
    /// point and lets them go one at a time, so nothing depends on timing.
    #[test(restate_core::test(flavor = "multi_thread", worker_threads = 4))]
    async fn background_backfill_waits_out_a_held_export_fence() -> googletest::Result<()> {
        use restate_partition_store::walk::WalkStatsSnapshot;
        use restate_types::identifiers::SnapshotId;

        RocksDbManager::init();
        let (fences_tx, mut fences) = tokio::sync::mpsc::unbounded_channel();
        *spawn_processor_task::FENCE_TAP.lock() = Some(fences_tx);
        let stores = PartitionStoreManager::create(true).await?;
        let (config, faults) = seeded_partition(&stores, 300).await;
        let (reached, mut resume) = faults.pause(FaultPoint::WalkStep);
        let processors = start_processors(config, Arc::clone(&stores)).await?;
        processors.set_membership(PartitionId::MIN, Version::MIN, true);
        let (_, fence_sender) = fences.recv().await.unwrap();

        // The first step takes the walk's first slice.
        reached.await.unwrap();
        let store = stores.get_partition_store(PartitionId::MIN).await.unwrap();
        assert_eq!(store.walk_stats(), WalkStatsSnapshot::default());
        next_step(&faults, &mut resume).await;
        let progressed = store.walk_stats();
        assert!(
            progressed.examined > 0 && progressed.examined < 300,
            "{progressed:?}"
        );

        // The export of the partition, paused once its fence holds. The held step
        // does not hold the processor, which grants the fence.
        let (captured, release) = faults.pause(FaultPoint::SnapshotCapture);
        let exports = restate_test_util::tempdir().unwrap();
        let export = tokio::spawn({
            let stores = Arc::clone(&stores);
            let path = exports.path().join("export");
            async move {
                stores
                    .export_partition(
                        PartitionId::MIN,
                        Some(&fence_sender),
                        None,
                        SnapshotId::new(),
                        &path,
                    )
                    .await
            }
        });
        captured.await.unwrap();
        assert_eq!(store.walk_stats(), progressed);
        // Every due step skips without work, and schedules the next.
        for skips in 1..=3 {
            next_step(&faults, &mut resume).await;
            assert_eq!(
                store.walk_stats(),
                WalkStatsSnapshot {
                    fence_skips: progressed.fence_skips + skips,
                    ..progressed
                },
            );
        }

        drop(release);
        // This fixture's processor applies no record, so the export, once released,
        // stops at its applied-LSN precondition before capturing; that releases the
        // fence just as a completed capture does.
        assert!(matches!(
            export.await.unwrap(),
            Err(restate_partition_store::SnapshotError {
                kind: restate_partition_store::SnapshotErrorKind::Export(_),
                ..
            })
        ));
        // Unheld from here, the backfill runs to its end, each entry examined once.
        let (idle, _) = faults.pause(FaultPoint::WalksIdle);
        drop(resume);
        idle.await.unwrap();
        let done = store.walk_stats();
        assert_eq!((done.examined, done.fence_skips), (300, 3), "{done:?}");
        assert!(ready(&store));
        let snapshot = store.query_read(Default::default()).unwrap().0;
        assert_eq!(service_total(&snapshot), 300);
        drop((snapshot, store));
        TaskCenter::shutdown_node("test completed", 0).await;
        Ok(())
    }

    /// Processors replaced in the middle of a backfill commit: one while a slice's
    /// native write is queued, which is then skipped, and one after a slice's write
    /// but before its publication. Each replacement reloads the committed walk and
    /// indexes on reopen, and the backfill completes with every entry counted
    /// once. Pause points sequence every step, so nothing depends on timing.
    #[test(restate_core::test(flavor = "multi_thread", worker_threads = 4))]
    async fn background_backfill_survives_processor_replacement_mid_commit()
    -> googletest::Result<()> {
        use restate_rocksdb::Priority;

        let rocksdb = RocksDbManager::init();
        let stores = PartitionStoreManager::create(true).await?;
        let (mut config, faults) = seeded_partition(&stores, 300).await;
        config.worker.storage.background_migrations_throttling = slices_of(16);
        // Commits queue on the storage pool, where a paused pool holds them. Commits
        // read this option from the global configuration.
        config.worker.storage.always_commit_in_background = true;
        restate_types::config::set_current_config(config.clone());
        let (reached, mut resume) = faults.pause(FaultPoint::WalkStep);
        let processors = start_processors(config, Arc::clone(&stores)).await?;
        let mut version = Version::MIN;
        let mut member = |member: bool| {
            processors.set_membership(PartitionId::MIN, version, member);
            version = version.next();
        };
        let examined = async || {
            stores
                .get_partition_store(PartitionId::MIN)
                .await
                .unwrap()
                .walk_stats()
                .examined
        };

        // The walk's first slice commits.
        member(true);
        reached.await.unwrap();
        next_step(&faults, &mut resume).await;
        assert_eq!(examined().await, 16);

        // The next slice's native write is queued on the held pool when its
        // processor stops, dropping the commit. The write is then skipped.
        let paused = rocksdb.pause_background(Priority::High).await;
        let (queued, mut write) = faults.pause(FaultPoint::NativeWriteQueued);
        drop(resume);
        queued.await.unwrap();
        member(false);
        write.closed().await;
        drop(paused);

        // The replacement resumes after the committed slice. Its first slice's
        // write completes, and its processor stops before the publication.
        let (reached, resume) = faults.pause(FaultPoint::WalkStep);
        member(true);
        reached.await.unwrap();
        assert_eq!(examined().await, 16, "the dropped slice charged nothing");
        let (written, mut publication) = faults.pause(FaultPoint::WalkPublication);
        drop(resume);
        written.await.unwrap();
        member(false);
        publication.closed().await;

        // The next replacement installs that slice from disk and continues after
        // it: only the remaining entries are examined again.
        let (idle, _) = faults.pause(FaultPoint::WalksIdle);
        member(true);
        idle.await.unwrap();
        let store = stores.get_partition_store(PartitionId::MIN).await.unwrap();
        assert_eq!(store.walk_stats().examined, 300 - 16);
        assert!(ready(&store));
        let snapshot = store.query_read(Default::default()).unwrap().0;
        assert_eq!(service_total(&snapshot), 300, "every entry counted once");
        drop((snapshot, store));
        TaskCenter::shutdown_node("test completed", 0).await;
        Ok(())
    }
}
