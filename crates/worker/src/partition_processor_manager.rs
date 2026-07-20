// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod apply_progress_tracker;
mod introspection;
mod processor_state;
mod spawn_processor_task;

pub use introspection::{LeaderQueryGuard, PartitionLeaderHandlesRegistry};

use apply_progress_tracker::{
    HeartbeatView, LoopState, ProbeResult, TailObservation, TickInput, TrackerEffect, TrackerEntry,
    pick_next_consistent_read_sweep_target,
};

use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::ops::Add;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ahash::{HashMap, HashSet};
use anyhow::{Context, bail};
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::{Either, Itertools};
use metrics::{counter, gauge};
use rand::RngExt;
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, error, info, info_span, instrument, trace, warn};
use ulid::Ulid;

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
use restate_metadata_server::{MetadataStoreClient, ReadModifyWriteError};
use restate_metadata_store::{ReadWriteError, RetryError, retry_on_retryable_error};
use restate_partition_store::PartitionStoreManager;
use restate_partition_store::snapshots::{
    PartitionSnapshotStatus, SnapshotPartitionTask, SnapshotRepository,
};
use restate_partition_store::{SnapshotError, SnapshotErrorKind};
use restate_types::GenerationalNodeId;
use restate_types::cluster::cluster_state::ReplayStatus;
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
use restate_types::config::{Configuration, StallDetectionOptions};
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
    PartitionManagerService, ProcessorCommand, Snapshot, SnapshotError as NetSnapshotError,
};
use restate_types::net::{RpcRequest as _, UnaryMessage};
use restate_types::nodes_config::{NodesConfigError, NodesConfiguration, WorkerState};
use restate_types::partition_table::PartitionTable;
use restate_types::partitions::Partition;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::protobuf::common::WorkerStatus;
use restate_types::time::MillisSinceEpoch;
use restate_util_string::format_restring;
use restate_util_time::DurationExt;
use restate_wal_protocol::Envelope;
use restate_worker_api::invoker::capacity::InvokerCapacity;
use restate_worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};

use crate::metric_definitions::PARTITION_STOP_STUCK;
use crate::metric_definitions::{
    ERROR_STOP, FLARE_REASON_MIGRATION_BARRIER, FLARE_REASON_SNAPSHOT_UNAVAILABLE,
    FLARE_REASON_VERSION_BARRIER, GAP_STOP, PARTITION_BLOCKED_FLARE, PARTITION_IS_EFFECTIVE_LEADER,
    PARTITION_START, REASON_LABEL, STARTUP_ERROR_STOP, TYPE_LABEL,
};
use crate::metric_definitions::{NORMAL_STOP, PARTITION_TIME_SINCE_LAST_STATUS_UPDATE};
use crate::metric_definitions::{NUM_ACTIVE_PARTITIONS, PARTITION_APPLIED_LSN_LAG};
use crate::metric_definitions::{NUM_PARTITIONS, SNAPSHOT_AGE};
use crate::metric_definitions::{
    PARTITION_APPLY_PHASE_STUCK, PARTITION_APPLY_STALLED, PHASE_LABEL,
};
use crate::metric_definitions::{PARTITION_LABEL, PARTITION_STOP};
use crate::partition::leadership;
use crate::partition::{LeadershipInfo, NodeContext, ProcessorError};
use crate::partition_processor_manager::processor_state::{
    LeaderEpochToken, ProcessorState, StartedProcessor,
};
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
    tail_observations: HashMap<PartitionId, TailObservation>,
    leader_handles_registry: PartitionLeaderHandlesRegistry,

    /// Apply-stall detection (see `apply_progress_tracker`): per-partition tracker state, plus
    /// the local view of each partition's `LoopHeartbeat`. Both are sticky across incarnations by
    /// design and are only dropped once a partition both stops running here and leaves this
    /// node's replica set (rule 9 in the design doc).
    trackers: HashMap<PartitionId, TrackerEntry>,
    heartbeat_views: HashMap<PartitionId, HeartbeatView>,
    /// At most one `ConsistentRead` sweep probe in flight node-wide at a time.
    consistent_read_sweep_inflight: bool,
    /// When each currently-`Stopping` partition was first observed in that state, for the
    /// `stop_stuck_timeout` flare. Never spawns a second processor -- observability only.
    stopping_since: HashMap<PartitionId, Instant>,

    asynchronous_operations: JoinSet<AsynchronousEvent>,

    pending_snapshots: HashMap<PartitionId, PendingSnapshotTask>,
    latest_snapshots: HashMap<PartitionId, PartitionSnapshotStatus>,
    pending_snapshot_status_refreshes: HashSet<PartitionId>,
    snapshot_export_tasks: FuturesUnordered<TaskHandle<SnapshotResultInternal>>,
    snapshot_repository: Option<SnapshotRepository>,
    fast_forward_on_startup: HashMap<PartitionId, Lsn>,

    partition_table: Live<PartitionTable>,
    wait_for_partition_table_update: bool,

    invoker_capacity: InvokerCapacity,

    ingestion_client: IngestionClient<T, Envelope>,

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

enum RestartDelay {
    Immediate,
    Fixed,
    Exponential {
        // `ProcessorState::start_time` (processor_state.rs) is std::time::Instant; kept as such
        // here too rather than the tokio::time::Instant used for the apply-stall tracker's
        // paused-clock-aware deadline math below.
        start_time: std::time::Instant,
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
        ingestion_client: IngestionClient<T, Envelope>,
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
            tail_observations: HashMap::default(),
            leader_handles_registry: PartitionLeaderHandlesRegistry::default(),
            trackers: HashMap::default(),
            heartbeat_views: HashMap::default(),
            consistent_read_sweep_inflight: false,
            stopping_since: HashMap::default(),
            asynchronous_operations: JoinSet::default(),
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

    pub fn leader_handles_registry(&self) -> PartitionLeaderHandlesRegistry {
        self.leader_handles_registry.clone()
    }

    pub fn handle(&self) -> ProcessorsManagerHandle {
        ProcessorsManagerHandle::new(self.tx.clone())
    }

    pub fn rule_book_cache_handle(&self) -> RuleBookCacheHandle {
        self.rule_book_cache.clone()
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());

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

        // Explicit deadline driver for the apply-progress tracker: fires all time-based
        // transitions (grace, recovery window, bail, probe backoff) even if tail queries stop
        // completing entirely -- the 1s Fast poller above only ever *launches* queries.
        let mut tracker_tick = tokio::time::interval(Duration::from_millis(500));
        tracker_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Mandatory ConsistentRead sweep (A5 fairness): the Fast poller above can be looking at
        // a frozen cached tail forever once its background refresher exits, which would hide a
        // stall from detection indefinitely -- see the apply_progress_tracker module doc.
        let mut consistent_read_sweep = tokio::time::interval(
            self.updateable_config
                .live_load()
                .worker
                .stall_detection
                .sweep_interval(),
        );
        consistent_read_sweep.set_missed_tick_behavior(MissedTickBehavior::Delay);

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
                _ = tracker_tick.tick(), if self.updateable_config.live_load().worker.stall_detection.enabled => {
                    self.evaluate_trackers();
                }
                _ = consistent_read_sweep.tick(), if self.updateable_config.live_load().worker.stall_detection.enabled && !self.consistent_read_sweep_inflight => {
                    self.spawn_consistent_read_sweep();
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

        for task in self.snapshot_export_tasks.iter() {
            task.cancel();
        }

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
                                    self.on_processor_incarnation_started(partition_id);

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
                        // todo: metrics
                        info!(%partition_id, %err, "Partition processor failed to start");
                        self.processor_states.remove(&partition_id);
                        if !self.restart_partition_processor_if_replica(
                            partition_id,
                            RestartDelay::Fixed,
                        ) {
                            self.gc_tracker(partition_id);
                        }
                    }
                }
                gauge!(NUM_ACTIVE_PARTITIONS).set(self.processor_states.len() as f64);
            }
            EventKind::Stopped(result) => {
                self.unregister_pp_rpc_shard(partition_id);
                let delay = match self.processor_states.remove(&partition_id) {
                    None => {
                        debug!("Stopped partition processor which is no longer running.");
                        // immediately try to restart if we are still part of the partition's membership
                        counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => NORMAL_STOP).increment(1);
                        RestartDelay::Immediate
                    }
                    Some(processor_state) => match processor_state {
                        ProcessorState::Starting { .. } => {
                            counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => STARTUP_ERROR_STOP).increment(1);
                            warn!(%partition_id, "Partition processor failed to start: {result:?}");
                            RestartDelay::Fixed
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
                                    RestartDelay::MaxBackoff
                                }
                                Err(e @ ProcessorError::MigrationBarrier { .. }) => {
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => ERROR_STOP).increment(1);
                                    gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL => partition_id.to_string(), REASON_LABEL => FLARE_REASON_MIGRATION_BARRIER).set(1);
                                    error!(%partition_id, "Partition processor start error: {e}");
                                    RestartDelay::MaxBackoff
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

                                        RestartDelay::Immediate
                                    } else {
                                        error!(
                                            %partition_id,
                                            "Partition processor stopped due to a log gap [{sequence_number}..{to_lsn}], and no snapshot repository is configured. \
                                                Cannot recover without a partition snapshot!",
                                        );
                                        gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL => partition_id.to_string(), REASON_LABEL => FLARE_REASON_SNAPSHOT_UNAVAILABLE).set(1);
                                        // configuration problem; until we have peer-to-peer state exchange we can only wait
                                        RestartDelay::MaxBackoff
                                    }
                                }
                                Err(ProcessorError::ActionEffect(
                                    leadership::Error::AnnounceNotApplied {
                                        leader_epoch,
                                        committed_for,
                                        bail_lsn,
                                    },
                                )) => {
                                    // A2/A4 (Option D): our own committed AnnounceLeader marker
                                    // never applied -- authoritative lag evidence in its own
                                    // right. Quarantine the same way an A-bail would, using the
                                    // exact FSM LSN captured at the watchdog's deadline.
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => ERROR_STOP).increment(1);
                                    warn!(
                                        %partition_id, %leader_epoch, committed_for = ?committed_for, %bail_lsn,
                                        "Partition processor's committed AnnounceLeader marker was never applied; quarantining and restarting"
                                    );

                                    let now = Instant::now();
                                    let cfg =
                                        self.updateable_config.live_load().worker.stall_detection;
                                    let is_first_bail = !self.is_apply_stalled(partition_id);
                                    let tracker =
                                        self.trackers.entry(partition_id).or_insert_with(|| {
                                            TrackerEntry::new(Ulid::new(), now, &cfg)
                                        });
                                    tracker.on_announce_not_applied(now, *bail_lsn, &cfg);

                                    if is_first_bail {
                                        RestartDelay::Fixed
                                    } else {
                                        RestartDelay::Exponential {
                                            start_time,
                                            last_delay: delay,
                                        }
                                    }
                                }
                                Err(err) => {
                                    let next_delay = RestartDelay::Exponential {
                                        start_time,
                                        last_delay: delay,
                                    };
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => ERROR_STOP).increment(1);
                                    error!(%partition_id, %err, "Partition processor exited unexpectedly, {}", next_delay);
                                    next_delay
                                }
                                Ok(_) => {
                                    counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => NORMAL_STOP).increment(1);
                                    info!(%partition_id, "Partition processor stopped");
                                    RestartDelay::Immediate
                                }
                            }
                        }
                        ProcessorState::Stopping { processor, .. } => {
                            if let Some(processor) = processor {
                                self.leader_handles_registry
                                    .unregister_all(processor.key_range());
                            }
                            counter!(PARTITION_STOP, PARTITION_LABEL => partition_id.to_string(), TYPE_LABEL => NORMAL_STOP).increment(1);
                            RestartDelay::Immediate
                        }
                    },
                };

                if !self.restart_partition_processor_if_replica(partition_id, delay) {
                    debug!("Partition processor stopped: {result:?}");
                    // No longer a replica for this partition: safe to forget its apply-progress
                    // tracker (rule 9 -- retained until both conditions hold).
                    self.gc_tracker(partition_id);
                }

                gauge!(NUM_ACTIVE_PARTITIONS).set(self.processor_states.len() as f64);
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
                    self.tail_observations.remove(&partition_id);
                    return;
                };

                let now = Instant::now();
                match self.tail_observations.entry(partition_id) {
                    Entry::Occupied(mut o) => o.get_mut().observe_fast(tail_lsn, now),
                    Entry::Vacant(v) => {
                        v.insert(TailObservation::new(tail_lsn, now));
                    }
                }
            }
            EventKind::ConsistentReadSweepCompleted { probed, result } => {
                self.consistent_read_sweep_inflight = false;
                let now = Instant::now();
                let Some(probed) = probed else {
                    // Nothing running to sweep this round.
                    return;
                };
                let Some(obs) = self.tail_observations.get_mut(&probed) else {
                    return;
                };
                match result {
                    Some(tail_lsn) => obs.observe_consistent(tail_lsn, now),
                    None => obs.mark_consistent_attempt(now),
                }
            }
            EventKind::ConsistentReadProbeCompleted {
                incarnation,
                nonce,
                result,
            } => {
                let now = Instant::now();
                if let Some(obs) = self.tail_observations.get_mut(&partition_id)
                    && let Some(tail_lsn) = result.as_ref().and_then(|r| match r {
                        ProbeResult::Confirmed { tail_lsn } => Some(*tail_lsn),
                        _ => None,
                    })
                {
                    obs.observe_consistent(tail_lsn, now);
                } else if let Some(obs) = self.tail_observations.get_mut(&partition_id) {
                    obs.mark_consistent_attempt(now);
                }
                if let Some(tracker) = self.trackers.get_mut(&partition_id) {
                    let cfg = self.updateable_config.live_load().worker.stall_detection;
                    tracker.on_probe_result(
                        now,
                        incarnation,
                        nonce,
                        result.unwrap_or(ProbeResult::Failed),
                        &cfg,
                    );
                }
            }
            EventKind::SnapshotStatusUpdated { snapshot_status } => {
                self.pending_snapshot_status_refreshes.remove(&partition_id);
                self.update_snapshot_status(partition_id, snapshot_status);
            }
            EventKind::SnapshotStatusUpdateSkipped => {
                self.pending_snapshot_status_refreshes.remove(&partition_id);
                // No-op: no snapshot found or error fetching status (logged upstream)
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

    /// Rule 8: a fresh processor incarnation started running for `partition_id`. Creates a new
    /// apply-progress tracker on first start, or resets live detection (retaining the sticky
    /// quarantine and restart history) on every subsequent restart.
    fn on_processor_incarnation_started(&mut self, partition_id: PartitionId) {
        let now = Instant::now();
        let cfg = self.updateable_config.live_load().worker.stall_detection;
        let incarnation = Ulid::new();
        match self.trackers.entry(partition_id) {
            Entry::Occupied(mut e) => e.get_mut().on_incarnation_started(now, incarnation),
            Entry::Vacant(v) => {
                v.insert(TrackerEntry::new(incarnation, now, &cfg));
            }
        }
        self.heartbeat_views
            .insert(partition_id, HeartbeatView::new(now));
    }

    /// Rule 9 (GC): forget a partition's apply-progress tracker once it has both stopped running
    /// here and left this node's replica set. Called only from that combined condition.
    fn gc_tracker(&mut self, partition_id: PartitionId) {
        self.trackers.remove(&partition_id);
        self.heartbeat_views.remove(&partition_id);
        self.tail_observations.remove(&partition_id);
    }

    /// The explicit deadline driver for the apply-progress tracker (`tracker_tick`): evaluates
    /// every running partition's tracker against its current status, tail observation, and
    /// heartbeat, and acts on the resulting effect (issue a confirmation probe, or bail).
    fn evaluate_trackers(&mut self) {
        let now = Instant::now();
        let cfg = self.updateable_config.live_load().worker.stall_detection;

        self.evaluate_stop_stuck(now, &cfg);

        let partition_ids: Vec<PartitionId> = self.processor_states.keys().cloned().collect();
        for partition_id in partition_ids {
            let Some(ProcessorState::Started {
                processor: Some(started),
                ..
            }) = self.processor_states.get(&partition_id)
            else {
                continue;
            };

            let Some(status) = self
                .processor_states
                .get(&partition_id)
                .and_then(ProcessorState::partition_processor_status)
            else {
                continue;
            };

            let heartbeat_view = self
                .heartbeat_views
                .entry(partition_id)
                .or_insert_with(|| HeartbeatView::new(now));
            heartbeat_view.observe(started.heartbeat().sample(), now);
            let loop_state = heartbeat_view.loop_state(now, &cfg);

            let phase_age = heartbeat_view.phase_age(now);
            gauge!(
                PARTITION_APPLY_PHASE_STUCK,
                PARTITION_LABEL => partition_id.to_string(),
                PHASE_LABEL => format!("{:?}", heartbeat_view.phase())
            )
            .set(
                if loop_state == LoopState::Busy && phase_age >= cfg.hard_grace() {
                    1.0
                } else {
                    0.0
                },
            );

            let fresh_tail = self
                .tail_observations
                .get(&partition_id)
                .filter(|obs| obs.is_fresh(now, cfg.tail_ttl()))
                .map(TailObservation::lsn);

            let Some(tracker) = self.trackers.get_mut(&partition_id) else {
                continue;
            };
            let input = TickInput {
                last_applied_lsn: status.last_applied_log_lsn,
                replay_status: status.replay_status,
                loop_state,
                fresh_tail,
            };
            let effect = tracker.on_sample(now, input, &cfg);
            self.act_on_tracker_effect(partition_id, effect);
        }
    }

    /// Flares `restate.partition.stop_stuck` for a partition that has been `Stopping` for longer
    /// than `stop_stuck_timeout`. Observability only: per the builder guardrails, this never
    /// spawns a second processor over the same store or forces a process exit -- a genuinely
    /// wedged cooperative shutdown needs external supervision.
    fn evaluate_stop_stuck(&mut self, now: Instant, cfg: &StallDetectionOptions) {
        let stopping: HashSet<PartitionId> = self
            .processor_states
            .iter()
            .filter(|(_, state)| matches!(state, ProcessorState::Stopping { .. }))
            .map(|(id, _)| *id)
            .collect();

        self.stopping_since.retain(|id, _| stopping.contains(id));
        for &partition_id in &stopping {
            let since = *self.stopping_since.entry(partition_id).or_insert(now);
            let stuck = now.saturating_duration_since(since) >= cfg.stop_stuck_timeout();
            let labels = [(PARTITION_LABEL, partition_id.to_string())];
            gauge!(PARTITION_STOP_STUCK, &labels).set(if stuck { 1.0 } else { 0.0 });
            if stuck {
                error!(%partition_id, "Partition processor has been Stopping for longer than stop_stuck_timeout; it may require external supervision");
            }
        }
    }

    fn act_on_tracker_effect(&mut self, partition_id: PartitionId, effect: TrackerEffect) {
        match effect {
            TrackerEffect::None => {}
            TrackerEffect::IssueProbe { incarnation, nonce } => {
                self.spawn_consistent_read_probe(partition_id, incarnation, nonce);
            }
            TrackerEffect::Bail => {
                warn!(%partition_id, "Apply-stall detected: cooperatively restarting the partition processor");
                if let Some(processor_state) = self.processor_states.get_mut(&partition_id) {
                    processor_state.stop();
                }
            }
        }
    }

    /// Spawns the confirmation probe a suspected partition's tracker requested. Bounded by
    /// `probe_timeout`; a timeout is reported as a probe failure, matching the guardrail that no
    /// action may ever be taken on unconfirmed evidence.
    fn spawn_consistent_read_probe(
        &mut self,
        partition_id: PartitionId,
        incarnation: Ulid,
        nonce: u64,
    ) {
        let bifrost = self.bifrost.clone();
        let probe_timeout = self
            .updateable_config
            .live_load()
            .worker
            .stall_detection
            .probe_timeout();

        self.asynchronous_operations.spawn(
            async move {
                let log_id = Metadata::with_current(|m| {
                    m.partition_table_ref()
                        .get(&partition_id)
                        .map(Partition::log_id)
                });
                let result = match log_id {
                    Some(log_id) => {
                        match tokio::time::timeout(
                            probe_timeout,
                            bifrost.find_tail(log_id, FindTailOptions::ConsistentRead),
                        )
                        .await
                        {
                            Ok(Ok(tail)) => Some(ProbeResult::Confirmed {
                                tail_lsn: tail.offset(),
                            }),
                            Ok(Err(_)) | Err(_) => Some(ProbeResult::Failed),
                        }
                    }
                    None => Some(ProbeResult::Failed),
                };

                AsynchronousEvent {
                    partition_id,
                    inner: EventKind::ConsistentReadProbeCompleted {
                        incarnation,
                        nonce,
                        result,
                    },
                }
            }
            .in_current_tc(),
        );
    }

    /// The mandatory `ConsistentRead` sweep (A5): picks the running partition with the oldest
    /// `last_consistent_attempt_at` (fairness over *attempts*, not successes) and probes it, at
    /// most one such sweep probe in flight node-wide at a time. This is what reveals lag that a
    /// frozen `Fast` cache would otherwise hide indefinitely -- see the module doc.
    fn spawn_consistent_read_sweep(&mut self) {
        let running: Vec<PartitionId> = self
            .processor_states
            .iter()
            .filter(|(_, state)| matches!(state, ProcessorState::Started { .. }))
            .map(|(id, _)| *id)
            .collect();
        let now = Instant::now();
        for partition_id in &running {
            self.tail_observations
                .entry(*partition_id)
                .or_insert_with(|| TailObservation::new(Lsn::INVALID, now));
        }

        let Some(target) = pick_next_consistent_read_sweep_target(
            running
                .iter()
                .filter_map(|id| self.tail_observations.get(id).map(|obs| (*id, obs))),
        ) else {
            return;
        };

        self.consistent_read_sweep_inflight = true;
        let bifrost = self.bifrost.clone();
        let probe_timeout = self
            .updateable_config
            .live_load()
            .worker
            .stall_detection
            .probe_timeout();

        self.asynchronous_operations.spawn(
            async move {
                let log_id = Metadata::with_current(|m| {
                    m.partition_table_ref().get(&target).map(Partition::log_id)
                });
                let result = match log_id {
                    Some(log_id) => tokio::time::timeout(
                        probe_timeout,
                        bifrost.find_tail(log_id, FindTailOptions::ConsistentRead),
                    )
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .map(|tail| tail.offset()),
                    None => None,
                };

                AsynchronousEvent {
                    partition_id: target,
                    inner: EventKind::ConsistentReadSweepCompleted {
                        probed: Some(target),
                        result,
                    },
                }
            }
            .in_current_tc(),
        );
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

    /// Collect enriched processor status from all running partitions. Routed through
    /// `effective_status` so the apply-stall overlay (downgraded replay status,
    /// `apply_stalled_since`) is applied uniformly, including for a quarantined processor that is
    /// currently `Stopping` (which otherwise reports no status at all).
    fn get_state(&self) -> BTreeMap<PartitionId, PartitionProcessorStatus> {
        self.processor_states
            .keys()
            .filter_map(|partition_id| {
                let mut status = self.effective_status(*partition_id)?;
                let labels = [(PARTITION_LABEL, partition_id.to_string())];

                gauge!(PARTITION_TIME_SINCE_LAST_STATUS_UPDATE, &labels)
                    .set(status.updated_at.elapsed());

                gauge!(PARTITION_IS_EFFECTIVE_LEADER, &labels).set(
                    if status.is_effective_leader() {
                        1.0
                    } else {
                        0.0
                    },
                );

                gauge!(PARTITION_APPLY_STALLED, &labels).set(
                    if self.is_apply_stalled(*partition_id) {
                        1.0
                    } else {
                        0.0
                    },
                );

                // todo: PartitionProcessorStatus struct is shared across PP and PPM, consider splitting it
                status.last_archived_log_lsn = self
                    .latest_snapshots
                    .get(partition_id)
                    .map(|s| s.archived_lsn);

                let current_tail_lsn = self
                    .tail_observations
                    .get(partition_id)
                    .map(TailObservation::lsn);
                let target_tail_lsn = if current_tail_lsn > status.target_tail_lsn {
                    current_tail_lsn
                } else {
                    status.target_tail_lsn
                };

                match target_tail_lsn {
                    None => {
                        // unknown might indicate an issue, so we set the metric to infinity
                        gauge!(PARTITION_APPLIED_LSN_LAG, &labels).set(f64::INFINITY);
                    }
                    Some(target_tail_lsn) => {
                        status.target_tail_lsn = Some(target_tail_lsn);

                        // tail lsn always points to the next "free" lsn slot. Therefor the lag is calculate as `lsn-1`
                        // hence we do target_tail_lsn.prev() below
                        gauge!(PARTITION_APPLIED_LSN_LAG, &labels).set(
                            target_tail_lsn.prev().as_u64().saturating_sub(
                                status.last_applied_log_lsn.unwrap_or(Lsn::OLDEST).as_u64(),
                            ) as f64,
                        );
                    }
                }

                Some((*partition_id, status))
            })
            .collect()
    }

    /// Single source of effective status: `Started`/`Starting` return their live status enriched
    /// with the apply-stall overlay (downgraded replay status, `apply_stalled_since`);
    /// `Stopping` -- which normally reports no status -- synthesizes a minimal one whenever the
    /// partition is quarantined, so the quarantine signal never drops out during a self-bail's
    /// restart cycle. See the `apply_progress_tracker` module doc.
    fn effective_status(&self, partition_id: PartitionId) -> Option<PartitionProcessorStatus> {
        let processor_state = self.processor_states.get(&partition_id)?;
        let quarantine = self
            .trackers
            .get(&partition_id)
            .and_then(TrackerEntry::quarantine);

        match processor_state {
            ProcessorState::Stopping { .. } => {
                let quarantine = quarantine?;
                let last_applied_log_lsn = self
                    .trackers
                    .get(&partition_id)
                    .and_then(TrackerEntry::last_known_lsn);
                Some(PartitionProcessorStatus {
                    replay_status: ReplayStatus::CatchingUp,
                    last_applied_log_lsn,
                    apply_stalled_since: Some(quarantine.since()),
                    updated_at: MillisSinceEpoch::now(),
                    ..Default::default()
                })
            }
            _ => {
                let mut status = processor_state.partition_processor_status()?;
                if let Some(quarantine) = quarantine {
                    status.apply_stalled_since = Some(quarantine.since());
                    if status.replay_status == ReplayStatus::Active {
                        status.replay_status = ReplayStatus::CatchingUp;
                    }
                }
                Some(status)
            }
        }
    }

    fn is_apply_stalled(&self, partition_id: PartitionId) -> bool {
        self.trackers
            .get(&partition_id)
            .is_some_and(TrackerEntry::is_quarantined)
    }

    /// A partition processor is eligible to publish snapshots when its underlying processor
    /// reports `Active` *and* it isn't currently quarantined for an apply stall (the overlay would
    /// otherwise leave a stale, non-advancing snapshot target).
    fn should_publish_snapshots(&self, partition_id: PartitionId) -> bool {
        self.processor_states
            .get(&partition_id)
            .is_some_and(ProcessorState::should_publish_snapshots)
            && !self.is_apply_stalled(partition_id)
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
        if !self.processor_states.contains_key(&partition_id) {
            let _ = sender.send(Err(SnapshotError {
                partition_id,
                kind: SnapshotErrorKind::PartitionNotFound,
            }));
            return;
        }

        let snapshot_repository = self.snapshot_repository.clone();
        let Some(snapshot_repository) = snapshot_repository else {
            let _ = sender.send(Err(SnapshotError {
                partition_id,
                kind: SnapshotErrorKind::RepositoryNotConfigured,
            }));
            return;
        };

        if !self.should_publish_snapshots(partition_id) {
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
        gauge!(SNAPSHOT_AGE, PARTITION_LABEL => partition_id.to_string())
            .set(snapshot_status.latest_snapshot_created_at.elapsed());
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
        let snapshot_interval = snapshots_options.snapshot_interval;
        let records_per_snapshot = snapshots_options.snapshot_interval_num_records;
        if snapshot_interval.is_none() && records_per_snapshot.is_none() {
            return;
        };

        // Partitions with a status suitable for taking a snapshot without a running snapshot task
        let candidate_partitions = self
            .processor_states
            .keys()
            .filter(|partition_id| !self.pending_snapshots.contains_key(partition_id))
            .filter_map(|partition_id| {
                self.effective_status(*partition_id)
                    .filter(|status| {
                        status.effective_mode == RunMode::Leader
                            && status.replay_status == ReplayStatus::Active
                    })
                    .map(|status| (*partition_id, status))
            });

        let (mut candidate_partitions, unknown_latest_snapshot): (Vec<_>, Vec<_>) =
            candidate_partitions.partition_map(|(partition_id, processor_status)| {
                match self.latest_snapshots.get(&partition_id) {
                    Some(latest_snapshot) => {
                        Either::Left((partition_id, latest_snapshot.clone(), processor_status))
                    }
                    None => Either::Right(partition_id),
                }
            });

        for partition_id in &unknown_latest_snapshot {
            self.spawn_update_latest_snapshot(*partition_id);
        }

        // Limit the number of snapshots we schedule automatically
        const MAX_CONCURRENT_SNAPSHOTS: usize = 4;
        let limit = MAX_CONCURRENT_SNAPSHOTS.saturating_sub(self.pending_snapshots.len());

        candidate_partitions.shuffle(&mut rand::rng());
        let snapshot_partitions = candidate_partitions
            .into_iter()
            .filter_map(|(partition_id, latest_snapshot, status)| {
                let applied_lsn = status.last_applied_log_lsn.unwrap_or(Lsn::INVALID);
                // At this point, at least one of time-based or record-based interval is set; if
                // both requirements are configured, then both must be met.
                let next_snapshot_target_lsn = records_per_snapshot.map(|target_delta| {
                    latest_snapshot
                        .latest_snapshot_lsn
                        .add(Lsn::from(target_delta.get()))
                });
                let latest_snapshot_age = latest_snapshot.latest_snapshot_created_at.elapsed();
                if next_snapshot_target_lsn.is_none_or(|target| applied_lsn >= target)
                    && snapshot_interval.is_none_or(|interval| {
                        latest_snapshot.latest_snapshot_created_at.elapsed()
                            > Duration::from(interval).add_jitter(0.1)
                    })
                {
                    Some(partition_id)
                } else {
                    trace!(
                        ?partition_id,
                        ?applied_lsn,
                        latest_snapshot_lsn = ?latest_snapshot.latest_snapshot_lsn,
                        ?latest_snapshot_age,
                        ?next_snapshot_target_lsn,
                        "Not snapshotting partition yet"
                    );
                    None
                }
            })
            .take(limit);

        for partition_id in snapshot_partitions {
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
                let (node_name, cluster_name, cluster_fingerprint) = Metadata::with_current(|m| {
                    let nodes_config = m.nodes_config_ref();
                    let node_name = nodes_config
                        .find_node_by_id(m.my_node_id())
                        .expect("my node must be present")
                        .name
                        .clone();
                    (
                        node_name,
                        nodes_config.cluster_name().to_owned(),
                        nodes_config.cluster_fingerprint(),
                    )
                });

                let create_snapshot_task = SnapshotPartitionTask {
                    snapshot_id,
                    partition_id,
                    min_target_lsn,
                    snapshot_base_path,
                    partition_store_manager: self.partition_store_manager.clone(),
                    cluster_name,
                    cluster_fingerprint,
                    node_name,
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
                    match psm
                        .refresh_latest_partition_snapshot_status(partition_id)
                        .await
                    {
                        Ok(Some(snapshot_status)) => AsynchronousEvent {
                            partition_id,
                            inner: EventKind::SnapshotStatusUpdated { snapshot_status },
                        },
                        Ok(None) => {
                            // Partition snapshot not found in repository
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
        let mut running_processors: HashSet<_> = self.processor_states.keys().copied().collect();

        // Not ideal to have to iterate over all replica states. An index per node id could help.
        // In practice, this is probably not a problem because the replica sets won't change that
        // often.
        for (partition_id, membership_state) in replica_set_states.iter() {
            if membership_state.contains(my_node_id) {
                if !self.processor_states.contains_key(&partition_id) {
                    self.start_partition_processor(partition_id, None);
                }

                running_processors.remove(&partition_id);
            }
        }

        // All the remaining running processors are no longer part of the observed partition
        // configuration. Let's terminate them.
        for partition_id in running_processors.into_iter() {
            if let Some(processor) = self.processor_states.get_mut(&partition_id) {
                debug!(%partition_id, "Stop partition processor because it is no longer a member of the partition configuration");
                processor.stop();

                if self.pending_snapshots.contains_key(&partition_id) {
                    info!(%partition_id,
                        "Partition processor stop requested with snapshot task result outstanding"
                    );
                }
                self.latest_snapshots.remove(&partition_id);
            }
        }

        gauge!(NUM_ACTIVE_PARTITIONS).set(self.processor_states.len() as f64);
    }

    /// Starts a partition processor if this node is part of the replica set of the given partition.
    /// Returns true if this node is part of the replica set of the given partition. Otherwise, false.
    fn restart_partition_processor_if_replica(
        &mut self,
        partition_id: PartitionId,
        delay: RestartDelay,
    ) -> bool {
        // only restart partition processors if the partition processor manager is still supposed to run
        if restate_core::is_cancellation_requested() {
            return false;
        }

        if self
            .replica_set_states
            .membership_state(partition_id)
            .contains(Metadata::with_current(|m| m.my_node_id().as_plain()))
        {
            self.start_partition_processor(
                partition_id,
                delay.next_delay().map(|d| d.add_jitter(0.3)),
            );
            true
        } else {
            false
        }
    }

    #[instrument(level = "info", skip_all, fields(partition_id = %partition_id))]
    fn start_partition_processor(&mut self, partition_id: PartitionId, delay: Option<Duration>) {
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
    /// The mandatory node-wide `ConsistentRead` sweep probe completed (or there was nothing to
    /// probe this round).
    ConsistentReadSweepCompleted {
        probed: Option<PartitionId>,
        result: Option<Lsn>,
    },
    /// A `ConsistentRead` probe issued on behalf of a suspected partition's tracker completed.
    ConsistentReadProbeCompleted {
        incarnation: Ulid,
        nonce: u64,
        result: Option<ProbeResult>,
    },
    SnapshotStatusUpdated {
        snapshot_status: PartitionSnapshotStatus,
    },
    SnapshotStatusUpdateSkipped,
}

#[cfg(test)]
mod tests {
    use crate::partition_processor_manager::PartitionProcessorManager;
    use googletest::IntoTestResult;
    use restate_bifrost::BifrostService;
    use restate_bifrost::providers::memory_loglet;
    use restate_core::partitions::PartitionRouting;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_ingestion_client::{IngestionClient, SessionOptions};
    use restate_partition_store::PartitionStoreManager;
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::Configuration;
    use restate_types::health::HealthStatus;
    use restate_types::identifiers::PartitionId;
    use restate_types::live::Live;
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::net::address::AdvertisedAddress;
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
    use restate_types::partitions::state::{
        MemberState, PartitionReplicaSetStates, ReplicaSetState,
    };
    use restate_types::{GenerationalNodeId, RestateVersion, Version};
    use std::num::NonZeroUsize;
    use std::time::Duration;
    use test_log::test;
    use tracing::info;

    /// This test ensures that the lifecycle of partition processors is properly managed by the
    /// [`PartitionProcessorManager`]. See https://github.com/restatedev/restate/issues/2258 for
    /// more details.
    #[test(restate_core::test)]
    async fn proper_partition_processor_lifecycle() -> googletest::Result<()> {
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

        RocksDbManager::init();

        let bifrost_svc = BifrostService::new(env_builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let replica_set_states = PartitionReplicaSetStates::default();

        let partition_store_manager = PartitionStoreManager::create(true).await?;

        let ingestion_client = IngestionClient::new(
            env_builder.networking.clone(),
            env_builder.metadata.updateable_partition_table(),
            PartitionRouting::new(replica_set_states.clone(), TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(),
            SessionOptions::default(),
        );

        let partition_processor_manager = PartitionProcessorManager::new(
            health_status,
            Live::from_value(Configuration::default()),
            env_builder.metadata_writer.clone(),
            partition_store_manager,
            replica_set_states.clone(),
            &mut env_builder.router_builder,
            bifrost,
            None,
            ingestion_client,
        );

        // only needed for setting up the metadata
        let _env = env_builder.build().await;
        let processors_manager_handle = partition_processor_manager.handle();

        bifrost_svc.start().await.into_test_result()?;
        TaskCenter::spawn(
            TaskKind::SystemService,
            "partition-processor-manager",
            partition_processor_manager.run(),
        )?;

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
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    /// Stage 3 / A2 / A4: a Candidate-watchdog (Option D) bail must establish the same
    /// `ApplyStall` quarantine episode an A-bail would, using the exact FSM LSN captured at the
    /// watchdog's deadline, and that episode must remain visible through `effective_status` once
    /// the manager schedules the restart (`Started` -> `Stopped(AnnounceNotApplied)` ->
    /// `Starting`). Drives `on_asynchronous_event` directly rather than through a full leadership
    /// election -- the watchdog's own state machine is covered by
    /// `partition::leadership::tests`; this test is only about the PPM-side quarantine handoff.
    #[test(restate_core::test)]
    async fn d_bail_establishes_quarantine() -> googletest::Result<()> {
        use std::sync::Arc;

        use tokio::sync::watch;
        use tokio_util::sync::CancellationToken;

        use crate::partition::leadership;
        use crate::partition::{LoopHeartbeat, ProcessorError, TargetLeaderState};
        use crate::partition_processor_manager::apply_progress_tracker::QuarantineEpisode;
        use crate::partition_processor_manager::processor_state::{
            LeaderState, ProcessorState, StartedProcessor,
        };
        use crate::partition_processor_manager::{AsynchronousEvent, EventKind};
        use restate_core::network::ShardSender;
        use restate_types::cluster::cluster_state::PartitionProcessorStatus;
        use restate_types::identifiers::LeaderEpoch;
        use restate_types::sharding::KeyRange;

        let mut nodes_config = NodesConfiguration::new_for_testing();
        let node_id = GenerationalNodeId::new(44, 44);
        let node_config = NodeConfig::builder()
            .name("44".to_owned())
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

        RocksDbManager::init();

        let bifrost_svc = BifrostService::new(env_builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let replica_set_states = PartitionReplicaSetStates::default();
        let partition_store_manager = PartitionStoreManager::create(true).await?;

        let ingestion_client = IngestionClient::new(
            env_builder.networking.clone(),
            env_builder.metadata.updateable_partition_table(),
            PartitionRouting::new(replica_set_states.clone(), TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(),
            SessionOptions::default(),
        );

        let mut partition_processor_manager = PartitionProcessorManager::new(
            health_status,
            Live::from_value(Configuration::default()),
            env_builder.metadata_writer.clone(),
            partition_store_manager,
            replica_set_states.clone(),
            &mut env_builder.router_builder,
            bifrost,
            None,
            ingestion_client,
        );

        let _env = env_builder.build().await;
        bifrost_svc.start().await.into_test_result()?;

        // Normally done by `run()`'s startup; needed here since this test drives the manager
        // directly rather than through its own event loop.
        let (_pp_rpc_control, pp_rpc_shards) =
            partition_processor_manager.pp_rpc_svc.take().start();
        partition_processor_manager.pp_rpc_shards = Some(pp_rpc_shards);

        // This node must still be a replica of the partition, or the Stopped handler would GC
        // the tracker this test is about to create instead of carrying it into the restart.
        let current_replica_set = ReplicaSetState {
            version: Version::MIN,
            members: vec![MemberState {
                node_id: node_id.as_plain(),
                durable_lsn: Lsn::INVALID,
            }],
        };
        replica_set_states.note_observed_membership(
            PartitionId::MIN,
            Default::default(),
            &current_replica_set,
            &None,
        );

        // Craft a `Started` processor state directly: this test only needs to exercise the
        // D-bail handler and the quarantine overlay, not a real running partition processor.
        let (control_tx, _control_rx) = watch::channel(TargetLeaderState::Follower);
        let (rpc_tx, _rpc_rx) = ShardSender::new();
        let (_watch_tx, watch_rx) = watch::channel(PartitionProcessorStatus::default());
        let started = StartedProcessor::new(
            CancellationToken::new(),
            KeyRange::FULL,
            control_tx,
            rpc_tx,
            watch_rx,
            Arc::new(LoopHeartbeat::new()),
        );
        partition_processor_manager.processor_states.insert(
            PartitionId::MIN,
            ProcessorState::Started {
                processor: Some(started),
                leader_state: LeaderState::Follower,
                start_time: std::time::Instant::now(),
                delay: None,
            },
        );

        let bail_lsn = Lsn::new(42);
        partition_processor_manager.on_asynchronous_event(AsynchronousEvent {
            partition_id: PartitionId::MIN,
            inner: EventKind::Stopped(Err(ProcessorError::ActionEffect(
                leadership::Error::AnnounceNotApplied {
                    leader_epoch: LeaderEpoch::from(1),
                    committed_for: Duration::from_secs(1),
                    bail_lsn,
                },
            ))),
        });

        let quarantine = partition_processor_manager
            .trackers
            .get(&PartitionId::MIN)
            .and_then(|tracker| tracker.quarantine())
            .expect("D-bail must establish a quarantine episode");
        assert!(matches!(
            quarantine,
            QuarantineEpisode::ApplyStall {
                bail_lsn: lsn,
                ..
            } if lsn == bail_lsn
        ));

        let status = partition_processor_manager
            .effective_status(PartitionId::MIN)
            .expect("status must remain visible across the restart");
        assert!(
            status.apply_stalled_since.is_some(),
            "apply_stalled_since must survive Started -> Stopped -> Starting"
        );

        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
