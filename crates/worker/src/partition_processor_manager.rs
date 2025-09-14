// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod processor_state;
mod spawn_processor_task;

use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::ops::{Add, RangeInclusive};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ahash::{HashMap, HashSet};
use anyhow::{Context, bail};
use futures::stream::{FuturesUnordered, StreamExt};
use gardal::Limit;
use itertools::{Either, Itertools};
use metrics::gauge;
use rand::Rng;
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, info_span, instrument, trace, warn};

use restate_bifrost::Bifrost;
use restate_bifrost::loglet::FindTailOptions;
use restate_core::network::{
    BackPressureMode, Incoming, MessageRouterBuilder, Rpc, ServiceMessage, ServiceReceiver, Verdict,
};
use restate_core::worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};
use restate_core::{
    Metadata, MetadataWriter, TaskCenterFutureExt, TaskHandle, TaskKind, cancellation_watcher,
    my_node_id,
};
use restate_core::{RuntimeTaskHandle, TaskCenter};
use restate_invoker_api::StatusHandle;
use restate_invoker_impl::{ChannelStatusReader, TokenBucket};
use restate_metadata_server::{MetadataStoreClient, ReadModifyWriteError};
use restate_metadata_store::{ReadWriteError, RetryError, retry_on_retryable_error};
use restate_partition_store::PartitionStoreManager;
use restate_partition_store::snapshots::{
    PartitionSnapshotMetadata, SnapshotPartitionTask, SnapshotRepository,
};
use restate_partition_store::{SnapshotError, SnapshotErrorKind};
use restate_time_util::DurationExt;
use restate_types::cluster::cluster_state::ReplayStatus;
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
use restate_types::config::Configuration;
use restate_types::epoch::EpochMetadata;
use restate_types::health::HealthStatus;
use restate_types::identifiers::SnapshotId;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::live::Live;
use restate_types::logs::{LogId, Lsn, SequenceNumber};
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
use restate_types::retries::with_jitter;
use restate_types::{GenerationalNodeId, SharedString};

use crate::metric_definitions::NUM_PARTITIONS;
use crate::metric_definitions::PARTITION_IS_EFFECTIVE_LEADER;
use crate::metric_definitions::PARTITION_LABEL;
use crate::metric_definitions::PARTITION_TIME_SINCE_LAST_STATUS_UPDATE;
use crate::metric_definitions::{NUM_ACTIVE_PARTITIONS, PARTITION_APPLIED_LSN_LAG};
use crate::partition::ProcessorError;
use crate::partition_processor_manager::processor_state::{
    LeaderEpochToken, ProcessorState, StartedProcessor,
};
use crate::partition_processor_manager::spawn_processor_task::SpawnPartitionProcessorTask;

#[derive(Debug, Clone, derive_more::Display)]
#[display("{}", snapshot_id)]
pub struct SnapshotCreated {
    pub snapshot_id: SnapshotId,
    pub log_id: LogId,
    pub min_applied_lsn: Lsn,
}

impl From<&PartitionSnapshotMetadata> for SnapshotCreated {
    fn from(metadata: &PartitionSnapshotMetadata) -> SnapshotCreated {
        SnapshotCreated {
            snapshot_id: metadata.snapshot_id,
            log_id: metadata.log_id,
            min_applied_lsn: metadata.min_applied_lsn,
        }
    }
}

pub struct PartitionProcessorManager {
    health_status: HealthStatus<WorkerStatus>,
    updateable_config: Live<Configuration>,
    processor_states: BTreeMap<PartitionId, ProcessorState>,
    name_cache: BTreeMap<PartitionId, SharedString>,

    metadata_writer: MetadataWriter,
    partition_store_manager: Arc<PartitionStoreManager>,
    ppm_svc_rx: ServiceReceiver<PartitionManagerService>,
    pp_rpc_rx: ServiceReceiver<PartitionLeaderService>,
    bifrost: Bifrost,
    rx: mpsc::Receiver<ProcessorsManagerCommand>,
    tx: mpsc::Sender<ProcessorsManagerCommand>,

    replica_set_states: PartitionReplicaSetStates,
    target_tail_lsns: HashMap<PartitionId, Lsn>,
    archived_lsns: HashMap<PartitionId, Lsn>,
    invokers_status_reader: MultiplexedInvokerStatusReader,

    asynchronous_operations: JoinSet<AsynchronousEvent>,

    pending_snapshots: HashMap<PartitionId, PendingSnapshotTask>,
    latest_snapshots: HashMap<PartitionId, SnapshotCreated>,
    snapshot_export_tasks: FuturesUnordered<TaskHandle<SnapshotResultInternal>>,
    snapshot_repository: Option<SnapshotRepository>,
    fast_forward_on_startup: HashMap<PartitionId, Lsn>,

    partition_table: Live<PartitionTable>,
    wait_for_partition_table_update: bool,

    // throttling
    invocation_token_bucket: Option<TokenBucket>,
    action_token_bucket: Option<TokenBucket>,
}

type SnapshotResult = Result<SnapshotCreated, SnapshotError>;
type SnapshotResultInternal = Result<PartitionSnapshotMetadata, SnapshotError>;

struct PendingSnapshotTask {
    snapshot_id: SnapshotId,
    sender: Option<oneshot::Sender<SnapshotResult>>,
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

type ChannelStatusReaderList = Vec<(RangeInclusive<PartitionKey>, ChannelStatusReader)>;

#[derive(Debug, Clone, Default)]
pub struct MultiplexedInvokerStatusReader {
    readers: Arc<parking_lot::RwLock<ChannelStatusReaderList>>,
}

impl MultiplexedInvokerStatusReader {
    fn push(&mut self, key_range: RangeInclusive<PartitionKey>, reader: ChannelStatusReader) {
        self.readers.write().push((key_range, reader));
    }

    fn remove(&mut self, key_range: &RangeInclusive<PartitionKey>) {
        self.readers.write().retain(|elem| &elem.0 != key_range);
    }
}

impl StatusHandle for MultiplexedInvokerStatusReader {
    type Iterator =
        std::iter::Flatten<std::vec::IntoIter<<ChannelStatusReader as StatusHandle>::Iterator>>;

    async fn read_status(&self, keys: RangeInclusive<PartitionKey>) -> Self::Iterator {
        let mut overlapping_partitions = Vec::new();

        // first clone the readers while holding the lock, then release the lock before reading the
        // status to avoid holding the lock across await points
        for (range, reader) in self.readers.read().iter() {
            if keys.start() <= range.end() && keys.end() >= range.start() {
                // if this partition is actually overlapping with the search range
                overlapping_partitions.push((range.clone(), reader.clone()))
            }
        }
        // although we never have a single scans that cross partitions (thus overlapping_partitions.len() == 1),
        // we can make this code path a bit more future resilient cheaply, by ordering the partitions by their start key.
        // (this uniquely defines the order between the partitions)
        overlapping_partitions.sort_by(|(a, _), (b, _)| a.start().cmp(b.start()));

        let mut result = Vec::with_capacity(overlapping_partitions.len());

        for (_, reader) in overlapping_partitions {
            result.push(reader.read_status(keys.clone()).await);
        }

        result.into_iter().flatten()
    }
}

impl PartitionProcessorManager {
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
    ) -> Self {
        let ppm_svc_rx = router_builder.register_service(24, BackPressureMode::PushBack);
        let pp_rpc_rx = router_builder.register_service(24, BackPressureMode::PushBack);

        let config = updateable_config.pinned();

        let invocation_token_bucket =
            config
                .worker
                .invoker
                .invocation_throttling
                .as_ref()
                .map(|opts| {
                    let limit = Limit::from(opts.clone());
                    let capacity = limit.burst();
                    let bucket = TokenBucket::from_parts(limit, gardal::TokioClock::default());
                    bucket.add_tokens(capacity.get());
                    bucket
                });

        let action_token_bucket = config
            .worker
            .invoker
            .action_throttling
            .as_ref()
            .map(|opts| {
                let limit = Limit::from(opts.clone());
                let capacity = limit.burst();
                let bucket = TokenBucket::from_parts(limit, gardal::TokioClock::default());
                bucket.add_tokens(capacity.get());
                bucket
            });

        let (tx, rx) = mpsc::channel(updateable_config.pinned().worker.internal_queue_length());
        Self {
            health_status,
            updateable_config,
            processor_states: BTreeMap::default(),
            name_cache: Default::default(),
            metadata_writer,
            partition_store_manager,
            ppm_svc_rx,
            pp_rpc_rx,
            bifrost,
            rx,
            tx,
            replica_set_states,
            archived_lsns: HashMap::default(),
            target_tail_lsns: HashMap::default(),
            invokers_status_reader: MultiplexedInvokerStatusReader::default(),
            asynchronous_operations: JoinSet::default(),
            pending_snapshots: HashMap::default(),
            latest_snapshots: HashMap::default(),
            snapshot_export_tasks: FuturesUnordered::default(),
            snapshot_repository,
            fast_forward_on_startup: HashMap::default(),
            partition_table: Metadata::with_current(|m| m.updateable_partition_table()),
            wait_for_partition_table_update: false,
            invocation_token_bucket,
            action_token_bucket,
        }
    }

    pub fn invokers_status_reader(&self) -> MultiplexedInvokerStatusReader {
        self.invokers_status_reader.clone()
    }

    pub fn handle(&self) -> ProcessorsManagerHandle {
        ProcessorsManagerHandle::new(self.tx.clone())
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());

        let metadata = Metadata::current();

        let mut partition_table_version_watcher = metadata.watch(MetadataKind::PartitionTable);
        gauge!(NUM_PARTITIONS).set(self.partition_table.live_load().len() as f64);

        let mut snapshot_check_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(rand::rng().random_range(30..60)), // delay scheduled snapshots on startup
            with_jitter(Duration::from_secs(1), 0.1),
        );
        snapshot_check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut update_target_tail_lsns = tokio::time::interval(Duration::from_secs(1));
        update_target_tail_lsns.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut ppm_svc_rx = self.ppm_svc_rx.take().start();
        let mut pp_rpc_rx = self.pp_rpc_rx.take().start();
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
                _ = snapshot_check_interval.tick() => {
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
                Some(partition_processor_rpc) = pp_rpc_rx.next() => {
                    self.on_partition_processor_rpc(partition_processor_rpc);
                }
                Some(result) = self.snapshot_export_tasks.next() => {
                    if let Ok(result) = result {
                        self.on_create_snapshot_task_completed(result);
                    } else {
                        debug!("Create snapshot task failed: {}", result.unwrap_err());
                    }
                }
                () = &mut replica_set_states_changed => {
                    // register for the next replica set states updates to not miss any
                    replica_set_states_changed.set(replica_set_states.changed());
                    self.on_replica_set_state_changes(&replica_set_states);
                }
                _ = &mut shutdown => {
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

    fn on_partition_processor_rpc(&self, msg: ServiceMessage<PartitionLeaderService>) {
        // We want the partition processor to decode the request and that we we only do the routing here.
        let Some(sort_code) = msg.sort_code() else {
            msg.fail(Verdict::SortCodeNotFound);
            return;
        };

        // if this doesn't fit, then the partition id is not valid.
        let Ok(partition_id) = u16::try_from(sort_code) else {
            error!(%sort_code, "Invalid partition id in RPC request. This indicates a protocol bug!");
            msg.fail(Verdict::MessageUnrecognized);
            return;
        };

        let partition_id = PartitionId::from(partition_id);
        match self.processor_states.get(&partition_id) {
            None => msg.fail(Verdict::SortCodeNotFound),
            Some(processor_state) => {
                processor_state.try_send_rpc(msg);
            }
        }
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
                                    self.invokers_status_reader.push(
                                        started_processor.key_range().clone(),
                                        started_processor.invoker_status_reader().clone(),
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
                    }
                }
                gauge!(NUM_ACTIVE_PARTITIONS).set(self.processor_states.len() as f64);
            }
            EventKind::Stopped(result) => {
                let delay = match self.processor_states.remove(&partition_id) {
                    None => {
                        debug!("Stopped partition processor which is no longer running.");
                        // immediately try to restart if we are still part of the partition's membership
                        RestartDelay::Immediate
                    }
                    Some(processor_state) => match processor_state {
                        ProcessorState::Starting { .. } => {
                            warn!(%partition_id, "Partition processor failed to start: {result:?}");
                            RestartDelay::Fixed
                        }
                        ProcessorState::Started {
                            processor,
                            start_time,
                            delay,
                            ..
                        } => {
                            self.invokers_status_reader
                                .remove(processor.as_ref().expect("must be some").key_range());

                            match &result {
                                Err(ProcessorError::TrimGapEncountered {
                                    trim_gap_end: to_lsn,
                                    read_pointer: sequence_number,
                                }) => {
                                    if self.partition_store_manager.is_repository_configured() {
                                        info!(
                                            %partition_id,
                                            trim_gap_to_lsn = ?to_lsn,
                                            ?sequence_number,
                                            "Partition processor stopped due to a log trim gap, will attempt to fast-forward on restart",
                                        );
                                        self.fast_forward_on_startup.insert(partition_id, *to_lsn);
                                        RestartDelay::Immediate
                                    } else {
                                        error!(
                                            %partition_id,
                                            trim_gap_to_lsn = ?to_lsn,
                                            "Partition processor stopped due to a log trim gap, and no snapshot repository is configured",
                                        );
                                        // configuration problem; until we have peer-to-peer state exchange we can only wait
                                        RestartDelay::MaxBackoff
                                    }
                                }
                                Err(err) => {
                                    let next_delay = RestartDelay::Exponential {
                                        start_time,
                                        last_delay: delay,
                                    };
                                    error!(%partition_id, %err, "Partition processor exited unexpectedly, {}", next_delay);
                                    next_delay
                                }
                                Ok(_) => {
                                    info!(%partition_id, "Partition processor stopped");
                                    RestartDelay::Immediate
                                }
                            }
                        }
                        ProcessorState::Stopping { processor, .. } => {
                            if let Some(processor) = processor {
                                self.invokers_status_reader.remove(processor.key_range());
                            }
                            RestartDelay::Immediate
                        }
                    },
                };

                if !self.restart_partition_processor_if_replica(partition_id, delay) {
                    debug!("Partition processor stopped: {result:?}");
                }

                gauge!(NUM_ACTIVE_PARTITIONS).set(self.processor_states.len() as f64);
            }
            EventKind::NewLeaderEpoch {
                leader_epoch_token,
                result,
            } => {
                if let Some(processor_state) = self.processor_states.get_mut(&partition_id) {
                    match result {
                        Ok(leader_epoch) => {
                            processor_state
                                .on_leader_epoch_obtained(leader_epoch, leader_epoch_token);
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
            EventKind::NewArchivedLsn { archived_lsn } => {
                self.archived_lsns
                    .entry(partition_id)
                    .and_modify(|lsn| *lsn = archived_lsn.max(*lsn))
                    .or_insert(archived_lsn);
            }
        }
    }

    fn await_runtime_task_result(
        &mut self,
        partition_id: PartitionId,
        runtime_task_handle: RuntimeTaskHandle<Result<(), ProcessorError>>,
    ) {
        self.asynchronous_operations
            .build_task()
            .name(&format!("runtime-result-{partition_id}"))
            .spawn(
                async move {
                    let result = runtime_task_handle.await;
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
        self.processor_states
            .iter()
            .filter_map(|(partition_id, processor_state)| {
                let mut status = processor_state.partition_processor_status()?;
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

                // it is a bit unfortunate that we share PartitionProcessorStatus between the
                // PP and the PPManager :-(. Maybe at some point we want to split the struct for it.
                status.last_archived_log_lsn = self.archived_lsns.get(partition_id).cloned();
                let current_tail_lsn = self.target_tail_lsns.get(partition_id).cloned();

                let target_tail_lsn = if current_tail_lsn > status.target_tail_lsn {
                    current_tail_lsn
                } else {
                    status.target_tail_lsn
                };

                match target_tail_lsn {
                    None => {
                        // current tail lsn is unknown.
                        // This might indicate an issue, so we set the metric to infinity
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
            Ok(metadata) => {
                self.archived_lsns
                    .insert(metadata.partition_id, metadata.min_applied_lsn);

                let response = SnapshotCreated::from(&metadata);
                self.latest_snapshots
                    .entry(metadata.partition_id)
                    .and_modify(|e| {
                        if response.min_applied_lsn > e.min_applied_lsn {
                            *e = response.clone();
                        }
                    })
                    .or_insert_with(|| response.clone());

                (metadata.partition_id, Ok(response))
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

    fn trigger_periodic_partition_snapshots(&mut self) {
        let Some(snapshot_repository) = self.snapshot_repository.clone() else {
            return;
        };

        let Some(records_per_snapshot) = self
            .updateable_config
            .live_load()
            .worker
            .snapshots
            .snapshot_interval_num_records
        else {
            return;
        };

        let potential_snapshot_partitions = self
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

        let (mut known_archived_lsn, unknown_archived_lsn): (Vec<_>, Vec<_>) =
            potential_snapshot_partitions.partition_map(|(partition_id, status)| {
                match self.archived_lsns.get(&partition_id) {
                    Some(&archived_lsn) => Either::Left((
                        partition_id,
                        status.last_applied_log_lsn.unwrap_or(Lsn::INVALID),
                        archived_lsn,
                    )),
                    None => Either::Right(partition_id),
                }
            });

        for partition_id in unknown_archived_lsn {
            self.spawn_update_archived_lsn_task(partition_id);
        }

        // Limit the number of snapshots we schedule automatically
        const MAX_CONCURRENT_SNAPSHOTS: usize = 4;
        let limit = MAX_CONCURRENT_SNAPSHOTS.saturating_sub(self.pending_snapshots.len());

        known_archived_lsn.shuffle(&mut rand::rng());
        let snapshot_partitions = known_archived_lsn
            .into_iter()
            .filter_map(|(partition_id, applied_lsn, archived_lsn)| {
                if applied_lsn >= archived_lsn.add(Lsn::from(records_per_snapshot.get())) {
                    Some(partition_id)
                } else {
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
            && min_target_lsn.is_some_and(|target_lsn| snapshot.min_applied_lsn >= target_lsn)
            && let Some(sender) = sender
        {
            sender
                        .send(Ok(snapshot.clone()))
                        .inspect_err(|err| {
                            debug!(
                                ?min_target_lsn,
                                snapshot_id = ?snapshot.snapshot_id,
                                latest_snapshot_min_applied_lsn = ?snapshot.min_applied_lsn,
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

    fn spawn_update_archived_lsn_task(&mut self, partition_id: PartitionId) {
        let psm = self.partition_store_manager.clone();
        self.asynchronous_operations
            .build_task()
            .name(&format!("update-archived-lsn-{partition_id}"))
            .spawn(
                async move {
                    let archived_lsn = psm
                        .refresh_latest_archived_lsn(partition_id)
                        .await
                        .expect("repository must be configured here");

                    AsynchronousEvent {
                        partition_id,
                        inner: EventKind::NewArchivedLsn { archived_lsn },
                    }
                }
                .in_current_tc(),
            )
            .expect("to spawn update archived LSN task");
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
    ) -> Result<LeaderEpoch, ReadModifyWriteError> {
        let epoch: EpochMetadata = metadata_store_client
            .read_modify_write(partition_processor_epoch_key(partition_id), |epoch| {
                let next_epoch = epoch
                    .map(|epoch: EpochMetadata| epoch.claim_leadership(node_id, partition_id))
                    .ok_or("missing epoch metadata".to_owned())?;

                Ok(next_epoch)
            })
            .await?;
        Ok(epoch.epoch())
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
                        snapshot_id: snapshot.snapshot_id,
                        log_id: snapshot.log_id,
                        min_applied_lsn: snapshot.min_applied_lsn,
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
                self.archived_lsns.remove(&partition_id);
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
                delay.next_delay().map(|d| with_jitter(d, 0.3)),
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

        // the name is also used as thread names for the corresponding tokio runtimes, let's keep
        // it short.
        let task_name = self
            .name_cache
            .entry(partition_id)
            .or_insert_with(|| SharedString::from(Arc::from(format!("pp-{partition_id}"))));

        let starting_task = SpawnPartitionProcessorTask::new(
            task_name.clone(),
            partition,
            self.updateable_config.clone(),
            self.bifrost.clone(),
            self.replica_set_states.clone(),
            self.partition_store_manager.clone(),
            self.fast_forward_on_startup.remove(&partition_id),
            self.invocation_token_bucket.clone(),
            self.action_token_bucket.clone(),
        );

        self.asynchronous_operations
            .build_task()
            .name(&format!("start-pp-{partition_id}"))
            .spawn(
                async move {
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
        result: anyhow::Result<LeaderEpoch>,
    },
    NewTargetTail {
        tail: Option<Lsn>,
    },
    NewArchivedLsn {
        archived_lsn: Lsn,
    },
}

#[cfg(test)]
mod tests {
    use crate::partition_processor_manager::PartitionProcessorManager;
    use googletest::IntoTestResult;
    use restate_bifrost::BifrostService;
    use restate_bifrost::providers::memory_loglet;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_partition_store::PartitionStoreManager;
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::{CommonOptions, Configuration};
    use restate_types::health::HealthStatus;
    use restate_types::identifiers::PartitionId;
    use restate_types::live::{Constant, Live};
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::net::AdvertisedAddress;
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
    use restate_types::partitions::state::{
        MemberState, PartitionReplicaSetStates, ReplicaSetState,
    };
    use restate_types::{GenerationalNodeId, Version};
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
            .address(AdvertisedAddress::Uds("foobar1".into()))
            .roles(Role::Worker | Role::Admin)
            .build();
        nodes_config.upsert_node(node_config);

        let mut env_builder = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_my_node_id(node_id)
            .set_nodes_config(nodes_config);
        let health_status = HealthStatus::default();

        RocksDbManager::init(Constant::new(CommonOptions::default()));

        let bifrost_svc = BifrostService::new(env_builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let replica_set_states = PartitionReplicaSetStates::default();

        let partition_store_manager = PartitionStoreManager::create().await?;

        let partition_processor_manager = PartitionProcessorManager::new(
            health_status,
            Live::from_value(Configuration::default()),
            env_builder.metadata_writer.clone(),
            partition_store_manager,
            replica_set_states.clone(),
            &mut env_builder.router_builder,
            bifrost,
            None,
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

        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
