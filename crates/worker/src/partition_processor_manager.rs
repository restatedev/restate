// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod message_handler;
mod persisted_lsn_watchdog;
mod processor_state;
mod spawn_processor_task;

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Add, RangeInclusive};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use itertools::{Either, Itertools};
use metrics::gauge;
use rand::Rng;
use rand::seq::SliceRandom;
use restate_types::retries::with_jitter;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, info_span, instrument, warn};

use restate_bifrost::Bifrost;
use restate_bifrost::loglet::FindTailOptions;
use restate_core::network::{Incoming, MessageRouterBuilder, MessageStream};
use restate_core::worker_api::{
    ProcessorsManagerCommand, ProcessorsManagerHandle, SnapshotCreated, SnapshotError,
    SnapshotResult,
};
use restate_core::{
    Metadata, ShutdownError, TaskCenterFutureExt, TaskHandle, TaskKind, cancellation_watcher,
    my_node_id,
};
use restate_core::{RuntimeTaskHandle, TaskCenter};
use restate_invoker_api::StatusHandle;
use restate_invoker_impl::{BuildError, ChannelStatusReader};
use restate_metadata_server::{MetadataStoreClient, ReadModifyWriteError};
use restate_partition_store::PartitionStoreManager;
use restate_partition_store::snapshots::PartitionSnapshotMetadata;
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
use restate_types::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcRequest,
};
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::common::WorkerStatus;
use restate_types::{GenerationalNodeId, SharedString, Version};

use crate::metric_definitions::PARTITION_IS_ACTIVE;
use crate::metric_definitions::PARTITION_IS_EFFECTIVE_LEADER;
use crate::metric_definitions::PARTITION_LABEL;
use crate::metric_definitions::PARTITION_LAST_APPLIED_LOG_LSN;
use crate::metric_definitions::PARTITION_LAST_PERSISTED_LOG_LSN;
use crate::metric_definitions::PARTITION_TIME_SINCE_LAST_RECORD;
use crate::metric_definitions::PARTITION_TIME_SINCE_LAST_STATUS_UPDATE;
use crate::metric_definitions::{NUM_ACTIVE_PARTITIONS, PARTITION_LAST_APPLIED_LSN_LAG};
use crate::partition::ProcessorError;
use crate::partition::snapshots::{SnapshotPartitionTask, SnapshotRepository};
use crate::partition_processor_manager::message_handler::PartitionProcessorManagerMessageHandler;
use crate::partition_processor_manager::processor_state::{
    LeaderEpochToken, ProcessorState, StartedProcessor,
};
use crate::partition_processor_manager::spawn_processor_task::SpawnPartitionProcessorTask;

pub struct PartitionProcessorManager {
    health_status: HealthStatus<WorkerStatus>,
    updateable_config: Live<Configuration>,
    processor_states: BTreeMap<PartitionId, ProcessorState>,
    name_cache: BTreeMap<PartitionId, SharedString>,

    metadata_store_client: MetadataStoreClient,
    partition_store_manager: PartitionStoreManager,
    incoming_update_processors: MessageStream<ControlProcessors>,
    incoming_partition_processor_rpc: MessageStream<PartitionProcessorRpcRequest>,
    bifrost: Bifrost,
    rx: mpsc::Receiver<ProcessorsManagerCommand>,
    tx: mpsc::Sender<ProcessorsManagerCommand>,

    persisted_lsns_rx: watch::Receiver<BTreeMap<PartitionId, Lsn>>,
    target_tail_lsns: HashMap<PartitionId, Lsn>,
    archived_lsns: HashMap<PartitionId, Lsn>,
    invokers_status_reader: MultiplexedInvokerStatusReader,
    pending_control_processors: Option<PendingControlProcessors>,

    asynchronous_operations: JoinSet<AsynchronousEvent>,

    pending_snapshots: HashMap<PartitionId, PendingSnapshotTask>,
    latest_snapshots: HashMap<PartitionId, SnapshotCreated>,
    snapshot_export_tasks: FuturesUnordered<TaskHandle<SnapshotResultInternal>>,
    snapshot_repository: Option<SnapshotRepository>,
    fast_forward_on_startup: HashMap<PartitionId, Lsn>,
}

struct PendingSnapshotTask {
    snapshot_id: SnapshotId,
    sender: Option<oneshot::Sender<SnapshotResult>>,
}

type SnapshotResultInternal = Result<PartitionSnapshotMetadata, SnapshotError>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("failed updating the metadata store: {0}")]
    MetadataStore(#[from] ReadModifyWriteError),
    #[error(transparent)]
    InvokerBuild(#[from] BuildError),
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
                overlapping_partitions.push(reader.clone())
            }
        }

        let mut result = Vec::with_capacity(overlapping_partitions.len());

        for reader in overlapping_partitions {
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
        metadata_store_client: MetadataStoreClient,
        partition_store_manager: PartitionStoreManager,
        router_builder: &mut MessageRouterBuilder,
        bifrost: Bifrost,
        snapshot_repository: Option<SnapshotRepository>,
        persisted_lsns_rx: watch::Receiver<BTreeMap<PartitionId, Lsn>>,
    ) -> Self {
        let incoming_update_processors = router_builder.subscribe_to_stream(2);
        let incoming_partition_processor_rpc = router_builder.subscribe_to_stream(128);

        let (tx, rx) = mpsc::channel(updateable_config.pinned().worker.internal_queue_length());
        Self {
            health_status,
            updateable_config,
            processor_states: BTreeMap::default(),
            name_cache: Default::default(),
            metadata_store_client,
            partition_store_manager,
            incoming_update_processors,
            incoming_partition_processor_rpc,
            bifrost,
            rx,
            tx,
            persisted_lsns_rx,
            archived_lsns: HashMap::default(),
            target_tail_lsns: HashMap::default(),
            invokers_status_reader: MultiplexedInvokerStatusReader::default(),
            pending_control_processors: None,
            asynchronous_operations: JoinSet::default(),
            snapshot_export_tasks: FuturesUnordered::default(),
            pending_snapshots: HashMap::default(),
            latest_snapshots: HashMap::default(),
            snapshot_repository,
            fast_forward_on_startup: HashMap::default(),
        }
    }

    pub fn invokers_status_reader(&self) -> MultiplexedInvokerStatusReader {
        self.invokers_status_reader.clone()
    }

    pub fn handle(&self) -> ProcessorsManagerHandle {
        ProcessorsManagerHandle::new(self.tx.clone())
    }

    pub(crate) fn message_handler(&self) -> PartitionProcessorManagerMessageHandler {
        PartitionProcessorManagerMessageHandler::new(self.handle())
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());

        // let watchdog = PersistedLogLsnWatchdog::new(
        //     self.updateable_config
        //         .clone()
        //         .map(|config| &config.worker.storage),
        //     self.partition_store_manager.clone(),
        //     persisted_lsns_tx,
        // );
        // TaskCenter::spawn_child(TaskKind::Watchdog, "persisted-lsn-watchdog", watchdog.run())?;

        let metadata = Metadata::current();

        let mut logs_version_watcher = metadata.watch(MetadataKind::Logs);
        let mut partition_table_version_watcher = metadata.watch(MetadataKind::PartitionTable);

        let mut snapshot_check_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(rand::rng().random_range(30..60)), // delay scheduled snapshots on startup
            with_jitter(Duration::from_secs(1), 0.1),
        );
        snapshot_check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut update_target_tail_lsns = tokio::time::interval(Duration::from_secs(1));
        update_target_tail_lsns.set_missed_tick_behavior(MissedTickBehavior::Delay);

        self.health_status.update(WorkerStatus::Ready);
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
                Some(control_processors) = self.incoming_update_processors.next() => {
                    self.pending_control_processors = Some(PendingControlProcessors::new(control_processors.peer(), control_processors.into_body()));
                    self.on_control_processors();
                }
                _ = logs_version_watcher.changed(), if self.pending_control_processors.is_some() => {
                    // logs version has changed. and we have a control_processors message
                    // waiting for processing. We can check now if logs version matches
                    // and if we can apply this now.
                    self.on_control_processors();
                }
                _ = partition_table_version_watcher.changed(), if self.pending_control_processors.is_some() => {
                    // partition table version has changed. and we have a control_processors message
                    // waiting for processing. We can check now if logs version matches
                    // and if we can apply this now.
                    self.on_control_processors();
                }
                Some(event) = self.asynchronous_operations.join_next() => {
                    self.on_asynchronous_event(event.expect("asynchronous operations must not panic"));
                }
                Some(partition_processor_rpc) = self.incoming_partition_processor_rpc.next() => {
                    self.on_partition_processor_rpc(partition_processor_rpc);
                }
                Some(result) = self.snapshot_export_tasks.next() => {
                    if let Ok(result) = result {
                        self.on_create_snapshot_task_completed(result);
                    } else {
                        debug!("Create snapshot task failed: {}", result.unwrap_err());
                    }
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

    fn on_partition_processor_rpc(
        &self,
        partition_processor_rpc: Incoming<PartitionProcessorRpcRequest>,
    ) {
        let partition_id = partition_processor_rpc.body().partition_id;

        match self.processor_states.get(&partition_id) {
            None => {
                // ignore shutdown errors
                let _ = TaskCenter::spawn(
                    TaskKind::Disposable,
                    "partition-processor-rpc-response",
                    async move {
                        partition_processor_rpc
                            .to_rpc_response(Err(PartitionProcessorRpcError::NotLeader(
                                partition_id,
                            )))
                            .send()
                            .await
                            .map_err(Into::into)
                    },
                );
            }
            Some(processor_state) => {
                processor_state.try_send_rpc(partition_id, partition_processor_rpc);
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
                                ProcessorState::Starting { target_run_mode } => {
                                    debug!(%target_run_mode, "Partition processor was successfully created.");
                                    self.invokers_status_reader.push(
                                        started_processor.key_range().clone(),
                                        started_processor.invoker_status_reader().clone(),
                                    );

                                    let mut new_state = ProcessorState::started(started_processor);
                                    // check whether we need to obtain a new leader epoch
                                    if *target_run_mode == RunMode::Leader {
                                        if let Some(leader_epoch_token) = new_state.run_as_leader()
                                        {
                                            Self::obtain_new_leader_epoch(
                                                partition_id,
                                                leader_epoch_token,
                                                self.metadata_store_client.clone(),
                                                &mut self.asynchronous_operations,
                                            );
                                        }
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
                            gauge!(NUM_ACTIVE_PARTITIONS).set(self.processor_states.len() as f64);
                        }
                    }
                    Err(err) => {
                        info!(%partition_id, %err, "Partition processor failed to start");
                        self.processor_states.remove(&partition_id);
                    }
                }
            }
            EventKind::Stopped(result) => {
                match self.processor_states.remove(&partition_id) {
                    None => {
                        debug!("Stopped partition processor which is no longer running.");
                    }
                    Some(processor_state) => match processor_state {
                        ProcessorState::Starting { .. } => {
                            warn!(%partition_id, "Partition processor failed to start: {result:?}");
                        }
                        ProcessorState::Started { processor, .. } => {
                            self.invokers_status_reader
                                .remove(processor.as_ref().expect("must be some").key_range());

                            match result {
                                Err(ProcessorError::TrimGapEncountered {
                                    trim_gap_end: to_lsn,
                                    read_pointer: sequence_number,
                                }) => {
                                    if self.snapshot_repository.is_some() {
                                        info!(
                                            trim_gap_to_lsn = ?to_lsn,
                                            ?sequence_number,
                                            "Partition processor stopped due to a log trim gap, will attempt to fast-forward on restart",
                                        );
                                        self.fast_forward_on_startup.insert(partition_id, to_lsn);
                                    } else {
                                        warn!(
                                            trim_gap_to_lsn = ?to_lsn,
                                            "Partition processor stopped due to a log trim gap, and no snapshot repository is configured",
                                        );
                                    }
                                }
                                Err(err) => {
                                    warn!(%err, "Partition processor exited unexpectedly")
                                }
                                Ok(_) => {
                                    info!("Partition processor stopped")
                                }
                            }
                        }
                        ProcessorState::Stopping {
                            processor,
                            restart_as,
                        } => {
                            if let Some(processor) = processor {
                                self.invokers_status_reader.remove(processor.key_range());
                            }
                            debug!("Partition processor stopped: {result:?}");

                            if let Some(restart_as) = restart_as {
                                self.on_control_processor(
                                    ControlProcessor {
                                        partition_id,
                                        command: ProcessorCommand::from(restart_as),
                                    },
                                    &Metadata::with_current(|m| m.partition_table_ref()),
                                );
                            }
                        }
                    },
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
                            if let Err(err) = processor_state
                                .on_leader_epoch_obtained(leader_epoch, leader_epoch_token)
                            {
                                info!(%partition_id, %err, "Partition processor failed to process new leader epoch. Stopping it now");
                                processor_state.stop();
                            }
                        }
                        Err(err) => {
                            if processor_state.is_valid_leader_epoch_token(leader_epoch_token) {
                                info!(%partition_id, %err, "Failed obtaining new leader epoch. Continue running as follower");
                                if let Err(err) = processor_state.run_as_follower() {
                                    info!(%partition_id, %err, "Partition processor failed to run as follower. Stopping it now");
                                    processor_state.stop();
                                }
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
            .name(&format!("runtime-result-{}", partition_id))
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
                    let tail = bifrost
                        .find_tail(LogId::from(partition_id), FindTailOptions::Fast)
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
            .name(&format!("obtain-leader-epoch-{}", partition_id))
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

    fn get_state(&self) -> BTreeMap<PartitionId, PartitionProcessorStatus> {
        let persisted_lsns = self.persisted_lsns_rx.borrow();

        // For all running partitions, collect state, enrich it, and send it back.
        self.processor_states
            .iter()
            .filter_map(|(partition_id, processor_state)| {
                let mut status = processor_state.partition_processor_status()?;

                gauge!(PARTITION_TIME_SINCE_LAST_STATUS_UPDATE,
                        PARTITION_LABEL => partition_id.to_string())
                .set(status.updated_at.elapsed());

                gauge!(PARTITION_IS_EFFECTIVE_LEADER,
                        PARTITION_LABEL => partition_id.to_string())
                .set(if status.is_effective_leader() {
                    1.0
                } else {
                    0.0
                });

                gauge!(PARTITION_IS_ACTIVE,
                        PARTITION_LABEL => partition_id.to_string())
                .set(if status.replay_status == ReplayStatus::Active {
                    1.0
                } else {
                    0.0
                });

                if let Some(last_applied_log_lsn) = status.last_applied_log_lsn {
                    gauge!(PARTITION_LAST_APPLIED_LOG_LSN,
                        PARTITION_LABEL => partition_id.to_string())
                    .set(last_applied_log_lsn.as_u64() as f64);
                }

                if let Some(last_persisted_log_lsn) = status.last_persisted_log_lsn {
                    gauge!(PARTITION_LAST_PERSISTED_LOG_LSN,
                        PARTITION_LABEL => partition_id.to_string())
                    .set(last_persisted_log_lsn.as_u64() as f64);
                }

                if let Some(last_record_applied_at) = status.last_record_applied_at {
                    gauge!(PARTITION_TIME_SINCE_LAST_RECORD,
                        PARTITION_LABEL => partition_id.to_string())
                    .set(last_record_applied_at.elapsed());
                }

                // it is a bit unfortunate that we share PartitionProcessorStatus between the
                // PP and the PPManager :-(. Maybe at some point we want to split the struct for it.
                status.last_persisted_log_lsn = persisted_lsns.get(partition_id).cloned();
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
                        gauge!(PARTITION_LAST_APPLIED_LSN_LAG, PARTITION_LABEL => partition_id.to_string())
                        .set(f64::INFINITY);
                    },
                    Some(target_tail_lsn) => {
                        status.target_tail_lsn = Some(target_tail_lsn);

                        // tail lsn always points to the next "free" lsn slot. Therefor the lag is calculate as `lsn-1`
                        // hence we do target_tail_lsn.prev() below
                        gauge!(PARTITION_LAST_APPLIED_LSN_LAG, PARTITION_LABEL => partition_id.to_string())
                        .set(target_tail_lsn.prev().as_u64().saturating_sub(status.last_applied_log_lsn.unwrap_or(Lsn::OLDEST).as_u64()) as f64);
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
            ProcessorsManagerCommand::CreateSnapshot {
                partition_id,
                min_target_lsn,
                tx,
            } => {
                self.on_create_snapshot(partition_id, min_target_lsn, tx);
            }
        }
    }

    fn on_control_processors(&mut self) {
        let (current_logs_version, current_partition_table_version) =
            Metadata::with_current(|m| (m.logs_version(), m.partition_table_version()));
        if self
            .pending_control_processors
            .as_ref()
            .is_some_and(|control_processors| {
                control_processors.min_logs_version() <= current_logs_version
                    && control_processors.min_partition_table_version()
                        <= current_partition_table_version
            })
        {
            let pending_control_processors = self
                .pending_control_processors
                .take()
                .expect("must be some");
            let partition_table = Metadata::with_current(|m| m.partition_table_snapshot());

            info_span!("on_control_processors", from_cluster_controller = %pending_control_processors.sender).in_scope(|| {
                for control_processor in pending_control_processors.control_processors.commands {
                    self.on_control_processor(control_processor, &partition_table);
                }
            });
        }
    }

    fn on_control_processor(
        &mut self,
        control_processor: ControlProcessor,
        partition_table: &PartitionTable,
    ) {
        let partition_id = control_processor.partition_id;

        match control_processor.command {
            ProcessorCommand::Stop => {
                if let Some(processor_state) = self.processor_states.get_mut(&partition_id) {
                    debug!(%partition_id, "Asked to stop partition processor by cluster controller");
                    processor_state.stop();
                }
                if self.pending_snapshots.contains_key(&partition_id) {
                    info!(%partition_id, "Partition processor stop requested with snapshot task result outstanding");
                }
                self.archived_lsns.remove(&partition_id);
                self.latest_snapshots.remove(&partition_id);
            }
            ProcessorCommand::Follower | ProcessorCommand::Leader => {
                if let Some(processor_state) = self.processor_states.get_mut(&partition_id) {
                    if control_processor.command == ProcessorCommand::Leader {
                        if let Some(leader_epoch_token) = processor_state.run_as_leader() {
                            debug!(%partition_id, "Asked to run as leader by cluster controller. Obtaining required leader epoch");
                            Self::obtain_new_leader_epoch(
                                partition_id,
                                leader_epoch_token,
                                self.metadata_store_client.clone(),
                                &mut self.asynchronous_operations,
                            );
                        }
                    } else if control_processor.command == ProcessorCommand::Follower {
                        debug!(%partition_id, "Asked to run as follower by cluster controller");
                        if let Err(err) = processor_state.run_as_follower() {
                            info!(%partition_id, %err, "Partition processor failed to run as follower. Stopping it now");
                            processor_state.stop();
                        }
                    }
                } else if let Some(partition_key_range) = partition_table
                    .get_partition(&partition_id)
                    .map(|partition| &partition.key_range)
                {
                    debug!(%partition_id, "Starting new partition processor to run as {}", control_processor.command);
                    let starting_task = self.create_start_partition_processor_task(
                        partition_id,
                        partition_key_range.clone(),
                    );

                    self.asynchronous_operations
                        .build_task()
                        .name(&format!("start-pp-{}", partition_id))
                        .spawn(
                            async move {
                                let result = starting_task.run();
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
                        ProcessorState::starting(
                            control_processor
                                .command
                                .as_run_mode()
                                .expect("to be follower/leader command"),
                        ),
                    );

                    gauge!(NUM_ACTIVE_PARTITIONS).set(self.processor_states.len() as f64);
                } else {
                    debug!(
                        %partition_id,
                        "Unknown partition id. Ignoring {} command.",
                        control_processor.command
                    );
                }
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
                let _ = sender.send(Err(SnapshotError::PartitionNotFound(partition_id)));
                return;
            }
        };

        let snapshot_repository = self.snapshot_repository.clone();
        let Some(snapshot_repository) = snapshot_repository else {
            let _ = sender.send(Err(SnapshotError::RepositoryNotConfigured(partition_id)));
            return;
        };

        if !processor_state.should_publish_snapshots() {
            let _ = sender.send(Err(SnapshotError::InvalidState(partition_id)));
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
            Err(snapshot_error) => (snapshot_error.partition_id(), Err(snapshot_error)),
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
            self.spawn_update_archived_lsn_task(partition_id, snapshot_repository.clone());
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
        if let Some(snapshot) = self.latest_snapshots.get(&partition_id) {
            if min_target_lsn.is_some_and(|target_lsn| snapshot.min_applied_lsn >= target_lsn) {
                if let Some(sender) = sender {
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
                }
                return;
            }
        }

        match self.pending_snapshots.entry(partition_id) {
            Entry::Vacant(entry) => {
                let config = self.updateable_config.live_load();

                let snapshot_base_path = config.worker.snapshots.snapshots_dir(partition_id);
                let snapshot_id = SnapshotId::new();

                let create_snapshot_task = SnapshotPartitionTask {
                    snapshot_id,
                    partition_id,
                    min_target_lsn,
                    snapshot_base_path,
                    partition_store_manager: self.partition_store_manager.clone(),
                    cluster_name: config.common.cluster_name().into(),
                    node_name: config.common.node_name().into(),
                    snapshot_repository,
                };

                let jitter = if sender.is_some() {
                    Duration::ZERO
                } else {
                    Duration::from_millis(rand::rng().random_range(0..10_000))
                };
                let spawn_task_result = TaskCenter::spawn_unmanaged(
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
                            let _ = sender.send(Err(SnapshotError::InvalidState(partition_id)));
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
                    let _ = sender.send(Err(SnapshotError::SnapshotInProgress(partition_id)));
                }
            }
        }
    }

    fn spawn_update_archived_lsn_task(
        &mut self,
        partition_id: PartitionId,
        snapshot_repository: SnapshotRepository,
    ) {
        self.asynchronous_operations
            .build_task()
            .name(&format!("update-archived-lsn-{}", partition_id))
            .spawn(
                async move {
                    let archived_lsn = snapshot_repository
                        .get_latest_archived_lsn(partition_id)
                        .await
                        .inspect_err(|err| {
                            info!(?partition_id, "Unable to get latest archived LSN: {}", err)
                        })
                        .ok()
                        .unwrap_or(Lsn::INVALID);

                    AsynchronousEvent {
                        partition_id,
                        inner: EventKind::NewArchivedLsn { archived_lsn },
                    }
                }
                .in_current_tc(),
            )
            .expect("to spawn update archived LSN task");
    }

    /// Creates a task that when started will spawn a new partition processor.
    ///
    /// This allows multiple partition processors to be started concurrently without holding
    /// and exclusive lock to [`Self`]
    fn create_start_partition_processor_task(
        &mut self,
        partition_id: PartitionId,
        key_range: RangeInclusive<PartitionKey>,
    ) -> SpawnPartitionProcessorTask {
        // the name is also used as thread names for the corresponding tokio runtimes, let's keep
        // it short.
        let task_name = self
            .name_cache
            .entry(partition_id)
            .or_insert_with(|| SharedString::from(Arc::from(format!("pp-{partition_id}"))));

        SpawnPartitionProcessorTask::new(
            task_name.clone(),
            partition_id,
            key_range,
            self.updateable_config.clone(),
            self.bifrost.clone(),
            self.partition_store_manager.clone(),
            self.snapshot_repository.clone(),
            self.fast_forward_on_startup.remove(&partition_id),
        )
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
                    .unwrap_or_else(|| EpochMetadata::new(node_id, partition_id));

                Ok(next_epoch)
            })
            .await?;
        Ok(epoch.epoch())
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
        result: anyhow::Result<LeaderEpoch>,
    },
    NewTargetTail {
        tail: Option<Lsn>,
    },
    NewArchivedLsn {
        archived_lsn: Lsn,
    },
}

struct PendingControlProcessors {
    // Cluster controller which sent the `ControlProcessors` instructions
    sender: GenerationalNodeId,
    control_processors: ControlProcessors,
}

impl PendingControlProcessors {
    fn new(sender: GenerationalNodeId, control_processors: ControlProcessors) -> Self {
        Self {
            sender,
            control_processors,
        }
    }

    fn min_partition_table_version(&self) -> Version {
        self.control_processors.min_partition_table_version
    }

    fn min_logs_version(&self) -> Version {
        self.control_processors.min_logs_table_version
    }
}

#[cfg(test)]
mod tests {
    use crate::partition_processor_manager::PartitionProcessorManager;
    use googletest::IntoTestResult;
    use restate_bifrost::BifrostService;
    use restate_bifrost::providers::memory_loglet;
    use restate_core::network::MockPeerConnection;
    use restate_core::network::protobuf::network::Header;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_partition_store::PartitionStoreManager;
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::{CommonOptions, Configuration, RocksDbOptions, StorageOptions};
    use restate_types::health::HealthStatus;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::live::{Constant, Live};
    use restate_types::locality::NodeLocation;
    use restate_types::net::AdvertisedAddress;
    use restate_types::net::partition_processor_manager::{
        ControlProcessor, ControlProcessors, ProcessorCommand,
    };
    use restate_types::nodes_config::{
        LogServerConfig, MetadataServerConfig, NodeConfig, NodesConfiguration, Role,
    };
    use restate_types::{GenerationalNodeId, Version};
    use std::collections::BTreeMap;
    use std::time::Duration;
    use test_log::test;
    use tokio::sync::watch;

    /// This test ensures that the lifecycle of partition processors is properly managed by the
    /// [`PartitionProcessorManager`]. See https://github.com/restatedev/restate/issues/2258 for
    /// more details.
    #[test(restate_core::test)]
    async fn proper_partition_processor_lifecycle() -> googletest::Result<()> {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        let node_id = GenerationalNodeId::new(42, 42);
        let node_config = NodeConfig::new(
            "42".to_owned(),
            node_id,
            NodeLocation::default(),
            AdvertisedAddress::Uds("foobar1".into()),
            Role::Worker | Role::Admin,
            LogServerConfig::default(),
            MetadataServerConfig::default(),
        );
        nodes_config.upsert_node(node_config);

        let mut env_builder =
            TestCoreEnvBuilder::with_incoming_only_connector().set_nodes_config(nodes_config);
        let health_status = HealthStatus::default();

        RocksDbManager::init(Constant::new(CommonOptions::default()));

        let bifrost_svc = BifrostService::new(env_builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let partition_store_manager = PartitionStoreManager::create(
            Constant::new(StorageOptions::default()),
            Constant::new(RocksDbOptions::default()).boxed(),
            &[(PartitionId::MIN, 0..=PartitionKey::MAX)],
        )
        .await?;

        let partition_processor_manager = PartitionProcessorManager::new(
            health_status,
            Live::from_value(Configuration::default()),
            env_builder.metadata_store_client.clone(),
            partition_store_manager,
            &mut env_builder.router_builder,
            bifrost,
            None,
            watch::channel(BTreeMap::default()).1,
        );

        let env = env_builder.build().await;
        let processors_manager_handle = partition_processor_manager.handle();

        bifrost_svc.start().await.into_test_result()?;
        TaskCenter::spawn(
            TaskKind::SystemService,
            "partition-processor-manager",
            partition_processor_manager.run(),
        )?;

        let connection = MockPeerConnection::connect(
            node_id,
            env.metadata.nodes_config_version(),
            env.metadata.nodes_config_ref().cluster_name().to_owned(),
            env.networking.connection_manager(),
            10,
        )
        .await
        .into_test_result()?;

        let start_processor_command = ControlProcessors {
            min_logs_table_version: Version::MIN,
            min_partition_table_version: Version::MIN,
            commands: vec![ControlProcessor {
                partition_id: PartitionId::MIN,
                command: ProcessorCommand::Follower,
            }],
        };
        let stop_processor_command = ControlProcessors {
            min_logs_table_version: Version::MIN,
            min_partition_table_version: Version::MIN,
            commands: vec![ControlProcessor {
                partition_id: PartitionId::MIN,
                command: ProcessorCommand::Stop,
            }],
        };

        // let's check whether we can start and stop the partition processor multiple times
        for i in 0..=10 {
            connection
                .send_raw(
                    if i % 2 == 0 {
                        start_processor_command.clone()
                    } else {
                        stop_processor_command.clone()
                    },
                    Header::default(),
                )
                .await
                .into_test_result()?;
        }

        loop {
            let current_state = processors_manager_handle.get_state().await?;

            if current_state.contains_key(&PartitionId::MIN) {
                // wait until we see the PartitionId::MIN partition processor running
                break;
            } else {
                // make sure that we eventually start the partition processor
                connection
                    .send_raw(start_processor_command.clone(), Header::default())
                    .await
                    .into_test_result()?;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
