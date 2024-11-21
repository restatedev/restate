// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
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
mod snapshot_task;
mod spawn_processor_task;

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Add, RangeInclusive};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use metrics::gauge;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, instrument, warn};

use crate::metric_definitions::NUM_ACTIVE_PARTITIONS;
use crate::metric_definitions::PARTITION_IS_ACTIVE;
use crate::metric_definitions::PARTITION_IS_EFFECTIVE_LEADER;
use crate::metric_definitions::PARTITION_LABEL;
use crate::metric_definitions::PARTITION_LAST_APPLIED_LOG_LSN;
use crate::metric_definitions::PARTITION_LAST_PERSISTED_LOG_LSN;
use crate::metric_definitions::PARTITION_TIME_SINCE_LAST_RECORD;
use crate::metric_definitions::PARTITION_TIME_SINCE_LAST_STATUS_UPDATE;
use crate::partition_processor_manager::message_handler::PartitionProcessorManagerMessageHandler;
use crate::partition_processor_manager::persisted_lsn_watchdog::PersistedLogLsnWatchdog;
use crate::partition_processor_manager::processor_state::{
    LeaderEpochToken, ProcessorState, StartedProcessor,
};
use crate::partition_processor_manager::snapshot_task::SnapshotPartitionTask;
use crate::partition_processor_manager::spawn_processor_task::SpawnPartitionProcessorTask;
use restate_bifrost::Bifrost;
use restate_core::network::{Incoming, MessageRouterBuilder, MessageStream};
use restate_core::worker_api::{
    ProcessorsManagerCommand, ProcessorsManagerHandle, SnapshotCreated, SnapshotError,
    SnapshotResult,
};
use restate_core::{cancellation_watcher, Metadata, ShutdownError, TaskHandle, TaskKind};
use restate_core::{RuntimeRootTaskHandle, TaskCenter};
use restate_invoker_api::StatusHandle;
use restate_invoker_impl::{BuildError, ChannelStatusReader};
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_partition_store::snapshots::PartitionSnapshotMetadata;
use restate_partition_store::PartitionStoreManager;
use restate_types::cluster::cluster_state::ReplayStatus;
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
use restate_types::config::Configuration;
use restate_types::epoch::EpochMetadata;
use restate_types::health::HealthStatus;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, SnapshotId};
use restate_types::live::Live;
use restate_types::logs::{Lsn, SequenceNumber};
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
use restate_types::GenerationalNodeId;

pub struct PartitionProcessorManager {
    task_center: TaskCenter,
    health_status: HealthStatus<WorkerStatus>,
    updateable_config: Live<Configuration>,
    processor_states: BTreeMap<PartitionId, ProcessorState>,
    name_cache: BTreeMap<PartitionId, &'static str>,

    metadata: Metadata,
    metadata_store_client: MetadataStoreClient,
    partition_store_manager: PartitionStoreManager,
    incoming_update_processors: MessageStream<ControlProcessors>,
    incoming_partition_processor_rpc: MessageStream<PartitionProcessorRpcRequest>,
    bifrost: Bifrost,
    rx: mpsc::Receiver<ProcessorsManagerCommand>,
    tx: mpsc::Sender<ProcessorsManagerCommand>,

    persisted_lsns_rx: Option<watch::Receiver<BTreeMap<PartitionId, Lsn>>>,
    archived_lsns: HashMap<PartitionId, Lsn>,
    invokers_status_reader: MultiplexedInvokerStatusReader,
    pending_control_processors: Option<ControlProcessors>,

    asynchronous_operations: JoinSet<AsynchronousEvent>,

    pending_snapshots: HashMap<PartitionId, PendingSnapshotTask>,
    snapshot_export_tasks: FuturesUnordered<TaskHandle<SnapshotResultInternal>>,
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
        task_center: TaskCenter,
        health_status: HealthStatus<WorkerStatus>,
        updateable_config: Live<Configuration>,
        metadata: Metadata,
        metadata_store_client: MetadataStoreClient,
        partition_store_manager: PartitionStoreManager,
        router_builder: &mut MessageRouterBuilder,
        bifrost: Bifrost,
    ) -> Self {
        let incoming_update_processors = router_builder.subscribe_to_stream(2);
        let incoming_partition_processor_rpc = router_builder.subscribe_to_stream(128);

        let (tx, rx) = mpsc::channel(updateable_config.pinned().worker.internal_queue_length());
        Self {
            task_center,
            health_status,
            updateable_config,
            processor_states: BTreeMap::default(),
            name_cache: Default::default(),
            metadata,
            metadata_store_client,
            partition_store_manager,
            incoming_update_processors,
            incoming_partition_processor_rpc,
            bifrost,
            rx,
            tx,
            persisted_lsns_rx: None,
            archived_lsns: HashMap::default(),
            invokers_status_reader: MultiplexedInvokerStatusReader::default(),
            pending_control_processors: None,
            asynchronous_operations: JoinSet::default(),
            snapshot_export_tasks: FuturesUnordered::default(),
            pending_snapshots: HashMap::default(),
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
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        let (persisted_lsns_tx, persisted_lsns_rx) = watch::channel(BTreeMap::default());
        self.persisted_lsns_rx = Some(persisted_lsns_rx);

        let watchdog = PersistedLogLsnWatchdog::new(
            self.updateable_config
                .clone()
                .map(|config| &config.worker.storage),
            self.partition_store_manager.clone(),
            persisted_lsns_tx,
        );
        self.task_center.spawn_child(
            TaskKind::Watchdog,
            "persisted-lsn-watchdog",
            None,
            watchdog.run(),
        )?;

        let mut logs_version_watcher = self.metadata.watch(MetadataKind::Logs);
        let mut partition_table_version_watcher = self.metadata.watch(MetadataKind::PartitionTable);

        let mut latest_snapshot_check_interval = tokio::time::interval(Duration::from_secs(5));
        latest_snapshot_check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        self.health_status.update(WorkerStatus::Ready);
        loop {
            tokio::select! {
                Some(command) = self.rx.recv() => {
                    self.on_command(command);
                }
                _ = latest_snapshot_check_interval.tick() => {
                    self.trigger_periodic_partition_snapshots();
                }
                Some(control_processors) = self.incoming_update_processors.next() => {
                    self.pending_control_processors = Some(control_processors.into_body());
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
                    self.health_status.update(WorkerStatus::Unknown);
                    for task in self.snapshot_export_tasks.iter() {
                        task.cancel();
                    }
                    return Ok(());
                }
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
                let _ = self.task_center.spawn(
                    TaskKind::Disposable,
                    "partition-processor-rpc-response",
                    None,
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
                processor_state.try_send_rpc(
                    partition_id,
                    partition_processor_rpc,
                    &self.task_center,
                );
            }
        }
    }

    #[instrument(level = "debug", skip_all, fields(partition_id = %event.partition_id, event = %<&'static str as From<&EventKind>>::from(&event.inner)
    ))]
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
                                                self.task_center.clone(),
                                                self.metadata_store_client.clone(),
                                                self.metadata.my_node_id(),
                                                &mut self.asynchronous_operations,
                                            );
                                        }
                                    }

                                    *processor_state = new_state;

                                    self.await_runtime_task_result(partition_id, runtime_handle);
                                }
                                ProcessorState::Started { .. } => {
                                    panic!("Started two processors for the same partition '{partition_id}'");
                                }
                                ProcessorState::Stopping { processor, .. } => {
                                    assert!(processor.is_none(), "Started two processor for the same partition '{partition_id}'");

                                    debug!("Started partition processor is no longer needed. Stopping it.");
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
                        info!(%partition_id, "Partition processor failed to start: {err}");
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
                            warn!(%partition_id, "Partition processor exited unexpectedly: {result:?}");
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
                                    &self.metadata.partition_table_ref(),
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
                                info!(%partition_id, "Partition processor failed to process new leader epoch: {err}. Stopping it now.");
                                processor_state.stop();
                            }
                        }
                        Err(err) => {
                            if processor_state.is_valid_leader_epoch_token(leader_epoch_token) {
                                info!(%partition_id, "Failed obtaining new leader epoch: {err}. Continue running as follower.");
                                if let Err(err) = processor_state.run_as_follower() {
                                    info!(%partition_id, "Partition processor failed to run as follower: {err}. Stopping it now.");
                                    processor_state.stop();
                                }
                            } else {
                                debug!("Received outdated new leader epoch. Ignoring it.");
                            }
                        }
                    }
                } else {
                    debug!("Partition processor is no longer running. Ignoring new leader epoch result.");
                }
            }
        }
    }

    fn await_runtime_task_result(
        &mut self,
        partition_id: PartitionId,
        runtime_task_handle: RuntimeRootTaskHandle<anyhow::Result<()>>,
    ) {
        let tc = self.task_center.clone();
        self.asynchronous_operations.spawn(async move {
            tc.run_in_scope("await-runtime-task-result", Some(partition_id), async {
                let result = runtime_task_handle.await;
                AsynchronousEvent {
                    partition_id,
                    inner: EventKind::Stopped(result),
                }
            })
            .await
        });
    }

    fn obtain_new_leader_epoch(
        partition_id: PartitionId,
        leader_epoch_token: LeaderEpochToken,
        task_center: TaskCenter,
        metadata_store_client: MetadataStoreClient,
        my_node_id: GenerationalNodeId,
        asynchronous_operations: &mut JoinSet<AsynchronousEvent>,
    ) {
        asynchronous_operations.spawn(async move {
            task_center
                .run_in_scope(
                    "obtain-new-leader-epoch",
                    Some(partition_id),
                    Self::obtain_new_leader_epoch_task(
                        leader_epoch_token,
                        partition_id,
                        metadata_store_client,
                        my_node_id,
                    ),
                )
                .await
        });
    }

    fn get_state(&self) -> BTreeMap<PartitionId, PartitionProcessorStatus> {
        let persisted_lsns = self.persisted_lsns_rx.as_ref().map(|w| w.borrow());

        // For all running partitions, collect state, enrich it, and send it back.
        self.processor_states
            .iter()
            .filter_map(|(partition_id, processor_state)| {
                let status = processor_state.partition_processor_status();

                if let Some(mut status) = status {
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
                    status.last_persisted_log_lsn = persisted_lsns
                        .as_ref()
                        .and_then(|lsns| lsns.get(partition_id).cloned());

                    status.last_archived_log_lsn = self.archived_lsns.get(partition_id).cloned();

                    Some((*partition_id, status))
                } else {
                    None
                }
            })
            .collect()
    }

    fn on_command(&mut self, command: ProcessorsManagerCommand) {
        match command {
            ProcessorsManagerCommand::CreateSnapshot(partition_id, sender) => {
                self.on_create_snapshot(partition_id, sender);
            }
            ProcessorsManagerCommand::GetState(sender) => {
                let _ = sender.send(self.get_state());
            }
        }
    }

    fn on_control_processors(&mut self) {
        if self
            .pending_control_processors
            .as_ref()
            .is_some_and(|control_processors| {
                control_processors.min_logs_table_version <= self.metadata.logs_version()
                    && control_processors.min_partition_table_version
                        <= self.metadata.partition_table_version()
            })
        {
            let control_processors = self
                .pending_control_processors
                .take()
                .expect("must be some");
            let partition_table = self.metadata.partition_table_snapshot();

            for control_processor in control_processors.commands {
                self.on_control_processor(control_processor, &partition_table);
            }
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
                    processor_state.stop();
                }
                if self.pending_snapshots.contains_key(&partition_id) {
                    info!(%partition_id, "Partition processor stop requested with snapshot task result outstanding.");
                }
            }
            ProcessorCommand::Follower | ProcessorCommand::Leader => {
                if let Some(processor_state) = self.processor_states.get_mut(&partition_id) {
                    if control_processor.command == ProcessorCommand::Leader {
                        if let Some(leader_epoch_token) = processor_state.run_as_leader() {
                            Self::obtain_new_leader_epoch(
                                partition_id,
                                leader_epoch_token,
                                self.task_center.clone(),
                                self.metadata_store_client.clone(),
                                self.metadata.my_node_id(),
                                &mut self.asynchronous_operations,
                            );
                        }
                    } else if control_processor.command == ProcessorCommand::Follower {
                        if let Err(err) = processor_state.run_as_follower() {
                            info!("Partition processor '{partition_id}' failed to run as follower: {err}. Stopping it now.");
                            processor_state.stop();
                        }
                    }
                } else if let Some(partition_key_range) = partition_table
                    .get_partition(&partition_id)
                    .map(|partition| &partition.key_range)
                {
                    let starting_task = self
                        .start_partition_processor_task(partition_id, partition_key_range.clone());

                    // We spawn the partition processors start tasks on the blocking thread pool due to a macOS issue
                    // where doing otherwise appears to starve the Tokio event loop, causing very slow startup.
                    let handle = self.task_center.spawn_blocking_unmanaged(
                        "starting-partition-processor",
                        Some(partition_id),
                        starting_task.run(),
                    );

                    self.asynchronous_operations.spawn(async move {
                        let result = handle.await.expect("task must not panic");
                        AsynchronousEvent {
                            partition_id,
                            inner: EventKind::Started(result),
                        }
                    });

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
                        "Unknown partition id '{partition_id}'. Ignoring {} command.",
                        control_processor.command
                    );
                }
            }
        }
    }

    fn on_create_snapshot(
        &mut self,
        partition_id: PartitionId,
        sender: oneshot::Sender<SnapshotResult>,
    ) {
        let processor_state = match self.processor_states.get(&partition_id) {
            Some(state) => state,
            None => {
                let _ = sender.send(Err(SnapshotError::PartitionNotFound(partition_id)));
                return;
            }
        };

        if !processor_state.should_publish_snapshots() {
            let _ = sender.send(Err(SnapshotError::InvalidState(partition_id)));
            return;
        }

        self.spawn_create_snapshot_task(partition_id, Some(sender));
    }

    fn on_create_snapshot_task_completed(&mut self, result: SnapshotResultInternal) {
        let (partition_id, response) = match result {
            Ok(metadata) => {
                self.archived_lsns
                    .insert(metadata.partition_id, metadata.min_applied_lsn);

                (
                    metadata.partition_id,
                    Ok(SnapshotCreated {
                        snapshot_id: metadata.snapshot_id,
                        partition_id: metadata.partition_id,
                    }),
                )
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
        let Some(records_per_snapshot) = self
            .updateable_config
            .live_load()
            .worker
            .snapshots
            .snapshot_interval_num_records
        else {
            return;
        };

        let snapshot_partitions: Vec<_> = self
            .processor_states
            .iter()
            .filter_map(|(partition_id, state)| {
                state
                    .partition_processor_status()
                    .map(|status| (*partition_id, status))
            })
            .filter(|(_, status)| {
                status.effective_mode == RunMode::Leader
                    && status.replay_status == ReplayStatus::Active
                    && status.last_applied_log_lsn.unwrap_or(Lsn::INVALID)
                        >= status
                            .last_archived_log_lsn
                            .unwrap_or(Lsn::OLDEST)
                            .add(Lsn::from(records_per_snapshot.get()))
            })
            .collect();

        for (partition_id, status) in snapshot_partitions {
            debug!(
                %partition_id,
                last_archived_lsn = %status.last_archived_log_lsn.unwrap_or(SequenceNumber::OLDEST),
                last_applied_lsn = %status.last_applied_log_lsn.unwrap_or(SequenceNumber::INVALID),
                "Requesting partition snapshot",
            );
            self.spawn_create_snapshot_task(partition_id, None);
        }
    }

    /// Spawn a task to create a snapshot of the given partition. Optionally, a sender will be
    /// notified of the result on completion.
    fn spawn_create_snapshot_task(
        &mut self,
        partition_id: PartitionId,
        sender: Option<oneshot::Sender<SnapshotResult>>,
    ) {
        match self.pending_snapshots.entry(partition_id) {
            Entry::Vacant(entry) => {
                let config = self.updateable_config.live_load();

                let snapshot_base_path = config.worker.snapshots.snapshots_dir(partition_id);
                let snapshot_id = SnapshotId::new();

                let create_snapshot_task = SnapshotPartitionTask {
                    snapshot_id,
                    partition_id,
                    snapshot_base_path,
                    partition_store_manager: self.partition_store_manager.clone(),
                    cluster_name: config.common.cluster_name().into(),
                    node_name: config.common.node_name().into(),
                };

                let spawn_task_result = restate_core::task_center().spawn_unmanaged(
                    TaskKind::PartitionSnapshotProducer,
                    "create-snapshot",
                    Some(partition_id),
                    create_snapshot_task.run(),
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
                info!(
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

    /// Creates a task that when started will spawn a new partition processor.
    ///
    /// This allows multiple partition processors to be started concurrently without holding
    /// and exclusive lock to [`Self`]
    fn start_partition_processor_task(
        &mut self,
        partition_id: PartitionId,
        key_range: RangeInclusive<PartitionKey>,
    ) -> SpawnPartitionProcessorTask {
        // the name is also used as thread names for the corresponding tokio runtimes, let's keep
        // it short.
        let task_name = self
            .name_cache
            .entry(partition_id)
            .or_insert_with(|| Box::leak(Box::new(format!("pp-{}", partition_id))));

        SpawnPartitionProcessorTask::new(
            task_name,
            self.metadata.my_node_id(),
            partition_id,
            key_range,
            self.updateable_config.clone(),
            self.metadata.clone(),
            self.bifrost.clone(),
            self.partition_store_manager.clone(),
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
    Started(anyhow::Result<(StartedProcessor, RuntimeRootTaskHandle<anyhow::Result<()>>)>),
    Stopped(anyhow::Result<()>),
    NewLeaderEpoch {
        leader_epoch_token: LeaderEpochToken,
        result: anyhow::Result<LeaderEpoch>,
    },
}

#[cfg(test)]
mod tests {
    use crate::partition_processor_manager::PartitionProcessorManager;
    use googletest::IntoTestResult;
    use restate_bifrost::providers::memory_loglet;
    use restate_bifrost::BifrostService;
    use restate_core::network::MockPeerConnection;
    use restate_core::{TaskKind, TestCoreEnvBuilder};
    use restate_partition_store::PartitionStoreManager;
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::{CommonOptions, Configuration, RocksDbOptions, StorageOptions};
    use restate_types::health::HealthStatus;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::live::{Constant, Live};
    use restate_types::net::partition_processor_manager::{
        ControlProcessor, ControlProcessors, ProcessorCommand,
    };
    use restate_types::net::AdvertisedAddress;
    use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role};
    use restate_types::protobuf::node::Header;
    use restate_types::{GenerationalNodeId, Version};
    use std::time::Duration;
    use test_log::test;

    /// This test ensures that the lifecycle of partition processors is properly managed by the
    /// [`PartitionProcessorManager`]. See https://github.com/restatedev/restate/issues/2258 for
    /// more details.
    #[test(tokio::test)]
    async fn proper_partition_processor_lifecycle() -> googletest::Result<()> {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        let node_id = GenerationalNodeId::new(42, 42);
        let node_config = NodeConfig::new(
            "42".to_owned(),
            node_id,
            AdvertisedAddress::Uds("foobar1".into()),
            Role::Worker | Role::Admin,
            LogServerConfig::default(),
        );
        nodes_config.upsert_node(node_config);

        let mut env_builder =
            TestCoreEnvBuilder::with_incoming_only_connector().set_nodes_config(nodes_config);
        let health_status = HealthStatus::default();

        env_builder
            .tc
            .run_in_scope_sync("db-manager-init", None, || {
                RocksDbManager::init(Constant::new(CommonOptions::default()));
            });

        let bifrost_svc = BifrostService::new(env_builder.tc.clone(), env_builder.metadata.clone())
            .with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let partition_store_manager = PartitionStoreManager::create(
            Constant::new(StorageOptions::default()),
            Constant::new(RocksDbOptions::default()).boxed(),
            &[(PartitionId::MIN, 0..=PartitionKey::MAX)],
        )
        .await?;

        let partition_processor_manager = PartitionProcessorManager::new(
            env_builder.tc.clone(),
            health_status,
            Live::from_value(Configuration::default()),
            env_builder.metadata.clone(),
            env_builder.metadata_store_client.clone(),
            partition_store_manager,
            &mut env_builder.router_builder,
            bifrost,
        );

        let env = env_builder.build().await;
        let processors_manager_handle = partition_processor_manager.handle();

        env.tc
            .run_in_scope("init-bifrost", None, bifrost_svc.start())
            .await
            .into_test_result()?;
        env.tc.spawn(
            TaskKind::SystemService,
            "partition-processor-manager",
            None,
            partition_processor_manager.run(),
        )?;
        let tc = env.tc.clone();
        tc.run_in_scope("test", None, async move {
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

            googletest::Result::Ok(())
        })
        .await?;

        tc.shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
