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
mod spawn_processor_task;

use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::stream::StreamExt;
use metrics::gauge;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tracing::{debug, debug_span, error, info, instrument, trace, warn, Instrument};

use restate_bifrost::Bifrost;
use restate_core::network::rpc_router::{RpcError, RpcRouter};
use restate_core::network::{Incoming, MessageRouterBuilder, MessageStream};
use restate_core::network::{Networking, TransportConnect};
use restate_core::worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};
use restate_core::TaskCenter;
use restate_core::{cancellation_watcher, Metadata, ShutdownError, TaskKind};
use restate_invoker_api::StatusHandle;
use restate_invoker_impl::{BuildError, ChannelStatusReader};
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_partition_store::PartitionStoreManager;
use restate_types::cluster::cluster_state::ReplayStatus;
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
use restate_types::config::Configuration;
use restate_types::health::HealthStatus;
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::live::Live;
use restate_types::logs::Lsn;
use restate_types::net::cluster_controller::AttachRequest;
use restate_types::net::cluster_controller::{Action, AttachResponse};
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
    PartitionProcessorHandleError, ProcessorState, StartedProcessor,
};
use crate::partition_processor_manager::spawn_processor_task::SpawnPartitionProcessorTask;

pub struct PartitionProcessorManager<T> {
    task_center: TaskCenter,
    health_status: HealthStatus<WorkerStatus>,
    updateable_config: Live<Configuration>,
    running_partition_processors: BTreeMap<PartitionId, ProcessorState>,
    name_cache: BTreeMap<PartitionId, &'static str>,

    metadata: Metadata,
    metadata_store_client: MetadataStoreClient,
    partition_store_manager: PartitionStoreManager,
    attach_router: RpcRouter<AttachRequest>,
    incoming_update_processors: MessageStream<ControlProcessors>,
    incoming_partition_processor_rpc: MessageStream<PartitionProcessorRpcRequest>,
    networking: Networking<T>,
    bifrost: Bifrost,
    rx: mpsc::Receiver<ProcessorsManagerCommand>,
    tx: mpsc::Sender<ProcessorsManagerCommand>,
    latest_attach_response: Option<(GenerationalNodeId, AttachResponse)>,

    persisted_lsns_rx: Option<watch::Receiver<BTreeMap<PartitionId, Lsn>>>,
    invokers_status_reader: MultiplexedInvokerStatusReader,
    pending_control_processors: Option<ControlProcessors>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("failed updating the metadata store: {0}")]
    MetadataStore(#[from] ReadModifyWriteError),
    #[error("could not send command to partition processor since it is busy")]
    PartitionProcessorBusy,
    #[error(transparent)]
    InvokerBuild(#[from] BuildError),
}

#[derive(Debug, thiserror::Error)]
enum AttachError {
    #[error("No cluster controller found in nodes configuration")]
    NoClusterController,
    #[error(transparent)]
    ShutdownError(#[from] ShutdownError),
}

impl From<PartitionProcessorHandleError> for Error {
    fn from(value: PartitionProcessorHandleError) -> Self {
        match value {
            PartitionProcessorHandleError::Shutdown(err) => Error::Shutdown(err),
            PartitionProcessorHandleError::FailedSend => Error::PartitionProcessorBusy,
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

impl<T: TransportConnect> PartitionProcessorManager<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_center: TaskCenter,
        health_status: HealthStatus<WorkerStatus>,
        updateable_config: Live<Configuration>,
        metadata: Metadata,
        metadata_store_client: MetadataStoreClient,
        partition_store_manager: PartitionStoreManager,
        router_builder: &mut MessageRouterBuilder,
        networking: Networking<T>,
        bifrost: Bifrost,
    ) -> Self {
        let attach_router = RpcRouter::new(router_builder);
        let incoming_update_processors = router_builder.subscribe_to_stream(2);
        let incoming_partition_processor_rpc = router_builder.subscribe_to_stream(128);

        let (tx, rx) = mpsc::channel(updateable_config.pinned().worker.internal_queue_length());
        Self {
            task_center,
            health_status,
            updateable_config,
            running_partition_processors: BTreeMap::default(),
            name_cache: Default::default(),
            metadata,
            metadata_store_client,
            partition_store_manager,
            incoming_update_processors,
            incoming_partition_processor_rpc,
            networking,
            bifrost,
            attach_router,
            rx,
            tx,
            latest_attach_response: None,
            persisted_lsns_rx: None,
            invokers_status_reader: MultiplexedInvokerStatusReader::default(),
            pending_control_processors: None,
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

    async fn attach(&mut self) -> Result<Incoming<AttachResponse>, AttachError> {
        loop {
            // We try to get the admin node on every retry since it might change between retries.
            let admin_node = self
                .metadata
                .nodes_config_ref()
                .get_admin_node()
                .ok_or(AttachError::NoClusterController)?
                .current_generation;

            debug!(
                "Attempting to attach to cluster controller '{}'",
                admin_node
            );
            if admin_node == self.metadata.my_node_id() {
                // If this node is running the cluster controller, we need to wait a little to give cluster
                // controller time to start up. This is only done to reduce the chances of observing
                // connection errors in log. Such logs are benign since we retry, but it's still not nice
                // to print, specially in a single-node setup.
                trace!("This node is the cluster controller, giving cluster controller service time to start");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            match self
                .attach_router
                .call(&self.networking, admin_node, AttachRequest::default())
                .await
            {
                Ok(response) => return Ok(response),
                Err(RpcError::Shutdown(e)) => return Err(AttachError::ShutdownError(e)),
                Err(e) => {
                    warn!(
                        "Failed to send attach message to cluster controller: {}, retrying....",
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        let (processor_mgr_event_tx, mut processor_mgr_event_rx) = mpsc::channel(128);

        // Initial attach
        let response = tokio::time::timeout(Duration::from_secs(10), self.attach())
            .await
            .context("Timeout waiting to attach to a cluster controller")??;

        let from = *response.peer();
        let msg = response.into_body();
        self.apply_plan(&msg.actions, processor_mgr_event_tx.clone())
            .await?;
        self.latest_attach_response = Some((from, msg));
        info!("Plan applied from attaching to controller {}", from);

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

        self.health_status.update(WorkerStatus::Ready);
        loop {
            tokio::select! {
                Some(command) = self.rx.recv() => {
                    self.on_command(command);
                }
                Some(control_processors) = self.incoming_update_processors.next() => {
                    self.pending_control_processors = Some(control_processors.into_body());
                    self.on_control_processors(
                        processor_mgr_event_tx.clone()).await?;
                }
                _ = logs_version_watcher.changed(), if self.pending_control_processors.is_some() => {
                    // logs version has changed. and we have a control_processors message
                    // waiting for processing. We can check now if logs version matches
                    // and if we can apply this now.
                    self.on_control_processors(
                        processor_mgr_event_tx.clone()).await?;
                }
                _ = partition_table_version_watcher.changed(), if self.pending_control_processors.is_some() => {
                    // partition table version has changed. and we have a control_processors message
                    // waiting for processing. We can check now if logs version matches
                    // and if we can apply this now.
                    self.on_control_processors(
                        processor_mgr_event_tx.clone()).await?;
                }
                Some(event) = processor_mgr_event_rx.recv() => {
                    self.on_processor_mgr_event(event);
                }
                Some(partition_processor_rpc) = self.incoming_partition_processor_rpc.next() => {
                    self.on_partition_processor_rpc(partition_processor_rpc);
                }
                _ = &mut shutdown => {
                    self.health_status.update(WorkerStatus::Unknown);
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

        match self.running_partition_processors.get(&partition_id) {
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
            Some(ProcessorState::Started(partition_processor)) => {
                if let Err(err) = partition_processor.try_send_rpc(partition_processor_rpc) {
                    match err {
                        TrySendError::Full(req) => {
                            let _ = self.task_center.spawn(
                                TaskKind::Disposable,
                                "partition-processor-rpc",
                                None,
                                async move {
                                    req.into_outgoing(Err(PartitionProcessorRpcError::Busy))
                                        .send()
                                        .await
                                        .map_err(Into::into)
                                },
                            );
                        }
                        TrySendError::Closed(req) => {
                            let _ = self.task_center.spawn(
                                TaskKind::Disposable,
                                "partition-processor-rpc",
                                None,
                                async move {
                                    req.into_outgoing(Err(PartitionProcessorRpcError::NotLeader(
                                        partition_id,
                                    )))
                                    .send()
                                    .await
                                    .map_err(Into::into)
                                },
                            );
                        }
                    }
                }
            }
            Some(ProcessorState::Starting(_)) => {
                let _ = self.task_center.spawn(
                    TaskKind::Disposable,
                    "partition-processor-rpc",
                    None,
                    async move {
                        partition_processor_rpc
                            .into_outgoing(Err(PartitionProcessorRpcError::Starting))
                            .send()
                            .await
                            .map_err(Into::into)
                    },
                );
            }
        }
    }

    fn on_processor_mgr_event(&mut self, event: ManagerEvent) {
        let ManagerEvent {
            partition_id,
            event,
        } = event;

        match event {
            ProcessorEvent::Started(state) => {
                self.invokers_status_reader.push(
                    state.key_range().clone(),
                    state.invoker_status_reader().clone(),
                );
                self.running_partition_processors
                    .insert(partition_id, ProcessorState::Started(state));
            }
            ProcessorEvent::StartFailed(err) => {
                error!(%partition_id, error=%err, "Starting partition processor failed");
                self.running_partition_processors.remove(&partition_id);
            }
            ProcessorEvent::Stopped(err) => {
                if let Some(err) = err {
                    warn!(%partition_id, error=%err, "Partition processor exited unexpectedly");
                }

                if let Some(ProcessorState::Started(status)) =
                    self.running_partition_processors.remove(&partition_id)
                {
                    self.invokers_status_reader.remove(status.key_range());
                }
            }
        }
    }

    fn get_state(&self) -> BTreeMap<PartitionId, PartitionProcessorStatus> {
        let persisted_lsns = self.persisted_lsns_rx.as_ref().map(|w| w.borrow());

        // For all running partitions, collect state, enrich it, and send it back.
        self.running_partition_processors
            .iter()
            .filter_map(|(partition_id, status)| {
                let ProcessorState::Started(state) = status else {
                    return None;
                };

                let mut status = state.partition_processor_status();
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
                status.planned_mode = state.planned_mode();
                status.last_persisted_log_lsn = persisted_lsns
                    .as_ref()
                    .and_then(|lsns| lsns.get(partition_id).cloned());
                Some((*partition_id, status))
            })
            .collect()
    }

    fn on_command(&mut self, command: ProcessorsManagerCommand) {
        use ProcessorsManagerCommand::*;
        match command {
            CreateSnapshot(partition_id, sender) => {
                if let Some(processor_state) = self.running_partition_processors.get(&partition_id)
                {
                    match processor_state {
                        ProcessorState::Starting(_) => {
                            let _ = sender.send(Err(anyhow::anyhow!(
                                "Partition processors '{}' is still starting",
                                partition_id
                            )));
                        }
                        ProcessorState::Started(started_processor) => {
                            started_processor.create_snapshot(sender);
                        }
                    }
                } else {
                    let _ = sender.send(Err(anyhow::anyhow!(
                        "Partition processor '{}' not found",
                        partition_id
                    )));
                }
            }
            GetState(sender) => {
                let _ = sender.send(self.get_state());
            }
        }
    }

    #[instrument(parent=None, skip_all)]
    async fn on_control_processors(&mut self, events: EventSender) -> Result<(), Error> {
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
                self.on_control_processor(control_processor, &partition_table, events.clone())
                    .await?;
            }
        }

        Ok(())
    }

    #[instrument(level = "debug", skip_all, fields(partition_id = %control_processor.partition_id))]
    async fn on_control_processor(
        &mut self,
        control_processor: ControlProcessor,
        partition_table: &PartitionTable,
        events: EventSender,
    ) -> Result<(), Error> {
        let partition_id = control_processor.partition_id;

        match control_processor.command {
            ProcessorCommand::Stop => {
                if let Some(status) = self.running_partition_processors.remove(&partition_id) {
                    status.stop(&self.task_center).await;
                }
            }
            ProcessorCommand::Follower | ProcessorCommand::Leader => {
                let run_mode = if control_processor.command == ProcessorCommand::Leader {
                    RunMode::Leader
                } else if control_processor.command == ProcessorCommand::Follower {
                    RunMode::Follower
                } else {
                    unreachable!()
                };

                if let Some(status) = self.running_partition_processors.get_mut(&partition_id) {
                    if let ProcessorState::Started(state) = status {
                        // if we error here, then the system is shutting down
                        if run_mode == RunMode::Follower {
                            state.step_down()?;
                        } else if run_mode == RunMode::Leader {
                            state
                                .run_for_leader(
                                    self.metadata_store_client.clone(),
                                    self.metadata.my_node_id(),
                                )
                                .await?;
                        }
                    } else {
                        // otherwise if it's still starting we can't control it yet
                        debug!(%partition_id, "Ignoring control partition processor message. Partition processor is still starting");
                    }
                } else if let Some(partition_key_range) = partition_table
                    .get_partition(&partition_id)
                    .map(|partition| &partition.key_range)
                {
                    let starting_task = self.start_partition_processor_task(
                        partition_id,
                        partition_key_range.clone(),
                        run_mode,
                        events,
                    );

                    // note: We starting each task on it's own thread due to an issue that shows up on MacOS
                    // where spawning many tasks of this kind causes a lock contention in tokio which leads to
                    // starvation of the event loop.
                    let handle = self.task_center.spawn_blocking_unmanaged(
                        "starting-partition-processor",
                        Some(partition_id),
                        starting_task.run().instrument(
                            debug_span!("starting_partition_processor", partition_id=%partition_id),
                        ),
                    );

                    self.running_partition_processors
                        .insert(partition_id, ProcessorState::Starting(handle));
                } else {
                    debug!(
                        "Unknown partition id '{partition_id}'. Ignoring {} command.",
                        control_processor.command
                    );
                }
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn apply_plan(&mut self, actions: &[Action], events: EventSender) -> Result<(), Error> {
        for action in actions {
            match action {
                Action::RunPartition(action) => {
                    #[allow(clippy::map_entry)]
                    if !self
                        .running_partition_processors
                        .contains_key(&action.partition_id)
                    {
                        let starting_task = self.start_partition_processor_task(
                            action.partition_id,
                            action.key_range_inclusive.clone().into(),
                            action.mode,
                            events.clone(),
                        );

                        // note: We starting each task on it's own thread due to an issue that shows up on MacOS
                        // where spawning many tasks of this kind causes a lock contention in tokio which leads to
                        // starvation of the event loop.
                        let handle = self.task_center.spawn_blocking_unmanaged(
                            "starting-partition-processor",
                            Some(action.partition_id),
                            starting_task.run().instrument(debug_span!("starting_partition_processor", partition_id=%action.partition_id)),
                        );

                        self.running_partition_processors
                            .insert(action.partition_id, ProcessorState::Starting(handle));
                    } else {
                        debug!(
                            "Partition processor for partition id '{}' is already running.",
                            action.partition_id
                        );
                    }
                }
            }
        }

        gauge!(NUM_ACTIVE_PARTITIONS).set(self.running_partition_processors.len() as f64);
        Ok(())
    }

    /// Creates a task that when started will spawn a new partition processor.
    ///
    /// This allows multiple partition processors to be started concurrently without holding
    /// and exclusive lock to [`Self`]
    fn start_partition_processor_task(
        &mut self,
        partition_id: PartitionId,
        key_range: RangeInclusive<PartitionKey>,
        run_mode: RunMode,
        events: EventSender,
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
            run_mode,
            key_range,
            self.updateable_config.clone(),
            self.metadata.clone(),
            self.bifrost.clone(),
            self.partition_store_manager.clone(),
            self.metadata_store_client.clone(),
            events,
        )
    }
}

type EventSender = mpsc::Sender<ManagerEvent>;

struct ManagerEvent {
    partition_id: PartitionId,
    event: ProcessorEvent,
}

enum ProcessorEvent {
    Started(StartedProcessor),
    StartFailed(anyhow::Error),
    Stopped(Option<anyhow::Error>),
}
