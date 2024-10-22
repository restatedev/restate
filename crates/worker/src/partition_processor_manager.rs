// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::future::OptionFuture;
use futures::stream::StreamExt;
use futures::Stream;
use metrics::gauge;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, instrument, trace, warn};

use restate_bifrost::Bifrost;
use restate_core::network::rpc_router::{RpcError, RpcRouter};
use restate_core::network::{Incoming, MessageRouterBuilder};
use restate_core::network::{MessageHandler, Networking, TransportConnect};
use restate_core::worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};
use restate_core::{cancellation_watcher, task_center, Metadata, ShutdownError, TaskId, TaskKind};
use restate_core::{RuntimeError, TaskCenter};
use restate_invoker_api::StatusHandle;
use restate_invoker_impl::Service as InvokerService;
use restate_invoker_impl::{BuildError, ChannelStatusReader};
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_partition_store::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::fsm_table::ReadOnlyFsmTable;
use restate_storage_api::StorageError;
use restate_types::cluster::cluster_state::ReplayStatus;
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
use restate_types::config::{Configuration, StorageOptions};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, SnapshotId};
use restate_types::live::Live;
use restate_types::live::LiveLoad;
use restate_types::logs::Lsn;
use restate_types::logs::SequenceNumber;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::cluster_controller::AttachRequest;
use restate_types::net::cluster_controller::{Action, AttachResponse};
use restate_types::net::metadata::MetadataKind;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, CreateSnapshotResponse, GetProcessorsState,
    ProcessorCommand, SnapshotError,
};
use restate_types::net::partition_processor_manager::{
    CreateSnapshotRequest, ProcessorsStateResponse,
};
use restate_types::partition_table::PartitionTable;
use restate_types::schema::Schema;
use restate_types::time::MillisSinceEpoch;
use restate_types::GenerationalNodeId;

use crate::invoker_integration::EntryEnricher;
use crate::metric_definitions::NUM_ACTIVE_PARTITIONS;
use crate::metric_definitions::PARTITION_IS_ACTIVE;
use crate::metric_definitions::PARTITION_IS_EFFECTIVE_LEADER;
use crate::metric_definitions::PARTITION_LABEL;
use crate::metric_definitions::PARTITION_LAST_APPLIED_LOG_LSN;
use crate::metric_definitions::PARTITION_LAST_PERSISTED_LOG_LSN;
use crate::metric_definitions::PARTITION_TIME_SINCE_LAST_RECORD;
use crate::metric_definitions::PARTITION_TIME_SINCE_LAST_STATUS_UPDATE;
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::PartitionProcessorControlCommand;
use crate::PartitionProcessorBuilder;

pub struct PartitionProcessorManager<T> {
    task_center: TaskCenter,
    updateable_config: Live<Configuration>,
    running_partition_processors: BTreeMap<PartitionId, ProcessorState>,
    name_cache: BTreeMap<PartitionId, &'static str>,

    metadata: Metadata,
    metadata_store_client: MetadataStoreClient,
    partition_store_manager: PartitionStoreManager,
    attach_router: RpcRouter<AttachRequest>,
    incoming_get_state:
        Pin<Box<dyn Stream<Item = Incoming<GetProcessorsState>> + Send + Sync + 'static>>,
    incoming_update_processors:
        Pin<Box<dyn Stream<Item = Incoming<ControlProcessors>> + Send + Sync + 'static>>,
    networking: Networking<T>,
    bifrost: Bifrost,
    rx: mpsc::Receiver<ProcessorsManagerCommand>,
    tx: mpsc::Sender<ProcessorsManagerCommand>,
    latest_attach_response: Option<(GenerationalNodeId, AttachResponse)>,

    persisted_lsns_rx: Option<watch::Receiver<BTreeMap<PartitionId, Lsn>>>,
    invokers_status_reader: MultiplexedInvokerStatusReader,
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

struct ProcessorState {
    partition_id: PartitionId,
    task_id: TaskId,
    _created_at: MillisSinceEpoch,
    _key_range: RangeInclusive<PartitionKey>,
    planned_mode: RunMode,
    handle: PartitionProcessorHandle,
    watch_rx: watch::Receiver<PartitionProcessorStatus>,
}

impl ProcessorState {
    fn new(
        partition_id: PartitionId,
        task_id: TaskId,
        key_range: RangeInclusive<PartitionKey>,
        handle: PartitionProcessorHandle,
        watch_rx: watch::Receiver<PartitionProcessorStatus>,
    ) -> Self {
        Self {
            partition_id,
            task_id,
            _created_at: MillisSinceEpoch::now(),
            _key_range: key_range,
            planned_mode: RunMode::Follower,
            handle,
            watch_rx,
        }
    }

    fn step_down(&mut self) -> Result<(), Error> {
        if self.planned_mode != RunMode::Follower {
            self.handle.step_down()?;
        }

        self.planned_mode = RunMode::Follower;

        Ok(())
    }

    async fn run_for_leader(
        &mut self,
        metadata_store_client: MetadataStoreClient,
        node_id: GenerationalNodeId,
    ) -> Result<(), Error> {
        if self.planned_mode != RunMode::Leader {
            let leader_epoch =
                Self::obtain_next_epoch(metadata_store_client, self.partition_id, node_id).await?;
            self.handle.run_for_leader(leader_epoch)?;
        }

        self.planned_mode = RunMode::Leader;

        Ok(())
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

#[derive(Debug, thiserror::Error)]
enum PartitionProcessorHandleError {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("command could not be sent")]
    FailedSend,
}

impl<T> From<TrySendError<T>> for PartitionProcessorHandleError {
    fn from(value: TrySendError<T>) -> Self {
        match value {
            TrySendError::Full(_) => PartitionProcessorHandleError::FailedSend,
            TrySendError::Closed(_) => PartitionProcessorHandleError::Shutdown(ShutdownError),
        }
    }
}

impl From<PartitionProcessorHandleError> for Error {
    fn from(value: PartitionProcessorHandleError) -> Self {
        match value {
            PartitionProcessorHandleError::Shutdown(err) => Error::Shutdown(err),
            PartitionProcessorHandleError::FailedSend => Error::PartitionProcessorBusy,
        }
    }
}

struct PartitionProcessorHandle {
    control_tx: mpsc::Sender<PartitionProcessorControlCommand>,
}

impl PartitionProcessorHandle {
    fn new(control_tx: mpsc::Sender<PartitionProcessorControlCommand>) -> Self {
        Self { control_tx }
    }

    fn step_down(&self) -> Result<(), PartitionProcessorHandleError> {
        self.control_tx
            .try_send(PartitionProcessorControlCommand::StepDown)?;
        Ok(())
    }

    fn run_for_leader(
        &self,
        leader_epoch: LeaderEpoch,
    ) -> Result<(), PartitionProcessorHandleError> {
        self.control_tx
            .try_send(PartitionProcessorControlCommand::RunForLeader(leader_epoch))?;
        Ok(())
    }

    fn create_snapshot(
        &self,
        sender: Option<oneshot::Sender<anyhow::Result<SnapshotId>>>,
    ) -> Result<(), PartitionProcessorHandleError> {
        self.control_tx
            .try_send(PartitionProcessorControlCommand::CreateSnapshot(sender))?;
        Ok(())
    }
}

/// RPC message handler for Partition Processor management operations.
pub struct PartitionProcessorManagerMessageHandler {
    processors_manager_handle: ProcessorsManagerHandle,
}

impl PartitionProcessorManagerMessageHandler {
    fn new(
        processors_manager_handle: ProcessorsManagerHandle,
    ) -> PartitionProcessorManagerMessageHandler {
        Self {
            processors_manager_handle,
        }
    }
}

impl MessageHandler for PartitionProcessorManagerMessageHandler {
    type MessageType = CreateSnapshotRequest;

    async fn on_message(&self, msg: Incoming<Self::MessageType>) {
        debug!("Received '{:?}' from {}", msg.body(), msg.peer());

        let processors_manager_handle = self.processors_manager_handle.clone();
        task_center()
            .spawn_child(
                TaskKind::Disposable,
                "create-snapshot-request-rpc",
                None,
                async move {
                    let create_snapshot_result = processors_manager_handle
                        .create_snapshot(msg.body().partition_id)
                        .await;
                    debug!(
                        partition_id = ?msg.body().partition_id,
                        result = ?create_snapshot_result,
                        "Create snapshot completed",
                    );

                    match create_snapshot_result.as_ref() {
                        Ok(snapshot_id) => msg.to_rpc_response(CreateSnapshotResponse {
                            result: Ok(*snapshot_id),
                        }),
                        Err(error) => msg.to_rpc_response(CreateSnapshotResponse {
                            result: Err(SnapshotError::SnapshotCreationFailed(error.to_string())),
                        }),
                    }
                    .send()
                    .await
                    .map_err(|e| {
                        warn!(result = ?create_snapshot_result, "Failed to send response: {}", e);
                        anyhow::anyhow!("Failed to send response to create snapshot request: {}", e)
                    })
                },
            )
            .map_err(|e| {
                warn!("Failed to spawn request handler: {}", e);
            })
            .ok();
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
        updateable_config: Live<Configuration>,
        metadata: Metadata,
        metadata_store_client: MetadataStoreClient,
        partition_store_manager: PartitionStoreManager,
        router_builder: &mut MessageRouterBuilder,
        networking: Networking<T>,
        bifrost: Bifrost,
    ) -> Self {
        let attach_router = RpcRouter::new(router_builder);
        let incoming_get_state = router_builder.subscribe_to_stream(2);
        let incoming_update_processors = router_builder.subscribe_to_stream(2);

        let (tx, rx) = mpsc::channel(updateable_config.pinned().worker.internal_queue_length());
        Self {
            task_center,
            updateable_config,
            running_partition_processors: BTreeMap::default(),
            name_cache: Default::default(),
            metadata,
            metadata_store_client,
            partition_store_manager,
            incoming_get_state,
            incoming_update_processors,
            networking,
            bifrost,
            attach_router,
            rx,
            tx,
            latest_attach_response: None,
            persisted_lsns_rx: None,
            invokers_status_reader: MultiplexedInvokerStatusReader::default(),
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

        // Initial attach
        let response = tokio::time::timeout(Duration::from_secs(10), self.attach())
            .await
            .context("Timeout waiting to attach to a cluster controller")??;

        let (from, msg) = response.split();
        self.apply_plan(&msg.actions).await?;
        self.latest_attach_response = Some((*from.peer(), msg));
        info!("Plan applied from attaching to controller {}", from.peer());

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

        loop {
            tokio::select! {
                Some(command) = self.rx.recv() => {
                    self.on_command(command);
                }
                Some(get_state) = self.incoming_get_state.next() => {
                    self.on_get_state(get_state);
                }
                Some(update_processors) = self.incoming_update_processors.next() => {
                    if let Err(err) = self.on_control_processors(update_processors).await {
                        warn!("failed processing control processors command: {err}");
                    }
                }
                _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }

    fn on_get_state(&self, get_state_msg: Incoming<GetProcessorsState>) {
        let persisted_lsns = self.persisted_lsns_rx.as_ref().map(|w| w.borrow());

        // For all running partitions, collect state, enrich it, and send it back.
        let state: BTreeMap<PartitionId, PartitionProcessorStatus> = self
            .running_partition_processors
            .iter()
            .map(|(partition_id, state)| {
                let mut status = state.watch_rx.borrow().clone();
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
                status.planned_mode = state.planned_mode;
                status.last_persisted_log_lsn = persisted_lsns
                    .as_ref()
                    .and_then(|lsns| lsns.get(partition_id).cloned());
                (*partition_id, status)
            })
            .collect();

        // ignore shutdown errors.
        let _ = self.task_center.spawn(
            restate_core::TaskKind::Disposable,
            "get-processors-state-response",
            None,
            async move {
                Ok(get_state_msg
                    .to_rpc_response(ProcessorsStateResponse { state })
                    .send()
                    .await?)
            },
        );
    }

    fn on_command(&mut self, command: ProcessorsManagerCommand) {
        use ProcessorsManagerCommand::*;
        match command {
            GetLivePartitions(sender) => {
                let live_partitions = self.running_partition_processors.keys().cloned().collect();
                let _ = sender.send(live_partitions);
            }
            CreateSnapshot(partition_id, sender) => {
                self.running_partition_processors
                    .get(&partition_id)
                    .map(|store| store.handle.create_snapshot(Some(sender)));
            }
        }
    }

    async fn on_control_processors(
        &mut self,
        control_processor: Incoming<ControlProcessors>,
    ) -> Result<(), Error> {
        let (_, control_processors) = control_processor.split();

        self.metadata
            .wait_for_version(
                MetadataKind::Logs,
                control_processors.min_logs_table_version,
            )
            .await?;
        let partition_table = self
            .metadata
            .wait_for_partition_table(control_processors.min_partition_table_version)
            .await?
            .into_arc();

        for control_processor in control_processors.commands {
            self.on_control_processor(control_processor, &partition_table)
                .await?;
        }

        Ok(())
    }

    #[instrument(level = "debug", skip_all, fields(partition_id = %control_processor.partition_id))]
    async fn on_control_processor(
        &mut self,
        control_processor: ControlProcessor,
        partition_table: &PartitionTable,
    ) -> Result<(), Error> {
        let partition_id = control_processor.partition_id;

        match control_processor.command {
            ProcessorCommand::Stop => {
                if let Some(processor) = self.running_partition_processors.remove(&partition_id) {
                    if let Some(handle) = self.task_center.cancel_task(processor.task_id) {
                        if let Err(err) = handle.await {
                            warn!("Partition processor crashed while shutting down: {err}");
                        }
                    }
                } else {
                    debug!("No running partition processor. Ignoring stop command.");
                }
            }
            ProcessorCommand::Follower => {
                if let Some(state) = self.running_partition_processors.get_mut(&partition_id) {
                    // if we error here, then the system is shutting down
                    state.step_down()?;
                } else if let Some(partition_key_range) = partition_table
                    .get_partition(&partition_id)
                    .map(|partition| &partition.key_range)
                {
                    self.start_partition_processor(
                        partition_id,
                        partition_key_range,
                        RunMode::Follower,
                    )
                    .await?;
                } else {
                    debug!("Unknown partition id '{partition_id}'. Ignoring follower command.");
                }
            }
            ProcessorCommand::Leader => {
                if let Some(state) = self.running_partition_processors.get_mut(&partition_id) {
                    state
                        .run_for_leader(
                            self.metadata_store_client.clone(),
                            self.metadata.my_node_id(),
                        )
                        .await?;
                } else if let Some(partition_key_range) = partition_table
                    .get_partition(&partition_id)
                    .map(|partition| &partition.key_range)
                {
                    self.start_partition_processor(
                        partition_id,
                        partition_key_range,
                        RunMode::Leader,
                    )
                    .await?;
                } else {
                    debug!("Unknown partition id '{partition_id}'. Ignoring leader command.");
                }
            }
        }

        Ok(())
    }

    pub async fn apply_plan(&mut self, actions: &[Action]) -> Result<(), Error> {
        for action in actions {
            match action {
                Action::RunPartition(action) => {
                    #[allow(clippy::map_entry)]
                    if !self
                        .running_partition_processors
                        .contains_key(&action.partition_id)
                    {
                        self.start_partition_processor(
                            action.partition_id,
                            &action.key_range_inclusive.clone().into(),
                            action.mode,
                        )
                        .await?;
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

    async fn start_partition_processor(
        &mut self,
        partition_id: PartitionId,
        key_range: &RangeInclusive<PartitionKey>,
        mode: RunMode,
    ) -> Result<(), Error> {
        let mut state = self.spawn_partition_processor(partition_id, key_range.clone())?;

        if RunMode::Leader == mode {
            state
                .run_for_leader(
                    self.metadata_store_client.clone(),
                    self.metadata.my_node_id(),
                )
                .await?
        }

        self.running_partition_processors
            .insert(partition_id, state);
        Ok(())
    }

    fn spawn_partition_processor(
        &mut self,
        partition_id: PartitionId,
        key_range: RangeInclusive<PartitionKey>,
    ) -> Result<ProcessorState, Error> {
        let (control_tx, control_rx) = mpsc::channel(2);
        let status = PartitionProcessorStatus::new();
        let (watch_tx, watch_rx) = watch::channel(status.clone());

        let config = self.updateable_config.pinned();
        let options = &config.worker;

        let networking = self.networking.clone();
        let bifrost = self.bifrost.clone();
        let node_id = self.metadata.my_node_id();

        let schema = self.metadata.updateable_schema();

        let invoker: InvokerService<
            InvokerStorageReader<PartitionStore>,
            EntryEnricher<Schema, ProtobufRawEntryCodec>,
            Schema,
        > = InvokerService::from_options(
            &config.common.service_client,
            &config.worker.invoker,
            EntryEnricher::new(schema.clone()),
            schema,
        )?;

        self.invokers_status_reader
            .push(key_range.clone(), invoker.status_reader());

        let pp_builder = PartitionProcessorBuilder::new(
            node_id,
            partition_id,
            key_range.clone(),
            status,
            options,
            control_rx,
            watch_tx,
            invoker.handle(),
        );

        // the name is also used as thread names for the corresponding tokio runtimes, let's keep
        // it short.
        let task_name = self
            .name_cache
            .entry(partition_id)
            .or_insert_with(|| Box::leak(Box::new(format!("pp-{}", partition_id))));

        let invoker_name = Box::leak(Box::new(format!("invoker-{}", partition_id)));
        let invoker_config = self.updateable_config.clone().map(|c| &c.worker.invoker);
        let configuration = self.updateable_config.clone();

        let maybe_task_id: Result<TaskId, RuntimeError> = self.task_center.start_runtime(
            TaskKind::PartitionProcessor,
            task_name,
            Some(pp_builder.partition_id),
            {
                let storage_manager = self.partition_store_manager.clone();
                let options = options.clone();
                let key_range = key_range.clone();
                move || async move {
                    let partition_store = storage_manager
                        .open_partition_store(
                            partition_id,
                            key_range,
                            OpenMode::CreateIfMissing,
                            &options.storage.rocksdb,
                        )
                        .await?;

                    restate_core::task_center().spawn_child(
                        TaskKind::SystemService,
                        invoker_name,
                        Some(pp_builder.partition_id),
                        invoker.run(invoker_config),
                    )?;

                    pp_builder
                        .build::<ProtobufRawEntryCodec, T>(
                            networking,
                            bifrost,
                            partition_store,
                            configuration,
                        )
                        .await?
                        .run()
                        .await
                }
            },
        );

        let task_id = match maybe_task_id {
            Ok(task_id) => Ok(task_id),
            Err(RuntimeError::AlreadyExists(name)) => {
                panic!(
                    "The partition processor runtime {} is already running!",
                    name
                )
            }
            Err(RuntimeError::Shutdown(e)) => Err(e),
        }?;

        Ok(ProcessorState::new(
            partition_id,
            task_id,
            key_range,
            PartitionProcessorHandle::new(control_tx),
            watch_rx,
        ))
    }
}

/// Monitors the persisted log lsns and notifies the partition processor manager about it. The
/// current approach requires flushing the memtables to make sure that data has been persisted.
/// An alternative approach could be to register an event listener on flush events and using
/// table properties to retrieve the flushed log lsn. However, this requires that we update our
/// RocksDB binding to expose event listeners and table properties :-(
struct PersistedLogLsnWatchdog {
    configuration: Box<dyn LiveLoad<StorageOptions> + Send + Sync + 'static>,
    partition_store_manager: PartitionStoreManager,
    watch_tx: watch::Sender<BTreeMap<PartitionId, Lsn>>,
    persisted_lsns: BTreeMap<PartitionId, Lsn>,
    persist_lsn_interval: Option<time::Interval>,
    persist_lsn_threshold: Lsn,
}

impl PersistedLogLsnWatchdog {
    fn new(
        mut configuration: impl LiveLoad<StorageOptions> + Send + Sync + 'static,
        partition_store_manager: PartitionStoreManager,
        watch_tx: watch::Sender<BTreeMap<PartitionId, Lsn>>,
    ) -> Self {
        let options = configuration.live_load();

        let (persist_lsn_interval, persist_lsn_threshold) = Self::create_persist_lsn(options);

        PersistedLogLsnWatchdog {
            configuration: Box::new(configuration),
            partition_store_manager,
            watch_tx,
            persisted_lsns: BTreeMap::default(),
            persist_lsn_interval,
            persist_lsn_threshold,
        }
    }

    fn create_persist_lsn(options: &StorageOptions) -> (Option<time::Interval>, Lsn) {
        let persist_lsn_interval = options.persist_lsn_interval.map(|duration| {
            let mut interval = time::interval(duration.into());
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval
        });

        let persist_lsn_threshold = Lsn::from(options.persist_lsn_threshold);

        (persist_lsn_interval, persist_lsn_threshold)
    }

    async fn run(mut self) -> anyhow::Result<()> {
        debug!("Start running persisted lsn watchdog");

        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let mut config_watcher = Configuration::watcher();

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                },
                _ = OptionFuture::from(self.persist_lsn_interval.as_mut().map(|interval| interval.tick())) => {
                    let result = self.update_persisted_lsns().await;

                    if let Err(err) = result {
                        warn!("Failed updating the persisted applied lsns. This might prevent the log from being trimmed: {err}");
                    }
                }
                _ = config_watcher.changed() => {
                    self.on_config_update();
                }
            }
        }

        debug!("Stop persisted lsn watchdog");
        Ok(())
    }

    fn on_config_update(&mut self) {
        debug!("Updating the persisted log lsn watchdog");
        let options = self.configuration.live_load();

        (self.persist_lsn_interval, self.persist_lsn_threshold) = Self::create_persist_lsn(options);
    }

    async fn update_persisted_lsns(&mut self) -> Result<(), StorageError> {
        let partition_stores = self
            .partition_store_manager
            .get_all_partition_stores()
            .await;

        let mut new_persisted_lsns = BTreeMap::new();
        let mut modified = false;

        for mut partition_store in partition_stores {
            let partition_id = partition_store.partition_id();

            let applied_lsn = partition_store.get_applied_lsn().await?;

            if let Some(applied_lsn) = applied_lsn {
                let previously_applied_lsn = self
                    .persisted_lsns
                    .get(&partition_id)
                    .cloned()
                    .unwrap_or(Lsn::INVALID);

                // only flush if there was some activity compared to the last check
                if applied_lsn >= previously_applied_lsn + self.persist_lsn_threshold {
                    // since we cannot be sure that we have read the applied lsn from disk, we need
                    // to flush the memtables to be sure that it is persisted
                    trace!(
                        partition_id = %partition_id,
                        applied_lsn = %applied_lsn,
                        "Flush partition store to persist applied lsn"
                    );
                    partition_store.flush_memtables(true).await?;
                    new_persisted_lsns.insert(partition_id, applied_lsn);
                    modified = true;
                } else {
                    new_persisted_lsns.insert(partition_id, previously_applied_lsn);
                }
            }
        }

        if modified {
            self.persisted_lsns = new_persisted_lsns.clone();
            // ignore send failures which should only occur during shutdown
            let _ = self.watch_tx.send(new_persisted_lsns);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition_processor_manager::PersistedLogLsnWatchdog;
    use restate_core::{TaskKind, TestCoreEnv};
    use restate_partition_store::{OpenMode, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::fsm_table::FsmTable;
    use restate_storage_api::Transaction;
    use restate_types::config::{CommonOptions, RocksDbOptions, StorageOptions};
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::live::Constant;
    use restate_types::logs::{Lsn, SequenceNumber};
    use std::collections::BTreeMap;
    use std::ops::RangeInclusive;
    use std::time::Duration;
    use test_log::test;
    use tokio::sync::watch;
    use tokio::time::Instant;

    #[test(tokio::test(start_paused = true))]
    async fn persisted_log_lsn_watchdog_detects_applied_lsns() -> anyhow::Result<()> {
        let node_env = TestCoreEnv::create_with_single_node(1, 1).await;
        let storage_options = StorageOptions::default();
        let rocksdb_options = RocksDbOptions::default();

        node_env.tc.run_in_scope_sync("db-manager-init", None, || {
            RocksDbManager::init(Constant::new(CommonOptions::default()))
        });

        let all_partition_keys = RangeInclusive::new(0, PartitionKey::MAX);
        let partition_store_manager = PartitionStoreManager::create(
            Constant::new(storage_options.clone()).boxed(),
            Constant::new(rocksdb_options.clone()).boxed(),
            &[(PartitionId::MIN, all_partition_keys.clone())],
        )
        .await?;

        let mut partition_store = partition_store_manager
            .open_partition_store(
                PartitionId::MIN,
                all_partition_keys,
                OpenMode::CreateIfMissing,
                &rocksdb_options,
            )
            .await
            .expect("partition store present");

        let (watch_tx, mut watch_rx) = watch::channel(BTreeMap::default());

        let watchdog = PersistedLogLsnWatchdog::new(
            Constant::new(storage_options.clone()),
            partition_store_manager.clone(),
            watch_tx,
        );

        let now = Instant::now();

        node_env.tc.spawn(
            TaskKind::Watchdog,
            "persiste-log-lsn-test",
            None,
            watchdog.run(),
        )?;

        assert!(
            tokio::time::timeout(Duration::from_secs(1), watch_rx.changed())
                .await
                .is_err()
        );
        let mut txn = partition_store.transaction();
        let lsn = Lsn::OLDEST + Lsn::from(storage_options.persist_lsn_threshold);
        txn.put_applied_lsn(lsn).await;
        txn.commit().await?;

        watch_rx.changed().await?;
        assert_eq!(watch_rx.borrow().get(&PartitionId::MIN), Some(&lsn));
        let persist_lsn_interval: Duration = storage_options
            .persist_lsn_interval
            .expect("should be enabled")
            .into();
        assert!(now.elapsed() >= persist_lsn_interval);

        // we are short by one to hit the persist lsn threshold
        let next_lsn = lsn.prev() + Lsn::from(storage_options.persist_lsn_threshold);
        let mut txn = partition_store.transaction();
        txn.put_applied_lsn(next_lsn).await;
        txn.commit().await?;

        // await the persist lsn interval so that we have a chance to see the update
        tokio::time::sleep(persist_lsn_interval).await;

        // we should not receive a new notification because we haven't reached the threshold yet
        assert!(
            tokio::time::timeout(Duration::from_secs(1), watch_rx.changed())
                .await
                .is_err()
        );

        let next_persisted_lsn = next_lsn + Lsn::from(1);
        let mut txn = partition_store.transaction();
        txn.put_applied_lsn(next_persisted_lsn).await;
        txn.commit().await?;

        watch_rx.changed().await?;
        assert_eq!(
            watch_rx.borrow().get(&PartitionId::MIN),
            Some(&next_persisted_lsn)
        );

        Ok(())
    }
}
