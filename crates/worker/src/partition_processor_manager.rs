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
use std::time::Duration;

use anyhow::Context;
use futures::stream::BoxStream;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use restate_bifrost::Bifrost;
use restate_core::network::NetworkSender;
use restate_core::worker_api::{
    PartitionProcessorStatus, ProcessorsManagerCommand, ProcessorsManagerHandle,
};
use restate_core::{
    cancellation_watcher, is_cancellation_requested, task_center, Metadata, ShutdownError, TaskId,
    TaskKind,
};
use restate_invoker_impl::InvokerHandle;
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_network::Networking;
use restate_node_protocol::cluster_controller::{AttachementDetails, ClusterControllerMessage};
use restate_node_protocol::common::RequestId;
use restate_node_protocol::worker::{Action, AttachmentResponse, RunMode, WorkerMessage};
use restate_node_protocol::MessageEnvelope;
use restate_partition_store::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_types::arc_util::ArcSwapExt;
use restate_types::config::{UpdateableConfiguration, WorkerOptions};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::logs::{LogId, Payload};
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::time::MillisSinceEpoch;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::{Command as WalCommand, Destination, Envelope, Header, Source};

use crate::partition::storage::invoker::InvokerStorageReader;
use crate::partition::PartitionProcessorControlCommand;
use crate::PartitionProcessor;

pub struct PartitionProcessorManager {
    updateable_config: UpdateableConfiguration,
    running_partition_processors: BTreeMap<PartitionId, State>,

    metadata: Metadata,
    metadata_store_client: MetadataStoreClient,
    partition_store_manager: PartitionStoreManager,
    incoming_messages: BoxStream<'static, MessageEnvelope<WorkerMessage>>,
    networking: Networking,
    bifrost: Bifrost,
    invoker_handle: InvokerHandle<InvokerStorageReader<PartitionStore>>,
    rx: mpsc::Receiver<ProcessorsManagerCommand>,
    tx: mpsc::Sender<ProcessorsManagerCommand>,
    attach_state: AttachState,
    latest_attach_response: Option<(GenerationalNodeId, AttachmentResponse)>,
}

enum AttachState {
    /// Just started, we need to send an attach message to a cluster controller
    NotAttached,
    /// Attach was sent, we expect a response for the given [[RequestId]]
    WaitingForAttachmentResponse(RequestId),
    /// Attached to the given cluster controller instance
    Attached(GenerationalNodeId),
}

struct State {
    _created_at: MillisSinceEpoch,
    _key_range: RangeInclusive<PartitionKey>,
    _control_tx: mpsc::Sender<PartitionProcessorControlCommand>,
    _watch_rx: watch::Receiver<PartitionProcessorStatus>,
    _task_id: TaskId,
}

impl PartitionProcessorManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        updateable_config: UpdateableConfiguration,
        metadata: Metadata,
        metadata_store_client: MetadataStoreClient,
        partition_store_manager: PartitionStoreManager,
        incoming_messages: BoxStream<'static, MessageEnvelope<WorkerMessage>>,
        networking: Networking,
        bifrost: Bifrost,
        invoker_handle: InvokerHandle<InvokerStorageReader<PartitionStore>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(updateable_config.load().worker.internal_queue_length());
        Self {
            updateable_config,
            running_partition_processors: BTreeMap::default(),
            metadata,
            metadata_store_client,
            partition_store_manager,
            incoming_messages,
            networking,
            bifrost,
            invoker_handle,
            rx,
            tx,
            attach_state: AttachState::NotAttached,
            latest_attach_response: None,
        }
    }

    pub fn handle(&self) -> ProcessorsManagerHandle {
        ProcessorsManagerHandle::new(self.tx.clone())
    }

    async fn attach(
        &mut self,
    ) -> anyhow::Result<Pin<Box<dyn std::future::Future<Output = ()> + Send>>> {
        let request_id = RequestId::new();

        let mut attached = false;
        while !attached {
            if is_cancellation_requested() {
                return Err(ShutdownError.into());
            }
            // We try to get the admin node on every retry since it might change between retries.
            let admin_node = self
                .metadata
                .nodes_config()
                .get_admin_node()
                .context("No cluster controller found in nodes configuration")?
                .current_generation;

            info!(
                "Attempting to attach to cluster controller '{}'",
                admin_node
            );
            if admin_node == self.metadata.my_node_id() {
                // If this node is running the cluster controller, we need to wait a little to give cluster
                // controller time to start up. This is only done to reduce the chances of observing
                // connection errors in log. Such logs are benign since we retry, but it's still not nice
                // to print, specially in a single-node setup.
                info!( "This node is the cluster controller, giving cluster controller service 500ms to start");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let attach_message =
                ClusterControllerMessage::Attach(AttachementDetails { request_id });
            if let Err(e) = self
                .networking
                .send(admin_node.into(), &attach_message)
                .await
            {
                warn!(
                    "Failed to send attach message to cluster controller: {}, retrying....",
                    e
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            attached = true;
        }

        self.attach_state = AttachState::WaitingForAttachmentResponse(request_id);
        Ok(Box::pin(tokio::time::sleep_until(
            Instant::now() + Duration::from_secs(5),
        )))
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        let mut attach_deadline = self.attach().await?;
        loop {
            tokio::select! {
                Some(command) = self.rx.recv() => {
                    self.handle_command(command).await;
                    debug!("PartitionProcessorManager shutting down");
                }
                _ = &mut attach_deadline, if matches!(self.attach_state, AttachState::WaitingForAttachmentResponse(_)) => {
                    warn!("Attachment deadline reached, didn't hear back from cluster controller. Sending another request");
                    attach_deadline = self.attach().await?;
                }
                Some(message) = self.incoming_messages.next() => {
                    self.handle_network_message(message);
                }
              _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }

    async fn handle_command(&mut self, command: ProcessorsManagerCommand) {
        use ProcessorsManagerCommand::*;
        match command {
            GetLivePartitions(sender) => {
                let live_partitions = self.running_partition_processors.keys().cloned().collect();
                let _ = sender.send(live_partitions);
            }
        }
    }

    fn handle_network_message(&mut self, envelope: MessageEnvelope<WorkerMessage>) {
        let (from, msg) = envelope.split();
        match msg {
            WorkerMessage::AttachmentResponse(response) => {
                if let AttachState::WaitingForAttachmentResponse(request_id) = &self.attach_state {
                    if &response.request_id == request_id {
                        self.attach_state = AttachState::Attached(from);
                        self.latest_attach_response = Some((from, response.clone()));
                        info!("Applying plan from cluster controller {}", from);
                        if let Err(e) = self.apply_plan(&response.actions) {
                            info!("Didn't apply plan from cluster controller: {:?}", e);
                        }
                    } else {
                        warn!(
                            "Received an unexpected attachment response with request id '{}', ignoring!",
                            response.request_id
                        );
                    }
                }
            }
            WorkerMessage::GetPartitionStatus => todo!(),
        }
    }

    pub fn apply_plan(&mut self, actions: &[Action]) -> Result<(), ShutdownError> {
        let config = self.updateable_config.pinned();
        let options = &config.worker;

        for action in actions {
            match action {
                Action::RunPartition(action) => {
                    #[allow(clippy::map_entry)]
                    if !self
                        .running_partition_processors
                        .contains_key(&action.partition_id)
                    {
                        let (control_tx, control_rx) = mpsc::channel(2);
                        let status = PartitionProcessorStatus::new(action.mode);
                        let (watch_tx, watch_rx) = watch::channel(status.clone());

                        let _task_id = self.spawn_partition_processor(
                            options,
                            action.partition_id,
                            action.key_range_inclusive.clone().into(),
                            status,
                            control_rx,
                            watch_tx,
                        )?;
                        let state = State {
                            _created_at: MillisSinceEpoch::now(),
                            _key_range: action.key_range_inclusive.clone().into(),
                            _task_id,
                            _control_tx: control_tx,
                            _watch_rx: watch_rx,
                        };
                        self.running_partition_processors
                            .insert(action.partition_id, state);
                    } else {
                        debug!(
                            "Partition processor for partition id '{}' is already running.",
                            action.partition_id
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn spawn_partition_processor(
        &self,
        options: &WorkerOptions,
        partition_id: PartitionId,
        key_range: RangeInclusive<PartitionKey>,
        status: PartitionProcessorStatus,
        control_rx: mpsc::Receiver<PartitionProcessorControlCommand>,
        watch_tx: watch::Sender<PartitionProcessorStatus>,
    ) -> Result<TaskId, ShutdownError> {
        let planned_mode = status.planned_mode;
        let processor = PartitionProcessor::new(
            partition_id,
            key_range.clone(),
            status,
            options.num_timers_in_memory_limit(),
            options.internal_queue_length(),
            control_rx,
            watch_tx,
            self.invoker_handle.clone(),
        );
        let networking = self.networking.clone();
        let mut bifrost = self.bifrost.clone();
        let metadata_store_client = self.metadata_store_client.clone();
        let node_id = self.metadata.my_node_id();

        task_center().spawn_child(
            TaskKind::PartitionProcessor,
            "partition-processor",
            Some(processor.partition_id),
            {
                let storage_manager = self.partition_store_manager.clone();
                let options = options.clone();
                async move {
                    let partition_store = storage_manager
                        .open_partition_store(
                            partition_id,
                            key_range.clone(),
                            OpenMode::CreateIfMissing,
                            &options.storage.rocksdb,
                        )
                        .await?;

                    if planned_mode == RunMode::Leader {
                        Self::claim_leadership(
                            &mut bifrost,
                            metadata_store_client,
                            partition_id,
                            key_range,
                            node_id,
                        )
                        .await?;
                    }

                    processor.run(networking, bifrost, partition_store).await
                }
            },
        )
    }

    async fn claim_leadership(
        bifrost: &mut Bifrost,
        metadata_store_client: MetadataStoreClient,
        partition_id: PartitionId,
        partition_range: RangeInclusive<PartitionKey>,
        node_id: GenerationalNodeId,
    ) -> anyhow::Result<()> {
        let leader_epoch =
            Self::obtain_next_epoch(metadata_store_client, partition_id, node_id).await?;

        Self::announce_leadership(
            bifrost,
            node_id,
            partition_id,
            partition_range,
            leader_epoch,
        )
        .await?;

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

    async fn announce_leadership(
        bifrost: &mut Bifrost,
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        leader_epoch: LeaderEpoch,
    ) -> anyhow::Result<()> {
        let header = Header {
            dest: Destination::Processor {
                partition_key: *partition_key_range.start(),
                dedup: None,
            },
            source: Source::ControlPlane {},
        };

        let envelope = Envelope::new(
            header,
            WalCommand::AnnounceLeader(AnnounceLeader {
                node_id,
                leader_epoch,
            }),
        );
        let payload = Payload::from(envelope.to_bytes()?);

        bifrost
            .append(LogId::from(partition_id), payload)
            .await
            .context("failed to write AnnounceLeader record to bifrost")?;

        Ok(())
    }
}
