// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::storage::invoker::InvokerStorageReader;
use crate::PartitionProcessor;
use anyhow::Context;
use restate_bifrost::Bifrost;
use restate_core::worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};
use restate_core::{cancellation_watcher, task_center, Metadata, ShutdownError, TaskId, TaskKind};
use restate_invoker_impl::InvokerHandle;
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_network::Networking;
use restate_partition_store::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_types::arc_util::ArcSwapExt;
use restate_types::config::{UpdateableConfiguration, WorkerOptions};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::logs::{LogId, Payload};
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::{GenerationalNodeId, Version};
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::{Command as WalCommand, Destination, Envelope, Header, Source};
use std::collections::HashMap;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;
use tracing::{debug, info};

pub struct PartitionProcessorManager {
    updateable_config: UpdateableConfiguration,
    running_partition_processors: HashMap<PartitionId, TaskId>,

    metadata: Metadata,
    metadata_store_client: MetadataStoreClient,
    partition_store_manager: PartitionStoreManager,
    networking: Networking,
    bifrost: Bifrost,
    invoker_handle: InvokerHandle<InvokerStorageReader<PartitionStore>>,
    rx: mpsc::Receiver<ProcessorsManagerCommand>,
    tx: mpsc::Sender<ProcessorsManagerCommand>,
}

impl PartitionProcessorManager {
    pub fn new(
        updateable_config: UpdateableConfiguration,
        metadata: Metadata,
        metadata_store_client: MetadataStoreClient,
        partition_store_manager: PartitionStoreManager,
        networking: Networking,
        bifrost: Bifrost,
        invoker_handle: InvokerHandle<InvokerStorageReader<PartitionStore>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(updateable_config.load().worker.internal_queue_length());
        Self {
            updateable_config,
            running_partition_processors: HashMap::default(),
            metadata,
            metadata_store_client,
            partition_store_manager,
            networking,
            bifrost,
            invoker_handle,
            rx,
            tx,
        }
    }

    pub fn handle(&self) -> ProcessorsManagerHandle {
        ProcessorsManagerHandle::new(self.tx.clone())
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        self.attach_worker().await?;

        // simulating a plan after initial attachement
        let partition_table = self.metadata.wait_for_partition_table(Version::MIN).await?;
        let plan = PartitionProcessorPlan::new(
            partition_table.version(),
            partition_table
                .partitioner()
                .map(|(partition_id, _)| (partition_id, Action::Start(Role::Leader)))
                .collect(),
        );
        self.apply_plan(plan).await?;

        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                Some(command) = self.rx.recv() => {
                    self.handle_command(command).await;
                    debug!("PartitionProcessorManager shutting down");
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

    async fn attach_worker(&mut self) -> anyhow::Result<()> {
        let admin_address = self
            .metadata
            .nodes_config()
            .get_admin_node()
            .expect("at least one admin node")
            .address
            .clone();

        info!("Worker attaching to admin at '{admin_address}'");
        // todo: use Networking to attach to a cluster admin node.
        Ok(())
    }

    #[allow(clippy::map_entry)]
    pub async fn apply_plan(&mut self, plan: PartitionProcessorPlan) -> anyhow::Result<()> {
        let config = self.updateable_config.pinned();
        let options = &config.worker;
        let partition_table = self
            .metadata
            .wait_for_partition_table(plan.min_required_partition_table_version)
            .await?;

        for (partition_id, action) in plan.actions {
            match action {
                Action::Start(role) => {
                    if !self
                        .running_partition_processors
                        .contains_key(&partition_id)
                    {
                        let partition_range = partition_table
                            .partition_range(partition_id)
                            .expect("partition_range to be known");
                        let task_id = self.spawn_partition_processor(
                            options,
                            partition_id,
                            partition_range,
                            role,
                        )?;
                        self.running_partition_processors
                            .insert(partition_id, task_id);
                    } else {
                        debug!(
                            "Partition processor for partition id '{}' is already running.",
                            partition_id
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn spawn_partition_processor(
        &mut self,
        options: &WorkerOptions,
        partition_id: PartitionId,
        partition_range: RangeInclusive<PartitionKey>,
        role: Role,
    ) -> Result<TaskId, ShutdownError> {
        let processor =
            self.create_partition_processor(options, partition_id, partition_range.clone());
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
                            partition_range.clone(),
                            OpenMode::CreateIfMissing,
                            &options.storage.rocksdb,
                        )
                        .await?;

                    if role == Role::Leader {
                        Self::claim_leadership(
                            &mut bifrost,
                            metadata_store_client,
                            partition_id,
                            partition_range,
                            node_id,
                        )
                        .await?;
                    }

                    processor.run(networking, bifrost, partition_store).await
                }
            },
        )
    }

    fn create_partition_processor(
        &self,
        options: &WorkerOptions,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> PartitionProcessor {
        PartitionProcessor::new(
            partition_id,
            partition_key_range,
            options.num_timers_in_memory_limit(),
            options.internal_queue_length(),
            self.invoker_handle.clone(),
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

#[derive(Debug)]
pub struct PartitionProcessorPlan {
    min_required_partition_table_version: Version,
    actions: HashMap<PartitionId, Action>,
}

impl PartitionProcessorPlan {
    pub fn new(
        min_required_partition_table_version: Version,
        actions: HashMap<PartitionId, Action>,
    ) -> Self {
        Self {
            min_required_partition_table_version,
            actions,
        }
    }
}

#[derive(Debug)]
pub enum Action {
    Start(Role),
}

#[derive(Debug, PartialEq)]
pub enum Role {
    Leader,
    #[allow(dead_code)]
    Follower,
}
