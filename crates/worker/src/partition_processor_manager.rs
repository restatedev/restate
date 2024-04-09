// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::PartitionProcessor;
use anyhow::Context;
use restate_bifrost::Bifrost;
use restate_core::{metadata, task_center, ShutdownError, TaskId, TaskKind};
use restate_invoker_impl::ChannelServiceHandle;
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_network::Networking;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::arc_util::ArcSwapExt;
use restate_types::config::{UpdateableConfiguration, WorkerOptions};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::logs::{LogId, Payload};
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::{GenerationalNodeId, Version};
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};
use std::collections::HashMap;
use std::ops::RangeInclusive;
use tracing::debug;

pub struct PartitionProcessorManager {
    updateable_config: UpdateableConfiguration,
    node_id: GenerationalNodeId,
    running_partition_processors: HashMap<PartitionId, TaskId>,

    metadata_store_client: MetadataStoreClient,
    rocksdb_storage: RocksDBStorage,
    networking: Networking,
    bifrost: Bifrost,
    invoker_handle: ChannelServiceHandle,
}

impl PartitionProcessorManager {
    pub fn new(
        updateable_config: UpdateableConfiguration,
        node_id: GenerationalNodeId,
        metadata_store_client: MetadataStoreClient,
        rocksdb_storage: RocksDBStorage,
        networking: Networking,
        bifrost: Bifrost,
        invoker_handle: ChannelServiceHandle,
    ) -> Self {
        Self {
            updateable_config,
            running_partition_processors: HashMap::default(),
            node_id,
            metadata_store_client,
            rocksdb_storage,
            networking,
            bifrost,
            invoker_handle,
        }
    }

    #[allow(clippy::map_entry)]
    pub async fn apply_plan(&mut self, plan: PartitionProcessorPlan) -> anyhow::Result<()> {
        let config = self.updateable_config.pinned();
        let options = &config.worker;
        let partition_table = metadata()
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
        let node_id = self.node_id;

        task_center().spawn_child(
            TaskKind::PartitionProcessor,
            "partition-processor",
            Some(processor.partition_id),
            async move {
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

                processor.run(networking, bifrost).await
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
            options.num_timers_in_memory_limit,
            options.internal_queue_length,
            self.invoker_handle.clone(),
            self.rocksdb_storage.clone(),
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
            Command::AnnounceLeader(AnnounceLeader {
                node_id,
                leader_epoch,
            }),
        );
        let payload = Payload::from(envelope.encode_with_bincode()?);

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
