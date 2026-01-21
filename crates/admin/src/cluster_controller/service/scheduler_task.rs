// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::time::Duration;

use futures::future::OptionFuture;
use tracing::{debug, warn};

use restate_core::network::TransportConnect;
use restate_core::{
    Metadata, ShutdownError, TaskCenter, TaskHandle, TaskKind, cancellation_watcher,
};
use restate_metadata_store::MetadataStoreClient;
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::cluster_state::ClusterState;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partitions::PartitionTable;

use crate::cluster_controller::cluster_state_refresher::ClusterStateWatcher;
use crate::cluster_controller::service::scheduler::Scheduler;

pub struct SchedulerTask<T> {
    cluster_state_watcher: ClusterStateWatcher,
    metadata_client: MetadataStoreClient,
    scheduler: Scheduler<T>,
}

impl<T> SchedulerTask<T>
where
    T: TransportConnect,
{
    pub fn new(
        cluster_state_watcher: ClusterStateWatcher,
        scheduler: Scheduler<T>,
        metadata_client: MetadataStoreClient,
    ) -> Self {
        Self {
            cluster_state_watcher,
            scheduler,
            metadata_client,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        debug!("Running SchedulerTask");

        let mut cancellation = std::pin::pin!(cancellation_watcher());

        let mut nodes_config_watcher =
            Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));
        let mut nodes_config = Metadata::with_current(|m| m.updateable_nodes_config());

        let mut partition_table_watcher =
            Metadata::with_current(|m| m.watch(MetadataKind::PartitionTable));
        let mut partition_table = Metadata::with_current(|m| m.updateable_partition_table());

        let cs = TaskCenter::with_current(|tc| tc.cluster_state().clone());
        let mut cs_changed = std::pin::pin!(cs.changed());

        let mut fetch_epoch_metadata_task = Some(self.spawn_fetch_epoch_metadata_task()?);

        let mut next_fetch_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = &mut cancellation => {
                    debug!("Stopping SchedulerTask");
                    break;
                }
                Ok(_) = partition_table_watcher.changed() => {
                    // if the partition table changed, this can mean that the partition replication
                    // changed or that new partitions where added that we need to configure
                    self.on_cluster_state_change(&cs, &self.cluster_state_watcher.current(), nodes_config.live_load(), partition_table.live_load()).await
                },
                Ok(_) = nodes_config_watcher.changed() => {
                    // A changed nodes configuration might mean that nodes were added/removed from the
                    // cluster or that the worker state changed. Both might require the scheduler to
                    // take action.
                    self.on_cluster_state_change(&cs, &self.cluster_state_watcher.current(), nodes_config.live_load(), partition_table.live_load()).await
                },
                () = &mut cs_changed => {
                    // register waiting for the next update
                    cs_changed.set(cs.changed());

                    // A changed cluster state might mean that a node changed it's state from dead
                    // to alive or vice versa. This can mean that we need to select a new partition
                    // leader or reconfigure the partition.
                    self.on_cluster_state_change(&cs, &self.cluster_state_watcher.current(), nodes_config.live_load(), partition_table.live_load()).await
                },
                Ok(legacy_cluster_state) = self.cluster_state_watcher.next_cluster_state() => {
                    // A changed old cluster state might mean that a partition processor has caught
                    // up to the tail of the log and can now become a leader.
                    self.on_cluster_state_change(&cs, &legacy_cluster_state, nodes_config.live_load(), partition_table.live_load()).await
                }
                Some(epoch_metadata) = OptionFuture::from(fetch_epoch_metadata_task.as_mut()) => {
                    match epoch_metadata {
                        Ok(epoch_metadata) => {
                            for (partition_id, epoch_metadata) in epoch_metadata {
                                let (_, _, current, next) = epoch_metadata.into_inner();
                                self.scheduler.update_partition_configuration(partition_id, current, next);
                            }

                            // changed partition configurations might mean that we need to select a new
                            // leader or reconfigure the configuration if it is bad configuration
                            self.on_cluster_state_change(&cs, &self.cluster_state_watcher.current(), nodes_config.live_load(), partition_table.live_load()).await
                        },
                        Err(err) => {
                            warn!("fetching the epoch metadata from the metadata store panicked: {err}");
                        }
                    }

                    fetch_epoch_metadata_task = None;
                }
                _ = next_fetch_interval.tick() => {
                    if fetch_epoch_metadata_task.is_none() {
                        fetch_epoch_metadata_task = Some(self.spawn_fetch_epoch_metadata_task()?);
                    }
                }
            }
        }

        if let Some(fetch_epoch_metadata_task) = fetch_epoch_metadata_task {
            fetch_epoch_metadata_task.abort();

            // ignore errors during abort
            let _ = fetch_epoch_metadata_task.await;
        }

        Ok(())
    }

    fn spawn_fetch_epoch_metadata_task(
        &mut self,
    ) -> Result<TaskHandle<HashMap<PartitionId, EpochMetadata>>, ShutdownError> {
        TaskCenter::spawn_unmanaged(
            TaskKind::Background,
            "fetch-epoch-metadata",
            FetchEpochMetadataTask::new(self.metadata_client.clone()).run(),
        )
    }

    async fn on_cluster_state_change(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
    ) {
        if let Err(err) = self
            .scheduler
            .on_cluster_state_change(
                cluster_state,
                legacy_cluster_state,
                nodes_config,
                partition_table,
            )
            .await
        {
            warn!(%err, "Failed to react to cluster state changes. This can impair the overall cluster operations");
        }
    }
}

struct FetchEpochMetadataTask {
    metadata_client: MetadataStoreClient,
}

impl FetchEpochMetadataTask {
    pub fn new(metadata_client: MetadataStoreClient) -> Self {
        Self { metadata_client }
    }

    pub async fn run(self) -> HashMap<PartitionId, EpochMetadata> {
        let mut latest_epoch_metadata = HashMap::default();

        let partition_table = Metadata::with_current(|m| m.partition_table_snapshot());

        for partition_id in partition_table.iter_ids() {
            // todo replace with multi get
            match self
                .metadata_client
                .get::<EpochMetadata>(partition_processor_epoch_key(*partition_id))
                .await
            {
                Ok(Some(epoch_metadata)) => {
                    latest_epoch_metadata.insert(*partition_id, epoch_metadata);
                }
                Ok(None) => {}
                Err(err) => {
                    debug!(%err, "Failed to fetch epoch metadata for partition {partition_id}");
                }
            }
        }

        latest_epoch_metadata
    }
}
