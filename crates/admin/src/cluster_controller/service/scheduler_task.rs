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
use restate_types::Versioned;
use restate_types::partitions::state::PartitionReplicaSetStates;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use restate_core::network::TransportConnect;
use restate_core::{
    Metadata, ShutdownError, TaskCenter, TaskHandle, TaskKind, cancellation_watcher,
    is_cancellation_requested,
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
use crate::cluster_controller::service::scheduler::{self, Scheduler};
use crate::cluster_controller::service::scheduler_task::version_tracker::PartitionConfigVersionTracker;

pub struct SchedulerTask<T> {
    cluster_state_watcher: ClusterStateWatcher,
    metadata_client: MetadataStoreClient,
    scheduler: Scheduler<T>,
    replica_set_states: PartitionReplicaSetStates,
    sync_epoch_metadata_rx: mpsc::Receiver<Vec<PartitionId>>,
}

impl<T> SchedulerTask<T>
where
    T: TransportConnect,
{
    pub fn new(
        cluster_state_watcher: ClusterStateWatcher,
        scheduler: Scheduler<T>,
        replica_set_states: PartitionReplicaSetStates,
        metadata_client: MetadataStoreClient,
        sync_epoch_metadata_rx: mpsc::Receiver<Vec<PartitionId>>,
    ) -> Self {
        Self {
            cluster_state_watcher,
            scheduler,
            replica_set_states,
            metadata_client,
            sync_epoch_metadata_rx,
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

        let mut partition_config_version_tracker =
            PartitionConfigVersionTracker::new(self.replica_set_states.clone());

        let mut fetch_epoch_metadata_task = Some(self.spawn_fetch_epoch_metadata_task(Vec::new())?);

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
                    trace!("applying new epoch metadata fetched from the metadata store");
                    match epoch_metadata {
                        Ok(epoch_metadata) => {
                            for (partition_id, epoch_metadata) in epoch_metadata {
                                let (_, _, current, next, leadership_policy) = epoch_metadata.into_inner();
                                partition_config_version_tracker.note_observed_version(partition_id, current.version(), next.as_ref().map(|n| n.version()));
                                self.scheduler.update_partition_configuration(partition_id, current, next, leadership_policy);
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
                Some(partition_ids) = self.sync_epoch_metadata_rx.recv() => {
                    debug!("Received sync epoch metadata signal for {partition_ids:?}");
                    // Cancel any in-flight fetch and restart with the requested partitions.
                    if let Some(task) = fetch_epoch_metadata_task.take() {
                        task.abort();
                        let _ = task.await;
                    }
                    match self.spawn_fetch_epoch_metadata_task(partition_ids) {
                        Ok(task) => fetch_epoch_metadata_task = Some(task),
                        Err(err) => {
                            warn!("Failed to spawn fetch epoch metadata task: {err}");
                        }
                    }
                }
                _ = next_fetch_interval.tick() => {
                    trace!("triggering an epoch metadata fetch as part of the periodic refreshes");
                    if fetch_epoch_metadata_task.is_none() {
                        fetch_epoch_metadata_task = Some(self.spawn_fetch_epoch_metadata_task(Vec::new())?);
                    }
                }
                _ = partition_config_version_tracker.changed() => {
                    trace!("epoch metadata version change detected, triggering an epoch metadata fetch");
                    if fetch_epoch_metadata_task.is_none() {
                        fetch_epoch_metadata_task = Some(self.spawn_fetch_epoch_metadata_task(Vec::new())?);
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
        partition_ids: Vec<PartitionId>,
    ) -> Result<TaskHandle<HashMap<PartitionId, EpochMetadata>>, ShutdownError> {
        TaskCenter::spawn_unmanaged(
            TaskKind::Background,
            "fetch-epoch-metadata",
            FetchEpochMetadataTask::new(self.metadata_client.clone(), partition_ids).run(),
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
            match err {
                scheduler::Error::Shutdown(_) if is_cancellation_requested() => {
                    debug!(
                        "Scheduler is shutting down, possibly due to a leader-to-follower transition"
                    );
                }
                err => {
                    warn!(%err, "Failed to react to cluster state changes. This can impair the overall cluster operations");
                }
            }
        }
    }
}

struct FetchEpochMetadataTask {
    metadata_client: MetadataStoreClient,
    /// Partitions to fetch. Empty = all known partitions.
    partition_ids: Vec<PartitionId>,
}

impl FetchEpochMetadataTask {
    pub fn new(metadata_client: MetadataStoreClient, partition_ids: Vec<PartitionId>) -> Self {
        Self {
            metadata_client,
            partition_ids,
        }
    }

    pub async fn run(self) -> HashMap<PartitionId, EpochMetadata> {
        let mut latest_epoch_metadata = HashMap::default();

        let partition_ids: Vec<PartitionId> = if self.partition_ids.is_empty() {
            let partition_table = Metadata::with_current(|m| m.partition_table_snapshot());
            partition_table.iter_ids().cloned().collect()
        } else {
            self.partition_ids
        };

        for partition_id in &partition_ids {
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

mod version_tracker {
    use std::collections::HashMap;
    use std::collections::hash_map::Entry;

    use restate_types::identifiers::PartitionId;
    use restate_types::{Version, partitions::state::PartitionReplicaSetStates};

    /// Keeps tracks of the partition config version (current and next) observed in the replica set states struct
    /// and the ones fetched already by the scheduler task. This allows the scheduler loop to detect whenever a
    /// higher version that it hasn't yet fetched is available, reducing the staleness of the epoch metadata.
    pub(super) struct PartitionConfigVersionTracker {
        replica_set_states: PartitionReplicaSetStates,
        observed_current_versions: HashMap<PartitionId, Version>,
        observed_next_versions: HashMap<PartitionId, Version>,
    }

    impl PartitionConfigVersionTracker {
        pub(super) fn new(replica_set_states: PartitionReplicaSetStates) -> Self {
            Self {
                replica_set_states,
                observed_current_versions: HashMap::default(),
                observed_next_versions: HashMap::default(),
            }
        }

        /// Returns a future that will resolve whenever a newer version than what we've previously seen is observed in the replica set states.
        ///
        /// Under the hood, this subscribes to all changes to replica set states and only fires whenever there's a version change, effectively
        /// unsubscribing from durable lsn change notifications, etc
        pub(super) async fn changed(&mut self) {
            let replica_set_states = self.replica_set_states.clone();
            loop {
                let changed_future = replica_set_states.changed();
                if self.update_if_changed() {
                    break;
                }
                changed_future.await;
            }
        }

        /// Updates the observed version of the given partition id.
        pub(super) fn note_observed_version(
            &mut self,
            partition_id: PartitionId,
            current_version: Version,
            next_version: Option<Version>,
        ) -> bool {
            Self::update_version_if_newer(
                &mut self.observed_current_versions,
                partition_id,
                current_version,
            ) | Self::update_version_if_newer(
                &mut self.observed_next_versions,
                partition_id,
                next_version.unwrap_or(Version::INVALID),
            )
        }

        fn update_version_if_newer(
            map: &mut HashMap<PartitionId, Version>,
            partition_id: PartitionId,
            version: Version,
        ) -> bool {
            match map.entry(partition_id) {
                Entry::Occupied(mut occupied_entry) => {
                    if occupied_entry.get() < &version {
                        *occupied_entry.get_mut() = version;
                        true
                    } else {
                        false
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(version);
                    true
                }
            }
        }

        fn update_if_changed(&mut self) -> bool {
            let mut changed = false;
            for p in self.replica_set_states.partition_versions() {
                changed |= self.note_observed_version(p.id, p.current, p.next);
            }
            changed
        }
    }
}
