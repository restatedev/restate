// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use ahash::HashMap;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use rocksdb::event_listener::EventListenerExt;
use tracing::{debug, error, info, instrument, warn};

use restate_rocksdb::{CfPrefixPattern, DbName, DbSpecBuilder, RocksDb, RocksDbManager};
use restate_storage_api::fsm_table::ReadOnlyFsmTable;
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::live::LiveLoadExt;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::partitions::Partition;

use crate::durable_lsn_tracking::DurableLsnEventListener;
use crate::partition_db::{PartitionCell, PartitionDb};
use crate::snapshots::{LocalPartitionSnapshot, Snapshots};
use crate::{BuildError, OpenError, PartitionStore, SnapshotErrorKind};
use crate::{SnapshotError, cf_options};

const DB_NAME: &str = "db";
const PARTITION_CF_PREFIX: &str = "data-";

#[derive(Default)]
pub(crate) struct SharedState {
    partitions: RwLock<HashMap<PartitionId, Arc<PartitionCell>>>,
}

impl SharedState {
    /// Gets the partition cell or creates a default (closed) one if it doesn't exist.
    pub fn get_or_default(&self, partition: &Partition) -> Arc<PartitionCell> {
        let guard = self.partitions.upgradable_read();
        if let Some(cell) = guard.get(&partition.partition_id) {
            return cell.clone();
        }

        let mut wguard = RwLockUpgradableReadGuard::upgrade(guard);
        // check to avoid double-insertion
        if let Some(cell) = wguard.get(&partition.partition_id) {
            return cell.clone();
        }

        let cell = Arc::new(PartitionCell::new(partition.clone()));
        // do we have the partition locally?
        wguard.insert(partition.partition_id, cell.clone());
        cell
    }

    /// Gets the partition cell or creates a default (closed) one if it doesn't exist.
    ///
    /// Note that this doesn't _create_ the column family. If the column family is closed,
    /// the returned cell status will be `CfMissing`.
    pub async fn get_or_open(
        &self,
        partition: &Partition,
        rocksdb: &Arc<RocksDb>,
    ) -> Arc<PartitionCell> {
        let cell = self.get_or_default(partition);
        let mut state_guard = cell.inner.write().await;
        if !state_guard.is_unknown() {
            drop(state_guard);
            return cell;
        }

        cell.open_cf(&mut state_guard, rocksdb);

        drop(state_guard);
        cell
    }

    /// Note: we only modify entries, never insert, from the event listener. If we don't find
    /// an existing entry for the partition, that means that the PartitionStoreManager doesn't
    /// know about it.
    pub fn note_durable_lsn(&self, partition_id: PartitionId, durable_lsn: Lsn) {
        let guard = self.partitions.read();
        if let Some(cell) = guard.get(&partition_id) {
            cell.note_durable_lsn(durable_lsn);
        };
    }

    #[cfg(test)]
    pub async fn drop_partition(
        &self,
        partition_id: PartitionId,
    ) -> Result<(), restate_rocksdb::RocksError> {
        let Some(cell) = self.partitions.read().get(&partition_id).cloned() else {
            return Ok(());
        };
        let mut state_guard = cell.inner.write().await;
        cell.drop_cf(&mut state_guard).await
    }
}

#[derive(Clone)]
pub struct PartitionStoreManager {
    state: Arc<SharedState>,
    snapshots: Snapshots,
    rocksdb: Arc<RocksDb>,
}

impl PartitionStoreManager {
    pub async fn create() -> Result<Self, BuildError> {
        let mut live_config = Configuration::live();

        let config = live_config.live_load();
        let snapshots = Snapshots::create(config)
            .await
            .map_err(BuildError::Snapshots)?;

        let per_partition_memory_budget = config.worker.storage.rocksdb_memory_budget()
            / config
                .worker
                .storage
                .num_partitions_to_share_memory_budget() as usize;

        let mut db_opts = rocksdb::Options::default();
        // we always enable manual wal flushing in case that the user enables wal at runtime
        db_opts.set_manual_wal_flush(true);

        let state = Arc::new(SharedState::default());
        let event_listener = DurableLsnEventListener::new(state.clone());
        db_opts.add_event_listener(event_listener);

        let db_spec = DbSpecBuilder::new(
            DbName::new(DB_NAME),
            config.worker.storage.data_dir(),
            db_opts,
        )
        .add_cf_pattern(
            CfPrefixPattern::new(PARTITION_CF_PREFIX),
            cf_options(per_partition_memory_budget),
        )
        // This is added as an experiment. We might make this configurable to let users decide
        // on the trade-off between shutdown time and startup catchup time.
        .add_to_flush_on_shutdown(CfPrefixPattern::ANY)
        .build()
        .expect("valid spec");

        let manager = RocksDbManager::get();
        let rocksdb = manager
            .open_db(
                live_config.map(|opts| &opts.worker.storage.rocksdb),
                db_spec,
            )
            .await?;

        Ok(Self {
            state,
            snapshots,
            rocksdb: rocksdb.clone(),
        })
    }

    pub fn is_repository_configured(&self) -> bool {
        self.snapshots.is_repository_configured()
    }

    pub async fn refresh_latest_archived_lsn(&self, partition_id: PartitionId) -> Option<Lsn> {
        self.snapshots
            .refresh_latest_archived_lsn(partition_id)
            .await
    }

    pub async fn get_partition_db(&self, partition_id: PartitionId) -> Option<PartitionDb> {
        // note: we don't hold the map read lock while trying to acquire the partition cell's lock.
        // hence the `cloned()` call.
        let cell = self.state.partitions.read().get(&partition_id).cloned()?;
        cell.clone_db().await
    }

    pub async fn get_partition_store(&self, partition_id: PartitionId) -> Option<PartitionStore> {
        // note: we don't hold the map read lock while trying to acquire the partition cell's lock.
        // hence the `cloned()` call.
        let cell = self.state.partitions.read().get(&partition_id).cloned()?;
        cell.clone_db().await.map(PartitionStore::from)
    }

    /// Opens a partition store for the given partition.
    ///
    /// If `min_applied_lsn` is `None`, then the store will be opened from the local database
    /// regardless of whether there is a snapshot available or not.
    #[instrument(level = "error", skip_all, fields(partition_id = %partition.partition_id, cf_name = %partition.cf_name()))]
    pub async fn open(
        &self,
        partition: &Partition,
        min_applied_lsn: Option<Lsn>,
    ) -> Result<PartitionStore, OpenError> {
        // If we already have the partition locally and we don't have a fast-forward target, then
        // we simply return it.
        let cell = self.state.get_or_open(partition, &self.rocksdb).await;

        let mut state_guard = cell.inner.write().await;

        if let Some(db) = state_guard.get_db().cloned() {
            // we have a database, but perhaps it doesn't meet the min_applied_lsn requirement?
            let mut partition_store = PartitionStore::from(db);
            match min_applied_lsn {
                None => return Ok(partition_store),
                Some(min_applied_lsn) => {
                    let my_applied_lsn = partition_store.get_applied_lsn().await?;
                    if my_applied_lsn.unwrap_or(Lsn::INVALID) >= min_applied_lsn {
                        // We have an initialized partition store, and no fast-forward target - go on and open it.
                        return Ok(partition_store);
                    }
                }
            }
        }
        // **
        // We either don't have an existing local partition store initialized - or we have a
        // min-applied-lsn target higher than our local state (probably due to seeing a log trim-gap).
        // **

        // Attempt to get the latest available snapshot from the snapshot repository:
        let snapshot = self
            .snapshots
            .download_latest_snapshot(partition.partition_id)
            .await
            .map_err(OpenError::Snapshot)?;

        match (snapshot, min_applied_lsn) {
            (None, None) => {
                debug!("No snapshot found for partition, creating new partition store");
                let db = cell
                    .create_cf(&mut state_guard, self.rocksdb.clone())
                    .await?;
                Ok(PartitionStore::from(db))
            }

            (Some(snapshot), None) => {
                // Based on the assumptions for calling this method, we should only reach this point if
                // there is no existing store - we can import without first dropping the column family.
                info!("Found partition snapshot, restoring it");
                let db = cell
                    .import_cf(&mut state_guard, snapshot, self.rocksdb.clone())
                    .await?;

                Ok(PartitionStore::from(db))
            }

            // Good snapshot
            (Some(snapshot), Some(fast_forward_lsn))
                if snapshot.min_applied_lsn >= fast_forward_lsn =>
            {
                info!(
                    latest_snapshot_lsn = %snapshot.min_applied_lsn,
                    %fast_forward_lsn,
                    "Found snapshot with LSN >= target LSN, dropping local partition store state",
                );
                cell.drop_cf(&mut state_guard).await?;
                let db = cell
                    .import_cf(&mut state_guard, snapshot, self.rocksdb.clone())
                    .await?;
                Ok(PartitionStore::from(db))
            }
            (maybe_snapshot, Some(fast_forward_lsn)) => {
                // Play it safe and keep the partition store intact; we can't do much else at this
                // point. We'll likely halt again as soon as the processor starts up.
                let recovery_guide_msg = "The partition's log is trimmed to a point from which this processor can not resume. \
                Visit https://docs.restate.dev/operate/clusters#handling-missing-snapshots \
                to learn more about how to recover this processor.";

                if let Some(snapshot) = maybe_snapshot {
                    warn!(
                        latest_snapshot_lsn = %snapshot.min_applied_lsn,
                        %fast_forward_lsn,
                        "The latest available snapshot is from an LSN before the target LSN! {}",
                        recovery_guide_msg,
                    );
                    Err(OpenError::SnapshotUnsuitable)
                } else if !self.snapshots.is_repository_configured() {
                    error!(
                        %fast_forward_lsn,
                        "A log trim gap was encountered, but no snapshot repository is configured! {}",
                        recovery_guide_msg,
                    );
                    Err(OpenError::SnapshotRepositoryRequired)
                } else {
                    error!(
                        %fast_forward_lsn,
                        "A log trim gap was encountered, but no snapshot is available for this partition! {}",
                        recovery_guide_msg,
                    );
                    Err(OpenError::SnapshotRequired)
                }
            }
        }
    }

    pub async fn export_partition(
        &self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
        snapshot_base_path: &Path,
    ) -> Result<LocalPartitionSnapshot, SnapshotError> {
        // note: we don't hold the map read lock while trying to acquire the partition cell's lock.
        // hence the `cloned()` call.
        let cell = self
            .state
            .partitions
            .read()
            .get(&partition_id)
            .cloned()
            .ok_or(SnapshotError {
                partition_id,
                kind: SnapshotErrorKind::PartitionNotFound,
            })?;
        // Require a lock to prevent closing/reopening stores while a snapshot is ongoing. Failure
        // to do so can lead to exporting a partially-initialized store.
        let state_guard = cell.inner.read().await;
        let Some(db) = state_guard.get_db() else {
            return Err(SnapshotError {
                partition_id,
                kind: SnapshotErrorKind::PartitionNotFound,
            });
        };

        let partition_store = PartitionStore::from(db.clone());

        self.snapshots
            .create_local_snapshot(
                partition_store,
                min_target_lsn,
                snapshot_id,
                snapshot_base_path,
            )
            .await
    }

    #[cfg(test)]
    pub async fn drop_partition(
        &self,
        partition_id: PartitionId,
    ) -> Result<(), restate_rocksdb::RocksError> {
        self.state.drop_partition(partition_id).await
    }

    #[cfg(test)]
    pub async fn open_from_snapshot(
        &self,
        partition: &Partition,
        snapshot: LocalPartitionSnapshot,
    ) -> Result<PartitionStore, restate_rocksdb::RocksError> {
        let cell = self.state.get_or_default(partition);
        let mut state_guard = cell.inner.write().await;
        let db = cell
            .import_cf(&mut state_guard, snapshot, self.rocksdb.clone())
            .await?;
        Ok(PartitionStore::from(db))
    }

    #[cfg(test)]
    pub async fn close_partition_store(&self, partition_id: PartitionId) {
        let Some(cell) = self.state.partitions.read().get(&partition_id).cloned() else {
            return;
        };
        let mut state_guard = cell.inner.write().await;
        cell.reset_to_unknown(&mut state_guard).await;
    }
}
