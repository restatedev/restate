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
use std::sync::{Arc, Weak};

use ahash::HashMap;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, instrument, warn};

use restate_rocksdb::{CfPrefixPattern, DbSpecBuilder, RocksDb, RocksDbManager, RocksError};
use restate_storage_api::fsm_table::ReadFsmTable;
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::partitions::Partition;

use crate::SnapshotError;
use crate::memory::MemoryController;
use crate::partition_db::{AllDataCf, PartitionCell, PartitionDb, RocksConfigurator};
use crate::snapshots::{LocalPartitionSnapshot, Snapshots};
use crate::{BuildError, OpenError, PartitionStore, SnapshotErrorKind};

const PARTITION_CF_PREFIX: &str = "data-";
pub(crate) const IDX_CF_PREFIX: &str = "idx-";

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

    /// Gets the partition cell if it exists
    pub fn get(&self, partition_id: PartitionId) -> Option<Arc<PartitionCell>> {
        let guard = self.partitions.read();
        guard.get(&partition_id).cloned()
    }

    /// Used internally to introspect the state of the currently open partitions.
    pub(crate) async fn get_maybe_open_dbs(&self) -> Vec<crate::partition_db::State> {
        let cells: Vec<Arc<PartitionCell>> = self.partitions.read().values().cloned().collect();
        let mut dbs = Vec::with_capacity(cells.len());

        for cell in cells {
            let state = cell.inner.read().await;
            if state.maybe_open() {
                dbs.push(state.clone());
            }
        }
        dbs
    }

    /// Gets the partition cell or creates a default (closed) one if it doesn't exist.
    ///
    /// Note that this doesn't _create_ the column family. If the column family is closed,
    /// the returned cell status will be `CfMissing`.
    pub async fn get_or_open(
        &self,
        partition: &Partition,
        rocksdb: &Arc<RocksDb>,
    ) -> Result<Arc<PartitionCell>, OpenError> {
        let cell = self.get_or_default(partition);
        let mut state_guard = cell.inner.write().await;
        if !state_guard.is_unknown() {
            drop(state_guard);
            return Ok(cell);
        }

        cell.open_cf(&mut state_guard, rocksdb).await?;

        drop(state_guard);
        Ok(cell)
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

pub struct PartitionStoreManager {
    state: Arc<SharedState>,
    snapshots: Snapshots,
    db_cache: AsyncMutex<HashMap<restate_rocksdb::DbName, Weak<RocksDb>>>,
    memory_controller: MemoryController,
}

impl PartitionStoreManager {
    pub async fn create() -> Result<Arc<Self>, BuildError> {
        // Start the memory controller, how do we know when db is dropped?
        let state = Arc::new(SharedState::default());
        let memory_controller = MemoryController::start(state.clone())?;

        let psm = Arc::new(Self {
            state: state.clone(),
            snapshots: Snapshots::create(&Configuration::pinned())
                .await
                .map_err(BuildError::Snapshots)?,
            db_cache: Default::default(),
            memory_controller,
        });

        Ok(psm)
    }

    async fn open_rocksdb(&self, partition: &Partition) -> Result<Arc<RocksDb>, RocksError> {
        let mut db_cache_guard = self.db_cache.lock().await;
        let db_name = restate_rocksdb::DbName::from(partition.db_name());

        if let Some(db) = db_cache_guard.get(&db_name).and_then(|db| db.upgrade()) {
            return Ok(db);
        }

        // We need to create/open this database.
        let configurator = RocksConfigurator::<AllDataCf>::new(
            self.memory_controller.memory_budget.clone(),
            Arc::clone(&self.state),
        );

        let db_spec = DbSpecBuilder::new(
            db_name.clone(),
            Configuration::pinned().worker.storage.data_dir(&db_name),
            configurator.clone(),
        )
        .add_cf_pattern(
            CfPrefixPattern::new(PARTITION_CF_PREFIX),
            configurator.clone(),
        )
        // todo: use specialized configurator for index column families.
        .add_cf_pattern(CfPrefixPattern::new(IDX_CF_PREFIX), configurator)
        .add_to_flush_on_shutdown(CfPrefixPattern::ANY)
        .build()
        .expect("valid spec");

        let db = RocksDbManager::get().open_db(db_spec).await?;

        db_cache_guard.insert(db_name, Arc::downgrade(&db));

        Ok(db)
    }

    pub fn is_repository_configured(&self) -> bool {
        self.snapshots.is_repository_configured()
    }

    pub async fn refresh_latest_archived_lsn(&self, partition_id: PartitionId) -> Option<Lsn> {
        let db = self.get_partition_db(partition_id).await?;
        self.snapshots.refresh_latest_archived_lsn(db).await
    }

    /// Returns a partition db that's already open by a running partition processor
    pub async fn get_partition_db(&self, partition_id: PartitionId) -> Option<PartitionDb> {
        // note: we don't hold the map read lock while trying to acquire the partition cell's lock.
        // hence the `cloned()` call.
        let cell = self.state.partitions.read().get(&partition_id).cloned()?;
        cell.clone_db().await
    }

    /// Returns a partition store that's already open by a running partition processor
    pub async fn get_partition_store(&self, partition_id: PartitionId) -> Option<PartitionStore> {
        // note: we don't hold the map read lock while trying to acquire the partition cell's lock.
        // hence the `cloned()` call.
        let cell = self.state.partitions.read().get(&partition_id).cloned()?;
        cell.clone_db().await.map(PartitionStore::from)
    }

    /// Opens a partition store for the given partition, potentially re-creating it from a snapshot
    ///
    /// If `target_lsn` is `None`, then the store will be opened from the local database
    /// regardless of whether there is a snapshot available or not.
    #[instrument(level = "error", skip_all, fields(partition_id = %partition.partition_id, cf_name = %partition.data_cf_name()))]
    pub async fn open(
        &self,
        partition: &Partition,
        target_lsn: Option<Lsn>,
    ) -> Result<PartitionStore, OpenError> {
        let rocksdb = self.open_rocksdb(partition).await?;

        // If we already have the partition locally and we don't have a fast-forward target, or the
        // store already meets the target LSN requirement, then we simply return it.
        let cell = self.state.get_or_open(partition, &rocksdb).await?;

        let mut state_guard = cell.inner.write().await;

        if let Some(db) = state_guard.get_or_reopen() {
            // we have a database, but perhaps it doesn't meet the min_applied_lsn requirement?
            let mut partition_store = PartitionStore::from(db);
            match target_lsn {
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
        // target-lsn target higher than our local state (probably due to seeing a log trim-gap).
        // **

        // Attempt to get the latest available snapshot from the snapshot repository:
        let snapshot = self
            .snapshots
            .download_latest_snapshot(partition.partition_id)
            .await
            .map_err(OpenError::Snapshot)?;

        match (snapshot, target_lsn) {
            (None, None) => {
                debug!("No snapshot found for partition, creating new partition store");
                let db = cell.provision(&mut state_guard, rocksdb.clone()).await?;
                Ok(PartitionStore::from(db))
            }

            (Some(snapshot), None) => {
                // Based on the assumptions for calling this method, we should only reach this point if
                // there is no existing store - we can import without first dropping the column family.
                info!("Found partition snapshot, restoring it");
                let db = cell
                    .import_cf(&mut state_guard, snapshot, rocksdb.clone())
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
                    .import_cf(&mut state_guard, snapshot, rocksdb.clone())
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

    /// Closes a partition store for the given partition
    pub async fn close(&self, partition_id: PartitionId) {
        let Some(cell) = self.state.get(partition_id) else {
            return;
        };

        cell.inner.write().await.close();
        debug!("Closed partition db for partition {}", partition_id);
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
        let Some(db) = state_guard.db() else {
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
        let rocksdb = self.open_rocksdb(partition).await?;
        let cell = self.state.get_or_default(partition);
        let mut state_guard = cell.inner.write().await;
        let db = cell.import_cf(&mut state_guard, snapshot, rocksdb).await?;
        Ok(PartitionStore::from(db))
    }

    #[cfg(test)]
    pub async fn close_partition_store(&self, partition_id: PartitionId) {
        use crate::partition_db::State;
        let Some(cell) = self.state.partitions.read().get(&partition_id).cloned() else {
            return;
        };
        let mut state_guard = cell.inner.write().await;
        let db = match &*state_guard {
            State::Unknown | State::CfMissing | State::Closed { .. } => None,
            State::Open { db } => Some(db.clone()),
        };
        cell.reset_to_unknown(&mut state_guard).await;
        // making sure the rocksdb database is fully closed.
        if let Some(db) = db {
            db.into_rocksdb()
                .close()
                .await
                .expect("rocksdb cannot be closed if others are still holding references to it");
        }
    }
}
