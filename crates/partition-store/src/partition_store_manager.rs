// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_types::protobuf::common::DatabaseKind;

use crate::SnapshotError;
use crate::durable_lsn_tracking::DurableLsnSenders;
use crate::memory::MemoryController;
use crate::partition_db::{
    AllDataCf, PartitionCell, PartitionDb, PartitionStoreDb, RocksConfigurator,
};
use crate::snapshots::{LocalPartitionSnapshot, PartitionSnapshotStatus, Snapshots};
use crate::{BuildError, OpenError, PartitionStore, SnapshotErrorKind};

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
        database: &PartitionStoreDb,
    ) -> Arc<PartitionCell> {
        let cell = self.get_or_default(partition);
        let mut state_guard = cell.inner.write().await;
        if !state_guard.is_unknown() {
            drop(state_guard);
            return cell;
        }

        cell.open_cf(&mut state_guard, database).await;

        drop(state_guard);
        cell
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

struct CachedDb {
    rocksdb: Weak<RocksDb>,
    notifications: Arc<DurableLsnSenders>,
}

pub struct PartitionStoreManager {
    state: Arc<SharedState>,
    snapshots: Snapshots,
    db_cache: AsyncMutex<HashMap<restate_rocksdb::DbName, CachedDb>>,
    memory_controller: MemoryController,
    use_multi_db_layout: bool,
}

impl PartitionStoreManager {
    pub async fn create(use_multi_db_layout: bool) -> Result<Arc<Self>, BuildError> {
        crate::metric_definitions::describe_metrics();

        // Start the memory controller, how do we know when db is dropped?
        let state = Arc::new(SharedState::default());
        let memory_controller = MemoryController::start(state.clone())?;

        let (snapshots_options, staging_dir) = {
            let config = Configuration::pinned();
            (
                config.worker.snapshots.clone(),
                config.worker.storage.snapshots_staging_dir(),
            )
        };
        let psm = Arc::new(Self {
            state: state.clone(),
            snapshots: Snapshots::create(&snapshots_options, staging_dir)
                .await
                .map_err(BuildError::Snapshots)?,
            db_cache: Default::default(),
            memory_controller,
            use_multi_db_layout,
        });

        Ok(psm)
    }

    async fn open_rocksdb(&self, partition: &Partition) -> Result<PartitionStoreDb, RocksError> {
        let mut db_cache_guard = self.db_cache.lock().await;
        let db_name = restate_rocksdb::DbName::from(partition.db_name(self.use_multi_db_layout));

        if let Some(cached) = db_cache_guard.get(&db_name)
            && let Some(rocksdb) = cached.rocksdb.upgrade()
        {
            return Ok(PartitionStoreDb {
                rocksdb,
                notifications: cached.notifications.clone(),
            });
        }

        // We need to create/open this database.
        let notifications = Arc::new(DurableLsnSenders::default());
        let configurator = RocksConfigurator::<AllDataCf>::new(
            self.memory_controller.memory_budget.clone(),
            &notifications,
            self.use_multi_db_layout,
        );

        let db_spec = DbSpecBuilder::new(
            db_name.clone(),
            DatabaseKind::PartitionStore,
            Configuration::pinned().worker.storage.data_dir(&db_name),
            configurator.clone(),
        )
        .add_cf_pattern(CfPrefixPattern::new(PARTITION_CF_PREFIX), configurator)
        // This is added as an experiment. We might make this configurable to let users decide
        // on the trade-off between shutdown time and startup catchup time.
        .add_to_flush_on_shutdown(CfPrefixPattern::ANY)
        .build()
        .expect("valid spec");

        let rocksdb = RocksDbManager::get().open_db(db_spec).await?;

        db_cache_guard.insert(
            db_name,
            CachedDb {
                rocksdb: Arc::downgrade(&rocksdb),
                notifications: notifications.clone(),
            },
        );

        Ok(PartitionStoreDb {
            rocksdb,
            notifications,
        })
    }

    pub fn is_repository_configured(&self) -> bool {
        self.snapshots.is_repository_configured()
    }

    pub async fn refresh_latest_partition_snapshot_status(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Option<PartitionSnapshotStatus>> {
        let Some(db) = self.get_partition_db(partition_id).await else {
            return Ok(None);
        };
        self.snapshots
            .refresh_latest_partition_snapshot_status(db)
            .await
    }

    /// Returns a partition db that's already open by a running partition processor
    pub async fn get_partition_db(&self, partition_id: PartitionId) -> Option<PartitionDb> {
        // note: we don't hold the map read lock while trying to acquire the partition cell's lock.
        // hence the `cloned()` call.
        let cell = self.state.partitions.read().get(&partition_id).cloned()?;
        cell.clone_db().await
    }

    /// Returns the partition metadata if the database is open locally
    pub async fn get_local_partition_if_open(
        &self,
        partition_id: PartitionId,
    ) -> Option<Arc<Partition>> {
        // note: we don't hold the map read lock while trying to acquire the partition cell's lock.
        // hence the `cloned()` call.
        let cell = self.state.partitions.read().get(&partition_id).cloned()?;
        cell.get_partition_if_open().await
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
    #[instrument(level = "error", skip_all, fields(partition_id = %partition.partition_id, cf_name = %partition.cf_name()))]
    pub async fn open(
        &self,
        partition: &Partition,
        target_lsn: Option<Lsn>,
    ) -> Result<PartitionStore, OpenError> {
        let database = self.open_rocksdb(partition).await?;

        // If we already have the partition locally and we don't have a fast-forward target, or the
        // store already meets the target LSN requirement, then we simply return it.
        let cell = self.state.get_or_open(partition, &database).await;

        let mut state_guard = cell.inner.write().await;

        if let Some(db) = state_guard.get_or_reopen().await {
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
                let db = cell.provision(&mut state_guard, database).await?;
                Ok(PartitionStore::from(db))
            }

            (Some(snapshot), None) => {
                info!("Found partition snapshot, restoring it");
                let db = cell.import_cf(&mut state_guard, snapshot, database).await?;

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
                let db = cell.import_cf(&mut state_guard, snapshot, database).await?;
                Ok(PartitionStore::from(db))
            }
            (maybe_snapshot, Some(fast_forward_lsn)) => {
                // Play it safe and keep the partition store intact; we can't do much else at this
                // point. We'll likely halt again as soon as the processor starts up.
                let recovery_guide_msg = "The partition's log is trimmed to a point from which this processor can not resume. \
                Visit https://docs.restate.dev/server/clusters#handling-missing-snapshots \
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

    /// Seals transaction-commit admission and waits for admitted commits to finish.
    /// Retained handles stay sealed even after a later reopen.
    pub async fn close(&self, partition_id: PartitionId) {
        let Some(cell) = self.state.get(partition_id) else {
            return;
        };

        cell.inner.write().await.close().await;
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

    /// Opens a partition store by importing an already-downloaded snapshot, discarding any
    /// existing local state for the partition. Unlike [`Self::open`], this takes a snapshot the
    /// caller has fetched itself (e.g. via [`crate::snapshots::SnapshotRepository::get_latest`]),
    /// which lets the caller use the snapshot's own key range.
    pub async fn open_from_snapshot(
        &self,
        partition: &Partition,
        snapshot: LocalPartitionSnapshot,
    ) -> Result<PartitionStore, restate_rocksdb::RocksError> {
        let database = self.open_rocksdb(partition).await?;
        let cell = self.state.get_or_default(partition);
        let mut state_guard = cell.inner.write().await;
        let db = cell.import_cf(&mut state_guard, snapshot, database).await?;
        Ok(PartitionStore::from(db))
    }

    #[cfg(test)]
    pub async fn close_partition_store(&self, partition_id: PartitionId) {
        let Some(cell) = self.state.partitions.read().get(&partition_id).cloned() else {
            return;
        };
        let mut state_guard = cell.inner.write().await;
        let db = state_guard.db().cloned();
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::{Arc, mpsc};
    use std::task::{Context, Wake, Waker};
    use std::time::Duration;

    use parking_lot::Mutex;
    use rocksdb::AsColumnFamilyRef;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use restate_rocksdb::{Priority, RocksDbManager};
    use restate_storage_api::fsm_table::{ReadFsmTable, WriteFsmTable};
    use restate_storage_api::{StorageError, Transaction};
    use restate_types::config::Configuration;
    use restate_types::identifiers::{PartitionId, SnapshotId};
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;

    use super::PartitionStoreManager;
    use crate::PartitionStore;
    use crate::keys::KeyKind;
    use crate::partition_db::State;

    async fn write_applied_lsn(store: &mut PartitionStore, lsn: u64) -> anyhow::Result<()> {
        let mut tx = store.transaction();
        tx.put_applied_lsn(Lsn::new(lsn))?;
        tx.commit().await?;
        Ok(())
    }

    #[restate_core::test]
    async fn cancelled_commits_close_and_reopen_keep_the_old_lifetime_sealed() -> anyhow::Result<()>
    {
        const DEADLINE: Duration = Duration::from_secs(10);
        let rocksdb = RocksDbManager::init();
        let mut config = Configuration::pinned().clone();
        config.worker.storage.always_commit_in_background = true;
        Configuration::set(config);
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let sibling = Partition::new(PartitionId::from(1), KeyRange::FULL);

        for use_multi_db_layout in [false, true] {
            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let mut old = manager.open(&partition, None).await?;
            let sibling_store = manager.open(&sibling, None).await?;
            let cell = manager.state.get(partition.partition_id).unwrap();
            let admission = old.partition_db().admission().clone();
            let original_cf = old.cf_for_key_kind(KeyKind::Fsm).id();
            let pause = rocksdb.pause_background(Priority::High).await;
            let mut tx = old.transaction();
            tx.put_applied_lsn(Lsn::new(100))?;
            let mut commit = Box::pin(tx.commit());
            assert!(
                commit
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(commit);
            drop(tx);

            let mut close = Box::pin(manager.close(partition.partition_id));
            assert!(
                close
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(close);
            let closing_at = {
                let state = cell.inner.read().await;
                let State::Closing { closing_at, .. } = &*state else {
                    panic!("cancelled close must retain Closing state");
                };
                assert!(state.closed_since().is_none());
                assert!(state.maybe_open());
                assert!(Arc::ptr_eq(state.db().unwrap().admission(), &admission));
                *closing_at
            };
            assert!(admission.try_acquire().is_none());
            assert!(
                manager
                    .get_partition_store(partition.partition_id)
                    .await
                    .is_none()
            );
            assert!(
                sibling_store
                    .partition_db()
                    .admission()
                    .try_acquire()
                    .is_some()
            );

            let mut rejected = old.transaction();
            rejected.put_applied_lsn(Lsn::new(200))?;
            let buffered_size = rejected.estimated_size_in_bytes();
            assert!(matches!(
                rejected.commit().await,
                Err(StorageError::OperationalError)
            ));
            assert_eq!(rejected.estimated_size_in_bytes(), buffered_size);

            let mut reopen = Box::pin(manager.open(&partition, None));
            assert!(
                reopen
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(reopen);
            // A cancelled replacement must also leave the old accounting reachable.
            let mut replace = Box::pin(manager.drop_partition(partition.partition_id));
            assert!(
                replace
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(replace);
            assert!(matches!(
                *cell.inner.read().await,
                State::Closing { closing_at: started_at, .. } if started_at == closing_at
            ));
            assert!(
                pin!(admission.drain())
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );

            drop(pause);
            let mut reopened = timeout(DEADLINE, manager.open(&partition, None)).await??;
            assert_eq!(reopened.cf_for_key_kind(KeyKind::Fsm).id(), original_cf);
            assert_eq!(reopened.get_applied_lsn().await?, Some(Lsn::new(100)));
            assert!(!Arc::ptr_eq(
                &admission,
                reopened.partition_db().admission()
            ));
            assert!(matches!(
                rejected.commit().await,
                Err(StorageError::OperationalError)
            ));
            assert_eq!(rejected.estimated_size_in_bytes(), buffered_size);
            rejected.clear();
            rejected.commit().await?; // Empty commits remain a no-op on a sealed handle.
            drop(rejected);
            write_applied_lsn(&mut reopened, 300).await?;

            timeout(DEADLINE, manager.close(partition.partition_id)).await?;
            {
                let state = cell.inner.read().await;
                assert!(matches!(*state, State::Closed { .. }));
                assert!(state.closed_since().is_some());
            }
            manager.close(partition.partition_id).await;
            reopened = manager.open(&partition, None).await?;

            // Complete an actual replacement behind a cancelled commit waiter.
            let pause = rocksdb.pause_background(Priority::High).await;
            let mut tx = reopened.transaction();
            tx.put_applied_lsn(Lsn::new(400))?;
            let mut commit = Box::pin(tx.commit());
            assert!(
                commit
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(commit);
            drop(tx);
            let mut replace = Box::pin(manager.drop_partition(partition.partition_id));
            assert!(
                replace
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(pause);
            timeout(DEADLINE, replace).await??;
            let mut fresh = manager.open(&partition, None).await?;
            assert_ne!(fresh.cf_for_key_kind(KeyKind::Fsm).id(), original_cf);
            assert_eq!(fresh.get_applied_lsn().await?, None);
            let mut rejected = reopened.transaction();
            rejected.put_applied_lsn(Lsn::new(500))?;
            assert!(matches!(
                rejected.commit().await,
                Err(StorageError::OperationalError)
            ));
            drop(rejected);
            write_applied_lsn(&mut fresh, 600).await?;

            drop(old);
            drop(reopened);
            drop(fresh);
            drop(sibling_store);
            drop(cell);
            drop(manager);
            rocksdb.reset().await?;
        }
        Ok(())
    }

    #[restate_core::test]
    async fn delayed_flush_is_fenced_across_partition_replacement() -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);

        for use_multi_db_layout in [false, true] {
            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let mut store = manager.open(&partition, None).await?;
            let database = manager.open_rocksdb(&partition).await?;
            let original_cf_id = store.cf_for_key_kind(KeyKind::Fsm).id();
            let original_watch = store.get_durable_lsn().await?;
            write_applied_lsn(&mut store, 100).await?;

            let (reached, resume, completed) = database.notifications.pause_next_notification();
            store.partition_db().flush_memtables(false).await?;
            timeout(DEADLINE, reached).await??;
            manager.close(partition.partition_id).await;
            drop(store);
            drop(resume);
            timeout(DEADLINE, completed).await??;
            assert_eq!(
                *original_watch.borrow(),
                Lsn::new(100),
                "logically_closed_partition_receives_pending_flush"
            );

            let mut store = manager.open(&partition, None).await?;
            assert!(original_watch.same_channel(&store.get_durable_lsn().await?));
            write_applied_lsn(&mut store, 200).await?;
            let (reached, resume, completed) = database.notifications.pause_next_notification();
            store.partition_db().flush_memtables(false).await?;
            timeout(DEADLINE, reached).await??;
            manager.close(partition.partition_id).await;
            drop(store);
            timeout(DEADLINE, manager.drop_partition(partition.partition_id)).await??;
            assert_eq!(*original_watch.borrow(), Lsn::INVALID);

            let mut store = timeout(DEADLINE, manager.open(&partition, None)).await??;
            assert_ne!(store.cf_for_key_kind(KeyKind::Fsm).id(), original_cf_id);
            let fresh_watch = store.get_durable_lsn().await?;
            assert_eq!(*fresh_watch.borrow(), Lsn::INVALID);
            drop(resume);
            timeout(DEADLINE, completed).await??;
            assert_eq!(
                *fresh_watch.borrow(),
                Lsn::INVALID,
                "old_flush_cannot_initialize_new_cf"
            );
            assert_eq!(*original_watch.borrow(), Lsn::INVALID);
            assert_eq!(store.get_applied_lsn().await?, None);
            assert_eq!(*store.get_durable_lsn().await?.borrow(), Lsn::INVALID);

            write_applied_lsn(&mut store, 300).await?;
            let snapshots = tempfile::tempdir()?;
            let snapshot = store
                .create_local_snapshot(snapshots.path(), None, SnapshotId::new())
                .await?;
            write_applied_lsn(&mut store, 400).await?;
            store.partition_db().flush_memtables(true).await?;
            assert_eq!(*fresh_watch.borrow(), Lsn::new(400));
            write_applied_lsn(&mut store, 500).await?;
            let replaced_cf_id = store.cf_for_key_kind(KeyKind::Fsm).id();
            // Resume the callback before dropping its retired CF on unwind.
            let mut retired_store = store;
            let (reached, resume, completed) = database.notifications.pause_next_notification();
            retired_store.partition_db().flush_memtables(false).await?;
            timeout(DEADLINE, reached).await??;
            manager.close(partition.partition_id).await;

            // Direct import must reset the old watch itself, without an
            // explicit drop_partition call, even while its callback is delayed.
            let mut store =
                timeout(DEADLINE, manager.open_from_snapshot(&partition, snapshot)).await??;
            assert_ne!(store.cf_for_key_kind(KeyKind::Fsm).id(), replaced_cf_id);
            let imported_watch = store.get_durable_lsn().await?;
            assert_eq!(
                *fresh_watch.borrow(),
                Lsn::INVALID,
                "direct_import_resets_old_watch"
            );
            assert_eq!(*imported_watch.borrow(), Lsn::new(300));
            let recovered_watch = retired_store.get_durable_lsn().await;
            drop(resume);
            timeout(DEADLINE, completed).await??;
            let recovered_watch = recovered_watch?;
            assert!(fresh_watch.same_channel(&recovered_watch));
            assert_eq!(
                *recovered_watch.borrow(),
                Lsn::INVALID,
                "recovery_cannot_revive_retired_watch"
            );
            assert_eq!(*imported_watch.borrow(), Lsn::new(300));
            assert_eq!(store.get_applied_lsn().await?, Some(Lsn::new(300)));
            assert_eq!(*store.get_durable_lsn().await?.borrow(), Lsn::new(300));

            write_applied_lsn(&mut store, 600).await?;
            store.partition_db().flush_memtables(true).await?;
            assert_eq!(*imported_watch.borrow(), Lsn::new(600));
            assert_eq!(*fresh_watch.borrow(), Lsn::INVALID);
            assert_eq!(*original_watch.borrow(), Lsn::INVALID);
            drop(retired_store);
            drop(store);
            drop(manager);
            database
                .rocksdb
                .close()
                .await
                .expect("all_storage_users_released");
        }
        Ok(())
    }

    #[restate_core::test]
    async fn delayed_flush_does_not_regress_durable_lsn() -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let manager = PartitionStoreManager::create(false).await?;
        let mut store = manager.open(&partition, None).await?;
        let database = manager.open_rocksdb(&partition).await?;
        // Allow the second flush to complete while the first callback is paused.
        database
            .rocksdb
            .inner()
            .as_raw_db()
            .set_db_options(&[("max_background_flushes", "2")])?;
        let mut durable_lsn = store.get_durable_lsn().await?;
        write_applied_lsn(&mut store, 100).await?;
        let (reached, resume, completed) = database.notifications.pause_next_notification();
        store.partition_db().flush_memtables(false).await?;
        timeout(DEADLINE, reached).await??;

        write_applied_lsn(&mut store, 200).await?;
        store.partition_db().flush_memtables(false).await?;
        let newer_lsn = timeout(DEADLINE, durable_lsn.wait_for(|lsn| *lsn == Lsn::new(200)))
            .await
            .map(|result| result.map(|value| *value));
        // Release the callback even if waiting for the newer flush failed.
        drop(resume);
        timeout(DEADLINE, completed).await??;
        store.partition_db().flush_memtables(true).await?;

        assert_eq!(newer_lsn??, Lsn::new(200));
        assert_eq!(
            *durable_lsn.borrow(),
            Lsn::new(200),
            "older_callback_cannot_regress_durable_progress"
        );
        drop(store);
        drop(manager);
        database
            .rocksdb
            .close()
            .await
            .expect("all_storage_users_released");
        Ok(())
    }

    #[restate_core::test]
    async fn durable_notifications_follow_physical_database_opens() -> anyhow::Result<()> {
        RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let other_partition = Partition::new(PartitionId::from(1), KeyRange::FULL);

        for use_multi_db_layout in [false, true] {
            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let mut store = manager.open(&partition, None).await?;
            let database = manager.open_rocksdb(&partition).await?;
            let other_database = manager.open_rocksdb(&other_partition).await?;
            assert_eq!(
                Arc::ptr_eq(&database.rocksdb, &other_database.rocksdb),
                !use_multi_db_layout
            );
            assert_eq!(
                Arc::ptr_eq(&database.notifications, &other_database.notifications),
                !use_multi_db_layout,
                "registry_is_shared_exactly_when_database_is_shared"
            );
            if use_multi_db_layout {
                other_database
                    .rocksdb
                    .close()
                    .await
                    .expect("unused_database");
            } else {
                drop(other_database);
            }

            let old_watch = store.get_durable_lsn().await?;
            let old_cf_id = store.cf_for_key_kind(KeyKind::Fsm).id();
            write_applied_lsn(&mut store, 100).await?;
            store.partition_db().flush_memtables(true).await?;
            assert_eq!(*old_watch.borrow(), Lsn::new(100));
            let old_notifications = database.notifications;
            drop(database.rocksdb);
            drop(store);
            manager.close_partition_store(partition.partition_id).await;
            assert_eq!(*old_watch.borrow(), Lsn::INVALID);

            let mut store = manager.open(&partition, None).await?;
            let database = manager.open_rocksdb(&partition).await?;
            assert!(!Arc::ptr_eq(&old_notifications, &database.notifications));
            assert_eq!(store.cf_for_key_kind(KeyKind::Fsm).id(), old_cf_id);
            let new_watch = store.get_durable_lsn().await?;
            assert_eq!(*new_watch.borrow(), Lsn::new(100));
            write_applied_lsn(&mut store, 200).await?;
            store.partition_db().flush_memtables(true).await?;
            assert_eq!(*new_watch.borrow(), Lsn::new(200));
            assert_eq!(*old_watch.borrow(), Lsn::INVALID);
            drop(store);
            drop(manager);
            database
                .rocksdb
                .close()
                .await
                .expect("all_storage_users_released");
        }
        Ok(())
    }

    #[restate_core::test]
    async fn snapshot_import_failure_invalidates_only_after_validation() -> anyhow::Result<()> {
        RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let snapshots = tempfile::tempdir()?;

        for use_multi_db_layout in [false, true] {
            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let mut store = manager.open(&partition, None).await?;
            let database = store.partition_db().rocksdb().clone();
            let old_watch = store.get_durable_lsn().await?;
            write_applied_lsn(&mut store, 100).await?;
            let mut snapshot = store
                .create_local_snapshot(snapshots.path(), None, SnapshotId::new())
                .await?;
            snapshot.key_range = KeyRange::new(1, u64::MAX);
            assert!(matches!(
                manager.open_from_snapshot(&partition, snapshot).await,
                Err(restate_rocksdb::RocksError::SnapshotKeyRangeMismatch)
            ));
            assert!(old_watch.same_channel(&store.get_durable_lsn().await?));
            write_applied_lsn(&mut store, 200).await?;
            let mut snapshot = store
                .create_local_snapshot(snapshots.path(), None, SnapshotId::new())
                .await?;
            assert_eq!(*old_watch.borrow(), Lsn::new(200));

            // RocksDB rejects this after the old CF has been dropped. Retrying
            // must create a fresh store rather than return the deleted handle.
            snapshot.db_comparator_name = "invalid comparator".into();
            drop(store);
            assert!(
                manager
                    .open_from_snapshot(&partition, snapshot)
                    .await
                    .is_err()
            );
            assert_eq!(*old_watch.borrow(), Lsn::INVALID);
            assert!(
                manager
                    .get_partition_store(partition.partition_id)
                    .await
                    .is_none()
            );
            let mut store = manager.open(&partition, None).await?;
            let new_watch = store.get_durable_lsn().await?;
            assert_eq!(*new_watch.borrow(), Lsn::INVALID);
            assert_eq!(store.get_applied_lsn().await?, None);
            write_applied_lsn(&mut store, 300).await?;
            store.partition_db().flush_memtables(true).await?;
            assert_eq!(*new_watch.borrow(), Lsn::new(300));
            assert_eq!(*old_watch.borrow(), Lsn::INVALID);
            drop(store);
            drop(manager);
            database.close().await.expect("all_storage_users_released");
        }
        Ok(())
    }

    // Watch publication wakes receivers synchronously inside the native flush callback.
    // Dropping the resume sender releases it, including when the test fails.
    struct PauseOnWake {
        reached: Mutex<Option<oneshot::Sender<bool>>>,
        resume: Mutex<mpsc::Receiver<()>>,
    }

    impl Wake for PauseOnWake {
        fn wake(self: Arc<Self>) {
            if let Some(reached) = self.reached.lock().take() {
                let _ = reached.send(tokio::runtime::Handle::try_current().is_err());
                let _ = self.resume.lock().recv();
            }
        }
    }

    #[restate_core::test]
    async fn partition_database_does_not_retain_manager_state() -> anyhow::Result<()> {
        RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);

        for use_multi_db_layout in [false, true] {
            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let state = Arc::downgrade(&manager.state);
            let mut store = manager.open(&partition, None).await?;
            let durable_lsn = store.get_durable_lsn().await?;

            write_applied_lsn(&mut store, 100).await?;
            store.partition_db().flush_memtables(true).await?;
            assert_eq!(
                *durable_lsn.borrow(),
                Lsn::new(100),
                "live_manager_receives_flush_notifications"
            );

            drop(manager);
            assert!(
                state.upgrade().is_none(),
                "retained_database_does_not_keep_partition_map_alive"
            );

            // Notifications follow the physical database lifetime, even after
            // the manager and its partition cells have been released.
            write_applied_lsn(&mut store, 200).await?;
            store.partition_db().flush_memtables(true).await?;
            assert_eq!(
                *durable_lsn.borrow(),
                Lsn::new(200),
                "retained_store_receives_flush_notifications_after_manager_release"
            );

            let database = store.partition_db().rocksdb().clone();
            drop(store);
            database.close().await.expect("all_storage_users_released");

            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let mut store = manager.open(&partition, None).await?;
            assert_eq!(
                *store.get_durable_lsn().await?.borrow(),
                Lsn::new(200),
                "flush_after_manager_release_remains_durable_on_reopen"
            );
            let database = store.partition_db().rocksdb().clone();
            drop(store);
            drop(manager);
            database.close().await.expect("all_storage_users_released");
        }
        Ok(())
    }

    #[restate_core::test]
    async fn flush_callback_does_not_own_partition_storage() -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);

        RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);

        for use_multi_db_layout in [false, true] {
            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let state = Arc::downgrade(&manager.state);
            let mut store = manager.open(&partition, None).await?;
            let database = Arc::downgrade(store.partition_db().rocksdb());
            let mut durable_lsn = store.get_durable_lsn().await?;
            durable_lsn.borrow_and_update();

            write_applied_lsn(&mut store, 100).await?;

            let (reached_tx, reached_rx) = oneshot::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let mut resume_tx = Some(resume_tx);
            let waker = Waker::from(Arc::new(PauseOnWake {
                reached: Mutex::new(Some(reached_tx)),
                resume: Mutex::new(resume_rx),
            }));
            let mut changed = pin!(durable_lsn.changed());
            assert!(
                changed
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );

            store.partition_db().flush_memtables(false).await?;
            let callback_is_native = timeout(DEADLINE, reached_rx).await??;

            // If this regresses, finish the callback while the store is retained
            // so the test reports the ownership failure instead of self-deadlocking.
            let callback_retains_storage = Arc::strong_count(&manager.state) != 1;
            if callback_retains_storage {
                drop(resume_tx.take());
                store.partition_db().flush_memtables(true).await?;
            }

            let (closed_tx, mut closed_rx) = oneshot::channel();
            let closer = std::thread::spawn(move || {
                drop(manager);
                drop(store);
                let _ = closed_tx.send(());
            });

            if !callback_retains_storage {
                // Zero strong references marks entry into the final Arc drop,
                // not completion of the native database destructor.
                timeout(DEADLINE, async {
                    while database.strong_count() != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await?;
                assert!(state.upgrade().is_none(), "callback_does_not_pin_manager");
                assert!(matches!(
                    closed_rx.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                drop(resume_tx.take());
            }

            timeout(DEADLINE, closed_rx).await??;
            closer.join().expect("database_drop_thread_succeeded");
            changed.await?;
            assert!(callback_is_native, "callback_runs_outside_tokio");
            assert!(
                !callback_retains_storage,
                "flush_callback_must_not_retain_storage_owning_manager_state"
            );

            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let mut store = manager.open(&partition, None).await?;
            assert_eq!(
                *store.get_durable_lsn().await?.borrow(),
                Lsn::new(100),
                "native_drop_completes_and_database_reopens_durably"
            );
            let database = store.partition_db().rocksdb().clone();
            drop(store);
            drop(manager);
            database.close().await.expect("all_storage_users_released");
        }
        Ok(())
    }
}
