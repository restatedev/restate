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
use std::time::Duration;

use ahash::HashMap;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{Level, debug, error, info, instrument, warn};

use restate_futures_util::overdue::OverdueLoggingExt;
use restate_rocksdb::{
    CfPrefixPattern, DbRef, DbSpecBuilder, RocksDb, RocksDbManager, RocksError, SinglePoolExecutor,
};
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
// Closing and dropping a partition wait for its in-flight work and open handles, such as
// running queries. Warn if that takes longer than this.
const LOG_SLOW_TRANSITION_AFTER: Duration = Duration::from_secs(5);

pub(crate) struct SharedState {
    partitions: RwLock<HashMap<PartitionId, Arc<PartitionCell>>>,
    executor: SinglePoolExecutor,
}

impl SharedState {
    fn new(executor: SinglePoolExecutor) -> Self {
        Self {
            partitions: Default::default(),
            executor,
        }
    }
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

        let cell = Arc::new(PartitionCell::new(partition.clone(), self.executor.clone()));
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
    /// Skips partitions in the middle of a transition instead of waiting for them,
    /// so callers can look at them again later.
    pub(crate) fn get_maybe_open_dbs(&self) -> Vec<crate::partition_db::State> {
        self.partitions
            .read()
            .values()
            .filter_map(|cell| {
                let state = cell.try_state()?;
                state.maybe_open().then(|| state.clone())
            })
            .collect()
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
}

struct CachedDb {
    tracked: DbRef,
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
        // Partition creation, import, and deletion share the RocksDB lifecycle pool.
        let executor = RocksDbManager::get().lifecycle_executor().clone();
        let state = Arc::new(SharedState::new(executor));
        let memory_controller = MemoryController::start(state.clone())?;

        let psm = Arc::new(Self {
            state: state.clone(),
            snapshots: Snapshots::create(&Configuration::pinned())
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

        if let Some(cached) = db_cache_guard.get_mut(&db_name) {
            let result = match cached.rocksdb.upgrade() {
                Some(rocksdb) => Ok(rocksdb),
                None => RocksDbManager::get().get_db_by_ref(cached.tracked).await,
            };
            match result {
                Ok(rocksdb) => {
                    cached.rocksdb = Arc::downgrade(&rocksdb);
                    return Ok(PartitionStoreDb {
                        rocksdb,
                        notifications: cached.notifications.clone(),
                    });
                }
                Err(RocksError::NotOpen) => {
                    // The database's last `Arc` is gone, but it may still be closing. Wait until
                    // it's fully closed before opening the path again. The cache entry stays
                    // until then, so a caller cancelled here leaves it for the next one.
                    RocksDbManager::get().closed(cached.tracked)?.await;
                }
                Err(error) => {
                    db_cache_guard.remove(&db_name);
                    return Err(error);
                }
            }
            db_cache_guard.remove(&db_name);
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

        let (tracked, opening) = RocksDbManager::get().open_db(db_spec)?;
        // Cache the open's DbRef and durability listeners before awaiting. If this
        // caller is cancelled, the next one finds the entry and collects this same
        // open instead of starting another.
        db_cache_guard.insert(
            db_name.clone(),
            CachedDb {
                tracked,
                rocksdb: Weak::new(),
                notifications: notifications.clone(),
            },
        );
        let rocksdb = match opening.await {
            Ok(rocksdb) => rocksdb,
            Err(error) => {
                db_cache_guard.remove(&db_name);
                return Err(error);
            }
        };
        db_cache_guard
            .get_mut(&db_name)
            .expect("pending open")
            .rocksdb = Arc::downgrade(&rocksdb);

        Ok(PartitionStoreDb {
            rocksdb,
            notifications,
        })
    }

    pub fn is_repository_configured(&self) -> bool {
        self.snapshots.is_repository_configured()
    }

    /// Asks the snapshot repository for the partition's latest snapshot status and records
    /// its archived LSN. This doesn't need the partition's store: it works whether the store
    /// is open, closed, or not on this node at all. Returns `None` if no snapshot repository
    /// is configured.
    pub async fn refresh_latest_partition_snapshot_status(
        &self,
        partition: &Partition,
    ) -> anyhow::Result<Option<PartitionSnapshotStatus>> {
        if !self.snapshots.is_repository_configured() {
            return Ok(None);
        }
        // Asking the repository needs only the partition id, so nothing of the partition's
        // cell or store is held while the request is in flight.
        let status = self
            .snapshots
            .get_latest_snapshot_status(partition.partition_id)
            .await?
            // Without a snapshot, report an empty status, so the caller knows the partition
            // is still waiting for its first one.
            .unwrap_or_else(|| PartitionSnapshotStatus::none(partition.log_id()));
        // A partition this manager hasn't seen yet gets a cell in the `Unknown` state, so
        // its archived LSN is already known when its store is opened.
        self.state
            .get_or_default(partition)
            .note_archived_lsn(status.archived_lsn);
        Ok(Some(status))
    }

    /// Records the partition's archived LSN, as reported by the snapshot repository.
    /// Does nothing if this manager has never seen the partition.
    pub(crate) fn note_archived_lsn(&self, partition_id: PartitionId, archived_lsn: Lsn) {
        if let Some(cell) = self.state.get(partition_id) {
            cell.note_archived_lsn(archived_lsn);
        }
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

        if let Some(db) = state_guard.get_or_reopen().await? {
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

        async { cell.inner.write().await.close().await }
            .log_slow_after(
                LOG_SLOW_TRANSITION_AFTER,
                Level::WARN,
                format!("Closing partition {partition_id}"),
            )
            .await;
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

    /// Deletes this node's local copy of the partition, discarding all of its data.
    /// Returns `true` if there was a column family to delete, `false` if there was
    /// nothing to delete. It also finds a column family that this process hasn't
    /// opened yet.
    ///
    /// The partition stops accepting work right away, and its column family is deleted
    /// once nothing uses it anymore. So this waits until every [`PartitionStore`] of the
    /// partition is dropped: stop its partition processor first. Opening the partition
    /// meanwhile waits for the deletion. Once the partition has stopped accepting work,
    /// the deletion goes ahead even if this call is cancelled. If an earlier deletion
    /// failed, this retries it.
    #[instrument(level = "error", skip_all, fields(partition_id = %partition.partition_id, cf_name = %partition.cf_name()))]
    pub async fn drop_partition_store(&self, partition: &Partition) -> Result<bool, RocksError> {
        let dropped = async {
            let database = self.open_rocksdb(partition).await?;
            let cell = self.state.get_or_default(partition);
            let mut state_guard = cell.inner.write().await;
            cell.drop_partition_store(&mut state_guard, &database).await
        }
        .log_slow_after(
            LOG_SLOW_TRANSITION_AFTER,
            Level::WARN,
            format!(
                "Dropping local store of partition {}",
                partition.partition_id
            ),
        )
        .await?;

        if dropped {
            info!("Dropped local partition store");
        } else {
            debug!("No local partition store to drop");
        }
        Ok(dropped)
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

    use restate_rocksdb::{Priority, RocksDbManager, RocksError};
    use restate_storage_api::fsm_table::{ReadFsmTable, WriteFsmTable};
    use restate_storage_api::{StorageError, Transaction};
    use restate_types::config::Configuration;
    use restate_types::identifiers::{PartitionId, SnapshotId};
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;

    use super::PartitionStoreManager;
    use crate::PartitionStore;
    use crate::faults::FaultPoint;
    use crate::keys::KeyKind;
    use crate::partition_db::State;

    async fn write_applied_lsn(store: &mut PartitionStore, lsn: u64) -> anyhow::Result<()> {
        let mut tx = store.transaction();
        tx.put_applied_lsn(Lsn::new(lsn))?;
        tx.commit().await?;
        Ok(())
    }

    #[restate_core::test]
    async fn cancelled_open_is_adopted_with_its_original_notifications() -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        let rocksdb = RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        for layout in [false, true] {
            let manager = PartitionStoreManager::create(layout).await?;
            let paused = rocksdb.pause_lifecycle().await;
            let mut opening = Box::pin(manager.open_rocksdb(&partition));
            assert!(
                opening
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(opening);
            let db_name = restate_rocksdb::DbName::from(partition.db_name(layout));
            let (tracked, notifications) = {
                let cache = manager.db_cache.lock().await;
                let pending = &cache[&db_name];
                (pending.tracked, pending.notifications.clone())
            };
            let mut retry = Box::pin(manager.open_rocksdb(&partition));
            assert!(
                retry
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(retry);
            drop(paused);
            // Pausing the lifecycle pool waits until the queued open has finished and
            // stored its result. Resume right away; nothing collects the result here.
            drop(timeout(DEADLINE, rocksdb.pause_lifecycle()).await?);
            let db = timeout(DEADLINE, manager.open_rocksdb(&partition)).await??;
            assert_eq!(db.rocksdb.registration(), Some(tracked));
            assert!(Arc::ptr_eq(&notifications, &db.notifications));
            db.rocksdb.close().await.expect("last database pin");
            let reopened = timeout(DEADLINE, manager.open_rocksdb(&partition)).await??;
            assert_ne!(reopened.rocksdb.registration(), Some(tracked));
            assert!(!Arc::ptr_eq(&notifications, &reopened.notifications));
            reopened.rocksdb.close().await.expect("last database pin");
            drop(manager);
            rocksdb.reset().await?;
        }
        Ok(())
    }

    #[restate_core::test]
    async fn retirement_survives_cancelled_waiters_and_waits_for_physical_users()
    -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        let rocksdb = RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let sibling = Partition::new(PartitionId::from(1), KeyRange::FULL);
        for layout in [false, true] {
            let manager = PartitionStoreManager::create(layout).await?;
            let mut store = manager.open(&partition, None).await?;
            let mut neighbor = manager.open(&sibling, None).await?;
            write_applied_lsn(&mut store, 10).await?;
            // The memory controller skips a partition in the middle of a transition
            // instead of waiting for it, and sees it again once the transition is over.
            let maybe_open = || {
                let mut ids: Vec<_> = manager
                    .state
                    .get_maybe_open_dbs()
                    .iter()
                    .map(|state| state.db().unwrap().partition().partition_id)
                    .collect();
                ids.sort();
                ids
            };
            {
                let cell = manager.state.get(partition.partition_id).unwrap();
                let _transition = cell.inner.write().await;
                assert_eq!(maybe_open(), [sibling.partition_id]);
            }
            assert_eq!(maybe_open(), [partition.partition_id, sibling.partition_id]);
            let database = manager.open_rocksdb(&partition).await?;
            let cf_id = store.cf_for_key_kind(KeyKind::Fsm).id();
            let released = store.partition_db().released();
            let (started, reached) = oneshot::channel();
            let (resume, paused) = mpsc::channel::<()>();
            let mut read = Box::pin(store.partition_db().run_background_read_op(
                "retirement-read",
                restate_rocksdb::StorageTaskKind::MultiGet,
                Priority::High,
                move |_| {
                    let _ = started.send(());
                    let _ = paused.recv();
                },
            ));
            assert!(
                read.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            timeout(DEADLINE, reached).await??;
            drop(read);
            let mut retirement = Box::pin(manager.drop_partition_store(&partition));
            assert!(
                retirement
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(retirement);
            // The deletion was committed before the caller went away, even though the
            // running read still holds the partition's admission.
            let cell = manager.state.get(partition.partition_id).unwrap();
            assert!(matches!(&*cell.inner.read().await, State::Retiring { .. }));
            let mut released = Box::pin(released);
            assert!(
                released
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            let mut reopen = Box::pin(manager.open(&partition, None));
            assert!(
                reopen
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(reopen);
            write_applied_lsn(&mut neighbor, 20).await?;
            assert!(
                database
                    .rocksdb
                    .inner()
                    .cf_handle(partition.cf_name().as_ref())
                    .is_some()
            );
            let paused_cleanup = rocksdb.pause_lifecycle().await;
            drop(resume);
            timeout(DEADLINE, store.partition_db().admission().drain()).await?;
            // Reads count against the partition, so the drain above waited for them.
            // But `store` is still alive, so its column family must not be deleted yet.
            let mut retirement = Box::pin(manager.drop_partition_store(&partition));
            assert!(
                retirement
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(retirement);
            let mut result = match &*cell.inner.read().await {
                State::Retiring { result } => result.clone(),
                _ => panic!("retirement must remain reachable"),
            };
            drop(store);
            timeout(DEADLINE, released).await?;
            assert!(
                result.borrow().is_none(),
                "pin release is not native deletion"
            );
            drop(paused_cleanup);
            // The deletion finishes even though no drop_partition_store() or open() future is
            // being polled.
            timeout(DEADLINE, result.wait_for(Option::is_some))
                .await??
                .as_ref()
                .unwrap()
                .clone()?;
            assert!(
                database
                    .rocksdb
                    .inner()
                    .cf_handle(partition.cf_name().as_ref())
                    .is_none()
            );
            let mut fresh = timeout(DEADLINE, manager.open(&partition, None)).await??;
            assert_ne!(fresh.cf_for_key_kind(KeyKind::Fsm).id(), cf_id);
            assert_eq!(fresh.get_applied_lsn().await?, None);
            assert_eq!(neighbor.get_applied_lsn().await?, Some(Lsn::new(20)));
            drop((fresh, neighbor, database, cell, manager));
            rocksdb.reset().await?;
        }
        Ok(())
    }

    #[restate_core::test]
    async fn drop_partition_store_deletes_local_data_and_reopens() -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        let rocksdb = RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let sibling = Partition::new(PartitionId::from(1), KeyRange::FULL);
        for layout in [false, true] {
            // Leave both partitions on disk only, as after a restart.
            let manager = PartitionStoreManager::create(layout).await?;
            let mut store = manager.open(&partition, None).await?;
            let mut neighbor = manager.open(&sibling, None).await?;
            write_applied_lsn(&mut store, 10).await?;
            write_applied_lsn(&mut neighbor, 20).await?;
            drop((store, neighbor, manager));
            rocksdb.reset().await?;

            let manager = PartitionStoreManager::create(layout).await?;
            assert!(
                manager.drop_partition_store(&partition).await?,
                "finds a column family this process never opened"
            );
            assert!(!manager.drop_partition_store(&partition).await?);
            let mut neighbor = manager.open(&sibling, None).await?;
            assert_eq!(neighbor.get_applied_lsn().await?, Some(Lsn::new(20)));

            let database = manager.open_rocksdb(&partition).await?;
            database
                .rocksdb
                .inner()
                .open_cf(partition.cf_name().into())?;
            assert!(
                manager.drop_partition_store(&partition).await?,
                "finds a leftover column family while the cell is CfMissing"
            );
            assert!(
                database
                    .rocksdb
                    .inner()
                    .cf_handle(partition.cf_name().as_ref())
                    .is_none()
            );
            drop(database);

            // In the multi-db layout, each drop releases the partition's database, so
            // the next open has to wait until it has fully closed.
            for _ in 0..3 {
                let mut store = timeout(DEADLINE, manager.open(&partition, None)).await??;
                assert_eq!(store.get_applied_lsn().await?, None);
                write_applied_lsn(&mut store, 30).await?;
                drop(store);
                assert!(timeout(DEADLINE, manager.drop_partition_store(&partition)).await??);
            }
            assert_eq!(neighbor.get_applied_lsn().await?, Some(Lsn::new(20)));
            drop((neighbor, manager));
            rocksdb.reset().await?;
        }
        Ok(())
    }

    #[restate_core::test]
    async fn slow_and_failing_cf_work_keeps_the_partition_consistent() -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        let rocksdb = RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let sibling = Partition::new(PartitionId::from(1), KeyRange::FULL);
        for layout in [false, true] {
            let manager = PartitionStoreManager::create(layout).await?;
            let mut store = manager.open(&partition, None).await?;
            write_applied_lsn(&mut store, 10).await?;
            drop(store);
            let database = manager.open_rocksdb(&partition).await?;
            let has_cf = || {
                database
                    .rocksdb
                    .inner()
                    .cf_handle(partition.cf_name().as_ref())
                    .is_some()
            };
            let cell = manager.state.get(partition.partition_id).unwrap();

            // A slow deletion finishes even though its caller went away, and opening
            // the partition waits for it.
            let (reached, resume) = cell.faults.pause(FaultPoint::Delete);
            let mut dropping = Box::pin(manager.drop_partition_store(&partition));
            assert!(
                dropping
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(dropping);
            timeout(DEADLINE, reached).await??;
            assert!(has_cf());
            let mut result = match &*cell.inner.read().await {
                State::Retiring { result } => result.clone(),
                _ => panic!("the deletion must remain reachable"),
            };
            let mut reopen = Box::pin(manager.open(&partition, None));
            assert!(
                reopen
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(reopen);
            drop(resume);
            timeout(DEADLINE, result.wait_for(Option::is_some))
                .await??
                .as_ref()
                .unwrap()
                .clone()?;
            assert!(!has_cf());

            // A failed deletion is reported to its caller and keeps the partition
            // closed, with its column family, until a later drop retries it.
            let mut store = manager.open(&partition, None).await?;
            assert_eq!(store.get_applied_lsn().await?, None);
            write_applied_lsn(&mut store, 20).await?;
            drop(store);
            cell.faults.fail(FaultPoint::Delete, RocksError::ReadOnly);
            assert!(matches!(
                manager.drop_partition_store(&partition).await,
                Err(RocksError::ReadOnly)
            ));
            assert!(has_cf());
            assert!(manager.open(&partition, None).await.is_err());
            assert!(manager.drop_partition_store(&partition).await?);
            assert!(!has_cf());

            // A slow creation doesn't hold up other partitions. A drop that comes in
            // meanwhile waits for it and then deletes what it created, even though the
            // caller that started the creation went away.
            let (reached, resume) = cell.faults.pause(FaultPoint::Open);
            let mut opening = Box::pin(manager.open(&partition, None));
            assert!(
                opening
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            timeout(DEADLINE, reached).await??;
            // The paused creation holds no lifecycle thread.
            drop(timeout(DEADLINE, rocksdb.pause_lifecycle()).await?);
            let mut neighbor = timeout(DEADLINE, manager.open(&sibling, None)).await??;
            write_applied_lsn(&mut neighbor, 30).await?;
            let mut dropping = Box::pin(manager.drop_partition_store(&partition));
            assert!(
                dropping
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(opening);
            drop(resume);
            assert!(timeout(DEADLINE, dropping).await??);
            assert!(!has_cf());
            assert_eq!(neighbor.get_applied_lsn().await?, Some(Lsn::new(30)));

            drop((neighbor, cell, database, manager));
            rocksdb.reset().await?;
        }
        Ok(())
    }

    #[restate_core::test]
    async fn cancelled_cf_provision_is_adopted_or_closed_before_replacement() -> anyhow::Result<()>
    {
        const DEADLINE: Duration = Duration::from_secs(10);
        let rocksdb = RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        for layout in [false, true] {
            let manager = PartitionStoreManager::create(layout).await?;
            let database = manager.open_rocksdb(&partition).await?;
            let global_paused = rocksdb.pause_background(Priority::Low).await;
            let paused = rocksdb.pause_lifecycle().await;
            let mut opening = Box::pin(manager.open(&partition, None));
            assert!(
                opening
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(opening);
            let cell = manager.state.get(partition.partition_id).unwrap();
            let mut result = match &*cell.inner.read().await {
                State::Opening { result } => result.clone(),
                _ => panic!("accepted provision must remain reachable"),
            };
            let mut closing = Box::pin(manager.close(partition.partition_id));
            assert!(
                closing
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(closing);
            let mut deletion = Box::pin(manager.drop_partition_store(&partition));
            assert!(
                deletion
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(deletion);
            assert!(
                database
                    .rocksdb
                    .inner()
                    .cf_handle(partition.cf_name().as_ref())
                    .is_none()
            );
            drop(paused);
            timeout(DEADLINE, result.wait_for(Option::is_some)).await??;
            let (cf_id, admission, notifications) = {
                let result = result.borrow();
                let db = result.as_ref().unwrap().as_ref().unwrap();
                (
                    db.cf_handle().id(),
                    db.admission().clone(),
                    db.durable_lsn_sender().subscribe(),
                )
            };
            assert!(
                manager
                    .get_partition_store(partition.partition_id)
                    .await
                    .is_none()
            );
            timeout(DEADLINE, manager.close(partition.partition_id)).await?;
            assert!(admission.try_acquire().is_none());
            assert!(matches!(*cell.inner.read().await, State::Closed { .. }));
            drop(result); // The buffered result itself is a physical use pin.
            let mut adopted = timeout(DEADLINE, manager.open(&partition, None)).await??;
            assert_eq!(adopted.cf_for_key_kind(KeyKind::Fsm).id(), cf_id);
            assert!(notifications.same_channel(&adopted.get_durable_lsn().await?));
            assert!(!Arc::ptr_eq(&admission, adopted.partition_db().admission()));
            drop(adopted);
            timeout(DEADLINE, manager.drop_partition_store(&partition)).await??;
            // Creating, opening, and deleting the partition all finished while the
            // regular I/O pools were blocked, since they run on the lifecycle pool.
            drop(global_paused);
            drop((database, cell, manager));
            rocksdb.reset().await?;
        }
        Ok(())
    }

    #[restate_core::test]
    async fn cancelled_cf_import_retains_source_files_and_native_outcome() -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        let rocksdb = RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let snapshots = tempfile::tempdir()?;
        for layout in [false, true] {
            let manager = PartitionStoreManager::create(layout).await?;
            let database = manager.open_rocksdb(&partition).await?;
            for invalid in [false, true] {
                let mut store = manager.open(&partition, None).await?;
                write_applied_lsn(&mut store, 50).await?;
                let mut snapshot = store
                    .create_local_snapshot(snapshots.path(), None, SnapshotId::new())
                    .await?;
                let source = snapshot.base_dir.path().to_owned();
                if invalid {
                    snapshot.db_comparator_name = "invalid comparator".into();
                }
                drop(store);
                manager.drop_partition_store(&partition).await?;
                let paused = rocksdb.pause_lifecycle().await;
                let mut importing = Box::pin(manager.open_from_snapshot(&partition, snapshot));
                assert!(
                    importing
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                drop(importing);
                assert!(
                    source.exists(),
                    "queued native import still owns its source files"
                );
                let cell = manager.state.get(partition.partition_id).unwrap();
                let mut result = match &*cell.inner.read().await {
                    State::Opening { result } => result.clone(),
                    _ => panic!("accepted import must remain reachable"),
                };
                let mut reopening = Box::pin(manager.open(&partition, None));
                assert!(
                    reopening
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                drop(reopening);
                drop(paused);
                timeout(DEADLINE, result.wait_for(Option::is_some)).await??;
                assert!(
                    !source.exists(),
                    "native completion precedes source disposal and publication"
                );
                assert_eq!(result.borrow().as_ref().unwrap().is_err(), invalid);
                if !invalid {
                    let outcome = result.borrow();
                    let db = outcome.as_ref().unwrap().as_ref().unwrap();
                    assert_eq!(
                        *db.durable_lsn_sender().borrow(),
                        Lsn::new(50),
                        "cancelled import initializes durability before adoption"
                    );
                }
                drop(result);
                // A failed import belonged to the cancelled caller, so the next open
                // creates a fresh store instead of failing with that import's error.
                let mut adopted = timeout(DEADLINE, manager.open(&partition, None)).await??;
                assert_eq!(
                    adopted.get_applied_lsn().await?,
                    (!invalid).then_some(Lsn::new(50))
                );
                let expected = if invalid { Lsn::INVALID } else { Lsn::new(50) };
                assert_eq!(*adopted.get_durable_lsn().await?.borrow(), expected);
                if !invalid {
                    // Simulate a failed durable-LSN read during the import by resetting
                    // the LSN to unknown. get_durable_lsn() must then load it from disk
                    // (50), not from newer unflushed writes (60), and update the existing
                    // watch instead of creating a new one.
                    let watch = adopted.get_durable_lsn().await?;
                    write_applied_lsn(&mut adopted, 60).await?;
                    adopted
                        .partition_db()
                        .durable_lsn_sender()
                        .send_replace(Lsn::INVALID);
                    let recovered = adopted.get_durable_lsn().await?;
                    assert!(watch.same_channel(&recovered));
                    assert_eq!(*watch.borrow(), Lsn::new(50));
                    assert_eq!(adopted.get_applied_lsn().await?, Some(Lsn::new(60)));
                }
                drop(adopted);
            }
            drop((database, manager));
            rocksdb.reset().await?;
        }
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
            // The cancelled reopen leaves the old accounting reachable.
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
            // The abandoned queued commit is disposed of before reopening, not applied.
            assert_eq!(reopened.get_applied_lsn().await?, None);
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
            let mut replace = Box::pin(manager.drop_partition_store(&partition));
            assert!(
                replace
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(pause);
            let mut rejected = reopened.transaction();
            rejected.put_applied_lsn(Lsn::new(500))?;
            assert!(matches!(
                rejected.commit().await,
                Err(StorageError::OperationalError)
            ));
            drop(rejected);
            drop(old);
            drop(reopened);
            timeout(DEADLINE, replace).await??;
            let mut fresh = manager.open(&partition, None).await?;
            assert_ne!(fresh.cf_for_key_kind(KeyKind::Fsm).id(), original_cf);
            assert_eq!(fresh.get_applied_lsn().await?, None);
            write_applied_lsn(&mut fresh, 600).await?;

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
            timeout(DEADLINE, manager.drop_partition_store(&partition)).await??;
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
            // explicit drop_partition_store call, even while its callback is delayed.
            let recovered_watch = retired_store.get_durable_lsn().await?;
            assert!(fresh_watch.same_channel(&recovered_watch));
            drop(retired_store);
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
            drop(resume);
            timeout(DEADLINE, completed).await??;
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
    async fn delayed_flush_does_not_regress_durable_lsn_single_db() -> anyhow::Result<()> {
        delayed_flush_does_not_regress_durable_lsn(false).await
    }

    #[restate_core::test]
    async fn delayed_flush_does_not_regress_durable_lsn_multi_db() -> anyhow::Result<()> {
        delayed_flush_does_not_regress_durable_lsn(true).await
    }

    async fn delayed_flush_does_not_regress_durable_lsn(multi_db: bool) -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let manager = PartitionStoreManager::create(multi_db).await?;
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
            let mut native_closed = Box::pin(store.partition_db().rocksdb().closed());
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

            let (closed_tx, closed_rx) = oneshot::channel();
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
                assert!(
                    native_closed
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending(),
                    "paused callback prevents native completion"
                );
                drop(resume_tx.take());
            }

            timeout(DEADLINE, closed_rx).await??;
            closer.join().expect("database_drop_thread_succeeded");
            timeout(DEADLINE, native_closed).await?;
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
