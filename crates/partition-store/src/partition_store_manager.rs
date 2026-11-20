// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use ahash::{HashMap, HashSet};
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use tokio::sync::{Mutex as AsyncMutex, OwnedRwLockWriteGuard, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{Level, debug, error, field, info, instrument, warn};

use restate_core::io::IdleTracker;
use restate_core::{TaskCenter, TaskKind};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_memory::ByteCount;
use restate_rocksdb::{
    CfPrefixPattern, DbRef, DbSpec, DbSpecBuilder, RocksDb, RocksDbManager, RocksError,
    SinglePoolExecutor,
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
    AllDataCf, DroppedPartitionStore, PartitionCell, PartitionDb, PartitionStoreDb,
    RocksConfigurator, State,
};
use crate::snapshots::{
    CommitFenceRequest, ExportedSnapshot, GetSnapshotError, LocalPartitionSnapshot,
    PartitionSnapshotStatus, RestoreSnapshot, SnapshotExportMode, SnapshotFormatVersion, Snapshots,
};
use crate::{BuildError, OpenError, PartitionStore, SnapshotErrorKind};

const PARTITION_CF_PREFIX: &str = "data-";
// Closing and dropping a partition wait for its in-flight work and open handles, such as
// running queries. Warn if that takes longer than this.
const LOG_SLOW_TRANSITION_AFTER: Duration = Duration::from_secs(5);

pub(crate) struct SharedState {
    partitions: RwLock<HashMap<PartitionId, Arc<PartitionCell>>>,
    executor: IdleTracker<SinglePoolExecutor>,
}

impl SharedState {
    fn new(executor: IdleTracker<SinglePoolExecutor>) -> Self {
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
}

struct CachedDb {
    tracked: DbRef,
    rocksdb: Weak<RocksDb>,
    notifications: Arc<DurableLsnSenders>,
}

/// Content keys of a partition database incarnation's SST and blob files, keyed by file name and
/// size. It is cleared whenever the database is opened, closed, dropped or replaced; losing the
/// cache only costs re-hashing.
#[derive(Default)]
struct ChurnCache {
    files: HashMap<(String, u64), String>,
}

/// Restores a downloaded snapshot by importing it into the partition's database.
struct ImportSnapshot<'a> {
    cell: &'a PartitionCell,
    state: &'a mut State,
    database: PartitionStoreDb,
    target_lsn: Option<Lsn>,
}

impl RestoreSnapshot for ImportSnapshot<'_> {
    type Output = PartitionDb;

    fn restore<'a>(
        &'a mut self,
        snapshot: LocalPartitionSnapshot,
    ) -> futures::future::BoxFuture<'a, Result<PartitionDb, GetSnapshotError>> {
        Box::pin(async move {
            if snapshot.version == SnapshotFormatVersion::V3 {
                return Err(GetSnapshotError::Defective(anyhow::anyhow!(
                    "whole-database checkpoint snapshots (V3) cannot be restored by importing a column family"
                )));
            }
            let snapshot_lsn = snapshot.min_applied_lsn;
            match self.target_lsn {
                None => info!("Found partition snapshot, restoring it"),
                Some(fast_forward_lsn) => {
                    // The snapshot meets the target LSN requirement (filtered by candidate LSN
                    // and re-verified against the downloaded metadata in `download_and_restore`).
                    info!(
                        snapshot_lsn = %snapshot_lsn,
                        %fast_forward_lsn,
                        "Found snapshot with LSN >= target LSN, dropping local partition store state",
                    );
                }
            }
            self.cell
                .import_cf(self.state, snapshot, self.database.clone())
                .await
                .map_err(GetSnapshotError::from_import)
        })
    }
}

enum StagingVerifyError {
    /// The snapshot's data is unusable; its staging directory can be removed.
    Defective(anyhow::Error),
    /// The staging database could not be closed; leave its directory for recovery.
    CloseFailed(anyhow::Error),
}

enum WholeDbRestoreOutcome {
    Restored(PartitionStore),
    NotNeeded(OwnedRwLockWriteGuard<State>, Option<PartitionStoreDb>),
}

pub struct PartitionStoreManager {
    state: Arc<SharedState>,
    pub(crate) snapshots: Snapshots,
    db_cache: AsyncMutex<HashMap<restate_rocksdb::DbName, CachedDb>>,
    churn_cache: Mutex<HashMap<PartitionId, ChurnCache>>,
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
            churn_cache: Default::default(),
            memory_controller,
            use_multi_db_layout,
        });

        Ok(psm)
    }

    fn db_spec(
        &self,
        db_name: &restate_rocksdb::DbName,
        path: PathBuf,
        notifications: &Arc<DurableLsnSenders>,
    ) -> DbSpec {
        let configurator = RocksConfigurator::<AllDataCf>::new(
            self.memory_controller.memory_budget.clone(),
            notifications,
            self.use_multi_db_layout,
        );
        DbSpecBuilder::new(
            db_name.clone(),
            DatabaseKind::PartitionStore,
            path,
            configurator.clone(),
        )
        .add_cf_pattern(CfPrefixPattern::new(PARTITION_CF_PREFIX), configurator)
        // This is added as an experiment. We might make this configurable to let users decide
        // on the trade-off between shutdown time and startup catchup time.
        .add_to_flush_on_shutdown(CfPrefixPattern::ANY)
        .build()
        .expect("valid spec")
    }

    /// Opens a partition database at an arbitrary path, outside the manager's cache. Used to
    /// verify a downloaded checkpoint before it is renamed into place.
    async fn open_rocksdb_at(
        &self,
        db_name: &restate_rocksdb::DbName,
        path: PathBuf,
    ) -> Result<PartitionStoreDb, RocksError> {
        let notifications = Arc::new(DurableLsnSenders::default());
        let db_spec = self.db_spec(db_name, path, &notifications);
        let (_, opening) = RocksDbManager::get().open_db(db_spec)?;
        let rocksdb = opening.await?;
        Ok(PartitionStoreDb {
            rocksdb,
            notifications,
        })
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

        // We need to create/open this database. Any cached content keys belong to the previous
        // database incarnation.
        self.clear_churn_cache(partition.partition_id);
        let notifications = Arc::new(DurableLsnSenders::default());
        let path = Configuration::pinned().worker.storage.data_dir(&db_name);
        let db_spec = self.db_spec(&db_name, path, &notifications);

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

    /// Downloads and verifies the newest usable whole-database checkpoint (V3) into a staging
    /// directory. Returns `None` when the newest candidate is an older format, which the import
    /// path handles. A verified staging database is left closed and fsynced for the caller to
    /// install.
    async fn prepare_whole_db_snapshot(
        &self,
        partition: &Partition,
        target_lsn: Option<Lsn>,
        cancellation: &CancellationToken,
        cell: &PartitionCell,
    ) -> Result<Option<PathBuf>, OpenError> {
        if !self.use_multi_db_layout || !self.snapshots.is_repository_configured() {
            return Ok(None);
        }
        let db_name = restate_rocksdb::DbName::from(partition.db_name(self.use_multi_db_layout));

        let candidates = self
            .snapshots
            .get_snapshot_candidates(partition.partition_id)
            .await
            .map_err(OpenError::Snapshot)?;
        let candidates: Vec<_> = candidates
            .into_iter()
            .filter(|c| target_lsn.is_none_or(|min_lsn| c.min_applied_lsn >= min_lsn))
            .collect();

        let data_dir = Configuration::pinned().worker.storage.data_dir(&db_name);
        let mut saw_whole_db = false;
        let mut last_error: Option<anyhow::Error> = None;
        for snapshot_ref in &candidates {
            let metadata = match self
                .snapshots
                .get_snapshot_metadata(partition.partition_id, snapshot_ref)
                .await
            {
                Ok(metadata) => metadata,
                Err(GetSnapshotError::Unavailable(err)) => return Err(OpenError::Snapshot(err)),
                Err(err @ GetSnapshotError::Defective(_)) => {
                    warn!(
                        snapshot_id = %snapshot_ref.snapshot_id,
                        %err,
                        "Snapshot metadata is unusable, trying the next older one"
                    );
                    last_error = Some(err.into());
                    continue;
                }
            };

            if metadata.version != SnapshotFormatVersion::V3 {
                return Ok(None);
            }
            saw_whole_db = true;

            if let Some(min_lsn) = target_lsn
                && metadata.min_applied_lsn < min_lsn
            {
                last_error = Some(anyhow::anyhow!(
                    "snapshot {} has min_applied_lsn {} below required target {}",
                    snapshot_ref.snapshot_id,
                    metadata.min_applied_lsn,
                    min_lsn,
                ));
                continue;
            }
            if metadata.key_range.start() > partition.key_range.start()
                || metadata.key_range.end() < partition.key_range.end()
            {
                last_error = Some(anyhow::anyhow!(
                    "snapshot {} does not cover the partition's key range",
                    snapshot_ref.snapshot_id
                ));
                continue;
            }

            let staging_dir = restoring_dir(&data_dir)?;
            let mut metadata = metadata;
            match self
                .snapshots
                .download_whole_db_snapshot(
                    partition.partition_id,
                    snapshot_ref,
                    &mut metadata,
                    &staging_dir,
                    cancellation,
                )
                .await
            {
                Ok(()) => {}
                Err(GetSnapshotError::Unavailable(err)) => {
                    remove_dir_all_strict(&staging_dir)
                        .await
                        .map_err(|cleanup| OpenError::Snapshot(anyhow::Error::new(cleanup)))?;
                    if cancellation.is_cancelled() {
                        return Err(OpenError::Cancelled);
                    }
                    return Err(OpenError::Snapshot(err));
                }
                Err(err @ GetSnapshotError::Defective(_)) => {
                    remove_dir_all_strict(&staging_dir)
                        .await
                        .map_err(|cleanup| OpenError::Snapshot(anyhow::Error::new(cleanup)))?;
                    warn!(
                        snapshot_id = %snapshot_ref.snapshot_id,
                        %err,
                        "Snapshot is unusable, trying the next older one"
                    );
                    last_error = Some(err.into());
                    continue;
                }
            }

            match self
                .verify_staging_database(
                    partition,
                    &db_name,
                    &staging_dir,
                    cell,
                    target_lsn,
                    metadata.min_applied_lsn,
                )
                .await
            {
                Ok(()) => return Ok(Some(staging_dir)),
                Err(StagingVerifyError::Defective(err)) => {
                    remove_dir_all_strict(&staging_dir)
                        .await
                        .map_err(|cleanup| OpenError::Snapshot(anyhow::Error::new(cleanup)))?;
                    warn!(
                        snapshot_id = %snapshot_ref.snapshot_id,
                        %err,
                        "Downloaded snapshot failed verification, trying the next older one"
                    );
                    last_error = Some(err);
                }
                Err(StagingVerifyError::CloseFailed(err)) => {
                    // References to the staging database may remain; leave its directory for
                    // recovery on a later open, under the partition lock.
                    return Err(OpenError::Snapshot(err));
                }
            }
        }

        if saw_whole_db {
            Err(OpenError::Snapshot(last_error.unwrap_or_else(|| {
                anyhow::anyhow!("no whole-database snapshot could be restored")
            })))
        } else {
            Ok(None)
        }
    }

    /// Opens a downloaded checkpoint in place, checks its applied LSN, scans its checksums, and
    /// closes it again. The staging directory is never left open.
    async fn verify_staging_database(
        &self,
        partition: &Partition,
        db_name: &restate_rocksdb::DbName,
        staging_dir: &Path,
        cell: &PartitionCell,
        target_lsn: Option<Lsn>,
        metadata_min_applied_lsn: Lsn,
    ) -> Result<(), StagingVerifyError> {
        let database = self
            .open_rocksdb_at(db_name, staging_dir.to_path_buf())
            .await
            .map_err(|err| StagingVerifyError::Defective(err.into()))?;
        let verified = async {
            let Some(mut partition_db) = PartitionDb::new(
                Arc::new(partition.clone()),
                watch::Sender::new(None),
                database.clone(),
            ) else {
                return Err(anyhow::anyhow!(
                    "restored snapshot has no partition column family"
                ));
            };

            let applied_lsn = partition_db.get_applied_lsn().await?;
            if applied_lsn.unwrap_or(Lsn::INVALID) < metadata_min_applied_lsn {
                return Err(anyhow::anyhow!(
                    "restored snapshot applied LSN {} is below its metadata LSN {}",
                    applied_lsn.unwrap_or(Lsn::INVALID),
                    metadata_min_applied_lsn
                ));
            }
            if let Some(min_lsn) = target_lsn
                && applied_lsn.unwrap_or(Lsn::INVALID) < min_lsn
            {
                return Err(anyhow::anyhow!(
                    "restored snapshot applied LSN {} is below required target {}",
                    applied_lsn.unwrap_or(Lsn::INVALID),
                    min_lsn
                ));
            }

            cell.verify_restored_database(Arc::clone(&database.rocksdb))
                .await?;
            Ok(())
        }
        .await;

        let rocksdb = Arc::clone(&database.rocksdb);
        drop(database);
        let closed = rocksdb
            .close()
            .await
            .map_err(|_| anyhow::anyhow!("could not close the verified staging database"));
        match (verified, closed) {
            (Ok(()), Ok(())) => fsync_dir(staging_dir)
                .await
                .map_err(|err| StagingVerifyError::Defective(anyhow::Error::new(err))),
            // A close failure may leave references to the staging database; keep its directory
            // for recovery, whatever verification found.
            (_, Err(err)) => Err(StagingVerifyError::CloseFailed(err)),
            (Err(err), Ok(())) => Err(StagingVerifyError::Defective(err)),
        }
    }

    /// Installs a verified staging database, optionally replacing an existing database directory.
    /// Returns the directory that now holds the previous database, if any.
    async fn install_staging_database(
        &self,
        data_dir: &Path,
        staging_dir: &Path,
        old_dir: Option<&Path>,
    ) -> Result<Option<PathBuf>, OpenError> {
        let Some(parent) = data_dir.parent() else {
            return Err(OpenError::Snapshot(anyhow::anyhow!(
                "database path has no parent"
            )));
        };

        let replaced = if let Some(old_dir) = old_dir {
            let replaced = replaced_dir(old_dir)?;
            if let Err(err) = tokio::fs::rename(old_dir, &replaced).await {
                remove_dir_all_strict(staging_dir)
                    .await
                    .map_err(|cleanup| OpenError::Snapshot(anyhow::Error::new(cleanup)))?;
                return Err(OpenError::Snapshot(anyhow::anyhow!(
                    "failed to move the old database aside: {err}"
                )));
            }
            Some(replaced)
        } else {
            None
        };

        if let Err(err) = tokio::fs::rename(staging_dir, data_dir).await {
            remove_dir_all_strict(staging_dir)
                .await
                .map_err(|cleanup| OpenError::Snapshot(anyhow::Error::new(cleanup)))?;
            if let Some(replaced) = &replaced {
                let _ = tokio::fs::rename(replaced, data_dir).await;
            }
            return Err(OpenError::Snapshot(anyhow::anyhow!(
                "failed to install the restored database: {err}"
            )));
        }
        if let Err(err) = fsync_dir(parent).await {
            remove_dir_all_strict(data_dir)
                .await
                .map_err(|cleanup| OpenError::Snapshot(anyhow::Error::new(cleanup)))?;
            return Err(OpenError::Snapshot(anyhow::Error::new(err)));
        }
        Ok(replaced)
    }

    /// Recovers from an interrupted directory swap: restores a `.replaced-` directory when the
    /// final directory is missing, then removes leftover staging and replaced directories.
    ///
    /// Runs synchronously under the partition's lock, before the first await of an open, so
    /// cancellation cannot leave the recovery half done.
    fn recover_restore_dirs(&self, data_dir: &Path) -> Result<(), OpenError> {
        let Some(parent) = data_dir.parent() else {
            return Ok(());
        };
        let Some(name) = data_dir.file_name() else {
            return Ok(());
        };
        let name = name.to_string_lossy();
        let restoring_prefix = format!("{name}.restoring-");
        let replaced_prefix = format!("{name}.replaced-");

        let entries = match std::fs::read_dir(parent) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(OpenError::Snapshot(anyhow::Error::new(err))),
        };
        let mut restoring = Vec::new();
        let mut replaced = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|err| OpenError::Snapshot(anyhow::Error::new(err)))?;
            let entry_name = entry.file_name().to_string_lossy().into_owned();
            if entry_name.starts_with(&restoring_prefix) {
                restoring.push(entry.path());
            } else if entry_name.starts_with(&replaced_prefix) {
                replaced.push(entry.path());
            }
        }

        if !data_dir.exists() {
            match replaced.len() {
                0 => {}
                1 => {
                    std::fs::rename(&replaced[0], data_dir).map_err(|err| {
                        OpenError::Snapshot(anyhow::anyhow!(
                            "failed to recover replaced database directory: {err}"
                        ))
                    })?;
                    fsync_dir_sync(parent)
                        .map_err(|err| OpenError::Snapshot(anyhow::Error::new(err)))?;
                    replaced.clear();
                }
                _ => {
                    return Err(OpenError::Snapshot(anyhow::anyhow!(
                        "multiple replaced database directories and no final database: {}",
                        data_dir.display()
                    )));
                }
            }
        }

        for path in restoring.into_iter().chain(replaced) {
            if RocksDbManager::get().get_db_ref(&path).is_some() {
                warn!(
                    path = %path.display(),
                    "Skipping restore directory cleanup: the database is still tracked"
                );
                continue;
            }
            match std::fs::remove_dir_all(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(OpenError::Snapshot(anyhow::Error::new(err))),
            }
        }
        Ok(())
    }

    /// Reads the applied LSN from a database that has a partition column family but no
    /// initialized `PartitionDb`.
    async fn read_existing_applied_lsn(
        &self,
        partition: &Partition,
        database: &PartitionStoreDb,
    ) -> Result<Option<Lsn>, OpenError> {
        let Some(mut db) = PartitionDb::new(
            Arc::new(partition.clone()),
            watch::Sender::new(None),
            database.clone(),
        ) else {
            return Ok(None);
        };
        Ok(db.get_applied_lsn().await?)
    }

    /// The whole verified restore, owning the partition lock and any staging database it opens.
    /// The caller only awaits the task, so dropping the open future cannot release the lock while
    /// native staging or swap work is still running.
    async fn run_whole_db_restore(
        self: Arc<Self>,
        partition: Partition,
        cell: Arc<PartitionCell>,
        mut state_guard: OwnedRwLockWriteGuard<State>,
        mut database: Option<PartitionStoreDb>,
        target_lsn: Option<Lsn>,
        cancellation: CancellationToken,
    ) -> Result<WholeDbRestoreOutcome, OpenError> {
        let db_name = restate_rocksdb::DbName::from(partition.db_name(self.use_multi_db_layout));
        let data_dir = Configuration::pinned().worker.storage.data_dir(&db_name);
        let Some(staging) = self
            .prepare_whole_db_snapshot(&partition, target_lsn, &cancellation, &cell)
            .await?
        else {
            return Ok(WholeDbRestoreOutcome::NotNeeded(state_guard, database));
        };

        let replaces_existing = data_dir.exists();
        if replaces_existing {
            let old_database = match database.take() {
                Some(database) => database,
                None => self.open_rocksdb(&partition).await?,
            };
            cell.release_database_for_swap(&mut state_guard).await;
            let old_rocksdb = Arc::clone(&old_database.rocksdb);
            drop(old_database);
            if old_rocksdb.close().await.is_err() {
                remove_dir_all_strict(&staging)
                    .await
                    .map_err(|cleanup| OpenError::Snapshot(anyhow::Error::new(cleanup)))?;
                return Err(OpenError::Snapshot(anyhow::anyhow!(
                    "could not close the lagging database before fast-forwarding; keeping it untouched"
                )));
            }
        }

        let replaced = if replaces_existing {
            self.install_staging_database(&data_dir, &staging, Some(&data_dir))
                .await?
        } else {
            self.install_staging_database(&data_dir, &staging, None)
                .await?;
            None
        };

        let database = self.open_rocksdb(&partition).await?;
        if state_guard.is_unknown() {
            cell.open_cf(&mut state_guard, &database).await;
        }
        let Some(db) = state_guard.get_or_reopen().await? else {
            return Err(OpenError::Snapshot(anyhow::anyhow!(
                "restored database has no partition column family"
            )));
        };
        if let Some(replaced) = replaced {
            remove_dir_all_strict(&replaced)
                .await
                .map_err(|cleanup| OpenError::Snapshot(anyhow::Error::new(cleanup)))?;
        }
        Ok(WholeDbRestoreOutcome::Restored(PartitionStore::from(db)))
    }

    pub fn is_repository_configured(&self) -> bool {
        self.snapshots.is_repository_configured()
    }

    /// Number of flushes requested by snapshot exports. Test hook.
    #[cfg(test)]
    pub(crate) fn snapshot_triggered_flushes(&self) -> u64 {
        self.snapshots.snapshot_triggered_flushes()
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
    pub async fn open(
        self: &Arc<Self>,
        partition: &Partition,
        target_lsn: Option<Lsn>,
    ) -> Result<PartitionStore, OpenError> {
        self.open_cancellable(partition, target_lsn, &CancellationToken::new())
            .await
    }

    /// Like [`Self::open`], but aborts an in-progress snapshot download and returns
    /// [`OpenError::Cancelled`] once `cancellation` fires.
    #[instrument(level = "error", skip_all, fields(partition_id = %partition.partition_id, cf_name = %partition.cf_name()))]
    pub async fn open_cancellable(
        self: &Arc<Self>,
        partition: &Partition,
        target_lsn: Option<Lsn>,
        cancellation: &CancellationToken,
    ) -> Result<PartitionStore, OpenError> {
        // Serialize every restore, swap and startup recovery under the partition's lock. The
        // cell is created here and never removed from the shared map while a database is open.
        let cell = self.state.get_or_default(partition);
        let mut state_guard = cell.inner.clone().write_owned().await;

        let db_name = restate_rocksdb::DbName::from(partition.db_name(self.use_multi_db_layout));
        let data_dir = Configuration::pinned().worker.storage.data_dir(&db_name);
        self.recover_restore_dirs(&data_dir)?;
        let whole_db_restore_enabled = self.use_multi_db_layout
            && self.snapshots.is_repository_configured()
            && Configuration::pinned()
                .worker
                .storage
                .experimental_rocksdb_enable_blob_files;

        // A fresh partition can be restored straight into the final directory, but only after
        // the staging database has been fully verified. The spawned task owns the partition lock
        // and every staging handle, so dropping this future cannot release the lock early.
        if !data_dir.exists() && whole_db_restore_enabled {
            let task = TaskCenter::spawn_unmanaged(
                TaskKind::Disposable,
                format!("partition-{}-restore", partition.partition_id),
                Self::run_whole_db_restore(
                    Arc::clone(self),
                    partition.clone(),
                    Arc::clone(&cell),
                    state_guard,
                    None,
                    target_lsn,
                    cancellation.clone(),
                ),
            )
            .map_err(|err| OpenError::Snapshot(anyhow::Error::new(err)))?;
            match task
                .await
                .map_err(|err| OpenError::Snapshot(anyhow::Error::new(err)))?
            {
                Ok(WholeDbRestoreOutcome::Restored(store)) => return Ok(store),
                Ok(WholeDbRestoreOutcome::NotNeeded(guard, database)) => {
                    state_guard = guard;
                    debug_assert!(database.is_none());
                }
                Err(err) => return Err(err),
            }
        }

        let mut database = self.open_rocksdb(partition).await?;
        if state_guard.is_unknown() {
            cell.open_cf(&mut state_guard, &database).await;
        }

        // If we already have the partition locally and we don't have a fast-forward target, or the
        // store already meets the target LSN requirement, then we simply return it.
        let mut needs_restore = false;
        if let Some(db) = state_guard.get_or_reopen().await? {
            let mut partition_store = PartitionStore::from(db);
            match target_lsn {
                None => return Ok(partition_store),
                Some(min_applied_lsn) => {
                    let my_applied_lsn = partition_store.get_applied_lsn().await?;
                    if my_applied_lsn.unwrap_or(Lsn::INVALID) >= min_applied_lsn {
                        return Ok(partition_store);
                    }
                    needs_restore = true;
                }
            }
        } else if data_dir.exists() {
            let partition_cf_missing = database
                .rocksdb
                .inner()
                .cf_handle(partition.cf_name().as_ref())
                .is_none();
            if partition_cf_missing && whole_db_restore_enabled {
                // Local-store deletion drops the CF but keeps the directory. Restore a V3
                // checkpoint through the same verified staging swap.
                needs_restore = true;
            } else if let Some(min_applied_lsn) = target_lsn {
                let applied_lsn = self.read_existing_applied_lsn(partition, &database).await?;
                needs_restore = applied_lsn.unwrap_or(Lsn::INVALID) < min_applied_lsn;
            }
        }

        if needs_restore && whole_db_restore_enabled {
            let task = TaskCenter::spawn_unmanaged(
                TaskKind::Disposable,
                format!("partition-{}-restore", partition.partition_id),
                Self::run_whole_db_restore(
                    Arc::clone(self),
                    partition.clone(),
                    Arc::clone(&cell),
                    state_guard,
                    Some(database),
                    target_lsn,
                    cancellation.clone(),
                ),
            )
            .map_err(|err| OpenError::Snapshot(anyhow::Error::new(err)))?;
            match task
                .await
                .map_err(|err| OpenError::Snapshot(anyhow::Error::new(err)))?
            {
                Ok(WholeDbRestoreOutcome::Restored(store)) => return Ok(store),
                Ok(WholeDbRestoreOutcome::NotNeeded(guard, returned_database)) => {
                    state_guard = guard;
                    database = returned_database.expect("the existing database is returned");
                }
                Err(err) => return Err(err),
            }
        }

        // **
        // We either don't have an existing local partition store initialized - or we have a
        // target-lsn target higher than our local state (probably due to seeing a log trim-gap).
        // **

        // Attempt to get a suitable snapshot from the repository and restore it. A snapshot whose
        // data RocksDB rejects as corrupt is skipped in favour of an older retained one.
        let snapshot_result = {
            let mut restore = ImportSnapshot {
                cell: &cell,
                state: &mut state_guard,
                database: database.clone(),
                target_lsn,
            };
            self.snapshots
                .download_and_restore(
                    partition.partition_id,
                    target_lsn,
                    cancellation,
                    &mut restore,
                )
                .await
        };
        if snapshot_result.is_err() && cancellation.is_cancelled() {
            return Err(OpenError::Cancelled);
        }

        match (snapshot_result, target_lsn) {
            (Ok(Some(db)), _) => Ok(PartitionStore::from(db)),

            (Ok(None), None) => {
                debug!("No snapshot found for partition, creating new partition store");
                let db = cell.provision(&mut state_guard, database).await?;
                Ok(PartitionStore::from(db))
            }

            (Err(err), _) => {
                // A snapshot may well exist but we failed to fetch or import it (transient
                // repository error, or every retained candidate failed). Do NOT provision an
                // empty store and silently discard data; fail the open so the processor retries
                // later.
                Err(OpenError::Snapshot(err))
            }

            // We required a snapshot (fast-forward target) but none exists at or above the
            // target LSN.
            (Ok(None), Some(fast_forward_lsn)) => {
                let recovery_guide_msg = "The partition's log is trimmed to a point from which this processor can not resume. \
                Visit https://docs.restate.dev/server/clusters#handling-missing-snapshots \
                to learn more about how to recover this processor.";

                metrics::counter!(crate::metric_definitions::SNAPSHOT_FAST_FORWARD_FAILED)
                    .increment(1);

                if !self.snapshots.is_repository_configured() {
                    error!(
                        %fast_forward_lsn,
                        "A log trim gap was encountered, but no snapshot repository is configured! {}",
                        recovery_guide_msg,
                    );
                    Err(OpenError::SnapshotRepositoryRequired)
                } else {
                    error!(
                        %fast_forward_lsn,
                        "A log trim gap was encountered, but no suitable snapshot is available for this partition! {}",
                        recovery_guide_msg,
                    );
                    Err(OpenError::SnapshotRequired)
                }
            }
        }
    }

    /// Stops the partition from accepting new work, such as transaction commits and
    /// read snapshots, and waits for the work it already accepted to finish. That
    /// includes waiting until every read snapshot of the partition is dropped.
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
        self.clear_churn_cache(partition_id);
    }

    /// Exports a snapshot of the partition while nothing writes to it. An open partition needs
    /// `commit_fence_tx`, which reaches its running processor so that it holds back its commits.
    /// A closed partition is exported as it is. A partition that is still closing is refused.
    ///
    /// With a `new-files` mode, the checkpoint is dropped and [`ExportedSnapshot::Skipped`]
    /// returned unless enough of its SST and blob files are not in the latest published snapshot.
    #[allow(clippy::too_many_arguments)]
    pub async fn export_partition(
        &self,
        partition_id: PartitionId,
        commit_fence_tx: Option<&mpsc::Sender<CommitFenceRequest>>,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
        snapshot_base_path: &Path,
        mode: SnapshotExportMode,
    ) -> Result<ExportedSnapshot, SnapshotError> {
        let error = |kind| SnapshotError { partition_id, kind };
        // note: we don't hold the map read lock while trying to acquire the partition cell's lock.
        // hence the `cloned()` call.
        let cell = self
            .state
            .partitions
            .read()
            .get(&partition_id)
            .cloned()
            .ok_or_else(|| error(SnapshotErrorKind::PartitionNotFound))?;
        // Wait for a slot before taking the partition's lock, so that waiting for other exports
        // doesn't keep this partition's processor from stopping or starting.
        let permit = self.snapshots.export_slot().await;
        // The export holds this lock until it finishes, so that the partition can't be closed,
        // reopened or deleted under it, and no new processor starts writing to it.
        let cell_guard = cell.inner.clone().read_owned().await;
        let (db, commit_fence_tx) = match &*cell_guard {
            State::Open { db } => (
                db.clone(),
                Some(commit_fence_tx.ok_or_else(|| error(SnapshotErrorKind::InvalidState))?),
            ),
            State::Closed { db, .. } => (db.clone(), None),
            // Writes the partition accepted before closing may still be running.
            State::Closing { .. } => return Err(error(SnapshotErrorKind::InvalidState)),
            _ => return Err(error(SnapshotErrorKind::PartitionNotFound)),
        };

        let exported = self
            .snapshots
            .create_local_snapshot(
                PartitionStore::from(db),
                permit,
                cell_guard,
                commit_fence_tx,
                min_target_lsn,
                snapshot_id,
                snapshot_base_path,
                mode,
            )
            .await?;

        if !mode.is_new_files() {
            return Ok(ExportedSnapshot::Snapshot(
                exported.expect("a flushing export is never skipped"),
            ));
        }

        let Some(snapshot) = exported else {
            metrics::counter!(crate::metric_definitions::SNAPSHOT_ROUNDS_SKIPPED).increment(1);
            self.clear_churn_cache(partition_id);
            return Ok(ExportedSnapshot::Skipped);
        };

        let latest = self
            .snapshots
            .get_latest_snapshot_metadata(partition_id)
            .await
            .map_err(|err| error(SnapshotErrorKind::RepositoryIo(err)))?;
        let referenced: HashSet<String> = latest
            .as_ref()
            .map(|metadata| metadata.file_keys.values().cloned().collect())
            .unwrap_or_default();
        let churn = self
            .churn_since_latest(partition_id, &snapshot, &referenced)
            .await
            .map_err(|err| error(SnapshotErrorKind::Export(err)))?;
        self.retain_churn_cache(partition_id, &snapshot);
        let threshold = Configuration::pinned()
            .worker
            .snapshots
            .snapshot_min_new_bytes();

        debug!(
            %partition_id,
            churn = %ByteCount::from(churn),
            threshold = %ByteCount::from(threshold),
            ?mode,
            "Computed snapshot churn since latest snapshot"
        );

        if mode == SnapshotExportMode::NewFiles && churn < threshold {
            metrics::counter!(crate::metric_definitions::SNAPSHOT_ROUNDS_SKIPPED).increment(1);
            drop(snapshot);
            return Ok(ExportedSnapshot::Skipped);
        }

        Ok(ExportedSnapshot::Snapshot(snapshot))
    }

    /// Sums the sizes of the checkpoint's SST and blob files whose content key is not referenced
    /// by the latest published snapshot.
    async fn churn_since_latest(
        &self,
        partition_id: PartitionId,
        snapshot: &LocalPartitionSnapshot,
        referenced: &HashSet<String>,
    ) -> anyhow::Result<u64> {
        let mut entries = Vec::with_capacity(
            snapshot.files.len()
                + snapshot
                    .whole_db_files
                    .iter()
                    .filter(|file| file.name.ends_with(".blob"))
                    .count(),
        );
        for file in &snapshot.files {
            entries.push((
                file.name.trim_start_matches('/').to_owned(),
                file.size as u64,
                "sst",
            ));
        }
        for file in &snapshot.whole_db_files {
            if file.name.ends_with(".blob") {
                entries.push((file.name.clone(), file.size as u64, "blob"));
            }
        }

        let mut churn = 0;
        for (name, size, extension) in entries {
            let key = self
                .cached_content_key(
                    partition_id,
                    snapshot.base_dir.path(),
                    name,
                    size,
                    extension,
                )
                .await?;
            if !referenced.contains(&key) {
                churn += size;
            }
        }
        Ok(churn)
    }

    async fn cached_content_key(
        &self,
        partition_id: PartitionId,
        base_dir: &Path,
        name: String,
        size: u64,
        extension: &'static str,
    ) -> anyhow::Result<String> {
        {
            let cache = self.churn_cache.lock();
            if let Some(key) = cache
                .get(&partition_id)
                .and_then(|entry| entry.files.get(&(name.clone(), size)))
            {
                return Ok(key.clone());
            }
        }

        let fingerprint =
            crate::snapshots::sst_fingerprint::sst_fingerprint(&base_dir.join(&name)).await?;
        let key = format!("ssts/{fingerprint}.{extension}");
        self.churn_cache
            .lock()
            .entry(partition_id)
            .or_default()
            .files
            .insert((name, size), key.clone());
        Ok(key)
    }

    fn clear_churn_cache(&self, partition_id: PartitionId) {
        self.churn_cache.lock().remove(&partition_id);
    }

    /// Drops content keys of files that the checkpoint no longer has, so a long-lived partition
    /// does not keep an entry per SST or blob file it ever wrote.
    fn retain_churn_cache(&self, partition_id: PartitionId, snapshot: &LocalPartitionSnapshot) {
        let live_files: HashSet<String> = snapshot
            .files
            .iter()
            .map(|file| file.name.trim_start_matches('/').to_owned())
            .chain(
                snapshot
                    .whole_db_files
                    .iter()
                    .filter(|file| file.name.ends_with(".blob"))
                    .map(|file| file.name.clone()),
            )
            .collect();
        let mut cache = self.churn_cache.lock();
        let Some(entry) = cache.get_mut(&partition_id) else {
            return;
        };
        entry.files.retain(|(name, _), _| live_files.contains(name));
        if entry.files.is_empty() {
            cache.remove(&partition_id);
        }
    }

    /// The content keys currently cached for the partition. Test hook.
    #[cfg(test)]
    pub(crate) fn churn_cache_file_names(&self, partition_id: PartitionId) -> Vec<String> {
        self.churn_cache
            .lock()
            .get(&partition_id)
            .map(|entry| entry.files.keys().map(|(name, _)| name.clone()).collect())
            .unwrap_or_default()
    }

    /// A capture guard for an open partition that the test doesn't write to while exporting it.
    #[cfg(test)]
    pub(crate) async fn test_capture_guard(
        &self,
        partition_id: PartitionId,
    ) -> crate::snapshots::CaptureGuard {
        let cell = self.state.get(partition_id).expect("partition is known");
        let permit = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("new semaphore has a permit");
        crate::snapshots::CaptureGuard::new(permit, cell.inner.clone().read_owned().await, None)
    }

    /// Deletes this node's local copy of the partition, discarding all of its data.
    /// Returns what was deleted, or `None` if there was nothing to delete. It also finds
    /// a column family that this process hasn't opened yet.
    ///
    /// The partition stops accepting work right away, and its column family is deleted
    /// once nothing uses it anymore. So this waits until every [`PartitionStore`] and
    /// read snapshot of the partition is dropped: stop its partition processor first,
    /// and the queries reading from it. Opening the partition
    /// meanwhile waits for the deletion. Once the partition has stopped accepting work,
    /// the deletion goes ahead even if this call is cancelled. If an earlier deletion
    /// failed, this retries it.
    #[instrument(level = "error", skip_all, fields(partition_id = %partition.partition_id, cf_name = %partition.cf_name()))]
    pub async fn drop_partition_store(
        &self,
        partition: &Partition,
    ) -> Result<Option<DroppedPartitionStore>, RocksError> {
        let dropped = async {
            let cell = self.state.get_or_default(partition);
            let mut state_guard = cell.inner.write().await;
            let database = self.open_rocksdb(partition).await?;
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
        self.clear_churn_cache(partition.partition_id);

        match dropped {
            Some(DroppedPartitionStore { disk_usage }) => info!(
                disk_usage = disk_usage.map(|bytes| field::display(ByteCount::from(bytes))),
                "Dropped local partition store"
            ),
            None => debug!("No local partition store to drop"),
        }
        Ok(dropped)
    }

    /// Reports what [`Self::drop_partition_store`] would delete right now, without changing
    /// anything: this node's local copy of the partition with its disk usage, or `None` if
    /// there is no local copy.
    #[instrument(level = "error", skip_all, fields(partition_id = %partition.partition_id, cf_name = %partition.cf_name()))]
    pub async fn preview_drop_partition_store(
        &self,
        partition: &Partition,
    ) -> Result<Option<DroppedPartitionStore>, RocksError> {
        let cell = self.state.get_or_default(partition);
        let _state_guard = cell.inner.write().await;
        let database = self.open_rocksdb(partition).await?;
        // Measure through a handle of our own, so that the partition stays as it is.
        let Some(db) = PartitionDb::new(
            Arc::new(partition.clone()),
            watch::Sender::new(None),
            database,
        ) else {
            return Ok(None);
        };
        let disk_usage = db
            .disk_usage()
            .await
            .inspect_err(|error| warn!(%error, "Could not measure the partition store"))
            .ok();
        Ok(Some(DroppedPartitionStore { disk_usage }))
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
        if snapshot.version == SnapshotFormatVersion::V3 {
            return Err(RocksError::UnsupportedSnapshotFormat(
                "whole-database checkpoint snapshots (V3) cannot be restored by importing a column family",
            ));
        }
        let cell = self.state.get_or_default(partition);
        let mut state_guard = cell.inner.write().await;
        let database = self.open_rocksdb(partition).await?;
        let db = cell.import_cf(&mut state_guard, snapshot, database).await?;
        Ok(PartitionStore::from(db))
    }

    #[cfg(test)]
    pub(crate) async fn forget_cached_database(&self, db_name: &restate_rocksdb::DbName) {
        self.db_cache.lock().await.remove(db_name);
    }

    #[cfg(test)]
    pub async fn close_partition_store(&self, partition_id: PartitionId) {
        let Some(cell) = self.state.partitions.read().get(&partition_id).cloned() else {
            return;
        };
        let mut state_guard = cell.inner.write().await;
        let db = state_guard.db().cloned();
        cell.reset_to_unknown(&mut state_guard).await;
        self.clear_churn_cache(partition_id);
        // making sure the rocksdb database is fully closed.
        if let Some(db) = db {
            db.into_rocksdb()
                .close()
                .await
                .expect("rocksdb cannot be closed if others are still holding references to it");
        }
    }
}

fn replaced_dir(data_dir: &Path) -> Result<PathBuf, OpenError> {
    let name = data_dir
        .file_name()
        .ok_or_else(|| OpenError::Snapshot(anyhow::anyhow!("database path has no name")))?
        .to_string_lossy();
    Ok(data_dir.with_file_name(format!("{name}.replaced-{:016x}", rand::random::<u64>())))
}

fn restoring_dir(data_dir: &Path) -> Result<PathBuf, OpenError> {
    let name = data_dir
        .file_name()
        .ok_or_else(|| OpenError::Snapshot(anyhow::anyhow!("database path has no name")))?
        .to_string_lossy();
    Ok(data_dir.with_file_name(format!("{name}.restoring-{:016x}", rand::random::<u64>())))
}

async fn remove_dir_all_strict(path: &Path) -> std::io::Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

async fn fsync_dir(path: &Path) -> std::io::Result<()> {
    let dir = tokio::fs::File::open(path).await?;
    dir.sync_all().await
}

fn fsync_dir_sync(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
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
    use tokio::sync::{Semaphore, oneshot};
    use tokio::time::timeout;

    use restate_core::io::IoExecutor;
    use restate_rocksdb::{Priority, RocksDbManager, RocksError};
    use restate_storage_api::fsm_table::{ReadFsmTable, WriteFsmTable};
    use restate_storage_api::{StorageError, Transaction};
    use restate_types::config::Configuration;
    use restate_types::identifiers::{PartitionId, SnapshotId};

    use crate::snapshots::{ExportedSnapshot, SnapshotExportMode};
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;

    use super::PartitionStoreManager;
    use crate::faults::FaultPoint;
    use crate::keys::KeyKind;
    use crate::partition_db::{PartitionCell, State};
    use crate::snapshots::{CaptureGuard, CommitFenceRequest};
    use crate::{
        DroppedPartitionStore, PartitionStore, PartitionStoreTransaction, SnapshotErrorKind,
    };

    async fn write_applied_lsn(store: &mut PartitionStore, lsn: u64) -> anyhow::Result<()> {
        let mut tx = store.transaction();
        tx.put_applied_lsn(Lsn::new(lsn))?;
        tx.commit().await?;
        Ok(())
    }

    #[restate_core::test]
    async fn disk_usage_matches_live_files_also_when_closed() -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        let rocksdb = RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let sibling = Partition::new(PartitionId::from(1), KeyRange::FULL);
        for layout in [false, true] {
            let manager = PartitionStoreManager::create(layout).await?;
            let mut store = manager.open(&partition, None).await?;
            let mut neighbor = manager.open(&sibling, None).await?;
            let db = store.partition_db().clone();
            assert_eq!(db.disk_usage().await?, 0, "nothing flushed yet");

            // A single flush stays below the L0 compaction trigger: a background compaction
            // would change the files between the measurements below.
            write_applied_lsn(&mut store, 10).await?;
            write_applied_lsn(&mut neighbor, 10).await?;
            db.flush_memtables(true).await?;
            neighbor.partition_db().flush_memtables(true).await?;
            let live_files_size = db
                .rocksdb()
                .inner()
                .as_raw_db()
                .live_files()?
                .into_iter()
                .filter(|file| file.column_family_name == partition.cf_name().as_ref())
                .map(|file| file.size as u64)
                .sum::<u64>();
            assert!(live_files_size > 0);
            assert_eq!(
                db.disk_usage().await?,
                live_files_size,
                "counts the partition's own files only"
            );

            drop(store);
            timeout(DEADLINE, manager.close(partition.partition_id)).await?;
            assert_eq!(db.disk_usage().await?, live_files_size);

            drop((db, neighbor, manager));
            rocksdb.reset().await?;
        }
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
            let read = store.partition_db().prepare_read(
                "retirement-read",
                restate_rocksdb::StorageTaskKind::MultiGet,
                move |_, _| {
                    let _ = started.send(());
                    let _ = paused.recv();
                },
            );
            // Submitting is eager: the read starts without polling the result.
            let read = store
                .partition_db()
                .rocksdb()
                .executor()
                .high_pri()
                .spawn(read)?;
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
            timeout(DEADLINE, store.partition_db().gate().drain()).await?;
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
            let preview = manager.preview_drop_partition_store(&partition).await?;
            let dropped = manager
                .drop_partition_store(&partition)
                .await?
                .expect("finds a column family this process never opened");
            assert!(
                dropped.disk_usage.is_some_and(|size| size > 0),
                "measures a store this process never opened"
            );
            assert_eq!(
                preview,
                Some(dropped),
                "a preview reports what the drop deletes, and leaves it in place"
            );
            assert!(manager.drop_partition_store(&partition).await?.is_none());
            assert!(
                manager
                    .preview_drop_partition_store(&partition)
                    .await?
                    .is_none()
            );
            let mut neighbor = manager.open(&sibling, None).await?;
            assert_eq!(neighbor.get_applied_lsn().await?, Some(Lsn::new(20)));

            let database = manager.open_rocksdb(&partition).await?;
            database
                .rocksdb
                .inner()
                .open_cf(partition.cf_name().into())?;
            assert_eq!(
                manager.drop_partition_store(&partition).await?,
                Some(DroppedPartitionStore { disk_usage: None }),
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
                assert!(
                    timeout(DEADLINE, manager.drop_partition_store(&partition))
                        .await??
                        .is_some()
                );
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
            assert!(manager.drop_partition_store(&partition).await?.is_some());
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
            assert!(timeout(DEADLINE, dropping).await??.is_some());
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
                    db.gate().clone(),
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
            assert!(!Arc::ptr_eq(&admission, adopted.partition_db().gate()));
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
        let snapshots = restate_test_util::tempdir()?;
        for layout in [false, true] {
            let manager = PartitionStoreManager::create(layout).await?;
            let database = manager.open_rocksdb(&partition).await?;
            for invalid in [false, true] {
                let mut store = manager.open(&partition, None).await?;
                write_applied_lsn(&mut store, 50).await?;
                let mut snapshot = store
                    .create_local_snapshot(
                        manager.test_capture_guard(partition.partition_id).await,
                        snapshots.path(),
                        None,
                        SnapshotId::new(),
                    )
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
            let admission = old.partition_db().gate().clone();
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
                assert!(Arc::ptr_eq(state.db().unwrap().gate(), &admission));
                *closing_at
            };
            assert!(admission.try_acquire().is_none());
            assert!(
                manager
                    .get_partition_store(partition.partition_id)
                    .await
                    .is_none()
            );
            assert!(sibling_store.partition_db().gate().try_acquire().is_some());

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
            assert!(!Arc::ptr_eq(&admission, reopened.partition_db().gate()));
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
            let snapshots = restate_test_util::tempdir()?;
            let snapshot = store
                .create_local_snapshot(
                    manager.test_capture_guard(partition.partition_id).await,
                    snapshots.path(),
                    None,
                    SnapshotId::new(),
                )
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
        let snapshots = restate_test_util::tempdir()?;

        for use_multi_db_layout in [false, true] {
            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let mut store = manager.open(&partition, None).await?;
            let database = store.partition_db().rocksdb().clone();
            let old_watch = store.get_durable_lsn().await?;
            write_applied_lsn(&mut store, 100).await?;
            let mut snapshot = store
                .create_local_snapshot(
                    manager.test_capture_guard(partition.partition_id).await,
                    snapshots.path(),
                    None,
                    SnapshotId::new(),
                )
                .await?;
            snapshot.key_range = KeyRange::new(1, u64::MAX);
            assert!(matches!(
                manager.open_from_snapshot(&partition, snapshot).await,
                Err(restate_rocksdb::RocksError::SnapshotKeyRangeMismatch)
            ));
            assert!(old_watch.same_channel(&store.get_durable_lsn().await?));
            write_applied_lsn(&mut store, 200).await?;
            let mut snapshot = store
                .create_local_snapshot(
                    manager.test_capture_guard(partition.partition_id).await,
                    snapshots.path(),
                    None,
                    SnapshotId::new(),
                )
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

    /// An open partition is exported only while its processor holds back commits, and without a
    /// processor to ask, the export is refused. A closed partition is exported as it is. An
    /// export that waits for a free export slot doesn't keep the partition from closing.
    #[restate_core::test]
    async fn export_fences_open_partitions_and_captures_closed_ones() -> anyhow::Result<()> {
        RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        for use_multi_db_layout in [false, true] {
            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let mut store = manager.open(&partition, None).await?;
            write_applied_lsn(&mut store, 100).await?;
            let snapshots = restate_test_util::tempdir()?;
            let export = |commit_fence_tx| {
                manager.export_partition(
                    partition.partition_id,
                    commit_fence_tx,
                    None,
                    SnapshotId::new(),
                    snapshots.path(),
                    SnapshotExportMode::Flush,
                )
            };

            let (commit_fence_tx, mut commit_fence_rx) =
                tokio::sync::mpsc::channel::<CommitFenceRequest>(1);
            let mut txn = store.transaction();
            let processor = async {
                let request = commit_fence_rx
                    .recv()
                    .await
                    .expect("export asks for a fence");
                let _ = request.send(txn.fence_commits());
            };
            let (snapshot, ()) = tokio::join!(export(Some(&commit_fence_tx)), processor);
            let ExportedSnapshot::Snapshot(snapshot) = snapshot.unwrap() else {
                panic!("a flushing export is never skipped");
            };
            assert_eq!(snapshot.min_applied_lsn, Lsn::new(100));

            let refused = export(None).await.unwrap_err();
            assert!(matches!(refused.kind, SnapshotErrorKind::InvalidState));

            // With every export slot taken, the export waits without the partition's lock, so
            // the partition closes. Once it gets a slot, it exports the closed partition.
            let limit = Configuration::pinned()
                .worker
                .snapshots
                .export_concurrency_limit();
            let mut slots = Vec::new();
            for _ in 0..limit {
                slots.push(manager.snapshots.export_slot().await);
            }
            let mut queued = std::pin::pin!(export(None));
            assert!(
                queued
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            let cell = manager.state.get(partition.partition_id).unwrap();
            assert!(cell.inner.try_write().is_ok());
            manager.close(partition.partition_id).await;
            drop(slots);
            let ExportedSnapshot::Snapshot(queued) = queued.await.unwrap() else {
                panic!("a flushing export is never skipped");
            };
            assert_eq!(queued.min_applied_lsn, Lsn::new(100));
        }
        Ok(())
    }

    /// Whoever asked for a snapshot may stop waiting while its export runs or is still queued.
    /// The export keeps holding back the processor's commits, the partition's lock and its export
    /// slot until RocksDB is done, or until the worker discards the queued export. An abandoned
    /// export leaves no files behind.
    #[restate_core::test]
    async fn abandoned_export_keeps_its_capture_until_done() -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        let rocksdb = RocksDbManager::init();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        for use_multi_db_layout in [false, true] {
            let manager = PartitionStoreManager::create(use_multi_db_layout).await?;
            let mut store = manager.open(&partition, None).await?;
            write_applied_lsn(&mut store, 100).await?;
            let cell = manager.state.get(partition.partition_id).unwrap();
            let slots = Arc::new(Semaphore::new(1));
            let mut writer = store.clone();
            let mut txn = writer.transaction();
            let snapshots = restate_test_util::tempdir()?;

            // Abandoned while the worker runs it: pause right before RocksDB starts exporting.
            // The probe reports whether RocksDB had written the export when the guard is let go.
            struct ReleaseProbe(std::path::PathBuf, mpsc::Sender<bool>);
            impl Drop for ReleaseProbe {
                fn drop(&mut self) {
                    let _ = self.1.send(self.0.exists());
                }
            }
            let capture = CaptureGuard::new(
                slots.clone().acquire_owned().await?,
                cell.inner.clone().read_owned().await,
                txn.fence_commits(),
            );
            let export_dir = snapshots.path().join("running");
            let (started, reached) = oneshot::channel();
            let (resume, paused) = mpsc::channel::<()>();
            let (exported_tx, exported) = mpsc::channel();
            let probe = ReleaseProbe(export_dir.clone(), exported_tx);
            let export = store
                .partition_db()
                .prepare_export_cf(export_dir.clone(), move || {
                    let _ = started.send(());
                    let _ = paused.recv();
                    (probe, capture)
                });
            let export = rocksdb.lifecycle_executor().spawn(export)?;
            timeout(DEADLINE, reached).await??;
            drop(export);
            txn.put_applied_lsn(Lsn::new(200))?;
            assert_capture_held(&mut txn, &cell, &slots).await;
            drop(resume);
            assert_capture_released(&mut txn, &cell, &slots).await?;
            assert!(exported.recv_timeout(DEADLINE)?);
            timeout(DEADLINE, async {
                while export_dir.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;

            // Abandoned while it waits for a worker, which then discards it.
            let capture = CaptureGuard::new(
                slots.clone().acquire_owned().await?,
                cell.inner.clone().read_owned().await,
                txn.fence_commits(),
            );
            let paused = rocksdb.pause_lifecycle().await;
            let base = snapshots.path().join("queued");
            let mut export =
                Box::pin(store.create_local_snapshot(capture, &base, None, SnapshotId::new()));
            // The export creates its base directory right before it queues the capture, and the
            // poll after that queues it.
            while !base.exists() {
                assert!(
                    timeout(Duration::from_millis(10), &mut export)
                        .await
                        .is_err()
                );
            }
            assert!(
                timeout(Duration::from_millis(100), &mut export)
                    .await
                    .is_err()
            );
            drop(export);
            txn.put_applied_lsn(Lsn::new(300))?;
            assert_capture_held(&mut txn, &cell, &slots).await;
            drop(paused);
            assert_capture_released(&mut txn, &cell, &slots).await?;
        }
        Ok(())
    }

    /// Asserts that a snapshot capture still holds back `txn`'s commits, the partition's lock and
    /// the export slot.
    async fn assert_capture_held(
        txn: &mut PartitionStoreTransaction<'_>,
        cell: &PartitionCell,
        slots: &Semaphore,
    ) {
        assert!(
            timeout(Duration::from_millis(100), txn.commit())
                .await
                .is_err()
        );
        assert!(cell.inner.try_write().is_err());
        assert_eq!(slots.available_permits(), 0);
    }

    /// Waits for a snapshot capture to let go of `txn`'s commits, the partition's lock and the
    /// export slot.
    async fn assert_capture_released(
        txn: &mut PartitionStoreTransaction<'_>,
        cell: &PartitionCell,
        slots: &Semaphore,
    ) -> anyhow::Result<()> {
        const DEADLINE: Duration = Duration::from_secs(10);
        timeout(DEADLINE, txn.commit()).await??;
        drop(timeout(DEADLINE, cell.inner.write()).await?);
        drop(timeout(DEADLINE, slots.acquire()).await??);
        Ok(())
    }
}
