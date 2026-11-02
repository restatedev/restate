// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use rocksdb::table_properties::TablePropertiesExt;
use rocksdb::{
    AsColumnFamilyRef, BoundColumnFamily, DBRawIteratorWithThreadMode, ExportImportFilesMetaData,
    ReadOptions,
};
use tokio::sync::{RwLock as AsyncRwLock, watch};
use tokio::time::Instant;
use tracing::{debug, error, info, instrument, warn};

use restate_core::ShutdownError;
use restate_core::io::{AdmissionClosed, Gate, IoExecutor};
use restate_rocksdb::configuration::{CfConfigurator, DbConfigurator};
use restate_rocksdb::reads::ColumnFamilies;
use restate_rocksdb::{
    CfHandles, DbName, Priority, RawRocksDb, RocksAccess, RocksDb, RocksError, SinglePoolExecutor,
    StorageTaskKind,
};
use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::partitions::{CfName, Partition};
use restate_util_bytecount::ByteCount;
use restate_util_lifecycle::{KeepAlive, Scope, ScopePayload};
use restate_util_string::{ReString, ToReString};
use restate_util_time::DurationExt;

use crate::durable_lsn_tracking::{
    AppliedLsnCollectorFactory, DurableLsnEventListener, DurableLsnSenders,
};
use crate::faults::{self, FaultPoint, Faults};
use crate::keys::KeyKind;
use crate::memory::{MemoryBudget, PartitionDbMemoryConfig};
use crate::scan::PhysicalScan;
use crate::snapshots::{LocalPartitionSnapshot, SnapshotDir};
use crate::{PartitionReadSnapshot, TableKind};

/// A physical database and the notification registry installed in its listener.
/// Keep these together when reusing a database from the manager's weak cache.
#[derive(Clone)]
pub(crate) struct PartitionStoreDb {
    pub rocksdb: Arc<RocksDb>,
    pub notifications: Arc<DurableLsnSenders>,
}

/// An open partition, through which its data is read and written.
///
/// A partition's storage has two parts, which live for different lengths of
/// time:
///
/// ```text
/// database              the RocksDB database the partition lives in
///  └─ physical partition    the partition's column families
///      └─ session               one opening of the partition: a PartitionDb
/// ```
///
/// - The **physical partition** holds the partition's column families, and the
///   state that lives as long as they do, such as the partition's durable LSN.
///   It is created when the column families are opened, and stays the same
///   while the partition is closed and opened again. Once nothing holds it
///   anymore, it is released. Deleting the column families waits for that.
/// - A **session** is one opening of the partition. It has its own gate, which
///   lets work in while the partition is open. Closing the partition seals that
///   gate and waits for the work that was let in.
///
/// A sealed gate can't be opened again, so opening the partition again starts
/// a new session on the same physical partition. That's why the two are
/// separate: closing ends a session for good, but the column families have to
/// survive it, so that the partition can be opened again, flushed, exported, or
/// deleted.
///
/// Each session keeps its physical partition alive, and the physical partition
/// keeps the database alive. Shutting down the database seals every session
/// inside it.
///
/// Clones are cheap and share the same session. A clone kept from before the
/// partition was closed stays closed, even after the partition is opened again.
#[derive(Clone)]
pub struct PartitionDb(Session);

/// One opening of a partition: a scope whose payload is the scope of its
/// physical partition. See [`PartitionDb`].
type Session = Scope<Scope<PhysicalPartition>>;

/// A partition's column families, and the state that lives as long as they do.
/// See [`PartitionDb`] for how it relates to the partition's sessions.
///
/// Public only because it appears in the types of a snapshot's owned readers
/// and scans (see [`crate::PartitionSnapshotReader`]). It can't be named outside
/// this crate.
pub struct PhysicalPartition {
    meta: Arc<Partition>,
    durable_lsn: watch::Sender<Lsn>,
    archived_lsn: watch::Sender<Option<Lsn>>,
    notifications: Arc<DurableLsnSenders>,
    // Looked up once, when the partition is first opened, and shared by every
    // session. Last, because it holds the keep-alive of the database.
    cfs: CfHandles<KeyKind>,
}

impl PhysicalPartition {
    pub(crate) fn partition(&self) -> &Arc<Partition> {
        &self.meta
    }
}

impl ScopePayload for PhysicalPartition {
    type Parent = RocksDb;

    fn parent(&self) -> &KeepAlive<RocksDb> {
        self.cfs.database()
    }
}

// SAFETY: `cfs` is a field that is never replaced.
unsafe impl ColumnFamilies for PhysicalPartition {
    type Cf = KeyKind;

    fn handles(&self) -> &CfHandles<KeyKind> {
        &self.cfs
    }
}

impl PartitionDb {
    pub(crate) fn new(
        meta: Arc<Partition>,
        archived_lsn: watch::Sender<Option<Lsn>>,
        database: PartitionStoreDb,
    ) -> Option<Self> {
        // For now, every key kind lives in the partition's only column family.
        let cfs = CfHandles::new(&database.rocksdb, [meta.cf_name()], |_| 0).ok()?;
        let physical = Scope::new(
            meta.cf_name().into_inner(),
            PhysicalPartition {
                meta,
                durable_lsn: watch::Sender::new(Lsn::INVALID),
                archived_lsn,
                notifications: database.notifications,
                cfs,
            },
        );
        Some(Self::open(physical))
    }

    // Starts a new session on the partition's column families.
    fn open(physical: Scope<PhysicalPartition>) -> Self {
        let name = format!("partition {}", physical.payload().meta.partition_id);
        Self(Scope::new(name, physical))
    }

    pub(crate) fn physical_scope(&self) -> &Scope<PhysicalPartition> {
        self.0.payload()
    }

    fn physical(&self) -> &PhysicalPartition {
        self.physical_scope().payload()
    }

    pub fn partition(&self) -> &Arc<Partition> {
        &self.physical().meta
    }

    pub fn rocksdb(&self) -> &Arc<RocksDb> {
        self.physical().cfs.database()
    }

    /// The gate of this session. Closing the partition seals and drains it.
    pub(crate) fn gate(&self) -> &Arc<Gate> {
        self.0.gate()
    }

    /// Takes a read snapshot of this partition, on the calling thread. Reads through
    /// it see all of the partition's data as it was at this moment.
    ///
    /// The snapshot keeps the partition's column families alive and holds a lease
    /// from the partition's session. Closing the partition or deleting its column
    /// family therefore waits until the snapshot, and everything created from it,
    /// has been dropped.
    ///
    /// Fails once the partition is closing or closed. That includes a handle kept
    /// from before the partition was reopened, which stays closed.
    pub fn read_snapshot(&self) -> Result<PartitionReadSnapshot, AdmissionClosed> {
        PartitionReadSnapshot::new(self)
    }

    #[cfg(test)]
    pub fn into_rocksdb(self) -> Arc<RocksDb> {
        self.rocksdb().clone()
    }

    pub fn cf_handle(&self) -> &Arc<BoundColumnFamily<'_>> {
        self.cf_handle_for_key_kind(KeyKind::Fsm)
    }

    pub(crate) fn table_cf_handle(&self, _table_kind: TableKind) -> &Arc<BoundColumnFamily<'_>> {
        self.cf_handle()
    }

    pub(crate) fn cf_handle_for_key_kind(&self, key_kind: KeyKind) -> &Arc<BoundColumnFamily<'_>> {
        self.physical().cfs.get(key_kind)
    }

    pub fn cf_names(&self) -> Vec<ReString> {
        vec![self.partition().cf_name().into_inner()]
    }

    pub async fn flush_memtables(&self, wait: bool) -> Result<(), RocksError> {
        let cf_name = self.partition().cf_name().into();
        self.run_background_op(StorageTaskKind::FlushMemtables, move |db| {
            db.flush_memtables(std::slice::from_ref(&cf_name), wait)
        })
        .await?
    }

    /// Returns a future that completes once nothing holds this partition's column
    /// family handle anymore: all PartitionDb clones, and the background work that kept
    /// one, are gone. It doesn't mean the column family was deleted; that result is in
    /// [`State::Retiring`]. The future itself keeps nothing alive.
    pub(crate) fn released(&self) -> impl Future<Output = ()> + Send + 'static + use<> {
        self.physical_scope().released()
    }

    /// Exports the column family into `export_dir`, which RocksDB creates. Returns a
    /// guard that deletes the directory when dropped, along with the export's metadata.
    pub(crate) async fn export_cf(
        &self,
        export_dir: std::path::PathBuf,
    ) -> Result<(SnapshotDir, ExportImportFilesMetaData), RocksError> {
        let name = self.partition().cf_name().into();
        self.run_background_op(StorageTaskKind::ExportColumnFamily, move |db| {
            let metadata = db.export_cf(name, export_dir.clone())?;
            // Take ownership of the files here, on the worker. If the caller went
            // away while the export was running, nobody claims this result, and
            // dropping it deletes the files instead of leaving them behind.
            Ok((SnapshotDir::new(export_dir), metadata))
        })
        .await?
    }

    /// Runs `op` on the partition's column families on a background thread, at low
    /// priority. For work that isn't a read, such as flushing or exporting.
    ///
    /// The work isn't part of the partition's session. Closing the partition ends
    /// the session, but this work may keep running afterwards, and it may also
    /// start after the session has ended, for example to export a snapshot of a
    /// closed partition. So it counts against the database's gate instead of the
    /// session's: closing the partition doesn't wait for it, but shutting down the
    /// database does.
    ///
    /// It keeps the partition's column families alive until it is done, so
    /// deleting them waits for it.
    async fn run_background_op<R: Send + 'static>(
        &self,
        kind: StorageTaskKind,
        op: impl FnOnce(&RocksAccess) -> R + Send + 'static,
    ) -> Result<R, RocksError> {
        let physical = self.physical_scope().clone();
        let task = self.rocksdb().prepare(kind, move |db| {
            let result = op(db.inner());
            drop(physical);
            result
        });
        self.rocksdb()
            .executor()
            .low_pri()
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError.into())
    }

    /// Runs a read on a background thread, as part of the partition's session.
    /// `op` gets the database and the partition's column families, which stay
    /// alive until it is done.
    pub(crate) async fn run_background_read_op<OP, R>(
        &self,
        name: &'static str,
        kind: StorageTaskKind,
        priority: Priority,
        op: OP,
    ) -> Result<R, RocksError>
    where
        OP: FnOnce(&RawRocksDb, &CfHandles<KeyKind>) -> R + Send + 'static,
        R: Send + 'static,
    {
        let physical = self.physical_scope().clone();
        self.rocksdb()
            .run_background_read_op_in(self.gate(), name, kind, priority, move |db| {
                op(db, &physical.payload().cfs)
            })
            .await
    }

    /// The last (locally) known archived LSN for this partition
    pub fn get_archived_lsn(&self) -> Option<Lsn> {
        *self.physical().archived_lsn.borrow()
    }

    pub fn watch_archived_lsn(&self) -> watch::Receiver<Option<Lsn>> {
        self.physical().archived_lsn.subscribe()
    }

    pub(crate) fn durable_lsn_sender(&self) -> &watch::Sender<Lsn> {
        &self.physical().durable_lsn
    }

    pub(crate) fn note_durable_lsn(&self, lsn: Lsn) {
        self.physical().notifications.note_durable_lsn(
            self.partition().partition_id,
            self.cf_handle_for_key_kind(KeyKind::Fsm).id(),
            lsn,
        );
    }

    fn unregister_durable_lsn(&self) {
        self.physical().notifications.unregister(
            self.partition().partition_id,
            self.cf_handle_for_key_kind(KeyKind::Fsm).id(),
        );
    }

    fn register_durable_lsn(&self) {
        self.physical().notifications.register(
            self.partition().partition_id,
            self.cf_handle_for_key_kind(KeyKind::Fsm).id(),
            self.durable_lsn_sender().clone(),
        );
    }

    pub(crate) async fn update_memory_budget(&self, memory_budget: usize) {
        // impacts only this partition's column-families
        let cf_names = self.cf_names();
        let physical = self.physical_scope().clone();

        // NOTE: Updating rocksdb's options is a blocking operations and may take a few
        // tens of milliseconds on each column family. We need to perform this operation
        // on non-blocking threads to avoid stalling the tokio runtime (starving failure
        // detector, etc.).
        let _ = tokio::task::spawn_blocking(move || {
            let rocksdb = physical.payload().cfs.database();
            let opts = &Configuration::pinned().worker.storage;
            let mem_config = PartitionDbMemoryConfig::calculate(memory_budget, opts);
            for cf in cf_names {
                let max_bytes_for_level_base_str =
                    mem_config.max_bytes_for_level_base().to_restring();
                let write_buffer_size_str = mem_config.write_buffer_size().to_restring();
                let max_write_buffer_number_str = mem_config.num_write_buffers().to_restring();
                let target_file_size_base_str = mem_config.target_file_size_base().to_restring();
                let max_compaction_bytes = mem_config.max_compaction_bytes().to_restring();
                let level_zero_file_num_compaction_trigger = mem_config
                    .level_zero_file_num_compaction_trigger()
                    .to_restring();

                debug!(
                    "Updating memory budget for {}/{} to {}",
                    rocksdb.name(),
                    cf,
                    ByteCount::from(memory_budget)
                );

                if let Err(err) = rocksdb.inner().set_options_cf(
                    &cf,
                    &[
                        ("write_buffer_size", &write_buffer_size_str),
                        ("target_file_size_base", &target_file_size_base_str),
                        ("max_write_buffer_number", &max_write_buffer_number_str),
                        ("max_bytes_for_level_base", &max_bytes_for_level_base_str),
                        ("max_compaction_bytes", &max_compaction_bytes),
                        (
                            "level0_file_num_compaction_trigger",
                            &level_zero_file_num_compaction_trigger,
                        ),
                    ],
                ) {
                    warn!(
                        "Failed to update memory budget for {}/{cf}: {err}",
                        rocksdb.name(),
                    );
                }
            }
        })
        .await;
    }

    #[track_caller]
    pub(super) fn scan<B>(
        &self,
        scan: PhysicalScan<B>,
        mut opts: ReadOptions,
    ) -> Result<DBRawIteratorWithThreadMode<'_, rocksdb::DB>, StorageError>
    where
        B: AsRef<[u8]>,
    {
        scan.configure(&mut opts);
        let mut it = self
            .rocksdb()
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(self.cf_handle_for_key_kind(scan.key_kind()), opts);
        it.seek(scan.start());
        Ok(it)
    }
}

pub(crate) struct PartitionCell {
    meta: Arc<Partition>,
    executor: SinglePoolExecutor,
    archived_lsn: watch::Sender<Option<Lsn>>,
    pub(crate) inner: AsyncRwLock<State>,
    pub(crate) faults: Faults,
}

impl PartitionCell {
    pub fn new(partition: Partition, executor: SinglePoolExecutor) -> Self {
        Self {
            meta: Arc::new(partition),
            executor,
            archived_lsn: Default::default(),
            inner: AsyncRwLock::new(State::Unknown),
            faults: Faults::default(),
        }
    }

    pub fn cf_name(&self) -> CfName {
        self.meta.cf_name()
    }

    /// Moves the partition's archived LSN forward, as reported by the snapshot
    /// repository, and never back. The value belongs to the partition, not to its
    /// store, so this works whatever state the store is in and doesn't lock the cell's
    /// state.
    pub(crate) fn note_archived_lsn(&self, archived_lsn: Lsn) {
        self.archived_lsn.send_if_modified(|current| {
            if current.as_mut().is_none_or(|c| &archived_lsn > c) {
                *current = Some(archived_lsn);
                true
            } else {
                false
            }
        });
    }

    fn open_local_cf(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>, db: PartitionDb) {
        debug_assert!(
            guard.db().is_none(),
            "retire the previous admission lifetime first"
        );
        db.register_durable_lsn();
        **guard = State::Open { db };
    }

    async fn set_cf_missing(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>) {
        guard.close().await;
        if let Some(db) = guard.db() {
            db.unregister_durable_lsn();
        }
        // Release the notification lock before dropping any storage resources.
        **guard = State::CfMissing;
    }

    pub async fn open_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        database: &PartitionStoreDb,
    ) {
        debug_assert!(guard.is_unknown());
        match PartitionDb::new(
            self.meta.clone(),
            self.archived_lsn.clone(),
            database.clone(),
        ) {
            Some(db) => {
                self.open_local_cf(guard, db);
            }
            None => {
                self.set_cf_missing(guard).await;
            }
        }
    }

    // low-level opening of a column famili(es) for the partition.
    #[instrument(level = "error", skip_all, fields(partition_id = %self.meta.partition_id, cf_name = %self.meta.cf_name()))]
    pub async fn provision(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        database: PartitionStoreDb,
    ) -> Result<PartitionDb, RocksError> {
        self.drop_cf(guard).await?;
        let rocksdb = &database.rocksdb;
        // Defensive: drop any stale CF that might exist in RocksDB despite the
        // PartitionCell state being CfMissing. The exact sequence of events that leads
        // to this inconsistency hasn't been fully established yet, but we suspect that it
        // might happen if a previous provision/import created the CF and then failed
        // before the cell state was updated. See https://github.com/restatedev/restate/issues/4534
        self.drop_stale_cf(guard, rocksdb).await?;
        let cf_name = self.meta.cf_name();
        debug!("Creating new column family {}", cf_name);
        self.start_open(
            guard,
            database,
            StorageTaskKind::OpenColumnFamily,
            move |native| native.open_cf(cf_name.into()),
        );
        guard.finish_open().await??;
        Ok(guard.db().expect("adopted column family").clone())
    }

    // low-level importing a column family from a locally downloaded a snapshot
    #[instrument(level = "error", skip_all, fields(partition_id = %self.meta.partition_id, cf_name = %self.meta.cf_name(), path = %snapshot.base_dir.path().display()))]
    pub async fn import_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        snapshot: LocalPartitionSnapshot,
        database: PartitionStoreDb,
    ) -> Result<PartitionDb, RocksError> {
        // Sanity check
        if snapshot.key_range.start() > self.meta.key_range.start()
            || snapshot.key_range.end() < self.meta.key_range.end()
        {
            warn!(
                snapshot_range = ?snapshot.key_range,
                partition_range = ?self.meta.key_range,
                "The snapshot key range does fully cover the partition key range"
            );
            return Err(RocksError::SnapshotKeyRangeMismatch);
        }

        let mut import_metadata = ExportImportFilesMetaData::default();
        import_metadata.set_db_comparator_name(snapshot.db_comparator_name.as_str());
        import_metadata.set_files(&snapshot.files)?;

        // Validation failures leave the current store intact. Once replacement
        // starts, reset its watch even if dropping or importing the CF fails.
        self.drop_cf(guard).await?;
        let rocksdb = &database.rocksdb;

        info!(
            snapshot_applied_lsn = %snapshot.min_applied_lsn,
            path = ?snapshot.base_dir.path(),
            "Importing partition store snapshot"
        );

        // Defensive: see the comment in provision() and
        // https://github.com/restatedev/restate/issues/4534
        self.drop_stale_cf(guard, rocksdb).await?;

        let cf_name = self.cf_name();
        self.start_open(
            guard,
            database,
            StorageTaskKind::ImportColumnFamily,
            move |native| {
                let result = native.import_cf(cf_name.into(), import_metadata);
                // Remove the snapshot's files only after the import has finished. This
                // runs on the lifecycle thread, so it happens even if the caller or its
                // runtime goes away.
                drop(snapshot);
                result
            },
        );
        guard.finish_open().await??;
        Ok(guard.db().expect("adopted column family").clone())
    }

    fn start_open(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        database: PartitionStoreDb,
        kind: StorageTaskKind,
        op: impl FnOnce(&RocksAccess) -> Result<(), RocksError> + Send + 'static,
    ) {
        debug_assert!(matches!(**guard, State::CfMissing));
        let meta = self.meta.clone();
        let partition_id = meta.partition_id;
        let archived_lsn = self.archived_lsn.clone();
        let (completion, result) = watch::channel(None);
        **guard = State::Opening { result };
        let worker_completion = completion.clone();
        let PartitionStoreDb {
            rocksdb,
            notifications,
        } = database;
        let executor = self.executor.clone();
        // Hands the work to the lifecycle pool. `injected` is the outcome of a fault a
        // test armed; if it's an error, the work reports it instead of running.
        let submit = move |injected: Result<(), RocksError>| {
            let task = rocksdb.prepare(kind, move |rocksdb| {
                let result = injected.and_then(|()| op(rocksdb.inner())).and_then(|()| {
                    let cf_name = meta.cf_name();
                    let database = PartitionStoreDb {
                        rocksdb,
                        notifications,
                    };
                    let db = PartitionDb::new(meta, archived_lsn, database)
                        .ok_or_else(|| RocksError::UnknownColumnFamily(cf_name.into()))?;
                    // Load the durable LSN before anyone can see the partition or subscribe
                    // to its durability updates. If the read fails, keep the partition anyway
                    // (the column family itself is fine) with an unknown LSN;
                    // get_durable_lsn() reads it again later.
                    match crate::fsm_table::get_locally_durable_lsn(
                        &mut crate::PartitionStore::from(db.clone()),
                    ) {
                        Ok(Some(lsn)) => {
                            db.durable_lsn_sender().send_replace(lsn);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            warn!(partition_id = %db.partition().partition_id, %error,
                                    "Could not initialize durable LSN; recovery will retry on access");
                        }
                    }
                    Ok(db)
                });
                // Log the failure here: if the caller that started this work went away, the
                // next operation on the cell picks up the outcome but doesn't report it.
                if let Err(error) = &result {
                    warn!(
                        %partition_id,
                        %error,
                        "Could not create or import the partition's column family"
                    );
                }
                worker_completion.send_replace(Some(result));
            });
            let dispatch = executor.dispatch(task);
            if let Err(error) = dispatch {
                // The executor rejected the task and already dropped it, with everything it
                // captured, so it's safe to report the failure now.
                completion.send_replace(Some(Err(error.into())));
            }
        };
        // Submits right away unless a test armed a fault.
        faults::run_after(self.faults.take(FaultPoint::Open), submit);
        // If the task panics or is dropped without running, the result channel closes
        // without a result. The cell then stays `Opening`, and later operations on it
        // (close, reopen, replace) give up instead of assuming the work finished.
    }

    /// Drops the column family from RocksDB if it exists. This is a no-op if the CF
    /// is not present.
    async fn drop_cf_from_rocksdb(
        meta: &Partition,
        rocksdb: &Arc<RocksDb>,
        executor: &impl IoExecutor,
    ) -> Result<(), RocksError> {
        let cf_name = meta.cf_name();
        if rocksdb.inner().cf_handle(cf_name.as_ref()).is_some() {
            let task = rocksdb.clone().prepare_drop_cf(cf_name.into());
            // spawn_detached(), not spawn(): once submitted, the deletion runs even if
            // the task awaiting it here is cancelled or its runtime shuts down.
            executor
                .spawn_detached(task)?
                .await
                .map_err(|_| ShutdownError)??;
            debug!("Column family {} dropped", meta.cf_name());
        }
        Ok(())
    }

    /// Deletes a column family that exists in `rocksdb` although the cell has none.
    /// Returns whether there was one.
    async fn drop_stale_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        rocksdb: &Arc<RocksDb>,
    ) -> Result<bool, RocksError> {
        if rocksdb.inner().cf_handle(self.cf_name().as_ref()).is_none() {
            return Ok(false);
        }
        self.start_retirement(guard, rocksdb.clone(), std::future::ready(()));
        guard.finish_retirement().await?;
        Ok(true)
    }

    fn start_retirement(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        rocksdb: Arc<RocksDb>,
        released: impl Future<Output = ()> + Send + 'static,
    ) {
        let meta = self.meta.clone();
        let (completion, result) = watch::channel(None);
        // Switch the cell to `Retiring` before dropping its PartitionDb. If the caller
        // is cancelled from here on, the cell stays `Retiring` instead of becoming
        // `Unknown`, so nothing reopens the partition while it's being deleted.
        **guard = State::Retiring { result };
        // This task waits until nothing uses the column family, deletes it, and
        // publishes the result. It runs on its own, so cancelled callers don't stop
        // it. If the runtime shuts down first, the result channel closes without a
        // result and the cell stays `Retiring`, since we can't tell whether the
        // deletion happened. The deletion itself runs on the lifecycle executor.
        let executor = self.executor.clone();
        let fault = self.faults.take(FaultPoint::Delete);
        tokio::spawn(async move {
            released.await;
            let result = match faults::inject(fault).await {
                Ok(()) => Self::drop_cf_from_rocksdb(&meta, &rocksdb, &executor).await,
                Err(error) => Err(error),
            };
            if let Err(error) = &result {
                error!(
                    partition_id = %meta.partition_id,
                    cf_name = %meta.cf_name(),
                    db = %rocksdb.name(),
                    %error,
                    "Could not delete the partition's column family. The partition stays \
                    closed until a later drop of its store retries the deletion"
                );
            }
            drop(rocksdb);
            completion.send_replace(Some(result));
        });
    }

    /// Deletes the underlying column famil(ies) and closes the [`PartitionDb`].
    ///
    /// The partition stops accepting work right away. Waiting for work it already
    /// accepted, and for its last handle to be dropped, happens in the retirement
    /// task, so the deletion goes ahead even if this caller is cancelled.
    pub async fn drop_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
    ) -> Result<(), RocksError> {
        guard.finish_abandoned_open().await?;
        if let Some(db) = guard.db() {
            db.gate().seal();
            db.unregister_durable_lsn();
            let gate = db.gate().clone();
            let released = db.released();
            let rocksdb = db.rocksdb().clone();
            self.start_retirement(guard, rocksdb, async move {
                gate.drain().await;
                released.await;
            });
        }
        guard.finish_retirement().await?;
        self.set_cf_missing(guard).await;
        Ok(())
    }

    /// Deletes the partition's column family, whatever state the cell is in, and
    /// leaves the cell `CfMissing`. Returns whether there was a column family to delete.
    ///
    /// Unlike [`Self::drop_cf`], this also finds a column family the cell doesn't know
    /// about: one that is on disk but wasn't opened by this process yet (`Unknown`),
    /// or a leftover one while the cell is `CfMissing` (see [`Self::provision`]).
    pub async fn drop_partition_store(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        database: &PartitionStoreDb,
    ) -> Result<bool, RocksError> {
        if guard.is_unknown() {
            self.open_cf(guard, database).await;
        }
        // Pick up a finished creation or import, so that its column family counts.
        guard.finish_abandoned_open().await?;
        // Retry a deletion that failed. It had already waited for the partition's
        // work and handles before trying, so only the deletion itself runs again.
        let failed = matches!(
            &**guard,
            State::Retiring { result } if result.borrow().as_ref().is_some_and(Result::is_err)
        );
        if failed {
            info!("Retrying the deletion of the partition's column family");
            self.start_retirement(guard, database.rocksdb.clone(), std::future::ready(()));
        }
        let existed = guard.db().is_some() || matches!(**guard, State::Retiring { .. });
        self.drop_cf(guard).await?;
        Ok(self.drop_stale_cf(guard, &database.rocksdb).await? || existed)
    }

    /// Clone the underlying [`PartitionDb`], if it is open.
    pub async fn clone_db(&self) -> Option<PartitionDb> {
        let guard = self.inner.read().await;
        match &*guard {
            State::Unknown
            | State::CfMissing
            | State::Opening { .. }
            | State::Closing { .. }
            | State::Closed { .. }
            | State::Retiring { .. } => None,
            State::Open { db } => Some(db.clone()),
        }
    }

    /// Returns the underlying Partition information if the database is open.
    pub async fn get_partition_if_open(&self) -> Option<Arc<Partition>> {
        let guard = self.inner.read().await;
        match &*guard {
            State::Unknown
            | State::CfMissing
            | State::Opening { .. }
            | State::Closing { .. }
            | State::Closed { .. }
            | State::Retiring { .. } => None,
            State::Open { db } => Some(Arc::clone(db.partition())),
        }
    }

    /// Returns the cell's state without waiting, or `None` if the cell is in the
    /// middle of a transition (opening, closing or deleting the partition).
    pub fn try_state(&self) -> Option<tokio::sync::RwLockReadGuard<'_, State>> {
        self.inner.try_read().ok()
    }

    #[cfg(test)]
    pub async fn reset_to_unknown(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>) {
        guard
            .finish_abandoned_open()
            .await
            .expect("initialization completes");
        guard
            .finish_retirement()
            .await
            .expect("retirement completes");
        self.set_cf_missing(guard).await;
        **guard = State::Unknown;
    }
}

#[derive(Default, Clone)]
pub(crate) enum State {
    /// The state of local column family is still unknown.
    ///
    /// The column family may or may not exist locally and we will only know
    /// this after an attempt to opening it.
    #[default]
    Unknown,
    /// The column famil(ies) for this partition does not exist locally
    CfMissing,
    /// The column family is being created or imported on the lifecycle pool. Holds
    /// the outcome once it's ready, until an operation on the cell picks it up and
    /// moves the cell to `Open` or `CfMissing`. If the work panicked or was dropped
    /// without running, the cell stays here and later operations give up.
    Opening {
        result: watch::Receiver<Option<Result<PartitionDb, RocksError>>>,
    },
    Open {
        db: PartitionDb,
    },
    /// The partition no longer accepts new reads or writes, but work it already
    /// accepted may still be running. The cell stays in this state even if the
    /// caller waiting for the close is cancelled.
    Closing {
        db: PartitionDb,
        closing_at: Instant,
    },
    /// The partition no longer accepts reads or writes, and all accepted work has
    /// finished. Its column families still exist, so it can be reopened or deleted.
    Closed {
        db: PartitionDb,
        closed_at: Instant,
    },
    /// The column family is being deleted, or its deletion failed or was interrupted.
    /// Holds only the deletion's result, which every waiter can read, including ones
    /// that start after earlier waiters were cancelled. On success the cell becomes
    /// `CfMissing`. After a failure or interruption it stays here, so the partition
    /// isn't reopened over a half-deleted column family. A later
    /// [`PartitionCell::drop_partition_store`] retries a deletion that failed.
    Retiring {
        result: watch::Receiver<Option<Result<(), RocksError>>>,
    },
}

impl State {
    /// The state of local column family is still unknown.
    ///
    /// The column family may or may not exist locally and we will only know
    /// this after an attempt to opening it.
    pub fn is_unknown(&self) -> bool {
        matches!(self, State::Unknown)
    }

    pub fn closed_since(&self) -> Option<Duration> {
        match self {
            State::Unknown | State::CfMissing | State::Opening { .. } | State::Retiring { .. } => {
                None
            }
            State::Open { .. } | State::Closing { .. } => None,
            State::Closed { closed_at, .. } => Some(closed_at.elapsed()),
        }
    }

    pub fn maybe_open(&self) -> bool {
        matches!(
            self,
            State::Open { .. } | State::Closing { .. } | State::Closed { .. }
        )
    }

    pub async fn get_or_reopen(&mut self) -> Result<Option<PartitionDb>, RocksError> {
        self.finish_abandoned_open().await?;
        self.finish_retirement().await?;
        let db = match &*self {
            State::Unknown | State::CfMissing => return Ok(None),
            State::Opening { .. } | State::Retiring { .. } => {
                unreachable!("completed transitions remove their records")
            }
            State::Open { db } => return Ok(Some(db.clone())),
            State::Closing { db, closing_at } => {
                // A cancelled close may have left admitted work in flight.
                Self::drain_for_close(db, *closing_at).await;
                db
            }
            State::Closed { db, .. } => db,
        };
        let db = PartitionDb::open(db.physical_scope().clone());
        *self = State::Open { db: db.clone() };
        Ok(Some(db))
    }

    async fn finish_retirement(&mut self) -> Result<(), RocksError> {
        if let State::Retiring { result } = self {
            result
                .wait_for(Option::is_some)
                .await
                .map_err(|_| ShutdownError)?
                .as_ref()
                .expect("completed retirement")
                .clone()?;
            *self = State::CfMissing;
        }
        Ok(())
    }

    /// Waits for a creation or import to finish and moves the cell to `Open`, or to
    /// `CfMissing` if it failed.
    ///
    /// The two layers of the result mean different things:
    /// - The outer `Err(ShutdownError)` means the work ended without an outcome: its
    ///   task panicked or was dropped without running. We can't tell whether the
    ///   column family was created, so the cell stays `Opening` and every caller
    ///   must fail.
    /// - The inner `Err(RocksError)` means the work finished and the creation or
    ///   import itself failed. The cell is back to `CfMissing` and can be used
    ///   again. Only the caller that started the work returns this error; any other
    ///   caller uses [`Self::finish_abandoned_open`], which drops it.
    async fn finish_open(&mut self) -> Result<Result<(), RocksError>, ShutdownError> {
        if let State::Opening { result } = self {
            let result = result
                .wait_for(Option::is_some)
                .await
                .map_err(|_| ShutdownError)?
                .as_ref()
                .expect("completed initialization")
                .clone();
            // We have an outcome. A failed creation or import has fully finished, so
            // the cell can go back to `CfMissing` and be retried. A channel that closed
            // without an outcome returned early above and left the cell `Opening`.
            *self = State::CfMissing;
            let db = match result {
                Ok(db) => db,
                Err(error) => return Ok(Err(error)),
            };
            db.register_durable_lsn();
            *self = State::Open { db };
        }
        Ok(Ok(()))
    }

    /// Like [`Self::finish_open`], for a creation or import started by a caller that
    /// went away. If it failed, the cell becomes `CfMissing` and the error is dropped:
    /// it belonged to that caller, and the work already logged it. This way the next
    /// operation on the cell goes ahead instead of failing with someone else's error.
    async fn finish_abandoned_open(&mut self) -> Result<(), ShutdownError> {
        let _ = self.finish_open().await?;
        Ok(())
    }

    pub fn db(&self) -> Option<&PartitionDb> {
        match self {
            State::Unknown | State::CfMissing | State::Opening { .. } | State::Retiring { .. } => {
                None
            }
            State::Open { db } | State::Closing { db, .. } | State::Closed { db, .. } => Some(db),
        }
    }

    pub async fn close(&mut self) {
        if let Err(error) = self.finish_abandoned_open().await {
            warn!(%error, "Could not finish partition initialization while closing");
            return;
        }
        match &mut *self {
            State::Unknown
            | State::CfMissing
            | State::Opening { .. }
            | State::Closing { .. }
            | State::Closed { .. }
            | State::Retiring { .. } => {}
            State::Open { db } => {
                db.gate().seal();
                *self = State::Closing {
                    db: db.clone(),
                    closing_at: Instant::now(),
                };
            }
        }
        if let State::Closing { db, closing_at } = self {
            let closed_at = Self::drain_for_close(db, *closing_at).await;
            *self = State::Closed {
                db: db.clone(),
                closed_at,
            };
        }
    }

    async fn drain_for_close(db: &PartitionDb, closing_at: Instant) -> Instant {
        db.gate().drain().await;
        let closed_at = Instant::now();
        debug!(
            partition_id = %db.partition().partition_id,
            elapsed = %(closed_at - closing_at).friendly(),
            "Closed partition db"
        );
        closed_at
    }
}

// -- How to configure partition-db column families? --

pub struct RocksConfigurator<T> {
    memory_budget: Arc<MemoryBudget>,
    durable_lsn_notifications: Arc<DurableLsnSenders>,
    use_multi_db_layout: bool,
    _marker: PhantomData<T>,
}

impl<T> Clone for RocksConfigurator<T> {
    fn clone(&self) -> Self {
        Self {
            memory_budget: self.memory_budget.clone(),
            durable_lsn_notifications: self.durable_lsn_notifications.clone(),
            use_multi_db_layout: self.use_multi_db_layout,
            _marker: PhantomData,
        }
    }
}

impl<T> RocksConfigurator<T> {
    pub fn new(
        memory_budget: Arc<MemoryBudget>,
        notifications: &Arc<DurableLsnSenders>,
        use_multi_db_layout: bool,
    ) -> Self {
        Self {
            memory_budget,
            durable_lsn_notifications: notifications.clone(),
            use_multi_db_layout,
            _marker: PhantomData,
        }
    }
}

// Configuration of column families that hold all partition data.
pub struct AllDataCf;

impl DbConfigurator for RocksConfigurator<AllDataCf> {
    fn get_db_options(&self, db_name: &DbName, env: &restate_rocksdb::DbEnv) -> rocksdb::Options {
        let mut db_options =
            restate_rocksdb::configuration::create_default_db_options(env, db_name);

        let storage_config = &Configuration::pinned().worker.storage;
        self.apply_db_opts_from_config(&mut db_options, &storage_config.rocksdb);

        db_options.set_max_subcompactions(storage_config.rocksdb_max_sub_compactions());
        // Avoid pushing back on compaction delays, sacrifice storage and do not hinder
        // writes and flushes.
        db_options.set_soft_pending_compaction_bytes_limit(512 * 1024 * 1024 * 1024);
        // Do not push back on compaction delays, sacrifice storage and do not hinder
        // writes and flushes.
        db_options.set_hard_pending_compaction_bytes_limit(2 * 1024 * 1024 * 1024 * 1024);

        // Sets the available buffer for writing to SST files.
        db_options.set_writable_file_max_buffer_size(
            storage_config
                .rocksdb_writable_file_max_buffer_size
                .as_u64(),
        );

        if let Some(max_open_files) = storage_config.rocksdb_max_open_files {
            db_options.set_max_open_files(max_open_files.get().min(i32::MAX as u32) as i32);
        } else {
            db_options.set_max_open_files(-1);
        }

        if self.use_multi_db_layout {
            // In multi-db layout we don't want every database to grown the thread pool
            restate_rocksdb::configuration::set_background_work_budget(
                &mut db_options,
                NonZeroU32::MIN,
                NonZeroU32::MIN,
            );
        } else {
            restate_rocksdb::configuration::set_background_work_budget(
                &mut db_options,
                storage_config.rocksdb_max_background_flushes(),
                storage_config.rocksdb_max_background_compactions(),
            );
        }

        let event_listener = DurableLsnEventListener::new(self.durable_lsn_notifications.clone());
        db_options.add_event_listener(event_listener);

        db_options
    }
}

impl CfConfigurator for RocksConfigurator<AllDataCf> {
    fn get_cf_options(
        &self,
        db_name: &DbName,
        cf_name: &str,
        env: &restate_rocksdb::DbEnv,
    ) -> rocksdb::Options {
        let mut cf_options = restate_rocksdb::configuration::create_default_cf_options(
            env.write_buffer_manager.as_ref(),
        );

        let config = &Configuration::pinned().worker.storage;
        let mut block_options = restate_rocksdb::configuration::create_default_block_options(
            &config.rocksdb,
            env.cache.as_ref(),
        );
        if config.rocksdb_enable_l6_filters {
            // By default, optimize_filters_for_hits skips L6 filters to save memory. Enable
            // them to avoid unnecessary block reads when looking up missing invocation IDs.
            cf_options.set_optimize_filters_for_hits(false);
            // Use Ribbon filters to reduce L6 filter memory, keeping Bloom on other levels.
            // In internal write-heavy workloads, Ribbon filter size was 28.9% smaller:
            // for L6 240.4M entries, Ribbon=237.8MiB vs. Bloom=334.3MiB.
            block_options.set_hybrid_ribbon_filter(10.0, 6);
        }
        // Without whole keys, point lookups check the prefix filter (see the prefix extractor
        // below), which already tells apart the invocations and vqueues sharing a partition key.
        // On an 18.7M-invocation partition store (330.7M keys), the filters in the SST files added
        // up to 39.4 MiB with whole keys and no L6 filters, 320.2 MiB with whole keys and L6
        // filters, and 74.8 MiB with L6 filters and no whole keys. These are sizes on disk and an
        // upper bound for the block cache, which only holds the filters of L0 files (pinned) and
        // the ones that reads, flushes and compactions loaded. L6 filters without whole keys make
        // cold lookups of missing invocations 100x faster than the v1.7 layout (2.7 vs 268 µs),
        // but only once files written with the fixed 10-byte extractor are gone: they give point
        // lookups no filter at all without whole keys (512 vs 268 µs).
        // TODO(v1.9): always disable whole-key filtering and enable L6 filters by default.
        let whole_key_filtering = !config.rocksdb_disable_whole_key_filtering;
        block_options.set_whole_key_filtering(whole_key_filtering);

        cf_options.set_block_based_table_factory(&block_options);
        cf_options.set_merge_operator(
            "PartitionMerge",
            KeyKind::full_merge,
            KeyKind::partial_merge,
        );
        cf_options.set_max_successive_merges(config.rocksdb_max_successive_merges as usize);

        cf_options.set_disable_auto_compactions(config.rocksdb.rocksdb_disable_auto_compactions());
        if let Some(compaction_period) = config.rocksdb.rocksdb_periodic_compaction_seconds() {
            cf_options.set_periodic_compaction_seconds(compaction_period);
        }

        if !config.rocksdb_disable_compact_on_deletion {
            cf_options.add_compact_on_deletion_collector_factory_min_file_size(
                config.rocksdb_compact_on_deletions_window.get(),
                config.rocksdb_compact_on_deletions_count.get(),
                config.rocksdb_compact_on_deletions_ratio,
                config
                    .rocksdb_compact_on_deletions_min_sst_file_size
                    .as_u64(),
            );
        }

        // Keys shorter than the cap are their own prefix, so every key is in the extractor's domain.
        cf_options.set_capped_prefix_extractor(crate::PREFIX_EXTRACTOR_LENGTH);
        cf_options.set_memtable_prefix_bloom_ratio(0.2);
        cf_options.set_memtable_whole_key_filtering(whole_key_filtering);

        cf_options.set_num_levels(7);
        let l0_l1 = if config.rocksdb.rocksdb_disable_l0_l1_compression() {
            rocksdb::DBCompressionType::None
        } else {
            rocksdb::DBCompressionType::Lz4
        };
        let levels = restate_rocksdb::configuration::build_compression_per_level(
            7,
            l0_l1,
            rocksdb::DBCompressionType::Zstd,
        );
        cf_options.set_compression_per_level(&levels);

        // Always collect applied LSN table properties in partition store CFs
        cf_options.add_table_properties_collector_factory(AppliedLsnCollectorFactory);

        // -- Initial Memory Configuration --
        let mem_config = PartitionDbMemoryConfig::calculate(
            self.memory_budget.current_per_partition_budget(),
            config,
        );
        tracing::debug!(
            "Configured {db_name}/{cf_name} with memtable budget={}",
            ByteCount::from(mem_config.memory_budget())
        );
        cf_options.set_write_buffer_size(mem_config.write_buffer_size());

        // Do not slow down on l0 number of files writes even if it hurts read
        // amplification.
        cf_options.set_level_zero_slowdown_writes_trigger(1 << 30);
        cf_options.set_level_zero_stop_writes_trigger(1 << 30);

        cf_options.set_min_write_buffer_number_to_merge(
            mem_config.min_write_buffer_number_to_merge() as i32,
        );
        cf_options.set_max_write_buffer_number(mem_config.num_write_buffers() as i32);
        cf_options.set_level_zero_file_num_compaction_trigger(
            mem_config.level_zero_file_num_compaction_trigger() as i32,
        );
        cf_options.set_target_file_size_base(mem_config.target_file_size_base() as u64);
        cf_options.set_max_bytes_for_level_base(mem_config.max_bytes_for_level_base() as u64);
        cf_options.set_max_compaction_bytes(mem_config.max_compaction_bytes() as u64);

        // Avoid pushing back on compaction delays, sacrifice storage and do not hinder
        // writes and flushes.
        cf_options.set_soft_pending_compaction_bytes_limit(512 * 1024 * 1024 * 1024);
        // Do not push back on compaction delays, sacrifice storage and do not hinder
        // writes and flushes.
        cf_options.set_hard_pending_compaction_bytes_limit(2 * 1024 * 1024 * 1024 * 1024);

        cf_options
    }
}
