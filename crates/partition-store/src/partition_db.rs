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
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

use restate_core::ShutdownError;
use restate_core::io::FlightTracker;
use restate_rocksdb::configuration::{CfConfigurator, DbConfigurator};
use restate_rocksdb::{DbName, Priority, RawRocksDb, RocksDb, RocksError, StorageTaskKind};
use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::partitions::{CfName, Partition};
use restate_util_bytecount::ByteCount;
use restate_util_string::{ReString, ToReString};
use restate_util_time::DurationExt;

use crate::durable_lsn_tracking::{
    AppliedLsnCollectorFactory, DurableLsnEventListener, DurableLsnSenders,
};
use crate::keys::KeyKind;
use crate::memory::{MemoryBudget, PartitionDbMemoryConfig};
use crate::scan::PhysicalScan;
use crate::snapshots::LocalPartitionSnapshot;
use crate::{TableKind, configure_prefix_iterator_opts, configure_range_iterator_opts};

/// A physical database and the notification registry installed in its listener.
/// Keep these together when reusing a database from the manager's weak cache.
#[derive(Clone)]
pub(crate) struct PartitionStoreDb {
    pub rocksdb: Arc<RocksDb>,
    pub notifications: Arc<DurableLsnSenders>,
}

#[derive(Clone)]
pub struct PartitionDb {
    meta: Arc<Partition>,
    admission: Arc<FlightTracker>,
    durable_lsn: watch::Sender<Lsn>,
    archived_lsn: watch::Sender<Option<Lsn>>,
    physical: Arc<PhysicalPartition>,
}

struct PhysicalPartition {
    // Note: Rust will drop the fields in the order they are declared in the struct.
    // It's crucial to keep the column family and the database in this exact order.
    cf: PartitionBoundCfHandle,
    database: PartitionStoreDb,
    released: PhysicalRelease,
}

// Last field: notify only after the CF and database pins have been released.
struct PhysicalRelease(CancellationToken);

impl Drop for PhysicalRelease {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.0.cancel();
        }
    }
}

impl PartitionDb {
    pub(crate) fn new(
        meta: Arc<Partition>,
        archived_lsn: watch::Sender<Option<Lsn>>,
        database: PartitionStoreDb,
        cf: Arc<BoundColumnFamily<'_>>,
    ) -> Self {
        Self {
            meta,
            admission: Arc::new(FlightTracker::default()),
            durable_lsn: watch::Sender::new(Lsn::INVALID),
            archived_lsn,
            physical: Arc::new(PhysicalPartition {
                // SAFETY: the shared physical lifetime releases the CF before
                // its database pin; every PartitionDb clone retains that lifetime.
                cf: unsafe { PartitionBoundCfHandle::new(cf) },
                database,
                released: PhysicalRelease(CancellationToken::new()),
            }),
        }
    }

    pub fn partition(&self) -> &Arc<Partition> {
        &self.meta
    }

    pub fn rocksdb(&self) -> &Arc<RocksDb> {
        &self.physical.database.rocksdb
    }

    pub(crate) fn admission(&self) -> &Arc<FlightTracker> {
        &self.admission
    }

    #[cfg(test)]
    pub fn into_rocksdb(self) -> Arc<RocksDb> {
        self.rocksdb().clone()
    }

    pub fn cf_handle(&self) -> &Arc<BoundColumnFamily<'_>> {
        &self.physical.cf.0
    }

    pub(crate) fn table_cf_handle(&self, _table_kind: TableKind) -> &Arc<BoundColumnFamily<'_>> {
        self.cf_handle()
    }

    pub(crate) fn cf_handle_for_key_kind(&self, _key_kind: KeyKind) -> &Arc<BoundColumnFamily<'_>> {
        &self.physical.cf.0
    }

    pub fn cf_names(&self) -> Vec<ReString> {
        vec![self.meta.cf_name().into_inner()]
    }

    pub async fn flush_memtables(&self, wait: bool) -> Result<(), RocksError> {
        let physical = self.physical.clone();
        let cf_name = self.partition().cf_name().into();
        self.rocksdb()
            .clone()
            .run_background_read_op(
                "flush-partition",
                StorageTaskKind::FlushMemtables,
                Priority::Low,
                move |_| {
                    physical
                        .database
                        .rocksdb
                        .inner()
                        .flush_memtables(std::slice::from_ref(&cf_name), wait)
                },
            )
            .await?
    }

    /// Observe release of this lifetime's CF/database use pins. Native CF deletion
    /// has its own retirement result; this observer retains neither resource.
    pub(crate) fn released(&self) -> impl Future<Output = ()> + Send + 'static + use<> {
        self.physical.released.0.clone().cancelled_owned()
    }

    pub(crate) async fn export_cf(
        &self,
        export_dir: std::path::PathBuf,
    ) -> Result<ExportImportFilesMetaData, RocksError> {
        let physical = self.physical.clone();
        let name = self.partition().cf_name().into();
        self.rocksdb()
            .clone()
            .run_background_read_op(
                "export-column-family",
                StorageTaskKind::ExportColumnFamily,
                Priority::Low,
                move |_| {
                    physical
                        .database
                        .rocksdb
                        .inner()
                        .export_cf(name, export_dir)
                },
            )
            .await?
    }

    pub(crate) async fn run_background_read_op<OP, R>(
        &self,
        name: &'static str,
        kind: StorageTaskKind,
        priority: Priority,
        op: OP,
    ) -> Result<R, RocksError>
    where
        OP: FnOnce(&RawRocksDb) -> R + Send + 'static,
        R: Send + 'static,
    {
        let physical = self.physical.clone();
        self.rocksdb()
            .clone()
            .run_background_read_op(name, kind, priority, move |raw| {
                let result = op(raw);
                drop(physical);
                result
            })
            .await
    }

    pub(crate) fn note_archived_lsn(&self, archived_lsn: Lsn) -> bool {
        self.archived_lsn.send_if_modified(|current| {
            if current.as_mut().is_none_or(|c| &archived_lsn > c) {
                *current = Some(archived_lsn);
                true
            } else {
                false
            }
        })
    }

    /// The last (locally) known archived LSN for this partition
    pub fn get_archived_lsn(&self) -> Option<Lsn> {
        *self.archived_lsn.borrow()
    }

    pub fn watch_archived_lsn(&self) -> watch::Receiver<Option<Lsn>> {
        self.archived_lsn.subscribe()
    }

    pub(crate) fn durable_lsn_sender(&self) -> &watch::Sender<Lsn> {
        &self.durable_lsn
    }

    pub(crate) fn note_durable_lsn(&self, lsn: Lsn) {
        self.physical.database.notifications.note_durable_lsn(
            self.meta.partition_id,
            self.cf_handle_for_key_kind(KeyKind::Fsm).id(),
            lsn,
        );
    }

    fn unregister_durable_lsn(&self) {
        self.physical.database.notifications.unregister(
            self.meta.partition_id,
            self.cf_handle_for_key_kind(KeyKind::Fsm).id(),
        );
    }

    pub(crate) async fn update_memory_budget(&self, memory_budget: usize) {
        // impacts only this partition's column-families
        let cf_names = self.cf_names();
        let physical = self.physical.clone();

        // NOTE: Updating rocksdb's options is a blocking operations and may take a few
        // tens of milliseconds on each column family. We need to perform this operation
        // on non-blocking threads to avoid stalling the tokio runtime (starving failure
        // detector, etc.).
        let _ = tokio::task::spawn_blocking(move || {
            let rocksdb = &physical.database.rocksdb;
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
        match scan {
            PhysicalScan::Prefix(key_kind, prefix) => {
                configure_prefix_iterator_opts(&mut opts, prefix.as_ref());
                let table = self.cf_handle_for_key_kind(key_kind);
                let mut it = self
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .raw_iterator_cf_opt(table, opts);
                it.seek(prefix);
                Ok(it)
            }
            PhysicalScan::RangeExclusive(key_kind, scan_mode, start, end) => {
                configure_range_iterator_opts(&mut opts, scan_mode, start.as_ref(), end.as_ref());
                let table = self.cf_handle_for_key_kind(key_kind);
                let mut it = self
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .raw_iterator_cf_opt(table, opts);
                it.seek(start);
                Ok(it)
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct PartitionBoundCfHandle(Arc<BoundColumnFamily<'static>>);

impl PartitionBoundCfHandle {
    // SAFETY: the new BoundColumnFamily here just expanding lifetime to static,
    // It's safe to use if it's bound to a lifetime that guarantees that its associated
    // rocksdb is dropped after this handle.
    unsafe fn new(cf: Arc<BoundColumnFamily<'_>>) -> Self {
        // SAFETY: the new BoundColumnFamily here just expanding lifetime to static,
        // so that we can re-bind the lifetime to PartitionDb/PartitionStore instead of
        // the database.
        let static_cf = unsafe { Arc::from_raw(Arc::into_raw(cf).cast()) };
        Self(static_cf)
    }
}

pub(crate) struct PartitionCell {
    meta: Arc<Partition>,
    archived_lsn: watch::Sender<Option<Lsn>>,
    pub(crate) inner: AsyncRwLock<State>,
}

impl PartitionCell {
    pub fn new(partition: Partition) -> Self {
        Self {
            meta: Arc::new(partition),
            archived_lsn: Default::default(),
            inner: AsyncRwLock::new(State::Unknown),
        }
    }

    pub fn cf_name(&self) -> CfName {
        self.meta.cf_name()
    }

    fn open_local_cf(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>, db: PartitionDb) {
        debug_assert!(
            guard.db().is_none(),
            "retire the previous admission lifetime first"
        );
        db.physical.database.notifications.register(
            self.meta.partition_id,
            db.cf_handle_for_key_kind(KeyKind::Fsm).id(),
            db.durable_lsn_sender().clone(),
        );
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
        let cf_name = self.cf_name();
        match database.rocksdb.inner().cf_handle(cf_name.as_ref()) {
            Some(handle) => {
                let db = PartitionDb::new(
                    self.meta.clone(),
                    self.archived_lsn.clone(),
                    database.clone(),
                    handle,
                );
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
        rocksdb.clone().open_cf(self.meta.cf_name().into()).await?;
        let handle = rocksdb
            .inner()
            .cf_handle(cf_name.as_ref())
            .expect("cf must be open");
        let db = PartitionDb::new(
            self.meta.clone(),
            self.archived_lsn.clone(),
            database.clone(),
            handle,
        );
        self.open_local_cf(guard, db.clone());
        Ok(db)
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

        rocksdb
            .clone()
            .import_cf(self.meta.cf_name().into(), import_metadata)
            .await?;

        // Remove the remaining snapshot files in a non-blocking way.
        snapshot.base_dir.remove().await;

        let db = PartitionDb::new(
            self.meta.clone(),
            self.archived_lsn.clone(),
            database.clone(),
            rocksdb
                .inner()
                .cf_handle(self.meta.cf_name().as_ref())
                .expect("cf must exist after import"),
        );

        self.open_local_cf(guard, db.clone());
        Ok(db)
    }

    /// Drops the column family from RocksDB if it exists. This is a no-op if the CF
    /// is not present.
    async fn drop_cf_from_rocksdb(
        meta: &Partition,
        rocksdb: &Arc<RocksDb>,
    ) -> Result<(), RocksError> {
        let cf_name = meta.cf_name();
        if rocksdb.inner().cf_handle(cf_name.as_ref()).is_some() {
            rocksdb.clone().drop_cf(cf_name.into()).await?;
            debug!("Column family {} dropped", meta.cf_name());
        }
        Ok(())
    }

    async fn drop_stale_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        rocksdb: &Arc<RocksDb>,
    ) -> Result<(), RocksError> {
        if rocksdb.inner().cf_handle(self.cf_name().as_ref()).is_some() {
            self.start_retirement(guard, rocksdb.clone(), std::future::ready(()));
            guard.finish_retirement().await?;
        }
        Ok(())
    }

    fn start_retirement(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        rocksdb: Arc<RocksDb>,
        released: impl Future<Output = ()> + Send + 'static,
    ) {
        let meta = self.meta.clone();
        let (completion, result) = watch::channel(None);
        // Publish the resource-free retirement record before releasing the
        // cell's pin. A cancelled waiter cannot expose Unknown or reopen it.
        **guard = State::Retiring { result };
        // The readiness task outlives individual waiters. Runtime interruption
        // closes the channel and leaves the cell fenced; it cannot certify cleanup.
        // Once dispatched, native deletion runs on the database's lifecycle lane.
        tokio::spawn(async move {
            released.await;
            let result = Self::drop_cf_from_rocksdb(&meta, &rocksdb).await;
            drop(rocksdb);
            completion.send_replace(Some(result));
        });
    }

    /// Deletes the underlying column famil(ies) and closes the [`PartitionDb`].
    pub async fn drop_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
    ) -> Result<(), RocksError> {
        // Keep the sealed lifetime in the cell while draining. Cancelling this
        // waiter must not allow a reopen/replacement to bypass admitted writes.
        guard.close().await;
        if let Some(db) = guard.db() {
            db.unregister_durable_lsn();
            let released = db.released();
            let rocksdb = db.rocksdb().clone();
            self.start_retirement(guard, rocksdb, released);
        }
        guard.finish_retirement().await?;
        self.set_cf_missing(guard).await;
        Ok(())
    }

    /// Clone the underlying [`PartitionDb`], if it is open.
    pub async fn clone_db(&self) -> Option<PartitionDb> {
        let guard = self.inner.read().await;
        match &*guard {
            State::Unknown
            | State::CfMissing
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
            | State::Closing { .. }
            | State::Closed { .. }
            | State::Retiring { .. } => None,
            State::Open { db } => Some(Arc::clone(db.partition())),
        }
    }

    #[cfg(test)]
    pub async fn reset_to_unknown(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>) {
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
    Open {
        db: PartitionDb,
    },
    /// Transaction-commit admission is sealed; drainage has not yet been confirmed.
    /// Retain the sealed lifetime here even if the close waiter is cancelled.
    Closing {
        db: PartitionDb,
        closing_at: Instant,
    },
    /// Transaction-commit admission is sealed and admitted commits have drained.
    /// Physical resources remain available for maintenance, reopening, or deletion.
    Closed {
        db: PartitionDb,
        closed_at: Instant,
    },
    /// Contains no physical pin. Completion survives dropped observation futures;
    /// failed or interrupted cleanup remains fenced rather than becoming Unknown.
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
            State::Unknown | State::CfMissing | State::Retiring { .. } => None,
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
        self.finish_retirement().await?;
        let db = match &*self {
            State::Unknown | State::CfMissing => return Ok(None),
            State::Retiring { .. } => unreachable!("successful retirement removes the record"),
            State::Open { db } => return Ok(Some(db.clone())),
            State::Closing { db, closing_at } => {
                // A cancelled close may have left admitted writes in flight.
                Self::drain_for_close(db, *closing_at).await;
                db
            }
            State::Closed { db, .. } => db,
        };
        let mut db = db.clone();
        db.admission = Arc::new(FlightTracker::default());
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

    pub fn db(&self) -> Option<&PartitionDb> {
        match self {
            State::Unknown | State::CfMissing | State::Retiring { .. } => None,
            State::Open { db } | State::Closing { db, .. } | State::Closed { db, .. } => Some(db),
        }
    }

    pub async fn close(&mut self) {
        match &mut *self {
            State::Unknown
            | State::CfMissing
            | State::Closing { .. }
            | State::Closed { .. }
            | State::Retiring { .. } => {}
            State::Open { db } => {
                db.admission.seal();
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
        db.admission.drain().await;
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
