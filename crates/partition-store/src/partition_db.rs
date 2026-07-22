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
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use rocksdb::table_properties::TablePropertiesExt;
use rocksdb::{
    BoundColumnFamily, DBRawIteratorWithThreadMode, ExportImportFilesMetaData, ReadOptions,
};
use tokio::sync::{RwLock as AsyncRwLock, watch};
use tokio::time::Instant;
use tracing::{debug, info, instrument, warn};

use restate_core::ShutdownError;
use restate_rocksdb::configuration::{CfConfigurator, DbConfigurator};
use restate_rocksdb::{DbName, RocksDb, RocksError};
use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::logs::Lsn;
use restate_types::partitions::{CfName, Partition};
use restate_util_bytecount::ByteCount;

use crate::durable_lsn_tracking::{AppliedLsnCollectorFactory, DurableLsnEventListener};
use crate::keys::KeyKind;
use crate::memory::{MemoryBudget, PartitionDbMemoryConfig};
use crate::scan::PhysicalScan;
use crate::snapshots::LocalPartitionSnapshot;
use crate::{TableKind, configure_prefix_iterator_opts, configure_range_iterator_opts};

use restate_util_string::{ReString, ToReString};

#[derive(Clone)]
pub struct PartitionDb {
    meta: Arc<Partition>,
    durable_lsn: watch::Sender<Option<Lsn>>,
    archived_lsn: watch::Sender<Option<Lsn>>,
    // Note: Rust will drop the fields in the order they are declared in the struct.
    // It's crucial to keep the column family and the database in this exact order.
    cf: PartitionBoundCfHandle,
    rocksdb: Arc<RocksDb>,
}

impl PartitionDb {
    pub(crate) fn new(
        meta: Arc<Partition>,
        archived_lsn: watch::Sender<Option<Lsn>>,
        rocksdb: Arc<RocksDb>,
        cf: Arc<BoundColumnFamily<'_>>,
    ) -> Self {
        Self {
            meta,
            durable_lsn: watch::Sender::new(None),
            archived_lsn,
            // SAFETY: the new BoundColumnFamily here just expanding lifetime to static,
            // it's safe to use here as long as rocksdb is dropped last.
            cf: unsafe { PartitionBoundCfHandle::new(cf) },
            rocksdb,
        }
    }

    pub fn partition(&self) -> &Arc<Partition> {
        &self.meta
    }

    pub fn rocksdb(&self) -> &Arc<RocksDb> {
        &self.rocksdb
    }

    #[cfg(test)]
    pub fn into_rocksdb(self) -> Arc<RocksDb> {
        self.rocksdb
    }

    pub fn cf_handle(&self) -> &Arc<BoundColumnFamily<'_>> {
        &self.cf.0
    }

    pub(crate) fn table_cf_handle(&self, _table_kind: TableKind) -> &Arc<BoundColumnFamily<'_>> {
        self.cf_handle()
    }

    pub fn cf_names(&self) -> Vec<ReString> {
        vec![self.meta.cf_name().into_inner()]
    }

    pub async fn flush_memtables(&self, wait: bool) -> Result<(), RocksError> {
        self.rocksdb
            .clone()
            .flush_memtables(
                std::slice::from_ref(&restate_rocksdb::CfName::from(self.partition().cf_name())),
                wait,
            )
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

    pub(crate) fn durable_lsn_sender(&self) -> &watch::Sender<Option<Lsn>> {
        &self.durable_lsn
    }

    pub(crate) async fn update_memory_budget(&self, memory_budget: usize) {
        // impacts only this partition's column-families
        let cf_names = self.cf_names();
        let rocksdb = self.rocksdb.clone();

        // NOTE: Updating rocksdb's options is a blocking operations and may take a few
        // tens of milliseconds on each column family. We need to perform this operation
        // on non-blocking threads to avoid stalling the tokio runtime (starving failure
        // detector, etc.).
        let _ = tokio::task::spawn_blocking(move || {
            let opts = &Configuration::pinned().worker.storage;
            let mem_config = PartitionDbMemoryConfig::calculate(memory_budget, opts);
            for cf in cf_names {
                let max_bytes_for_level_base_str =
                    mem_config.max_bytes_for_level_base().to_restring();
                let write_buffer_size_str = mem_config.write_buffer_size().to_restring();
                let max_write_buffer_number_str =
                    mem_config.max_write_buffer_number().to_restring();
                let target_file_size_base_str = mem_config.target_file_size_base().to_restring();
                let max_compaction_bytes = mem_config.max_compaction_bytes().to_restring();

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
            PhysicalScan::Prefix(table, prefix) => {
                debug_assert!(table.has_key_kind(prefix.as_ref()));
                configure_prefix_iterator_opts(&mut opts, prefix.as_ref());
                let table = self.table_cf_handle(table);
                let mut it = self
                    .rocksdb
                    .inner()
                    .as_raw_db()
                    .raw_iterator_cf_opt(table, opts);
                it.seek(prefix);
                Ok(it)
            }
            PhysicalScan::RangeExclusive(table, scan_mode, start, end) => {
                debug_assert!(table.has_key_kind(start.as_ref()));
                configure_range_iterator_opts(&mut opts, scan_mode, start.as_ref(), end.as_ref());
                let table = self.table_cf_handle(table);
                let mut it = self
                    .rocksdb
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
    durable_lsn: RwLock<Option<watch::Sender<Option<Lsn>>>>,
    pub(crate) inner: AsyncRwLock<State>,
}

impl PartitionCell {
    pub fn new(partition: Partition) -> Self {
        Self {
            meta: Arc::new(partition),
            archived_lsn: Default::default(),
            durable_lsn: Default::default(),
            inner: AsyncRwLock::new(State::Unknown),
        }
    }

    pub fn cf_name(&self) -> CfName {
        self.meta.cf_name()
    }

    fn open_local_cf(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>, db: PartitionDb) {
        let mut durable_lsn_guard = self.durable_lsn.write();
        *durable_lsn_guard = Some(db.durable_lsn_sender().clone());
        **guard = State::Open { db };
    }

    pub fn set_cf_missing(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>) {
        let mut durable_lsn_guard = self.durable_lsn.write();
        **guard = State::CfMissing;
        if let Some(durable_lsn) = durable_lsn_guard.take() {
            durable_lsn.send_replace(None);
        }
    }

    pub fn open_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        rocksdb: &Arc<RocksDb>,
    ) {
        let cf_name = self.cf_name();
        match rocksdb.inner().cf_handle(cf_name.as_ref()) {
            Some(handle) => {
                let db = PartitionDb::new(
                    self.meta.clone(),
                    self.archived_lsn.clone(),
                    rocksdb.clone(),
                    handle,
                );
                self.open_local_cf(guard, db);
            }
            None => {
                self.set_cf_missing(guard);
            }
        }
    }

    // low-level opening of a column famili(es) for the partition.
    #[instrument(level = "error", skip_all, fields(partition_id = %self.meta.partition_id, cf_name = %self.meta.cf_name()))]
    pub async fn provision(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        rocksdb: Arc<RocksDb>,
    ) -> Result<PartitionDb, RocksError> {
        // Defensive: drop any stale CF that might exist in RocksDB despite the
        // PartitionCell state being CfMissing. The exact sequence of events that leads
        // to this inconsistency hasn't been fully established yet, but we suspect that it
        // might happen if a previous provision/import created the CF and then failed
        // before the cell state was updated. See https://github.com/restatedev/restate/issues/4534
        Self::drop_cf_from_rocksdb(&self.meta, &rocksdb).await?;
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
            rocksdb.clone(),
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
        rocksdb: Arc<RocksDb>,
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

        info!(
            snapshot_applied_lsn = %snapshot.min_applied_lsn,
            path = ?snapshot.base_dir.path(),
            "Importing partition store snapshot"
        );

        // Defensive: see the comment in provision() and
        // https://github.com/restatedev/restate/issues/4534
        Self::drop_cf_from_rocksdb(&self.meta, &rocksdb).await?;

        rocksdb
            .clone()
            .import_cf(self.meta.cf_name().into(), import_metadata)
            .await?;

        // Remove the remaining snapshot files in a non-blocking way.
        snapshot.base_dir.remove().await;

        let db = PartitionDb::new(
            self.meta.clone(),
            self.archived_lsn.clone(),
            rocksdb.clone(),
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
            let db = Arc::clone(rocksdb);
            let cf_name = cf_name.clone();
            tokio::task::spawn_blocking(move || db.inner().as_raw_db().drop_cf(cf_name.as_ref()))
                .await
                .map_err(|_| RocksError::Shutdown(ShutdownError))??;
            debug!("Column family {} dropped", meta.cf_name());
        }
        Ok(())
    }

    /// Deletes the underlying column famil(ies) and closes the [`PartitionDb`].
    pub async fn drop_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
    ) -> Result<(), RocksError> {
        // We set the state to Unknown in case we returned an error during the drop process.
        let state = std::mem::replace(guard.deref_mut(), State::Unknown);
        match state {
            State::Unknown | State::CfMissing => { /* nothing to do */ }
            State::Open { db } | State::Closed { db, .. } => {
                Self::drop_cf_from_rocksdb(&self.meta, &db.rocksdb).await?;
            }
        }
        self.set_cf_missing(guard);
        Ok(())
    }

    /// Clone the underlying [`PartitionDb`], if it is open.
    pub async fn clone_db(&self) -> Option<PartitionDb> {
        let guard = self.inner.read().await;
        match &*guard {
            State::Unknown | State::CfMissing | State::Closed { .. } => None,
            State::Open { db } => Some(db.clone()),
        }
    }

    /// Returns the underlying Partition information if the database is open.
    pub async fn get_partition_if_open(&self) -> Option<Arc<Partition>> {
        let guard = self.inner.read().await;
        match &*guard {
            State::Unknown | State::CfMissing | State::Closed { .. } => None,
            State::Open { db } => Some(Arc::clone(db.partition())),
        }
    }

    /// Updates the durable Lsn of the local partition store.
    pub fn note_durable_lsn(&self, lsn: Lsn) {
        let durable_lsn_guard = self.durable_lsn.read();
        if let Some(durable_lsn) = durable_lsn_guard.as_ref() {
            durable_lsn.send_replace(Some(lsn));
        }
    }

    #[cfg(test)]
    pub async fn reset_to_unknown(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>) {
        let mut durable_lsn_guard = self.durable_lsn.write();
        **guard = State::Unknown;
        if let Some(durable_lsn) = durable_lsn_guard.take() {
            durable_lsn.send_replace(None);
        }
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
    /// The partition exists locally but it's closed and not being actively used by a partition
    /// processor. In this state, the partition can be re-opened, or it can be dropped but it'll
    /// not be accounted against the total partition memory budget.
    Closed {
        db: PartitionDb,
        closed_at: Instant,
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
            State::Unknown | State::CfMissing => None,
            State::Open { .. } => None,
            State::Closed { closed_at, .. } => Some(closed_at.elapsed()),
        }
    }

    pub fn maybe_open(&self) -> bool {
        matches!(self, State::Closed { .. } | State::Open { .. })
    }

    pub fn get_or_reopen(&mut self) -> Option<PartitionDb> {
        match &mut *self {
            State::Unknown | State::CfMissing => None,
            State::Open { db } => Some(db.clone()),
            State::Closed { db, .. } => {
                let db = db.clone();
                *self = State::Open { db: db.clone() };
                Some(db)
            }
        }
    }

    pub fn db(&self) -> Option<&PartitionDb> {
        match self {
            State::Unknown | State::CfMissing => None,
            State::Open { db } => Some(db),
            State::Closed { db, .. } => Some(db),
        }
    }

    pub fn close(&mut self) {
        match &mut *self {
            State::Unknown | State::CfMissing | State::Closed { .. } => {}
            State::Open { db } => {
                *self = State::Closed {
                    db: db.clone(),
                    closed_at: Instant::now(),
                };
            }
        }
    }
}

// -- How to configure partition-db column families? --

pub struct RocksConfigurator<T> {
    memory_budget: Arc<MemoryBudget>,
    shared_state: Arc<crate::SharedState>,
    use_multi_db_layout: bool,
    _marker: PhantomData<T>,
}

impl<T> Clone for RocksConfigurator<T> {
    fn clone(&self) -> Self {
        Self {
            memory_budget: self.memory_budget.clone(),
            shared_state: self.shared_state.clone(),
            use_multi_db_layout: self.use_multi_db_layout,
            _marker: PhantomData,
        }
    }
}

impl<T> RocksConfigurator<T> {
    pub fn new(
        memory_budget: Arc<MemoryBudget>,
        psm_state: Arc<crate::SharedState>,
        use_multi_db_layout: bool,
    ) -> Self {
        Self {
            memory_budget,
            shared_state: psm_state,
            use_multi_db_layout,
            _marker: PhantomData,
        }
    }
}

// Configuration of column families that hold all partition data.
pub struct AllDataCf;

impl DbConfigurator for RocksConfigurator<AllDataCf> {
    fn get_db_options(
        &self,
        db_name: &DbName,
        env: &rocksdb::Env,
        write_buffer_manager: &rocksdb::WriteBufferManager,
        limiter: &rocksdb::RateLimiter,
    ) -> rocksdb::Options {
        let mut db_options = restate_rocksdb::configuration::create_default_db_options(
            env,
            db_name,
            write_buffer_manager,
            limiter,
        );

        let storage_config = &Configuration::pinned().worker.storage;
        self.apply_db_opts_from_config(&mut db_options, &storage_config.rocksdb);

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

        let event_listener = DurableLsnEventListener::new(&self.shared_state);
        db_options.add_event_listener(event_listener);

        db_options
    }
}

impl CfConfigurator for RocksConfigurator<AllDataCf> {
    fn get_cf_options(
        &self,
        db_name: &DbName,
        cf_name: &str,
        global_cache: &rocksdb::Cache,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options {
        let mut cf_options =
            restate_rocksdb::configuration::create_default_cf_options(Some(write_buffer_manager));

        let config = &Configuration::pinned().worker.storage;
        let block_options = restate_rocksdb::configuration::create_default_block_options(
            &config.rocksdb,
            // use global block cache
            Some(global_cache),
        );
        cf_options.set_block_based_table_factory(&block_options);
        cf_options.set_merge_operator(
            "PartitionMerge",
            KeyKind::full_merge,
            KeyKind::partial_merge,
        );
        cf_options.set_max_successive_merges(5000);

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

        // Actually, we would love to use CappedPrefixExtractor but unfortunately it's neither exposed
        // in the C API nor the rust binding. That's okay and we can change it later.
        cf_options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(
            crate::DB_PREFIX_LENGTH,
        ));
        cf_options.set_memtable_prefix_bloom_ratio(0.2);
        cf_options.set_memtable_whole_key_filtering(true);
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
        cf_options.set_max_write_buffer_number(mem_config.max_write_buffer_number() as i32);
        cf_options.set_level_zero_file_num_compaction_trigger(
            mem_config.level_zero_file_num_compaction_trigger() as i32,
        );
        cf_options.set_target_file_size_base(mem_config.target_file_size_base() as u64);
        cf_options.set_max_bytes_for_level_base(mem_config.max_bytes_for_level_base() as u64);
        cf_options.set_max_compaction_bytes(mem_config.max_compaction_bytes() as u64);

        cf_options
    }
}
