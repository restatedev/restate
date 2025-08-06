// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::path::Path;
use std::slice;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use bytes::BytesMut;
use enum_map::Enum;
use rocksdb::table_properties::TablePropertiesExt;
use rocksdb::{
    BoundColumnFamily, DBCompressionType, DBPinnableSlice, DBRawIteratorWithThreadMode,
    PrefixRange, ReadOptions, SliceTransform, SnapshotWithThreadMode,
};
use static_assertions::const_assert_eq;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tracing::trace;

use restate_core::ShutdownError;
use restate_rocksdb::{CfName, IoMode, IterAction, Priority, RocksDb, RocksError};
use restate_storage_api::fsm_table::ReadOnlyFsmTable;
use restate_storage_api::protobuf_types::{PartitionStoreProtobufValue, ProtobufStorageWrapper};
use restate_storage_api::{IsolationLevel, Storage, StorageError, Transaction};
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId, WithPartitionKey};
use restate_types::logs::Lsn;
use restate_types::partitions::Partition;
use restate_types::storage::StorageCodec;

use crate::durable_lsn_tracking::AppliedLsnCollectorFactory;
use crate::fsm_table::{
    get_last_executed_migration, get_locally_durable_lsn, put_last_executed_migration,
};
use crate::keys::KeyKind;
use crate::keys::TableKey;
use crate::migrations::{LATEST_MIGRATION, LastExecutedMigration};
use crate::partition_db::PartitionDb;
use crate::scan::PhysicalScan;
use crate::scan::TableScan;
use crate::snapshots::LocalPartitionSnapshot;

pub type DB = rocksdb::DB;

pub type DBIterator<'b> = DBRawIteratorWithThreadMode<'b, DB>;
pub type DBIteratorTransaction<'b> = DBRawIteratorWithThreadMode<'b, rocksdb::Transaction<'b, DB>>;

// Key prefix is 10 bytes (KeyKind(2) + PartitionKey/Id(8))
const DB_PREFIX_LENGTH: usize = KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>();

// If this changes, we need to know.
const_assert_eq!(DB_PREFIX_LENGTH, 10);

/// An internal representation of PartitionId that pads the underlying u16 into u64 to align with
/// partition-key length. This should only be used as a replacement to PartitionId when
/// compatibility with old u64-sized PartitionId is needed. Additionally. This must be aligned with
/// the size of PartitionKey to match the prefix length requirements in rocksdb.
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    derive_more::Add,
    derive_more::Display,
    derive_more::FromStr,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct PaddedPartitionId(u64);

impl PaddedPartitionId {
    pub fn new(id: PartitionId) -> Self {
        Self(u16::from(id) as u64)
    }
}

impl From<PartitionId> for PaddedPartitionId {
    fn from(value: PartitionId) -> Self {
        Self(u16::from(value) as u64)
    }
}

impl From<PaddedPartitionId> for PartitionId {
    fn from(value: PaddedPartitionId) -> Self {
        Self::from(u16::try_from(value.0).expect("partition_id must fit in u16"))
    }
}

// Ensures that both types have the same length, this makes it possible to
// share prefix extractor in rocksdb.
const_assert_eq!(
    std::mem::size_of::<PartitionKey>(),
    std::mem::size_of::<PaddedPartitionId>(),
);

pub(crate) type Result<T, E = StorageError> = std::result::Result<T, E>;

pub enum TableScanIterationDecision<R> {
    Emit(Result<R>),
    Continue,
    Break,
    BreakWith(Result<R>),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Enum, strum::VariantArray)]
pub enum TableKind {
    // By Partition ID
    PartitionStateMachine,
    Deduplication,
    Outbox,
    Timers,
    // By Partition Key
    State,
    InvocationStatus,
    ServiceStatus,
    Idempotency,
    Inbox,
    Journal,
    Promise,
}

impl TableKind {
    pub const fn key_kinds(self) -> &'static [KeyKind] {
        match self {
            Self::State => &[KeyKind::State],
            Self::InvocationStatus => &[KeyKind::InvocationStatusV1, KeyKind::InvocationStatus],
            Self::ServiceStatus => &[KeyKind::ServiceStatus],
            Self::Idempotency => &[KeyKind::Idempotency],
            Self::Inbox => &[KeyKind::Inbox],
            Self::Outbox => &[KeyKind::Outbox],
            Self::Deduplication => &[KeyKind::Deduplication],
            Self::PartitionStateMachine => &[KeyKind::Fsm],
            Self::Timers => &[KeyKind::Timers],
            Self::Journal => &[
                KeyKind::Journal,
                KeyKind::InvocationStatus,
                KeyKind::JournalV2,
                KeyKind::JournalV2CompletionIdToCommandIndex,
                KeyKind::JournalV2NotificationIdToNotificationIndex,
            ],
            Self::Promise => &[KeyKind::Promise],
        }
    }

    pub fn has_key_kind(self, prefix: &[u8]) -> bool {
        self.extract_key_kind(prefix).is_some()
    }

    pub fn extract_key_kind(self, prefix: &[u8]) -> Option<KeyKind> {
        if prefix.len() < KeyKind::SERIALIZED_LENGTH {
            return None;
        }
        let slice = prefix[..KeyKind::SERIALIZED_LENGTH].try_into().unwrap();
        let Some(kind) = KeyKind::from_bytes(slice) else {
            // warning
            return None;
        };
        self.key_kinds().iter().find(|k| **k == kind).copied()
    }
}

pub struct PartitionStore {
    db: PartitionDb,
    key_buffer: BytesMut,
    value_buffer: BytesMut,
}

impl std::fmt::Debug for PartitionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStore")
            .field("partition_id", &self.db.partition().partition_id)
            .field("cf", &self.db.partition().cf_name())
            .field("key_buffer", &self.key_buffer.len())
            .field("value_buffer", &self.value_buffer.len())
            .finish()
    }
}

impl Clone for PartitionStore {
    fn clone(&self) -> Self {
        PartitionStore {
            db: self.db.clone(),
            key_buffer: BytesMut::default(),
            value_buffer: BytesMut::default(),
        }
    }
}

pub(crate) fn cf_options(
    memory_budget: usize,
) -> impl Fn(rocksdb::Options) -> rocksdb::Options + Send + Sync + 'static {
    move |mut cf_options| {
        set_memory_related_opts(&mut cf_options, memory_budget);
        // Actually, we would love to use CappedPrefixExtractor but unfortunately it's neither exposed
        // in the C API nor the rust binding. That's okay and we can change it later.
        cf_options.set_prefix_extractor(SliceTransform::create_fixed_prefix(DB_PREFIX_LENGTH));
        cf_options.set_memtable_prefix_bloom_ratio(0.2);
        cf_options.set_memtable_whole_key_filtering(true);
        // Most of the changes are highly temporal, we try to delay flushing
        // As much as we can to increase the chances to observe a deletion.
        //
        cf_options.set_num_levels(7);
        cf_options.set_compression_per_level(&[
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
        ]);

        // Always collect applied LSN table properties in partition store CFs
        cf_options.add_table_properties_collector_factory(AppliedLsnCollectorFactory);

        cf_options
    }
}

fn set_memory_related_opts(opts: &mut rocksdb::Options, memtables_budget: usize) {
    // We set the budget to allow 1 mutable + 3 immutable.
    opts.set_write_buffer_size(memtables_budget / 4);

    // merge 2 memtables when flushing to L0
    opts.set_min_write_buffer_number_to_merge(2);
    opts.set_max_write_buffer_number(4);
    // start flushing L0->L1 as soon as possible. each file on level0 is
    // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
    // memtable_memory_budget.
    opts.set_level_zero_file_num_compaction_trigger(2);
    // doesn't really matter much, but we don't want to create too many files
    opts.set_target_file_size_base(memtables_budget as u64 / 8);
    // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
    opts.set_max_bytes_for_level_base(memtables_budget as u64);
}

impl From<PartitionDb> for PartitionStore {
    fn from(db: PartitionDb) -> Self {
        Self::new(db)
    }
}

impl PartitionStore {
    pub(crate) fn new(db: PartitionDb) -> Self {
        Self {
            db,
            key_buffer: BytesMut::new(),
            value_buffer: BytesMut::new(),
        }
    }

    pub fn partition_db(&self) -> &PartitionDb {
        &self.db
    }

    pub fn into_inner(self) -> PartitionDb {
        self.db
    }

    #[inline]
    pub fn partition_id(&self) -> PartitionId {
        self.db.partition().partition_id
    }

    pub fn partition_key_range(&self) -> &RangeInclusive<PartitionKey> {
        &self.db.partition().key_range
    }

    #[inline]
    pub(crate) fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) -> Result<()> {
        assert_partition_key_or_err(&self.db.partition().key_range, partition_key)
    }

    pub fn contains_partition_key(&self, key: PartitionKey) -> bool {
        self.db.partition().key_range.contains(&key)
    }

    fn table_handle(&self, _table_kind: TableKind) -> &Arc<BoundColumnFamily> {
        self.db.cf_handle()
    }

    fn new_prefix_iterator_opts(&self, _key_kind: KeyKind, prefix: Bytes) -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        opts.set_iterate_range(PrefixRange(prefix.clone()));
        opts.set_async_io(true);
        opts.set_total_order_seek(false);
        opts
    }

    fn new_range_iterator_opts(&self, scan_mode: ScanMode, from: Bytes, to: Bytes) -> ReadOptions {
        let mut opts = ReadOptions::default();
        // todo: use auto_prefix_mode, at the moment, rocksdb doesn't expose this through the C
        // binding.
        opts.set_total_order_seek(scan_mode == ScanMode::TotalOrder);
        opts.set_iterate_range(from..to);
        opts.set_async_io(true);
        opts
    }

    #[track_caller]
    fn iterator_from<K: TableKey>(
        &self,
        scan: TableScan<K>,
    ) -> Result<DBRawIteratorWithThreadMode<'_, DB>> {
        let scan: PhysicalScan = scan.into();
        match scan {
            PhysicalScan::Prefix(table, key_kind, prefix) => {
                assert!(table.has_key_kind(&prefix));
                let prefix = prefix.freeze();
                let opts = self.new_prefix_iterator_opts(key_kind, prefix.clone());
                let table = self.table_handle(table);
                let mut it = self
                    .db
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .raw_iterator_cf_opt(table, opts);
                it.seek(prefix);
                Ok(it)
            }
            PhysicalScan::RangeExclusive(table, _key_kind, scan_mode, start, end) => {
                assert!(table.has_key_kind(&start));
                let start = start.freeze();
                let end = end.freeze();
                let opts = self.new_range_iterator_opts(scan_mode, start.clone(), end);
                let table = self.table_handle(table);
                let mut it = self
                    .db
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .raw_iterator_cf_opt(table, opts);
                it.seek(start);
                Ok(it)
            }
            PhysicalScan::RangeOpen(table, _key_kind, start) => {
                // We delayed the generate the synthetic iterator upper bound until this point
                // because we might have different prefix length requirements based on the
                // table+key_kind combination and we should keep this knowledge as low-level as
                // possible.
                //
                // make the end has the same length as all prefixes to ensure rocksdb key
                // comparator can leverage bloom filters when applicable
                // (if auto_prefix_mode is enabled)
                let mut end = BytesMut::zeroed(DB_PREFIX_LENGTH);
                // We want to ensure that Range scans fall within the same key kind.
                // So, we limit the iterator to the upper bound of this prefix
                let kind_upper_bound = K::KEY_KIND.exclusive_upper_bound();
                end[..kind_upper_bound.len()].copy_from_slice(&kind_upper_bound);
                let start = start.freeze();
                let end = end.freeze();
                let opts = self.new_range_iterator_opts(ScanMode::TotalOrder, start.clone(), end);
                let table = self.table_handle(table);
                let mut it = self
                    .db
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .raw_iterator_cf_opt(table, opts);
                it.seek(start);
                Ok(it)
            }
        }
    }

    pub fn run_iterator<K: TableKey, O: Send + 'static>(
        &self,
        name: &'static str,
        priority: Priority,
        scan: TableScan<K>,
        f: impl Fn((&[u8], &[u8])) -> Result<O> + Send + 'static,
    ) -> Result<ReceiverStream<Result<O>>, ShutdownError> {
        let (tx, rx) = mpsc::channel(1);
        let scan: PhysicalScan = scan.into();
        let on_iter = move |item: Result<(&[u8], &[u8]), RocksError>| -> IterAction {
            let res = match item {
                // apply the caller's function
                Ok((key, value)) => match f((key, value)) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        return IterAction::Stop;
                    }
                },
                Err(e) => {
                    let _ = tx.blocking_send(Err(StorageError::Generic(e.into())));
                    return IterAction::Stop;
                }
            };
            if tx.blocking_send(Ok(res)).is_err() {
                return IterAction::Stop;
            }
            // the channel is not closed yet, keep iterating
            IterAction::Next
        };
        match scan {
            PhysicalScan::Prefix(table, key_kind, prefix) => {
                assert!(table.has_key_kind(&prefix));
                let prefix = prefix.freeze();
                let opts = self.new_prefix_iterator_opts(key_kind, prefix.clone());
                self.db.rocksdb().clone().run_background_iterator(
                    // todo(asoli): Pass an owned cf handle instead of name
                    self.db.partition().cf_name().into(),
                    name,
                    priority,
                    IterAction::Seek(prefix),
                    opts,
                    on_iter,
                )?;
            }
            PhysicalScan::RangeExclusive(table, _key_kind, scan_mode, start, end) => {
                assert!(table.has_key_kind(&start));
                let start = start.freeze();
                let end = end.freeze();
                let opts = self.new_range_iterator_opts(scan_mode, start.clone(), end);
                self.db.rocksdb().clone().run_background_iterator(
                    self.db.partition().cf_name().as_ref().into(),
                    name,
                    priority,
                    IterAction::Seek(start),
                    opts,
                    on_iter,
                )?;
            }
            PhysicalScan::RangeOpen(_table, _key_kind, start) => {
                // We delayed the generate the synthetic iterator upper bound until this point
                // because we might have different prefix length requirements based on the
                // table+key_kind combination and we should keep this knowledge as low-level as
                // possible.
                //
                // make the end has the same length as all prefixes to ensure rocksdb key
                // comparator can leverage bloom filters when applicable
                // (if auto_prefix_mode is enabled)
                let mut end = BytesMut::zeroed(DB_PREFIX_LENGTH);
                // We want to ensure that Range scans fall within the same key kind.
                // So, we limit the iterator to the upper bound of this prefix
                let kind_upper_bound = K::KEY_KIND.exclusive_upper_bound();
                end[..kind_upper_bound.len()].copy_from_slice(&kind_upper_bound);
                let start = start.freeze();
                let end = end.freeze();
                let opts = self.new_range_iterator_opts(ScanMode::TotalOrder, start.clone(), end);
                self.db.rocksdb().clone().run_background_iterator(
                    self.db.partition().cf_name().into(),
                    name,
                    priority,
                    IterAction::Seek(start),
                    opts,
                    on_iter,
                )?;
            }
        }
        Ok(ReceiverStream::new(rx))
    }

    pub fn transaction(&mut self) -> PartitionStoreTransaction {
        self.transaction_with_isolation(IsolationLevel::Committed)
    }

    /// Returns the durably persisted applied LSN, if any.
    pub async fn get_durable_lsn(&mut self) -> Result<watch::Receiver<Option<Lsn>>> {
        {
            let sender = self.db.durable_lsn_sender();
            if sender.borrow().is_some() {
                let mut rx = self.db.durable_lsn_sender().subscribe();
                rx.mark_changed();
                return Ok(rx);
            }
        }

        let local = get_locally_durable_lsn(self).await?;
        let sender = self.db.durable_lsn_sender();
        if let Some(lsn) = local {
            sender.send_if_modified(|current_mut| {
                if current_mut.is_none_or(|c| c < lsn) {
                    *current_mut = Some(lsn);
                    true
                } else {
                    false
                }
            });
        }

        let mut rx = sender.subscribe();
        rx.mark_changed();
        Ok(rx)
    }

    pub fn transaction_with_isolation(
        &mut self,
        isolation_level: IsolationLevel,
    ) -> PartitionStoreTransaction {
        // An optimization to avoid looking up the cf handle everytime, if we split into more
        // column families, we will need to cache those cfs here as well.
        let data_cf_handle = self.db.cf_handle();
        let snapshot = match isolation_level {
            IsolationLevel::Committed => None,
            IsolationLevel::RepeatableReads => {
                Some(self.db.rocksdb().inner().as_raw_db().snapshot())
            }
        };

        PartitionStoreTransaction {
            write_batch_with_index: rocksdb::WriteBatchWithIndex::new(0, true),
            data_cf_handle,
            rocksdb: self.db.rocksdb(),
            key_buffer: &mut self.key_buffer,
            value_buffer: &mut self.value_buffer,
            meta: self.db.partition(),
            snapshot,
        }
    }

    pub async fn flush_memtables(&self, wait: bool) -> Result<()> {
        self.db
            .rocksdb()
            .clone()
            .flush_memtables(
                slice::from_ref(&CfName::from(self.db.partition().cf_name())),
                wait,
            )
            .await
            .map_err(|err| StorageError::Generic(err.into()))?;
        Ok(())
    }

    /// Creates a snapshot of the partition in the given directory, which must not exist prior to
    /// the export. The snapshot is atomic and contains, at a minimum, the reported applied LSN.
    /// Additional log records may have been applied between when the LSN was read, and when the
    /// snapshot was actually created. The actual snapshot applied LSN will always be equal to, or
    /// greater than, the reported applied LSN.
    ///
    /// *NB:* Creating a snapshot causes an implicit flush of the column family!
    ///
    /// See [rocksdb::checkpoint::Checkpoint::export_column_family] for additional implementation details.
    pub(crate) async fn create_local_snapshot(
        &mut self,
        snapshot_base_path: &Path,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
    ) -> Result<LocalPartitionSnapshot> {
        let applied_lsn = self
            .get_applied_lsn()
            .await
            .map_err(|e| StorageError::SnapshotExport(e.into()))?
            .ok_or(StorageError::DataIntegrityError)?; // if PartitionStore::get_applied_lsn returns None

        if let Some(min_target_lsn) = min_target_lsn {
            if applied_lsn < min_target_lsn {
                return Err(StorageError::PreconditionFailed(anyhow!(
                    "Applied LSN below the target LSN: {} < {}",
                    applied_lsn,
                    min_target_lsn
                )));
            }
        };

        // RocksDB will create the snapshot directory but the parent must exist first:
        tokio::fs::create_dir_all(snapshot_base_path)
            .await
            .map_err(|e| StorageError::SnapshotExport(e.into()))?;
        let snapshot_dir = snapshot_base_path.join(snapshot_id.to_string());

        let export_files = self
            .db
            .rocksdb()
            .clone()
            .export_cf(self.db.partition().cf_name().into(), snapshot_dir.clone())
            .await
            .map_err(|e| StorageError::SnapshotExport(e.into()))?;

        trace!(
            cf_name = %self.db.partition().cf_name(),
            %applied_lsn,
            "Exported column family snapshot to {:?}",
            snapshot_dir
        );

        Ok(LocalPartitionSnapshot {
            base_dir: snapshot_dir,
            files: export_files.get_files(),
            db_comparator_name: export_files.get_db_comparator_name(),
            log_id: self.db.partition().log_id(),
            min_applied_lsn: applied_lsn,
            key_range: self.db.partition().key_range.clone(),
        })
    }

    pub fn partition(&self) -> &Arc<Partition> {
        self.db.partition()
    }

    pub async fn verify_and_run_migrations(&mut self) -> Result<()> {
        let mut last_executed_migration: LastExecutedMigration =
            get_last_executed_migration(self, self.partition_id())
                .await?
                .into();
        if last_executed_migration != LATEST_MIGRATION {
            // We need to run some migrations!
            last_executed_migration = last_executed_migration.run_all_migrations(self).await?;
            put_last_executed_migration(self, self.partition_id(), last_executed_migration as u16)
                .await?;
        }

        Ok(())
    }
}

impl Storage for PartitionStore {
    type TransactionType<'a> = PartitionStoreTransaction<'a>;

    fn transaction_with_isolation(
        &mut self,
        read_isolation: IsolationLevel,
    ) -> Self::TransactionType<'_> {
        PartitionStore::transaction_with_isolation(self, read_isolation)
    }
}

impl StorageAccess for PartitionStore {
    type DBAccess<'a>
        = DB
    where
        Self: 'a;

    fn iterator_from<K: TableKey>(
        &self,
        scan: TableScan<K>,
    ) -> Result<DBRawIteratorWithThreadMode<'_, Self::DBAccess<'_>>> {
        self.iterator_from(scan)
    }

    #[inline]
    fn cleared_key_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut {
        self.key_buffer.clear();
        self.key_buffer.reserve(min_size);
        &mut self.key_buffer
    }

    #[inline]
    fn cleared_value_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut {
        self.value_buffer.clear();
        self.value_buffer.reserve(min_size);
        &mut self.value_buffer
    }

    #[inline]
    fn get<K: AsRef<[u8]>>(&self, table: TableKind, key: K) -> Result<Option<DBPinnableSlice>> {
        let table = self.table_handle(table);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .get_pinned_cf(table, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    fn get_durable<K: AsRef<[u8]>>(
        &self,
        table: TableKind,
        key: K,
    ) -> Result<Option<DBPinnableSlice>> {
        let table = self.table_handle(table);
        let mut read_opts = ReadOptions::default();
        read_opts.set_read_tier(rocksdb::ReadTier::Persisted);

        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .get_pinned_cf_opt(table, key, &read_opts)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn put_cf(
        &mut self,
        table: TableKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let table = self.table_handle(table);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .put_cf(table, key, value)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) -> Result<()> {
        let table = self.table_handle(table);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .delete_cf(table, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ScanMode {
    /// Scan is bound to a single fixed key prefix (partition id, or a single partition key).
    WithinPrefix,
    /// Scan/iterator requires total order seek, this means that the iterator is not bound to a
    /// fixed prefix that matches the column family prefix extractor length. For instance, if
    /// scanning data across multiple partition IDs or multiple partition keys.
    TotalOrder,
}

pub struct PartitionStoreTransaction<'a> {
    meta: &'a Arc<Partition>,
    write_batch_with_index: rocksdb::WriteBatchWithIndex,
    rocksdb: &'a Arc<RocksDb>,
    data_cf_handle: &'a Arc<BoundColumnFamily<'a>>,
    key_buffer: &'a mut BytesMut,
    value_buffer: &'a mut BytesMut,
    snapshot: Option<SnapshotWithThreadMode<'a, rocksdb::DB>>,
}

impl PartitionStoreTransaction<'_> {
    fn read_options(&self) -> ReadOptions {
        let mut opts = ReadOptions::default();

        if let Some(snapshot) = self.snapshot.as_ref() {
            opts.set_snapshot(snapshot);
        }

        opts
    }

    pub(crate) fn prefix_iterator(
        &self,
        table: TableKind,
        _key_kind: KeyKind,
        prefix: Bytes,
    ) -> Result<DBIterator> {
        let table = self.table_handle(table);
        let mut opts = self.read_options();
        opts.set_iterate_range(PrefixRange(prefix.clone()));
        opts.set_prefix_same_as_start(true);
        opts.set_total_order_seek(false);

        let it = self
            .rocksdb
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(table, opts);
        let mut it = self.write_batch_with_index.iterator_with_base_cf(it, table);
        it.seek(prefix);
        Ok(it)
    }

    pub(crate) fn range_iterator(
        &self,
        table: TableKind,
        _key_kind: KeyKind,
        scan_mode: ScanMode,
        from: Bytes,
        to: Bytes,
    ) -> Result<DBIterator> {
        let table = self.table_handle(table);
        let mut opts = self.read_options();
        // todo: use auto_prefix_mode, at the moment, rocksdb doesn't expose this through the C
        // binding.
        opts.set_total_order_seek(scan_mode == ScanMode::TotalOrder);
        opts.set_iterate_range(from.clone()..to);

        let it = self
            .rocksdb
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(table, opts);
        let mut it = self.write_batch_with_index.iterator_with_base_cf(it, table);
        it.seek(from);
        Ok(it)
    }

    pub(crate) fn table_handle(&self, _table_kind: TableKind) -> &Arc<BoundColumnFamily> {
        // Right now, everything is in one cf, return a reference and save CPU.
        self.data_cf_handle
    }

    #[inline]
    pub(crate) fn partition_id(&self) -> PartitionId {
        self.meta.partition_id
    }

    #[inline]
    pub(crate) fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) -> Result<()> {
        assert_partition_key_or_err(&self.meta.key_range, partition_key)
    }
}

fn assert_partition_key_or_err(
    partition_key_range: &RangeInclusive<PartitionKey>,
    partition_key: &impl WithPartitionKey,
) -> Result<()> {
    let partition_key = partition_key.partition_key();
    if partition_key_range.contains(&partition_key) {
        return Ok(());
    }
    Err(StorageError::Generic(anyhow!(
        "Partition key '{partition_key}' is not part of PartitionStore's partition '{partition_key_range:?}'. This indicates a bug."
    )))
}

impl Transaction for PartitionStoreTransaction<'_> {
    async fn commit(self) -> Result<()> {
        // We cannot directly commit the txn because it might fail because of unrelated concurrent
        // writes to RocksDB. However, it is safe to write the WriteBatch for a given partition,
        // because there can only be a single writer (the leading PartitionProcessor).
        if self.write_batch_with_index.is_empty() {
            return Ok(());
        }
        let io_mode = if Configuration::pinned()
            .worker
            .storage
            .always_commit_in_background
        {
            IoMode::AlwaysBackground
        } else {
            IoMode::Default
        };
        let mut opts = rocksdb::WriteOptions::default();
        // We disable WAL since bifrost is our durable distributed log.
        opts.disable_wal(true);
        self.rocksdb
            .write_batch_with_index(
                "partition-store-txn-commit",
                Priority::High,
                io_mode,
                opts,
                self.write_batch_with_index,
            )
            .await
            .map_err(|error| StorageError::Generic(error.into()))
    }
}

impl StorageAccess for PartitionStoreTransaction<'_> {
    type DBAccess<'b>
        = DB
    where
        Self: 'b;

    fn iterator_from<K: TableKey>(
        &self,
        scan: TableScan<K>,
    ) -> Result<DBRawIteratorWithThreadMode<'_, Self::DBAccess<'_>>> {
        let scan: PhysicalScan = scan.into();
        match scan {
            PhysicalScan::Prefix(table, key_kind, prefix) => {
                self.prefix_iterator(table, key_kind, prefix.freeze())
            }
            PhysicalScan::RangeExclusive(table, key_kind, scan_mode, start, end) => {
                self.range_iterator(table, key_kind, scan_mode, start.freeze(), end.freeze())
            }
            PhysicalScan::RangeOpen(table, key_kind, start) => {
                // We delayed the generate the synthetic iterator upper bound until this point
                // because we might have different prefix length requirements based on the
                // table+key_kind combination and we should keep this knowledge as low-level as
                // possible.
                //
                // make the end has the same length as all prefixes to ensure rocksdb key
                // comparator can leverage bloom filters when applicable
                // (if auto_prefix_mode is enabled)
                let mut end = BytesMut::zeroed(DB_PREFIX_LENGTH);
                // We want to ensure that Range scans fall within the same key kind.
                // So, we limit the iterator to the upper bound of this prefix
                let kind_upper_bound = K::KEY_KIND.exclusive_upper_bound();
                end[..kind_upper_bound.len()].copy_from_slice(&kind_upper_bound);
                self.range_iterator(
                    table,
                    key_kind,
                    ScanMode::TotalOrder,
                    start.freeze(),
                    end.freeze(),
                )
            }
        }
    }

    #[inline]
    fn cleared_key_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut {
        self.key_buffer.clear();
        self.key_buffer.reserve(min_size);
        self.key_buffer
    }

    #[inline]
    fn cleared_value_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut {
        self.value_buffer.clear();
        self.value_buffer.reserve(min_size);
        self.value_buffer
    }

    #[inline]
    fn get<K: AsRef<[u8]>>(&self, table: TableKind, key: K) -> Result<Option<DBPinnableSlice>> {
        let table = self.table_handle(table);
        self.write_batch_with_index
            .get_pinned_from_batch_and_db_cf(
                self.rocksdb.inner().as_raw_db(),
                table,
                key,
                &self.read_options(),
            )
            .map_err(|error| StorageError::Generic(error.into()))
    }

    fn get_durable<K: AsRef<[u8]>>(
        &self,
        _table: TableKind,
        _key: K,
    ) -> Result<Option<DBPinnableSlice>> {
        unreachable!("ReadTier::Persisted is not meant to be used in WBI");
    }

    #[inline]
    fn put_cf(
        &mut self,
        _table: TableKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.write_batch_with_index
            .put_cf(self.data_cf_handle, key, value);
        Ok(())
    }

    #[inline]
    fn delete_cf(&mut self, _table: TableKind, key: impl AsRef<[u8]>) -> Result<()> {
        self.write_batch_with_index
            .delete_cf(self.data_cf_handle, key);
        Ok(())
    }
}

pub(crate) trait StorageAccess {
    type DBAccess<'a>: rocksdb::DBAccess
    where
        Self: 'a;

    fn iterator_from<K: TableKey>(
        &self,
        scan: TableScan<K>,
    ) -> Result<DBRawIteratorWithThreadMode<'_, Self::DBAccess<'_>>>;

    fn cleared_key_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut;

    fn cleared_value_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut;

    fn get<K: AsRef<[u8]>>(&self, table: TableKind, key: K) -> Result<Option<DBPinnableSlice>>;

    /// Forces a read from persistent storage, bypassing memtables and block cache.
    ///
    /// This means that that you might not get the latest value, but the value that was persisted
    /// (flushed) at the time of the call.
    fn get_durable<K: AsRef<[u8]>>(
        &self,
        table: TableKind,
        key: K,
    ) -> Result<Option<DBPinnableSlice>>;

    fn put_cf(
        &mut self,
        table: TableKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()>;

    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) -> Result<()>;

    #[inline]
    fn put_kv_raw<K: TableKey, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<()> {
        let key_buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buffer);
        let key_buffer = key_buffer.split();

        self.put_cf(K::TABLE, key_buffer, value)
    }

    #[inline]
    fn put_kv<K: TableKey, V: PartitionStoreProtobufValue + Clone + 'static>(
        &mut self,
        key: K,
        value: &V,
    ) -> Result<()> {
        let key_buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buffer);
        let key_buffer = key_buffer.split();

        let value_buffer = self.cleared_value_buffer_mut(0);
        StorageCodec::encode(
            &ProtobufStorageWrapper::<V::ProtobufType>(value.clone().into()),
            value_buffer,
        )
        .map_err(|e| StorageError::Generic(e.into()))?;
        let value_buffer = value_buffer.split();

        self.put_cf(K::TABLE, key_buffer, value_buffer)
    }

    #[inline]
    fn delete_key<K: TableKey>(&mut self, key: &K) -> Result<()> {
        let buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(buffer);
        let buffer = buffer.split();

        self.delete_cf(K::TABLE, buffer)
    }

    #[inline]
    fn get_value<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: TableKey,
        V: PartitionStoreProtobufValue,
        <<V as PartitionStoreProtobufValue>::ProtobufType as TryInto<V>>::Error:
            Into<anyhow::Error>,
    {
        let mut buf = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(&mut buf);
        let buf = buf.split();

        match self.get(K::TABLE, &buf) {
            Ok(value) => {
                let slice = value.as_ref().map(|v| v.as_ref());

                if let Some(mut slice) = slice {
                    Ok(Some(V::decode(&mut slice)?))
                } else {
                    Ok(None)
                }
            }
            Err(err) => Err(err),
        }
    }

    /// Forces a read from persistent storage, bypassing memtables and block cache.
    /// This means that that you might not get the latest value, but the value that was
    /// persisted (flushed) at the time of the call.
    #[inline]
    fn get_durable_value<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: TableKey,
        V: PartitionStoreProtobufValue,
        <<V as PartitionStoreProtobufValue>::ProtobufType as TryInto<V>>::Error:
            Into<anyhow::Error>,
    {
        let mut buf = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(&mut buf);
        let buf = buf.split();

        match self.get_durable(K::TABLE, &buf) {
            Ok(value) => {
                let slice = value.as_ref().map(|v| v.as_ref());

                if let Some(mut slice) = slice {
                    Ok(Some(V::decode(&mut slice)?))
                } else {
                    Ok(None)
                }
            }
            Err(err) => Err(err),
        }
    }

    #[inline]
    fn get_first_blocking<K, F, R>(&mut self, scan: TableScan<K>, f: F) -> Result<R>
    where
        K: TableKey,
        F: FnOnce(Option<(&[u8], &[u8])>) -> Result<R>,
    {
        let iterator = self.iterator_from(scan)?;
        f(iterator.item())
    }

    #[inline]
    fn get_kv_raw<K, F, R>(&mut self, key: K, f: F) -> Result<R>
    where
        K: TableKey,
        F: FnOnce(&[u8], Option<&[u8]>) -> Result<R>,
    {
        let mut buf = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(&mut buf);
        let buf = buf.split();

        match self.get(K::TABLE, &buf) {
            Ok(value) => {
                let slice = value.as_ref().map(|v| v.as_ref());
                f(&buf, slice)
            }
            Err(err) => Err(err),
        }
    }

    #[inline]
    fn for_each_key_value_in_place<K, F, R>(
        &self,
        scan: TableScan<K>,
        mut op: F,
    ) -> Result<Vec<Result<R>>>
    where
        K: TableKey,
        F: FnMut(&[u8], &[u8]) -> TableScanIterationDecision<R>,
    {
        let mut res = Vec::new(); // TODO: this should be passed in.

        let mut iterator = self.iterator_from(scan)?;

        while let Some((k, v)) = iterator.item() {
            match op(k, v) {
                TableScanIterationDecision::Emit(result) => {
                    res.push(result);
                    iterator.next();
                }
                TableScanIterationDecision::BreakWith(result) => {
                    res.push(result);
                    break;
                }
                TableScanIterationDecision::Continue => {
                    iterator.next();
                    continue;
                }
                TableScanIterationDecision::Break => {
                    break;
                }
            };
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use crate::keys::{KeyKind, TableKey};
    use crate::partition_store::StorageAccess;
    use crate::{PartitionStoreManager, TableKind};
    use bytes::{Buf, BufMut};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::{IsolationLevel, StorageError, Transaction};
    use restate_types::config::CommonOptions;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::live::Constant;
    use restate_types::partitions::Partition;

    impl TableKey for String {
        const TABLE: TableKind = TableKind::State;
        const KEY_KIND: KeyKind = KeyKind::State;

        fn is_complete(&self) -> bool {
            true
        }

        fn serialize_key_kind<B: BufMut>(bytes: &mut B) {
            Self::KEY_KIND.serialize(bytes);
        }

        fn serialize_to<B: BufMut>(&self, bytes: &mut B) {
            Self::serialize_key_kind(bytes);
            bytes.put_u32(self.len() as u32);
            bytes.put_slice(self.as_bytes());
        }

        fn deserialize_from<B: Buf>(bytes: &mut B) -> crate::partition_store::Result<Self> {
            let key_kind = KeyKind::deserialize(bytes)?;
            assert_eq!(key_kind, Self::KEY_KIND);

            let len = bytes.get_u32() as usize;
            let mut string_bytes = Vec::with_capacity(len);
            bytes.copy_to_slice(&mut string_bytes);
            Ok(String::from_utf8(string_bytes).expect("valid key"))
        }

        fn serialized_length(&self) -> usize {
            KeyKind::SERIALIZED_LENGTH + self.len()
        }
    }

    #[restate_core::test]
    async fn concurrent_writes_and_reads() -> googletest::Result<()> {
        let rocksdb = RocksDbManager::init(Constant::new(CommonOptions::default()));
        let partition_store_manager = PartitionStoreManager::create().await?;
        let mut partition_store = partition_store_manager
            .open(
                &Partition::new(PartitionId::MIN, PartitionKey::MIN..=PartitionKey::MAX),
                None,
            )
            .await?;
        let key_a = "a".to_owned();
        let key_b = "b".to_owned();

        // put the initial values
        partition_store.put_kv_raw(key_a.clone(), 0_u32.to_be_bytes())?;
        partition_store.put_kv_raw(key_b.clone(), 0_u32.to_be_bytes())?;

        let mut partition_store_clone = partition_store.clone();
        // repeatable reads shouldn't see writes of concurrently finishing transactions
        let mut read_txn =
            partition_store.transaction_with_isolation(IsolationLevel::RepeatableReads);

        let value_a = read_txn.get_kv_raw(key_a.clone(), decode_u32)?;

        // concurrent write which should not be visible by read_txn
        let mut write_txn = partition_store_clone.transaction();
        write_txn.put_kv_raw(key_a.clone(), 42_u32.to_be_bytes())?;
        write_txn.put_kv_raw(key_b.clone(), 42_u32.to_be_bytes())?;
        write_txn.commit().await?;

        let value_b = read_txn.get_kv_raw(key_b.clone(), decode_u32)?;

        assert_eq!(value_a, value_b);

        rocksdb.shutdown().await;
        Ok(())
    }

    fn decode_u32(_key: &[u8], bytes: Option<&[u8]>) -> Result<Option<u32>, StorageError> {
        if let Some(bytes) = bytes {
            bytes
                .try_into()
                .map(u32::from_be_bytes)
                .map(Some)
                .map_err(|err| StorageError::Conversion(err.into()))
        } else {
            Ok(None)
        }
    }
}
