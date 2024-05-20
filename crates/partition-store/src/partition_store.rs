// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use codederror::CodedError;
use restate_rocksdb::CfName;
use restate_rocksdb::IoMode;
use restate_rocksdb::Priority;
use rocksdb::DBCompressionType;
use rocksdb::DBPinnableSlice;
use rocksdb::DBRawIteratorWithThreadMode;
use rocksdb::MultiThreaded;
use rocksdb::PrefixRange;
use rocksdb::ReadOptions;
use rocksdb::{BoundColumnFamily, SliceTransform};
use static_assertions::const_assert_eq;

use enum_map::Enum;
use restate_core::ShutdownError;
use restate_rocksdb::{RocksDb, RocksError};
use restate_storage_api::{Storage, StorageError, Transaction};

use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode};

use crate::keys::KeyKind;
use crate::keys::TableKey;
use crate::scan::PhysicalScan;
use crate::scan::TableScan;

pub type DB = rocksdb::OptimisticTransactionDB<MultiThreaded>;
type TransactionDB<'a> = rocksdb::Transaction<'a, DB>;

pub type DBIterator<'b> = DBRawIteratorWithThreadMode<'b, DB>;
pub type DBIteratorTransaction<'b> = DBRawIteratorWithThreadMode<'b, rocksdb::Transaction<'b, DB>>;

// Key prefix is 10 bytes (KeyKind(2) + PartitionKey/Id(8))
const DB_PREFIX_LENGTH: usize = KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>();

// If this changes, we need to know.
const_assert_eq!(DB_PREFIX_LENGTH, 10);

// Ensures that both types have the same length, this makes it possible to
// share prefix extractor in rocksdb.
const_assert_eq!(
    std::mem::size_of::<PartitionKey>(),
    std::mem::size_of::<PartitionId>(),
);

pub(crate) type Result<T> = std::result::Result<T, StorageError>;

pub enum TableScanIterationDecision<R> {
    Emit(Result<R>),
    Continue,
    Break,
    BreakWith(Result<R>),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Enum, strum_macros::VariantArray)]
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
            Self::InvocationStatus => &[KeyKind::InvocationStatus],
            Self::ServiceStatus => &[KeyKind::ServiceStatus],
            Self::Idempotency => &[KeyKind::Idempotency],
            Self::Inbox => &[KeyKind::Inbox],
            Self::Outbox => &[KeyKind::Outbox],
            Self::Deduplication => &[KeyKind::Deduplication],
            Self::PartitionStateMachine => &[KeyKind::Fsm],
            Self::Timers => &[KeyKind::Timers],
            Self::Journal => &[KeyKind::Journal],
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

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error(transparent)]
    RocksDbManager(
        #[from]
        #[code]
        RocksError,
    ),
    #[error("db contains no storage format version")]
    #[code(restate_errors::RT0009)]
    MissingStorageFormatVersion,
    #[error(transparent)]
    #[code(unknown)]
    Other(#[from] rocksdb::Error),
    #[error(transparent)]
    #[code(unknown)]
    Shutdown(#[from] ShutdownError),
}

pub struct PartitionStore {
    raw_db: Arc<DB>,
    rocksdb: Arc<RocksDb>,
    partition_id: PartitionId,
    data_cf_name: CfName,
    key_range: RangeInclusive<PartitionKey>,
    key_buffer: BytesMut,
    value_buffer: BytesMut,
}

impl std::fmt::Debug for PartitionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStore")
            .field("db", &self.raw_db)
            .field("partition_id", &self.partition_id)
            .field("cf", &self.data_cf_name)
            .field("key_buffer", &self.key_buffer.len())
            .field("value_buffer", &self.value_buffer.len())
            .finish()
    }
}

impl Clone for PartitionStore {
    fn clone(&self) -> Self {
        PartitionStore {
            raw_db: self.raw_db.clone(),
            rocksdb: self.rocksdb.clone(),
            partition_id: self.partition_id,
            data_cf_name: self.data_cf_name.clone(),
            key_range: self.key_range.clone(),
            key_buffer: BytesMut::default(),
            value_buffer: BytesMut::default(),
        }
    }
}

pub(crate) fn cf_options(mut cf_options: rocksdb::Options) -> rocksdb::Options {
    // Actually, we would love to use CappedPrefixExtractor but unfortunately it's neither exposed
    // in the C API nor the rust binding. That's okay and we can change it later.
    cf_options.set_prefix_extractor(SliceTransform::create_fixed_prefix(DB_PREFIX_LENGTH));
    cf_options.set_memtable_prefix_bloom_ratio(0.2);
    // Most of the changes are highly temporal, we try to delay flushing
    // As much as we can to increase the chances to observe a deletion.
    //
    cf_options.set_max_write_buffer_number(3);
    cf_options.set_min_write_buffer_number_to_merge(2);
    //
    // Set compactions per level
    //
    cf_options.set_num_levels(7);
    cf_options.set_compression_per_level(&[
        DBCompressionType::None,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Zstd,
    ]);

    cf_options
}

impl PartitionStore {
    /// Returns the raw rocksdb handle, this should only be used for server operations that
    /// require direct access to rocksdb.
    pub fn inner(&self) -> Arc<DB> {
        self.raw_db.clone()
    }

    pub(crate) fn new(
        raw_db: Arc<DB>,
        rocksdb: Arc<RocksDb>,
        data_cf_name: CfName,
        partition_id: PartitionId,
        key_range: RangeInclusive<PartitionKey>,
    ) -> Self {
        Self {
            raw_db,
            rocksdb,
            partition_id,
            data_cf_name,
            key_range,
            key_buffer: BytesMut::new(),
            value_buffer: BytesMut::new(),
        }
    }

    pub fn partition_key_range(&self) -> &RangeInclusive<PartitionKey> {
        &self.key_range
    }

    pub fn contains_partition_key(&self, key: PartitionKey) -> bool {
        self.key_range.contains(&key)
    }

    fn table_handle(&self, table_kind: TableKind) -> Arc<BoundColumnFamily> {
        find_cf_handle(&self.rocksdb, &self.data_cf_name, table_kind)
    }

    fn prefix_iterator(&self, table: TableKind, _key_kind: KeyKind, prefix: Bytes) -> DBIterator {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        opts.set_iterate_range(PrefixRange(prefix.clone()));
        opts.set_async_io(true);
        opts.set_total_order_seek(false);
        let mut it = self.raw_db.raw_iterator_cf_opt(&table, opts);
        it.seek(prefix);
        it
    }

    fn range_iterator(
        &self,
        table: TableKind,
        _key: KeyKind,
        scan_mode: ScanMode,
        from: Bytes,
        to: Bytes,
    ) -> DBIterator {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();
        // todo: use auto_prefix_mode, at the moment, rocksdb doesn't expose this through the C
        // binding.
        opts.set_total_order_seek(scan_mode == ScanMode::TotalOrder);
        opts.set_iterate_range(from.clone()..to);
        opts.set_async_io(true);

        let mut it = self.raw_db.raw_iterator_cf_opt(&table, opts);
        it.seek(from);
        it
    }

    #[track_caller]
    fn iterator_from<K: TableKey>(
        &self,
        scan: TableScan<K>,
    ) -> DBRawIteratorWithThreadMode<'_, DB> {
        let scan: PhysicalScan = scan.into();
        match scan {
            PhysicalScan::Prefix(table, key_kind, prefix) => {
                assert!(table.has_key_kind(&prefix));
                self.prefix_iterator(table, key_kind, prefix.freeze())
            }
            PhysicalScan::RangeExclusive(table, key_kind, scan_mode, start, end) => {
                assert!(table.has_key_kind(&start));
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

    #[allow(clippy::needless_lifetimes)]
    pub fn transaction(&mut self) -> RocksDBTransaction {
        let rocksdb = self.rocksdb.clone();
        // An optimization to avoid looking up the cf handle everytime, if we split into more
        // column families, we will need to cache those cfs here as well.
        let data_cf_handle = self
            .rocksdb
            .inner()
            .cf_handle(&self.data_cf_name)
            .unwrap_or_else(|| {
                panic!(
                    "Access a column family that must exist: {}",
                    &self.data_cf_name
                )
            });

        RocksDBTransaction {
            txn: self.raw_db.transaction(),
            data_cf_handle,
            rocksdb,
            key_buffer: &mut self.key_buffer,
            value_buffer: &mut self.value_buffer,
        }
    }
}

fn find_cf_handle<'a>(
    db: &'a Arc<RocksDb>,
    data_cf_name: &CfName,
    _table_kind: TableKind,
) -> Arc<BoundColumnFamily<'a>> {
    // At the moment, everything is in one cf
    db.inner()
        .cf_handle(data_cf_name)
        .unwrap_or_else(|| panic!("Access a column family that must exist: {}", data_cf_name))
}

impl Storage for PartitionStore {
    type TransactionType<'a> = RocksDBTransaction<'a>;

    fn transaction(&mut self) -> Self::TransactionType<'_> {
        PartitionStore::transaction(self)
    }
}

impl StorageAccess for PartitionStore {
    type DBAccess<'a>
    = DB where
        Self: 'a,;

    fn iterator_from<K: TableKey>(
        &self,
        scan: TableScan<K>,
    ) -> DBRawIteratorWithThreadMode<'_, Self::DBAccess<'_>> {
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
        self.raw_db
            .get_pinned_cf(&table, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn put_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.raw_db.put_cf(&table, key, value).unwrap();
    }

    #[inline]
    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.raw_db.delete_cf(&table, key).unwrap();
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

pub struct RocksDBTransaction<'a> {
    txn: rocksdb::Transaction<'a, DB>,
    rocksdb: Arc<RocksDb>,
    data_cf_handle: Arc<BoundColumnFamily<'a>>,
    key_buffer: &'a mut BytesMut,
    value_buffer: &'a mut BytesMut,
}

impl<'a> RocksDBTransaction<'a> {
    pub(crate) fn prefix_iterator(
        &self,
        table: TableKind,
        _key_kind: KeyKind,
        prefix: Bytes,
    ) -> DBIteratorTransaction {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();
        opts.set_iterate_range(PrefixRange(prefix.clone()));
        opts.set_prefix_same_as_start(true);
        opts.set_async_io(true);
        opts.set_total_order_seek(false);

        let mut it = self.txn.raw_iterator_cf_opt(table, opts);
        it.seek(prefix);
        it
    }

    pub(crate) fn range_iterator(
        &self,
        table: TableKind,
        _key_kind: KeyKind,
        scan_mode: ScanMode,
        from: Bytes,
        to: Bytes,
    ) -> DBIteratorTransaction {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();
        // todo: use auto_prefix_mode, at the moment, rocksdb doesn't expose this through the C
        // binding.
        opts.set_total_order_seek(scan_mode == ScanMode::TotalOrder);
        opts.set_iterate_range(from.clone()..to);
        opts.set_async_io(true);
        let mut it = self.txn.raw_iterator_cf_opt(table, opts);
        it.seek(from);
        it
    }

    pub(crate) fn table_handle(&self, _table_kind: TableKind) -> &Arc<BoundColumnFamily> {
        // Right now, everything is in one cf, return a reference and save CPU.
        &self.data_cf_handle
    }
}

impl<'a> Transaction for RocksDBTransaction<'a> {
    async fn commit(self) -> Result<()> {
        // We cannot directly commit the txn because it might fail because of unrelated concurrent
        // writes to RocksDB. However, it is safe to write the WriteBatch for a given partition,
        // because there can only be a single writer (the leading PartitionProcessor).
        let write_batch = self.txn.get_writebatch();
        if write_batch.is_empty() {
            return Ok(());
        }
        let mut opts = rocksdb::WriteOptions::default();
        // We disable WAL since bifrost is our durable distributed log.
        opts.disable_wal(true);
        self.rocksdb
            .write_tx_batch(Priority::High, IoMode::default(), opts, write_batch)
            .await
            .map_err(|error| StorageError::Generic(error.into()))
    }
}

impl<'a> StorageAccess for RocksDBTransaction<'a> {
    type DBAccess<'b> = TransactionDB<'b> where Self: 'b;

    fn iterator_from<K: TableKey>(
        &self,
        scan: TableScan<K>,
    ) -> DBRawIteratorWithThreadMode<'_, Self::DBAccess<'_>> {
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
                    ScanMode::WithinPrefix,
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
        self.txn
            .get_pinned_cf(table, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn put_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.txn.put_cf(table, key, value).unwrap();
    }

    #[inline]
    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.txn.delete_cf(table, key).unwrap();
    }
}

pub(crate) trait StorageAccess {
    type DBAccess<'a>: rocksdb::DBAccess
    where
        Self: 'a;

    fn iterator_from<K: TableKey>(
        &self,
        scan: TableScan<K>,
    ) -> DBRawIteratorWithThreadMode<'_, Self::DBAccess<'_>>;

    fn cleared_key_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut;

    fn cleared_value_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut;

    fn get<K: AsRef<[u8]>>(&self, table: TableKind, key: K) -> Result<Option<DBPinnableSlice>>;

    fn put_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>);

    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>);

    #[inline]
    fn put_kv_raw<K: TableKey, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        let key_buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buffer);
        let key_buffer = key_buffer.split();

        self.put_cf(K::TABLE, key_buffer, value);
    }

    #[inline]
    fn put_kv<K: TableKey, V: StorageEncode>(&mut self, key: K, value: V) {
        let key_buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buffer);
        let key_buffer = key_buffer.split();

        let value_buffer = self.cleared_value_buffer_mut(0);
        StorageCodec::encode(&value, value_buffer).unwrap();
        let value_buffer = value_buffer.split();

        self.put_cf(K::TABLE, key_buffer, value_buffer);
    }

    #[inline]
    fn delete_key<K: TableKey>(&mut self, key: &K) {
        let buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(buffer);
        let buffer = buffer.split();

        self.delete_cf(K::TABLE, buffer);
    }

    #[inline]
    fn get_value<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: TableKey,
        V: StorageDecode,
    {
        let mut buf = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(&mut buf);
        let buf = buf.split();

        match self.get(K::TABLE, &buf) {
            Ok(value) => {
                let slice = value.as_ref().map(|v| v.as_ref());

                if let Some(mut slice) = slice {
                    Ok(Some(
                        StorageCodec::decode::<V, _>(&mut slice)
                            .map_err(|err| StorageError::Generic(err.into()))?,
                    ))
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
        let iterator = self.iterator_from(scan);
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
    fn for_each_key_value_in_place<K, F, R>(&self, scan: TableScan<K>, mut op: F) -> Vec<Result<R>>
    where
        K: TableKey,
        F: FnMut(&[u8], &[u8]) -> TableScanIterationDecision<R>,
    {
        let mut res = Vec::new();

        let mut iterator = self.iterator_from(scan);

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

        res
    }
}
