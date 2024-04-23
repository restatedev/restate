// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod deduplication_table;
pub mod fsm_table;
pub mod idempotency_table;
pub mod inbox_table;
pub mod invocation_status_table;
pub mod journal_table;
pub mod keys;
pub mod outbox_table;
mod owned_iter;
pub mod scan;
pub mod service_status_table;
pub mod state_table;
pub mod timer_table;
mod writer;

use crate::keys::TableKey;
use crate::scan::{PhysicalScan, TableScan};
use crate::writer::{Writer, WriterHandle};
use crate::TableKind::{
    Deduplication, Idempotency, Inbox, InvocationStatus, Journal, Outbox, PartitionStateMachine,
    ServiceStatus, State, Timers,
};
use bytes::BytesMut;
use codederror::CodedError;
use restate_core::ShutdownError;
use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, Owner, RocksDbManager, RocksError,
};
use restate_storage_api::{Storage, StorageError, Transaction};
use restate_types::arc_util::Updateable;
use restate_types::config::RocksDbOptions;
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode};
use rocksdb::BoundColumnFamily;
use rocksdb::DBCompressionType;
use rocksdb::DBPinnableSlice;
use rocksdb::DBRawIteratorWithThreadMode;
use rocksdb::MultiThreaded;
use rocksdb::PrefixRange;
use rocksdb::ReadOptions;
use std::path::PathBuf;
use std::sync::Arc;

pub use writer::JoinHandle as RocksDBWriterJoinHandle;
pub use writer::Writer as RocksDBWriter;

pub type DB = rocksdb::OptimisticTransactionDB<MultiThreaded>;
type TransactionDB<'a> = rocksdb::Transaction<'a, DB>;

pub type DBIterator<'b> = DBRawIteratorWithThreadMode<'b, DB>;
pub type DBIteratorTransaction<'b> = DBRawIteratorWithThreadMode<'b, rocksdb::Transaction<'b, DB>>;

type WriteBatch = rocksdb::WriteBatchWithTransaction<true>;

// matches the default directory name
const DB_NAME: &str = "db";

const STATE_TABLE_NAME: &str = "state";
const INVOCATION_STATUS_TABLE_NAME: &str = "invocation_status";
const SERVICE_STATUS_TABLE_NAME: &str = "service_status";
const IDEMPOTENCY_TABLE_NAME: &str = "idempotency";
const INBOX_TABLE_NAME: &str = "inbox";
const OUTBOX_TABLE_NAME: &str = "outbox";
const DEDUP_TABLE_NAME: &str = "dedup";
const FSM_TABLE_NAME: &str = "fsm";
const TIMERS_TABLE_NAME: &str = "timers";
const JOURNAL_TABLE_NAME: &str = "journal";

pub(crate) type Result<T> = std::result::Result<T, StorageError>;

pub enum TableScanIterationDecision<R> {
    Emit(Result<R>),
    Continue,
    Break,
    BreakWith(Result<R>),
}

#[inline]
const fn cf_name(kind: TableKind) -> &'static str {
    match kind {
        State => STATE_TABLE_NAME,
        InvocationStatus => INVOCATION_STATUS_TABLE_NAME,
        ServiceStatus => SERVICE_STATUS_TABLE_NAME,
        Inbox => INBOX_TABLE_NAME,
        Outbox => OUTBOX_TABLE_NAME,
        Deduplication => DEDUP_TABLE_NAME,
        PartitionStateMachine => FSM_TABLE_NAME,
        Timers => TIMERS_TABLE_NAME,
        Journal => JOURNAL_TABLE_NAME,
        Idempotency => IDEMPOTENCY_TABLE_NAME,
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableKind {
    State,
    InvocationStatus,
    ServiceStatus,
    Idempotency,
    Inbox,
    Outbox,
    Deduplication,
    PartitionStateMachine,
    Timers,
    Journal,
}

impl TableKind {
    pub const fn cf_name(&self) -> &'static str {
        cf_name(*self)
    }

    pub fn all() -> core::slice::Iter<'static, TableKind> {
        static VARIANTS: &[TableKind] = &[
            State,
            InvocationStatus,
            ServiceStatus,
            Idempotency,
            Inbox,
            Outbox,
            Deduplication,
            PartitionStateMachine,
            Timers,
            Journal,
        ];
        VARIANTS.iter()
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

pub struct RocksDBStorage {
    db: Arc<DB>,
    writer_handle: WriterHandle,
    key_buffer: BytesMut,
    value_buffer: BytesMut,
}

impl std::fmt::Debug for RocksDBStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDBStorage")
            .field("db", &self.db)
            .field("writer_handle", &self.writer_handle)
            .field("key_buffer", &self.key_buffer)
            .field("value_buffer", &self.value_buffer)
            .finish()
    }
}

impl Clone for RocksDBStorage {
    fn clone(&self) -> Self {
        RocksDBStorage {
            db: self.db.clone(),
            writer_handle: self.writer_handle.clone(),
            key_buffer: BytesMut::default(),
            value_buffer: BytesMut::default(),
        }
    }
}

fn db_options() -> rocksdb::Options {
    let mut db_options = rocksdb::Options::default();
    // no need to retain 1000 log files by default.
    //
    db_options.set_keep_log_file_num(1);

    db_options
}

fn cf_options(mut cf_options: rocksdb::Options) -> rocksdb::Options {
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

impl RocksDBStorage {
    /// Returns the raw rocksdb handle, this should only be used for server operations that
    /// require direct access to rocksdb.
    pub fn inner(&self) -> Arc<DB> {
        self.db.clone()
    }

    pub async fn open(
        data_dir: PathBuf,
        updateable_opts: impl Updateable<RocksDbOptions> + Send + 'static,
    ) -> std::result::Result<(Self, Writer), BuildError> {
        let cfs = vec![
            //
            // keyed by partition key + user key
            //
            CfName::new(cf_name(Inbox)),
            CfName::new(cf_name(State)),
            CfName::new(cf_name(InvocationStatus)),
            CfName::new(cf_name(ServiceStatus)),
            CfName::new(cf_name(Journal)),
            CfName::new(cf_name(Idempotency)),
            //
            // keyed by partition id + suffix
            //
            CfName::new(cf_name(Outbox)),
            CfName::new(cf_name(Timers)),
            // keyed by partition_id + partition_id
            CfName::new(cf_name(Deduplication)),
            // keyed by partition_id + u64
            CfName::new(cf_name(PartitionStateMachine)),
        ];

        let db_spec = DbSpecBuilder::new(
            DbName::new(DB_NAME),
            Owner::PartitionProcessor,
            data_dir,
            db_options(),
        )
        // At the moment, all CFs get the same options, that might change in the future.
        .add_cf_pattern(CfPrefixPattern::ANY, cf_options)
        .ensure_column_families(cfs)
        .build_as_optimistic_db();

        // todo remove this when open_db is async
        let rdb = tokio::task::spawn_blocking(move || {
            RocksDbManager::get().open_db(updateable_opts, db_spec)
        })
        .await
        .map_err(|_| ShutdownError)??;

        let writer = Writer::new(rdb.clone());
        let writer_handle = writer.create_writer_handle();

        Ok((
            Self {
                db: rdb,
                writer_handle,
                key_buffer: BytesMut::default(),
                value_buffer: BytesMut::default(),
            },
            writer,
        ))
    }

    fn table_handle(&self, table_kind: TableKind) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(cf_name(table_kind)).expect(
            "This should not happen, this is a Restate bug. Please contact the restate developers.",
        )
    }

    fn prefix_iterator<K: Into<Vec<u8>>>(&self, table: TableKind, prefix: K) -> DBIterator {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();

        opts.set_iterate_range(PrefixRange(prefix));

        self.db.raw_iterator_cf_opt(&table, opts)
    }

    fn range_iterator(&self, table: TableKind, range: impl rocksdb::IterateBounds) -> DBIterator {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();
        opts.set_iterate_range(range);
        self.db.raw_iterator_cf_opt(&table, opts)
    }

    fn iterator_from<K: TableKey>(
        &self,
        scan: TableScan<K>,
    ) -> DBRawIteratorWithThreadMode<'_, DB> {
        let scan: PhysicalScan = scan.into();
        match scan {
            PhysicalScan::Prefix(table, prefix) => {
                let mut it = self.prefix_iterator(table, prefix.clone());
                it.seek(prefix);
                it
            }
            PhysicalScan::RangeExclusive(table, start, end) => {
                let mut it = self.range_iterator(table, start.clone()..end);
                it.seek(start);
                it
            }
            PhysicalScan::RangeOpen(table, start) => {
                let mut it = self.range_iterator(table, start.clone()..);
                it.seek(start);
                it
            }
        }
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn transaction(&mut self) -> RocksDBTransaction {
        let db = self.db.clone();

        RocksDBTransaction {
            txn: self.db.transaction(),
            db,
            key_buffer: &mut self.key_buffer,
            value_buffer: &mut self.value_buffer,
            writer_handle: &self.writer_handle,
        }
    }
}

impl Storage for RocksDBStorage {
    type TransactionType<'a> = RocksDBTransaction<'a>;

    fn transaction(&mut self) -> Self::TransactionType<'_> {
        RocksDBStorage::transaction(self)
    }
}

impl StorageAccess for RocksDBStorage {
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
        self.db
            .get_pinned_cf(&table, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn put_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.db.put_cf(&table, key, value).unwrap();
    }

    #[inline]
    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.db.delete_cf(&table, key).unwrap();
    }
}

pub struct RocksDBTransaction<'a> {
    txn: rocksdb::Transaction<'a, DB>,
    db: Arc<DB>,
    key_buffer: &'a mut BytesMut,
    value_buffer: &'a mut BytesMut,
    writer_handle: &'a WriterHandle,
}

impl<'a> RocksDBTransaction<'a> {
    pub fn prefix_iterator<K: Into<Vec<u8>>>(
        &self,
        table: TableKind,
        prefix: K,
    ) -> DBIteratorTransaction {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();
        opts.set_iterate_range(PrefixRange(prefix));

        self.txn.raw_iterator_cf_opt(&table, opts)
    }

    pub fn range_iterator(
        &self,
        table: TableKind,
        range: impl rocksdb::IterateBounds,
    ) -> DBIteratorTransaction {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();
        opts.set_iterate_range(range);
        self.txn.raw_iterator_cf_opt(&table, opts)
    }

    fn table_handle(&self, table_kind: TableKind) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(cf_name(table_kind)).expect(
            "This should not happen, this is a Restate bug. Please contact the restate developers.",
        )
    }
}

impl<'a> Transaction for RocksDBTransaction<'a> {
    async fn commit(self) -> Result<()> {
        // We cannot directly commit the txn because it might fail because of unrelated concurrent
        // writes to RocksDB. However, it is safe to write the WriteBatch for a given partition,
        // because there can only be a single writer (the leading PartitionProcessor).
        let write_batch = self.txn.get_writebatch();

        self.writer_handle.write(write_batch).await
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
            PhysicalScan::Prefix(table, prefix) => {
                let mut it = self.prefix_iterator(table, prefix.clone());
                it.seek(prefix);
                it
            }
            PhysicalScan::RangeExclusive(table, start, end) => {
                let mut it = self.range_iterator(table, start.clone()..end);
                it.seek(start);
                it
            }
            PhysicalScan::RangeOpen(table, start) => {
                let mut it = self.range_iterator(table, start.clone()..);
                it.seek(start);
                it
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
            .get_pinned_cf(&table, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn put_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.txn.put_cf(&table, key, value).unwrap();
    }

    #[inline]
    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.txn.delete_cf(&table, key).unwrap();
    }
}

trait StorageAccess {
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

        self.put_cf(K::table(), key_buffer, value);
    }

    #[inline]
    fn put_kv<K: TableKey, V: StorageEncode>(&mut self, key: K, value: V) {
        let key_buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buffer);
        let key_buffer = key_buffer.split();

        let value_buffer = self.cleared_value_buffer_mut(0);
        StorageCodec::encode(&value, value_buffer).unwrap();
        let value_buffer = value_buffer.split();

        self.put_cf(K::table(), key_buffer, value_buffer);
    }

    #[inline]
    fn delete_key<K: TableKey>(&mut self, key: &K) {
        let buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(buffer);
        let buffer = buffer.split();

        self.delete_cf(K::table(), buffer);
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

        match self.get(K::table(), &buf) {
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

        match self.get(K::table(), &buf) {
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
