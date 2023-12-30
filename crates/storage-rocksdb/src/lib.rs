// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod codec;
pub mod deduplication_table;
pub mod fsm_table;
pub mod inbox_table;
pub mod journal_table;
pub mod keys;
pub mod outbox_table;
mod owned_iter;
pub mod scan;
pub mod state_table;
pub mod status_table;
pub mod timer_table;
mod writer;

use crate::codec::Codec;
use crate::keys::TableKey;
use crate::scan::{PhysicalScan, TableScan};
use crate::writer::{Writer, WriterHandle};
use crate::TableKind::{
    Deduplication, Inbox, Journal, Outbox, PartitionStateMachine, State, Status, Timers,
};
use bytes::BytesMut;
use codederror::CodedError;
use futures::{ready, FutureExt, Stream};
use futures_util::future::{ok, ready};
use futures_util::{stream, StreamExt};
use restate_storage_api::{GetFuture, GetStream, Storage, StorageError, Transaction};
use rocksdb::BlockBasedOptions;
use rocksdb::Cache;
use rocksdb::ColumnFamily;
use rocksdb::DBCompressionType;
use rocksdb::DBPinnableSlice;
use rocksdb::DBRawIteratorWithThreadMode;
use rocksdb::Error;
use rocksdb::PrefixRange;
use rocksdb::ReadOptions;
use rocksdb::SingleThreaded;
use rocksdb::WriteBatch;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

pub use writer::JoinHandle as RocksDBWriterJoinHandle;
pub use writer::Writer as RocksDBWriter;

type DB = rocksdb::DBWithThreadMode<SingleThreaded>;
pub type DBIterator<'b> = DBRawIteratorWithThreadMode<'b, DB>;

const STATE_TABLE_NAME: &str = "state";
const STATUS_TABLE_NAME: &str = "status";
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
fn cf_name(kind: TableKind) -> &'static str {
    match kind {
        State => STATE_TABLE_NAME,
        Status => STATUS_TABLE_NAME,
        Inbox => INBOX_TABLE_NAME,
        Outbox => OUTBOX_TABLE_NAME,
        Deduplication => DEDUP_TABLE_NAME,
        PartitionStateMachine => FSM_TABLE_NAME,
        Timers => TIMERS_TABLE_NAME,
        Journal => JOURNAL_TABLE_NAME,
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableKind {
    State,
    Status,
    Inbox,
    Outbox,
    Deduplication,
    PartitionStateMachine,
    Timers,
    Journal,
}

/// # Storage options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "StorageOptions", default)
)]
#[builder(default)]
pub struct Options {
    /// # Storage path
    ///
    /// The root path to use for the Rocksdb storage.
    pub path: String,

    /// # Threads
    ///
    /// The number of threads to reserve to Rocksdb background tasks.
    pub threads: usize,

    /// # Write Buffer size
    ///
    /// The size of a single memtable. Once memtable exceeds this size, it is marked immutable and a new one is created.
    pub write_buffer_size: usize,

    /// # Maximum total WAL size
    ///
    /// Max WAL size, that after this Rocksdb start flushing mem tables to disk.
    pub max_total_wal_size: u64,

    /// # Maximum cache size
    ///
    /// The memory size used for rocksdb caches.
    pub cache_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            path: "target/db/".to_string(),
            threads: 10,
            write_buffer_size: 0,
            max_total_wal_size: 2 * (1 << 30), // 2 GiB
            cache_size: 1 << 30,               // 1 GB
        }
    }
}

impl Options {
    pub fn build(self) -> std::result::Result<(RocksDBStorage, Writer), BuildError> {
        RocksDBStorage::new(self)
    }
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error("db is locked: {0}")]
    #[code(restate_errors::RT0005)]
    DbLocked(Error),
    #[error(transparent)]
    #[code(unknown)]
    Other(Error),
}

impl BuildError {
    fn from_rocksdb_error(err: Error) -> Self {
        let err_message = err.to_string();

        if err_message.starts_with("IO error: While lock file:")
            && err_message.ends_with("Resource temporarily unavailable")
        {
            BuildError::DbLocked(err)
        } else {
            BuildError::Other(err)
        }
    }
}

#[derive(Clone, Debug)]
pub struct RocksDBStorage {
    db: Arc<DB>,
    writer_handle: WriterHandle,
}

fn db_options(opts: &Options) -> rocksdb::Options {
    let mut db_options = rocksdb::Options::default();
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);
    if opts.threads > 0 {
        db_options.increase_parallelism(opts.threads as i32);
        db_options.set_max_background_jobs(opts.threads as i32);
    }

    db_options.set_atomic_flush(true);
    //
    // Disable WAL archiving.
    // the following two options has to be both 0 to disable WAL log archive.
    //
    db_options.set_wal_size_limit_mb(0);
    db_options.set_wal_ttl_seconds(0);
    //
    // Disable automatic WAL flushing.
    // We will call flush manually, when we commit a storage transaction.
    //
    db_options.set_manual_wal_flush(true);
    //
    // Once the WAL logs exceed this size, rocksdb start will start flush memtables to disk
    // We set this value to 10GB by default, to make sure that we don't flush
    // memtables prematurely.
    db_options.set_max_total_wal_size(opts.max_total_wal_size);
    //
    // write buffer
    //
    // we disable the total write buffer size, since in restate the column family
    // number is static.
    //
    db_options.set_db_write_buffer_size(0);
    //
    // Let rocksdb decide for level sizes.
    //
    db_options.set_level_compaction_dynamic_level_bytes(true);
    db_options.set_compaction_readahead_size(1 << 21);
    //
    // We use Tokio's background threads to access rocksdb, and
    // at most we have 512 threads.
    //
    db_options.set_table_cache_num_shard_bits(9);
    //
    // no need to retain 1000 log files by default.
    //
    db_options.set_keep_log_file_num(1);
    //
    // Allow mmap read and write.
    //
    db_options.set_allow_mmap_reads(true);
    db_options.set_allow_mmap_writes(true);

    db_options
}

fn cf_options(opts: &Options, cache: Option<Cache>) -> rocksdb::Options {
    let mut cf_options = rocksdb::Options::default();
    //
    //
    // write buffer
    //
    if opts.write_buffer_size > 0 {
        cf_options.set_write_buffer_size(opts.write_buffer_size)
    }
    //
    // Most of the changes are highly temporal, we try to delay flushing
    // As much as we can to increase the chances to observe a deletion.
    //
    cf_options.set_max_write_buffer_number(3);
    cf_options.set_min_write_buffer_number_to_merge(2);
    //
    // bloom filters and block cache.
    //
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, true);
    // use the latest Rocksdb table format.
    // https://github.com/facebook/rocksdb/blob/f059c7d9b96300091e07429a60f4ad55dac84859/include/rocksdb/table.h#L275
    block_opts.set_format_version(5);
    if let Some(cache) = cache {
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_block_cache(&cache);
    }
    cf_options.set_block_based_table_factory(&block_opts);
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
    fn new(opts: Options) -> std::result::Result<(Self, Writer), BuildError> {
        let cache = if opts.cache_size > 0 {
            Some(Cache::new_lru_cache(opts.cache_size))
        } else {
            None
        };

        let tables = [
            //
            // keyed by partition key + user key
            //
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Inbox), cf_options(&opts, cache.clone())),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(State), cf_options(&opts, cache.clone())),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Status), cf_options(&opts, cache.clone())),
            rocksdb::ColumnFamilyDescriptor::new(
                cf_name(Journal),
                cf_options(&opts, cache.clone()),
            ),
            //
            // keyed by partition id + suffix
            //
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Outbox), cf_options(&opts, cache.clone())),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Timers), cf_options(&opts, cache.clone())),
            // keyed by partition_id + partition_id
            rocksdb::ColumnFamilyDescriptor::new(
                cf_name(Deduplication),
                cf_options(&opts, cache.clone()),
            ),
            // keyed by partition_id + u64
            rocksdb::ColumnFamilyDescriptor::new(
                cf_name(PartitionStateMachine),
                cf_options(&opts, cache),
            ),
        ];

        let db_options = db_options(&opts);
        let rdb = DB::open_cf_descriptors(&db_options, opts.path, tables)
            .map_err(BuildError::from_rocksdb_error)?;
        let rdb = Arc::new(rdb);

        let rdb2 = Clone::clone(&rdb);
        let writer = Writer::new(rdb2);
        let writer_handle = writer.create_writer_handle();

        Ok((
            Self {
                db: rdb,
                writer_handle,
            },
            writer,
        ))
    }

    #[inline]
    fn table_handle(&self, table: TableKind) -> &ColumnFamily {
        let name = cf_name(table);
        self.db.cf_handle(name).expect(
            "This should not happen, this is a Restate bug. Please contact the restate developers.",
        )
    }

    fn get<K: AsRef<[u8]>>(&self, table: TableKind, key: K) -> Result<Option<DBPinnableSlice>> {
        let table = self.table_handle(table);
        self.db
            .get_pinned_cf(&table, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    pub fn prefix_iterator<K: Into<Vec<u8>>>(&self, table: TableKind, prefix: K) -> DBIterator {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();

        opts.set_iterate_range(PrefixRange(prefix));

        self.db.raw_iterator_cf_opt(&table, opts)
    }

    pub fn range_iterator(
        &self,
        table: TableKind,
        range: impl rocksdb::IterateBounds,
    ) -> DBIterator {
        let table = self.table_handle(table);
        let mut opts = ReadOptions::default();
        opts.set_iterate_range(range);
        self.db.raw_iterator_cf_opt(&table, opts)
    }

    pub fn full_iterator(&self, table: TableKind) -> DBIterator {
        let table = self.table_handle(table);
        self.db.raw_iterator_cf_opt(table, ReadOptions::default())
    }

    fn iterator_from<K: TableKey>(&self, scan: TableScan<K>) -> DBIterator {
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
    pub fn transaction(&self) -> RocksDBTransaction {
        RocksDBTransaction {
            write_batch: Default::default(),
            storage: self,
            key_buffer: Default::default(),
            value_buffer: Default::default(),
        }
    }

    #[inline]
    async fn commit_write_batch(
        &self,
        write_batch: WriteBatch,
    ) -> std::result::Result<(), StorageError> {
        self.writer_handle.write(write_batch).await
    }
}

impl Storage for RocksDBStorage {
    type TransactionType<'a> = RocksDBTransaction<'a>;

    #[allow(clippy::needless_lifetimes)]
    fn transaction(&self) -> Self::TransactionType<'_> {
        RocksDBStorage::transaction(self)
    }
}

pub struct RocksDBTransaction<'a> {
    write_batch: Option<WriteBatch>,
    storage: &'a RocksDBStorage,
    key_buffer: BytesMut,
    value_buffer: BytesMut,
}

pub(crate) struct BackgroundScanStream<T> {
    rx: Option<Receiver<Result<T>>>,
    join_handle: Option<JoinHandle<()>>,
}

impl<T: Send + 'static> BackgroundScanStream<T> {
    fn new(rx: Receiver<Result<T>>, join_handle: JoinHandle<()>) -> Self {
        Self {
            rx: Some(rx),
            join_handle: Some(join_handle),
        }
    }
}

impl<T: Send + 'static> Stream for BackgroundScanStream<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(ref mut res) = self.rx {
            let maybe_value = ready!(res.poll_recv(cx));
            if maybe_value.is_some() {
                return Poll::Ready(maybe_value);
            }
            self.rx = None
        }
        if let Some(ref mut res) = self.join_handle {
            let maybe_join_result = ready!(res.poll_unpin(cx));
            self.join_handle = None;
            if let Err(err) = maybe_join_result {
                return Poll::Ready(Some(Err(StorageError::Generic(err.into()))));
            }
        }
        Poll::Ready(None)
    }
}

impl<'a> RocksDBTransaction<'a> {
    pub fn get_blocking<K, F, R>(&mut self, key: K, f: F) -> GetFuture<'static, R>
    where
        K: TableKey + Send + 'static,
        F: FnOnce(&[u8], Option<&[u8]>) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let mut buf = self.key_buffer(key.serialized_length());
        key.serialize_to(&mut buf);
        let buf = buf.split();

        let res = match self.storage.get(K::table(), &buf) {
            Ok(value) => {
                let slice = value.as_ref().map(|v| v.as_ref());
                f(&buf, slice)
            }
            Err(err) => Err(err),
        };

        ready(res).boxed()
    }

    #[inline]
    pub fn get_first_blocking<K, F, R>(&mut self, scan: TableScan<K>, f: F) -> GetFuture<'static, R>
    where
        K: TableKey + Send + 'static,
        F: FnOnce(Option<(&[u8], &[u8])>) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let iterator = self.storage.iterator_from(scan);
        let result = f(iterator.item());
        ready(result).boxed()
    }

    #[inline]
    pub fn for_each_key_value_in_place<K, F, R>(
        &self,
        scan: TableScan<K>,
        mut op: F,
    ) -> GetStream<'static, R>
    where
        K: TableKey + Send + 'static,
        F: FnMut(&[u8], &[u8]) -> TableScanIterationDecision<R> + Send + 'static,
        R: Send + 'static,
    {
        let mut res = Vec::new();

        let mut iterator = self.storage.iterator_from(scan);
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

        stream::iter(res).boxed()
    }

    pub fn for_each_key_value<K, F, R>(
        &self,
        scan: TableScan<K>,
        mut op: F,
    ) -> GetStream<'static, R>
    where
        K: TableKey + Send + 'static,
        F: FnMut(&[u8], &[u8]) -> TableScanIterationDecision<R> + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<R>>(256);
        let db = Clone::clone(self.storage);

        let background_task = move || {
            let mut iterator = db.iterator_from(scan);
            while let Some((k, v)) = iterator.item() {
                match op(k, v) {
                    TableScanIterationDecision::Emit(result) => {
                        if tx.blocking_send(result).is_err() {
                            return;
                        }
                        iterator.next();
                    }
                    TableScanIterationDecision::BreakWith(result) => {
                        let _ = tx.blocking_send(result);
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
        };

        let join_handle = tokio::task::spawn_blocking(background_task);
        BackgroundScanStream::new(rx, join_handle).boxed()
    }

    #[inline]
    fn key_buffer(&mut self, min_size: usize) -> &mut BytesMut {
        self.key_buffer.clear();
        self.key_buffer.reserve(min_size);
        &mut self.key_buffer
    }

    #[inline]
    fn value_buffer(&mut self, min_size: usize) -> &mut BytesMut {
        self.value_buffer.clear();
        self.value_buffer.reserve(min_size);
        &mut self.value_buffer
    }

    fn put_kv<K: TableKey, V: Codec>(&mut self, key: K, value: V) {
        self.ensure_write_batch();
        {
            let buffer = self.key_buffer(key.serialized_length());
            key.serialize_to(buffer);
        }
        {
            let buffer = self.value_buffer(value.serialized_length());
            value.encode(buffer);
        }
        let table = self.storage.table_handle(K::table());
        self.write_batch
            .as_mut()
            .unwrap()
            .put_cf(&table, &self.key_buffer, &self.value_buffer);
    }

    fn delete_key<K: TableKey>(&mut self, key: &K) {
        self.ensure_write_batch();
        {
            let buffer = self.key_buffer(key.serialized_length());
            key.serialize_to(buffer);
        }
        let table = self.storage.table_handle(K::table());
        self.write_batch
            .as_mut()
            .unwrap()
            .delete_cf(&table, &self.key_buffer);
    }

    #[inline]
    fn ensure_write_batch(&mut self) {
        if self.write_batch.is_none() {
            self.write_batch = Some(WriteBatch::default());
        }
    }
}

impl<'a> Transaction for RocksDBTransaction<'a> {
    fn commit<'b>(self) -> GetFuture<'b, ()>
    where
        Self: 'b,
    {
        if self.write_batch.is_none() {
            return ok(()).boxed();
        }
        let write_batch = self.write_batch.unwrap();
        self.storage.commit_write_batch(write_batch).boxed()
    }
}
