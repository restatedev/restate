pub mod codec;
pub mod deduplication_table;
pub mod fsm_table;
pub mod inbox_table;
pub mod journal_table;
pub mod keys;
pub mod outbox_table;
pub mod scan;
pub mod state_table;
pub mod status_table;
pub mod timer_table;

use crate::codec::Codec;
use crate::keys::TableKey;
use crate::scan::{PhysicalScan, TableScan};
use crate::TableKind::{
    Deduplication, Inbox, Journal, Outbox, PartitionStateMachine, State, Status, Timers,
};
use bytes::BytesMut;
use codederror::CodedError;
use futures::{ready, FutureExt, Stream};
use futures_util::future::ok;
use futures_util::StreamExt;
use restate_storage_api::{GetFuture, GetStream, PutFuture, Storage, StorageError, Transaction};
use rocksdb::DBCompressionType::Lz4;
use rocksdb::{
    ColumnFamily, DBCompressionType, DBPinnableSlice, DBRawIteratorWithThreadMode, PrefixRange,
    ReadOptions, SingleThreaded, WriteBatch,
};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
pub enum CompressionType {
    /// No compression
    None,
    /// Compression using lz4
    Lz4,
    /// Compression using zlib
    Zstd,
}

/// # Storage options
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "StorageOptions"))]
pub struct Options {
    /// # Storage path
    ///
    /// The root path to use for the Rocksdb storage
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_path")
    )]
    pub path: String,

    /// # Threads
    ///
    /// The number of threads to reserve to Rocksdb background tasks
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_threads")
    )]
    pub threads: usize,

    /// # Compression type
    ///
    /// The type of compression to use
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_compression")
    )]
    pub compression: CompressionType,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            path: Options::default_path(),
            threads: Options::default_threads(),
            compression: Options::default_compression(),
        }
    }
}

impl Options {
    fn default_path() -> String {
        "target/db/".to_string()
    }

    fn default_threads() -> usize {
        4
    }

    fn default_compression() -> CompressionType {
        CompressionType::Zstd
    }

    pub fn build(self) -> std::result::Result<RocksDBStorage, BuildError> {
        RocksDBStorage::new(self)
    }
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error("db is locked: {0}")]
    #[code(restate_errors::RT0005)]
    DbLocked(rocksdb::Error),
    #[error(transparent)]
    #[code(unknown)]
    Other(rocksdb::Error),
}

impl BuildError {
    fn from_rocksdb_error(err: rocksdb::Error) -> Self {
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
}

impl RocksDBStorage {
    fn new(opts: Options) -> std::result::Result<Self, BuildError> {
        let mut db_options = rocksdb::Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        if opts.threads > 0 {
            db_options.increase_parallelism(opts.threads as i32);
        }
        db_options.set_allow_concurrent_memtable_write(true);
        match opts.compression {
            CompressionType::Lz4 => db_options.set_compression_type(Lz4),
            CompressionType::Zstd => db_options.set_compression_type(DBCompressionType::Zstd),
            _ => {}
        }
        // TODO: set more rocksdb options from opts.
        let tables = [
            rocksdb::ColumnFamilyDescriptor::new(cf_name(State), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Status), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Inbox), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Outbox), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Deduplication), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(
                cf_name(PartitionStateMachine),
                db_options.clone(),
            ),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Timers), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Journal), db_options.clone()),
        ];

        let db = DB::open_cf_descriptors(&db_options, opts.path, tables)
            .map_err(BuildError::from_rocksdb_error)?;

        Ok(Self { db: Arc::new(db) })
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
}

impl Storage for RocksDBStorage {
    type TransactionType = RocksDBTransaction;

    #[allow(clippy::needless_lifetimes)]
    fn transaction(&self) -> Self::TransactionType {
        RocksDBTransaction {
            write_batch: Default::default(),
            storage: Clone::clone(self),
            key_buffer: Default::default(),
            value_buffer: Default::default(),
        }
    }
}

pub struct RocksDBTransaction {
    write_batch: Option<WriteBatch>,
    storage: RocksDBStorage,
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

impl RocksDBTransaction {
    pub fn get_blocking<K, F, R>(&mut self, key: K, f: F) -> GetFuture<'static, R>
    where
        K: TableKey + Send + 'static,
        F: FnOnce(&[u8], Option<&[u8]>) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let mut buf = self.key_buffer();
        key.serialize_to(&mut buf);
        let buf = buf.split();

        let db = Clone::clone(&self.storage);
        tokio::task::spawn_blocking(move || match db.get(K::table(), &buf) {
            Ok(value) => {
                let slice = value.as_ref().map(|v| v.as_ref());
                f(&buf, slice)
            }
            Err(err) => Err(err),
        })
        .map(|result| match result {
            Ok(internal_result) => internal_result,
            Err(join_error) => Err(StorageError::Generic(join_error.into())),
        })
        .boxed()
    }

    #[inline]
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
        let db = Clone::clone(&self.storage);

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
    fn key_buffer(&mut self) -> &mut BytesMut {
        self.key_buffer.clear();
        &mut self.key_buffer
    }

    #[inline]
    fn value_buffer(&mut self) -> &mut BytesMut {
        self.value_buffer.clear();
        &mut self.value_buffer
    }

    fn put_kv<K: TableKey, V: Codec>(&mut self, key: K, value: V) {
        self.ensure_write_batch();
        {
            let buffer = self.key_buffer();
            key.serialize_to(buffer);
        }
        {
            let buffer = self.value_buffer();
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
            let buffer = self.key_buffer();
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

impl Transaction for RocksDBTransaction {
    fn commit(self) -> GetFuture<'static, ()> {
        if self.write_batch.is_none() {
            return ok(()).boxed();
        }
        let db = Clone::clone(&self.storage.db);
        let write_batch = self.write_batch.unwrap();
        let f = move || {
            // TODO: take into account rather we need to use the WAL or not.
            // for now we always use the WAL when committing the write batch,
            // but once we will have the Raft log persisted else where, this becomes
            // only needed during a checkpoint operation.
            db.write(write_batch)
                .map_err(|error| StorageError::Generic(error.into()))
        };
        tokio::task::spawn_blocking(f)
            .map(|result| match result {
                Ok(db_write_result) => db_write_result,
                Err(join_error) => Err(StorageError::Generic(join_error.into())),
            })
            .boxed()
    }
}
