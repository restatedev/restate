mod composite_keys;
pub mod deduplication_table;
pub mod fsm_table;
pub mod inbox_table;
pub mod journal_table;
pub mod outbox_table;
pub mod state_table;
pub mod status_table;
pub mod timer_table;

use crate::TableKind::{
    Deduplication, Inbox, Journal, Outbox, PartitionStateMachine, State, Status, Timers,
};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{ready, FutureExt, Stream};
use futures_util::future::ok;
use futures_util::StreamExt;
use rocksdb::{
    ColumnFamily, DBCompressionType, DBPinnableSlice, DBRawIteratorWithThreadMode, PrefixRange,
    ReadOptions, SingleThreaded, WriteBatch,
};
use std::pin::Pin;

use restate_storage_api::{GetFuture, GetStream, PutFuture, Storage, StorageError, Transaction};
use rocksdb::DBCompressionType::Lz4;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

type DB = rocksdb::DBWithThreadMode<SingleThreaded>;
type DBIterator<'b> = DBRawIteratorWithThreadMode<'b, DB>;

const STATE_TABLE_NAME: &str = "state";
const STATUS_TABLE_NAME: &str = "status";
const INBOX_TABLE_NAME: &str = "inbox";
const OUTBOX_TABLE_NAME: &str = "outbox";
const DEDUP_TABLE_NAME: &str = "dedup";
const FSM_TABLE_NAME: &str = "fsm";
const TIMERS_TABLE_NAME: &str = "timers";
const JOURNAL_TABLE_NAME: &str = "journal";

pub(crate) type Result<T> = std::result::Result<T, StorageError>;

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

#[derive(Debug, Clone, Eq, PartialEq)]
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
#[serde(rename_all = "camelCase")]
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

    pub fn build(self) -> RocksDBStorage {
        RocksDBStorage::new(self)
    }
}

#[derive(Clone, Debug)]
pub struct RocksDBStorage {
    db: Arc<DB>,
}

impl RocksDBStorage {
    fn new(opts: Options) -> Self {
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
            .expect("Unable to open the database");

        Self { db: Arc::new(db) }
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

    fn get_owned<K: AsRef<[u8]>>(&self, table: TableKind, key: K) -> Result<Option<Bytes>> {
        let bytes = self
            .get(table, key)?
            .map(|slice| Bytes::copy_from_slice(slice.as_ref()));

        Ok(bytes)
    }

    fn get_proto<M: prost::Message + Default>(
        &self,
        table: TableKind,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<M>> {
        let maybe_slice = self.get(table, key)?;
        if maybe_slice.is_none() {
            return Ok(None);
        }
        let slice = maybe_slice.unwrap();
        let decoded_message =
            M::decode(slice.as_ref()).map_err(|error| StorageError::Generic(error.into()))?;
        Ok(Some(decoded_message))
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
    #[inline]
    fn spawn_blocking<F, R>(&self, f: F) -> GetFuture<'static, R>
    where
        F: FnOnce(RocksDBStorage) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let db = Clone::clone(&self.storage);

        tokio::task::spawn_blocking(move || f(db))
            .map(|result| match result {
                Ok(internal_error) => internal_error,
                Err(join_error) => Err(StorageError::Generic(join_error.into())),
            })
            .boxed()
    }

    #[inline]
    fn spawn_background_scan<F, R>(&self, f: F) -> GetStream<'static, R>
    where
        F: FnOnce(RocksDBStorage, Sender<Result<R>>) + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<R>>(256);
        let db = Clone::clone(&self.storage);
        let join_handle = tokio::task::spawn_blocking(move || f(db, tx));

        BackgroundScanStream::new(rx, join_handle).boxed()
    }

    fn key_buffer(&mut self) -> &mut BytesMut {
        self.key_buffer.clear();
        &mut self.key_buffer
    }

    fn clone_key_buffer(&mut self) -> BytesMut {
        self.key_buffer.split()
    }

    fn value_buffer(&mut self) -> &mut BytesMut {
        self.value_buffer.clear();
        &mut self.value_buffer
    }

    fn put_kv_buffer(&mut self, table: TableKind) {
        self.ensure_write_batch();
        let table = self.storage.table_handle(table);
        self.write_batch
            .as_mut()
            .unwrap()
            .put_cf(&table, &self.key_buffer, &self.value_buffer);
    }

    fn put_value_using_key_buffer<V: AsRef<[u8]>>(&mut self, table: TableKind, value: V) {
        self.ensure_write_batch();
        let table = self.storage.table_handle(table);
        self.write_batch
            .as_mut()
            .unwrap()
            .put_cf(&table, &self.key_buffer, value);
    }

    fn delete_key_buffer(&mut self, table: TableKind) {
        self.ensure_write_batch();
        let table = self.storage.table_handle(table);
        self.write_batch
            .as_mut()
            .unwrap()
            .delete_cf(&table, &self.key_buffer);
    }

    fn put_kv<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, table: TableKind, key: K, value: V) {
        self.ensure_write_batch();
        let table = self.storage.table_handle(table);
        self.write_batch
            .as_mut()
            .unwrap()
            .put_cf(&table, key, value);
    }

    fn delete_key(&mut self, table: TableKind, key: impl AsRef<[u8]>) {
        self.ensure_write_batch();
        let table = self.storage.table_handle(table);
        self.write_batch.as_mut().unwrap().delete_cf(&table, key);
    }

    fn delete_range<K: AsRef<[u8]>>(&mut self, table: TableKind, low: K, high: K) {
        self.ensure_write_batch();
        let table = self.storage.table_handle(table);
        self.write_batch
            .as_mut()
            .unwrap()
            .delete_range_cf(&table, low, high);
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

pub(crate) fn write_proto_infallible<M: prost::Message, T: BufMut>(target: &mut T, message: M) {
    message
        .encode(target)
        .expect("Unable to serialize a protobuf message");
}
