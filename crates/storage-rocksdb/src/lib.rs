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

use crate::codec::Codec;
use crate::keys::TableKey;
use crate::scan::{PhysicalScan, TableScan};
use crate::writer::{Writer, WriterHandle};
use crate::TableKind::{
    Deduplication, Idempotency, Inbox, InvocationStatus, Journal, Outbox, PartitionStateMachine,
    ServiceStatus, State, Timers,
};
use bytes::BytesMut;
use codederror::CodedError;
use restate_storage_api::{Storage, StorageError, Transaction};
use restate_types::DEFAULT_STORAGE_DIRECTORY;
use rocksdb::Cache;
use rocksdb::ColumnFamily;
use rocksdb::DBCompressionType;
use rocksdb::DBPinnableSlice;
use rocksdb::DBRawIteratorWithThreadMode;
use rocksdb::Error;
use rocksdb::PrefixRange;
use rocksdb::ReadOptions;
use rocksdb::SingleThreaded;
use rocksdb::{BlockBasedOptions, WriteOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use writer::JoinHandle as RocksDBWriterJoinHandle;
pub use writer::Writer as RocksDBWriter;

pub type DB = rocksdb::OptimisticTransactionDB<SingleThreaded>;
type TransactionDB<'a> = rocksdb::Transaction<'a, DB>;

pub type DBIterator<'b> = DBRawIteratorWithThreadMode<'b, DB>;
pub type DBIteratorTransaction<'b> = DBRawIteratorWithThreadMode<'b, rocksdb::Transaction<'b, DB>>;

type WriteBatch = rocksdb::WriteBatchWithTransaction<true>;

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

type StorageFormatVersion = u32;

/// Version of the used storage format. Whenever you introduce a breaking change, this version needs
/// to be incremented in order to distinguish the new from the old versions. Ideally, you also add
/// a migration path from the old to the new format.
const STORAGE_FORMAT_VERSION: StorageFormatVersion = 2;

/// Key under which the storage format version is stored in the default column family of RocksDB.
const STORAGE_FORMAT_VERSION_KEY: &[u8] = b"internal_storage_format_version";

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
    pub path: PathBuf,

    /// # Threads
    ///
    /// The number of threads to reserve to Rocksdb background tasks.
    pub threads: usize,

    /// # Write Buffer size
    ///
    /// The size of a single memtable. Once memtable exceeds this size, it is marked immutable and a new one is created.
    /// The default is set such that 3 column families per table will use a total of 50% of the global memory limit
    /// (`MEMORY_LIMIT`), which defaults to 3GiB, leading to a value of 64MiB with 8 tables.
    pub write_buffer_size: usize,

    /// # Maximum total WAL size
    ///
    /// Max WAL size, that after this Rocksdb start flushing mem tables to disk.
    pub max_total_wal_size: u64,

    /// # Maximum cache size
    ///
    /// The memory size used for rocksdb caches.
    /// The default is roughly 33% of the global memory limit (set with `MEMORY_LIMIT`), which defaults to 3GiB, leading to a value of 1GiB.
    pub cache_size: usize,

    /// Disable rocksdb statistics collection
    pub disable_statistics: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            path: Path::new(DEFAULT_STORAGE_DIRECTORY).join("db"),
            threads: 10,
            write_buffer_size: 0,
            max_total_wal_size: 2 * (1 << 30), // 2 GiB
            cache_size: 0,
            disable_statistics: false,
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
    #[error("db contains incompatible storage format version '{0}'; supported version is '{STORAGE_FORMAT_VERSION}'")]
    #[code(restate_errors::RT0008)]
    IncompatibleStorageFormat(StorageFormatVersion),
    #[error("db contains no storage format version")]
    #[code(restate_errors::RT0009)]
    MissingStorageFormatVersion,
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

pub struct RocksDBStorage {
    db: Arc<DB>,
    writer_handle: WriterHandle,
    key_buffer: BytesMut,
    value_buffer: BytesMut,
    cache: Option<Cache>,
    options: Arc<rocksdb::Options>,
}

impl std::fmt::Debug for RocksDBStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDBStorage")
            .field("db", &self.db)
            .field("writer_handle", &self.writer_handle)
            .field("key_buffer", &self.key_buffer)
            .field("value_buffer", &self.value_buffer)
            .field(
                "cache",
                if self.cache.is_some() {
                    &"<set>"
                } else {
                    &"<unset>"
                },
            )
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
            cache: self.cache.clone(),
            options: self.options.clone(),
        }
    }
}

fn db_options(opts: &Options) -> rocksdb::Options {
    let mut db_options = rocksdb::Options::default();
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);
    if opts.threads > 0 {
        db_options.increase_parallelism(opts.threads as i32);
        db_options.set_max_background_jobs(opts.threads as i32);
    }

    if !opts.disable_statistics {
        db_options.enable_statistics();
        // Reasonable default, but we might expose this as a config in the future.
        db_options.set_statistics_level(rocksdb::statistics::StatsLevel::ExceptDetailedTimers);
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
    /// Returns the raw rocksdb handle, this should only be used for server operations that
    /// require direct access to rocksdb.
    pub fn inner(&self) -> Arc<DB> {
        self.db.clone()
    }
    /// The database options object that was used at creation time, this can be used to extract
    /// running statistics after the database is opened.
    pub fn options(&self) -> Arc<rocksdb::Options> {
        self.options.clone()
    }

    /// Block cache wrapper handle, It's set if block cache is enabled.
    pub fn cache(&self) -> Option<Cache> {
        self.cache.clone()
    }

    fn new(opts: Options) -> std::result::Result<(Self, Writer), BuildError> {
        let cache = if opts.cache_size > 0 {
            Some(Cache::new_lru_cache(opts.cache_size))
        } else {
            None
        };

        let tables = vec![
            //
            // keyed by partition key + user key
            //
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Inbox), cf_options(&opts, cache.clone())),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(State), cf_options(&opts, cache.clone())),
            rocksdb::ColumnFamilyDescriptor::new(
                cf_name(InvocationStatus),
                cf_options(&opts, cache.clone()),
            ),
            rocksdb::ColumnFamilyDescriptor::new(
                cf_name(ServiceStatus),
                cf_options(&opts, cache.clone()),
            ),
            rocksdb::ColumnFamilyDescriptor::new(
                cf_name(Journal),
                cf_options(&opts, cache.clone()),
            ),
            rocksdb::ColumnFamilyDescriptor::new(
                cf_name(Idempotency),
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
                cf_options(&opts, cache.clone()),
            ),
        ];

        let db_options = db_options(&opts);

        let is_empty_db = Self::is_empty_db(&opts.path);

        let rdb = DB::open_cf_descriptors(&db_options, opts.path, tables)
            .map_err(BuildError::from_rocksdb_error)?;

        if is_empty_db {
            Self::write_storage_format_version(&rdb, STORAGE_FORMAT_VERSION)?;
        } else {
            Self::assert_compatible_storage_format_version(&rdb)?;
        }

        let rdb = Arc::new(rdb);

        let rdb2 = Clone::clone(&rdb);
        let writer = Writer::new(rdb2);
        let writer_handle = writer.create_writer_handle();

        Ok((
            Self {
                db: rdb,
                writer_handle,
                key_buffer: BytesMut::default(),
                value_buffer: BytesMut::default(),
                cache,
                options: Arc::new(db_options),
            },
            writer,
        ))
    }

    /// Checks if the RocksDB storage directory is empty and, therefore, the db being empty.
    /// A non existing directory is also considered empty.
    fn is_empty_db(path: impl AsRef<Path>) -> bool {
        let path = path.as_ref();

        !path.exists()
            || path
                .read_dir()
                .expect("RocksDB directory must exist")
                .count()
                == 0
    }

    /// Writes the current storage format version to RocksDB under the
    /// [`STORAGE_FORMAT_VERSION_KEY`].
    fn write_storage_format_version(
        db: &DB,
        storage_format_version: StorageFormatVersion,
    ) -> std::result::Result<(), BuildError> {
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(false);

        db.put_opt(
            STORAGE_FORMAT_VERSION_KEY,
            storage_format_version.to_be_bytes(),
            &write_opts,
        )
        .map_err(BuildError::Other)?;

        db.flush_wal(true).map_err(BuildError::Other)
    }

    /// Asserts that the opened RocksDB contains data that is compatible with the current storage
    /// format version. The storage format version is read from [`STORAGE_FORMAT_VERSION_KEY`] in
    /// the default column family.
    fn assert_compatible_storage_format_version(db: &DB) -> std::result::Result<(), BuildError> {
        let version_bytes = db
            .get(STORAGE_FORMAT_VERSION_KEY)
            .map_err(BuildError::Other)?;

        let version = if let Some(version_bytes) = version_bytes {
            let bytes: [u8; 4] = version_bytes
                .try_into()
                .expect("The storage format version needs to be an u32.");
            u32::from_be_bytes(bytes)
        } else {
            // if storage format version is not present, then the db must originate from Restate < 0.8
            return Err(BuildError::MissingStorageFormatVersion);
        };

        if version != STORAGE_FORMAT_VERSION {
            Err(BuildError::IncompatibleStorageFormat(version))
        } else {
            Ok(())
        }
    }

    fn table_handle(&self, table_kind: TableKind) -> &ColumnFamily {
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
        self.db.put_cf(table, key, value).unwrap();
    }

    #[inline]
    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.db.delete_cf(table, key).unwrap();
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

    fn table_handle(&self, table_kind: TableKind) -> &ColumnFamily {
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
        self.txn.put_cf(table, key, value).unwrap();
    }

    #[inline]
    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) {
        let table = self.table_handle(table);
        self.txn.delete_cf(table, key).unwrap();
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
    fn put_kv<K: TableKey, V: Codec>(&mut self, key: K, value: V) {
        let key_buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buffer);
        let key_buffer = key_buffer.split();

        let value_buffer = self.cleared_value_buffer_mut(value.serialized_length());
        value.encode(value_buffer);
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
    fn get_first_blocking<K, F, R>(&mut self, scan: TableScan<K>, f: F) -> Result<R>
    where
        K: TableKey,
        F: FnOnce(Option<(&[u8], &[u8])>) -> Result<R>,
    {
        let iterator = self.iterator_from(scan);
        f(iterator.item())
    }

    #[inline]
    fn get_blocking<K, F, R>(&mut self, key: K, f: F) -> Result<R>
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

#[cfg(test)]
mod tests {
    use crate::{BuildError, RocksDBStorage, STORAGE_FORMAT_VERSION};
    use googletest::matchers::eq;
    use googletest::{assert_that, pat};
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn non_existing_directory_is_empty_db() {
        let non_existing_directory = tempdir().unwrap().path().join("should_not_exist");
        assert!(RocksDBStorage::is_empty_db(non_existing_directory))
    }

    #[test]
    fn empty_directory_is_empty_db() {
        let empty_directory = tempdir().unwrap();
        assert!(RocksDBStorage::is_empty_db(empty_directory))
    }

    #[test]
    fn non_empty_directory_is_not_empty_db() {
        let directory = tempdir().unwrap();
        std::fs::File::create(directory.path().join("empty")).unwrap();

        assert!(!RocksDBStorage::is_empty_db(directory))
    }

    #[tokio::test]
    async fn incompatible_storage_format_version() -> Result<(), anyhow::Error> {
        //
        // create a rocksdb storage from options
        //
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.into_path();

        let opts = crate::Options {
            path,
            ..Default::default()
        };
        let (rocksdb, _) = opts
            .clone()
            .build()
            .expect("RocksDB storage creation should succeed");

        // fake a different storage format version
        let incompatible_version = STORAGE_FORMAT_VERSION + 1;
        RocksDBStorage::write_storage_format_version(&rocksdb.db, incompatible_version)?;

        drop(rocksdb);

        assert_that!(
            opts.build().err().unwrap(),
            pat!(BuildError::IncompatibleStorageFormat(eq(
                incompatible_version
            )))
        );

        Ok(())
    }
}
