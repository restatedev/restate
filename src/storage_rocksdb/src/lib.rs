use rocksdb::WriteBatch;
use std::sync::Arc;
use storage_api::TableKind::{Deduplication, Inbox, Outbox, PartitionStateMachine, State, Timers};
use storage_api::{Storage, StorageDeserializer, StorageReader, TableKind, WriteTransaction};

type DB = rocksdb::DBWithThreadMode<rocksdb::SingleThreaded>;

const STATE_TABLE_NAME: &str = "state";
const INBOX_TABLE_NAME: &str = "inbox";
const OUTBOX_TABLE_NAME: &str = "outbox";
const DEDUP_TABLE_NAME: &str = "dedup";
const FSM_TABLE_NAME: &str = "fsm";
const TIMERS_TABLE_NAME: &str = "timers";

fn cf_name(kind: TableKind) -> &'static str {
    match kind {
        State => STATE_TABLE_NAME,
        Inbox => INBOX_TABLE_NAME,
        Outbox => OUTBOX_TABLE_NAME,
        Deduplication => DEDUP_TABLE_NAME,
        PartitionStateMachine => FSM_TABLE_NAME,
        Timers => TIMERS_TABLE_NAME,
    }
}

#[derive(Debug, clap::Parser)]
#[group(skip)]
pub struct Options {
    ///  Storage path
    #[arg(
        long = "worker-storage-path",
        env = "WORKER_STORAGE_PATH",
        default_value = "target/db/"
    )]
    path: String,
}

impl Options {
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
        let Options { path, .. } = opts;

        let mut db_options = rocksdb::Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        //
        // TODO: set rocksdb options from opts.
        //
        let tables = [
            rocksdb::ColumnFamilyDescriptor::new(cf_name(State), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Inbox), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Outbox), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Deduplication), db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(
                cf_name(PartitionStateMachine),
                db_options.clone(),
            ),
            rocksdb::ColumnFamilyDescriptor::new(cf_name(Timers), db_options.clone()),
        ];

        let db = DB::open_cf_descriptors(&db_options, path, tables)
            .expect("Unable to open the database");

        Self { db: Arc::new(db) }
    }

    fn table_handle(&self, table: TableKind) -> impl rocksdb::AsColumnFamilyRef + '_ {
        let name = cf_name(table);
        self.db.cf_handle(name).expect("missing table name.")
    }
}

impl StorageReader for RocksDBStorage {
    fn get<K: AsRef<[u8]>, V: StorageDeserializer>(&self, table: TableKind, key: K) -> Option<V> {
        let table = self.table_handle(table);
        self.db
            .get_pinned_cf(&table, key)
            .expect("Unexpected database error")
            .map(|slice| V::from_bytes(slice.as_ref()))
    }

    fn copy_prefix_into<P, K, V>(
        &self,
        table: TableKind,
        start_key: P,
        start_key_prefix_len: usize,
        target: &mut Vec<(K, V)>,
    ) where
        P: AsRef<[u8]>,
        K: StorageDeserializer,
        V: StorageDeserializer,
    {
        let start = start_key.as_ref();
        let prefix = &start[..start_key_prefix_len];
        let table = self.table_handle(table);

        let mut iterator = self.db.raw_iterator_cf(&table);
        iterator.seek(start);
        while let Some((k, v)) = iterator.item() {
            if !k.starts_with(prefix) {
                break;
            }
            target.push((K::from_bytes(k), V::from_bytes(v)));
            iterator.next();
        }
    }
}

impl Storage for RocksDBStorage {
    type WriteTransactionType<'a> = RocksDBWriteTransaction<'a>;

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, table: TableKind, key: K, value: V) {
        let table = self.table_handle(table);
        self.db
            .put_cf(&table, key, value)
            .expect("Unexpected database error");
    }

    #[allow(clippy::needless_lifetimes)]
    fn transaction<'a>(&'a self) -> Self::WriteTransactionType<'a> {
        RocksDBWriteTransaction {
            write_batch: WriteBatch::default(),
            storage: self,
        }
    }
}

pub struct RocksDBWriteTransaction<'a> {
    write_batch: WriteBatch,
    storage: &'a RocksDBStorage,
}

impl<'a> WriteTransaction<'a> for RocksDBWriteTransaction<'a> {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, table: TableKind, key: K, value: V) {
        let table = self.storage.table_handle(table);
        self.write_batch.put_cf(&table, key, value);
    }

    fn delete(&mut self, table: TableKind, key: impl AsRef<[u8]>) {
        let table = self.storage.table_handle(table);
        self.write_batch.delete_cf(&table, key);
    }

    fn commit(self) {
        self.storage
            .db
            .write(self.write_batch)
            .expect("Unexpected database error");
    }
}
