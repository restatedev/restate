use crate::TableKind::{Dedup, Fsm, Inbox, Outbox, State, Timers};
use rocksdb::{WriteBatch};
use std::sync::Arc;
use tracing::info;

type DB = rocksdb::DBWithThreadMode<rocksdb::SingleThreaded>;

pub enum TableKind {
    State,
    Inbox,
    Outbox,
    Dedup,
    Fsm,
    Timers,
}

const STATE_TABLE_NAME: &str = "state";
const INBOX_TABLE_NAME: &str = "inbox";
const OUTBOX_TABLE_NAME: &str = "outbox";
const DEDUP_TABLE_NAME: &str = "dedup";
const FSM_TABLE_NAME: &str = "fsm";
const TIMERS_TABLE_NAME: &str = "timers";

impl From<TableKind> for &'static str {
    fn from(kind: TableKind) -> Self {
        match kind {
            State => STATE_TABLE_NAME,
            Inbox => INBOX_TABLE_NAME,
            Outbox => OUTBOX_TABLE_NAME,
            Dedup => DEDUP_TABLE_NAME,
            Fsm => FSM_TABLE_NAME,
            Timers => TIMERS_TABLE_NAME,
        }
    }
}

impl From<TableKind> for String {
    fn from(value: TableKind) -> Self {
        let s: &'static str = value.into();
        String::from(s)
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
    pub fn build(self) -> Storage {
        Storage::new(self)
    }
}

#[derive(Clone, Debug)]
pub struct Storage {
    db: Arc<DB>,
}

pub trait StorageDeserializer {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self;
}

impl Storage {
    fn new(opts: Options) -> Self {
        let Options { path, .. } = opts;

        let mut db_options = rocksdb::Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        //
        // TODO: set rocksdb options from opts.
        //
        let tables = [
            rocksdb::ColumnFamilyDescriptor::new(State, db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(Inbox, db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(Outbox, db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(Dedup, db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(Fsm, db_options.clone()),
            rocksdb::ColumnFamilyDescriptor::new(Timers, db_options.clone()),
        ];

        let db = DB::open_cf_descriptors(&db_options, &path, tables)
            .expect("unable to open the database");

        info!(?path, "Database opened successfully.");

        Self { db: Arc::new(db) }
    }

    pub fn get<K: AsRef<[u8]>, V: StorageDeserializer>(
        &self,
        table: TableKind,
        key: K,
    ) -> Option<V> {
        let table = self.table_handle(table);
        self
            .db
            .get_pinned_cf(&table, key)
            .expect("Unexpected database error")
        .map(|slice| V::from_bytes(slice.as_ref()))

    }

    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, table: TableKind, key: K, value: V) {
        let table = self.table_handle(table);
        self.db
            .put_cf(&table, key, value)
            .expect("Unexpected database error");
    }

    pub fn scan_prefix_into<P: AsRef<[u8]>, KD: StorageDeserializer, VD: StorageDeserializer>(
        &self,
        table: TableKind,
        key_prefix: P,
        mut storage: Vec<(KD, VD)>,
    ) {
        let prefix = key_prefix.as_ref();
        let table = self.table_handle(table);
        let iterator = self
            .db
            .prefix_iterator_cf(&table, prefix.as_ref())
            .map(|kv| kv.expect("Unexpected database error"))
            .take_while(|kv| kv.0.starts_with(prefix))
            .map(|(k, v)| (KD::from_bytes(k), VD::from_bytes(v)));

        storage.extend(iterator);
    }

    pub fn transaction(&self) -> WriteTransaction {
        WriteTransaction {
            write_batch: WriteBatch::default(),
            storage: self,
        }
    }

    fn table_handle(&self, table: TableKind) -> impl rocksdb::AsColumnFamilyRef + '_ {
        self.db
            .cf_handle(table.into())
            .expect("missing table name.")
    }
}

pub struct WriteTransaction<'a> {
    write_batch: WriteBatch,
    storage: &'a Storage,
}

impl<'a> WriteTransaction<'a> {
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, table: TableKind, key: K, value: V) {
        let table = self.storage.table_handle(table);
        self.write_batch.put_cf(&table, key, value);
    }

    pub fn delete(&mut self, table: TableKind, key: impl AsRef<[u8]>) {
        let table = self.storage.table_handle(table);
        self.write_batch.delete_cf(&table, key);
    }

    pub fn reset(&mut self) {
        self.write_batch.clear();
    }

    pub fn commit(self) {
        self.storage
            .db
            .write(self.write_batch)
            .expect("Unexpected database error");
    }
}
