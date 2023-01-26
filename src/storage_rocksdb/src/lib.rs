use std::sync::Arc;

type DB = rocksdb::DBWithThreadMode<rocksdb::SingleThreaded>;

#[derive(Debug, clap::Parser)]
#[group(skip)]
pub struct Options {
    ///  Storage path
    #[arg(
    long = "worker-storage-path",
    env = "WORKER_STORAGE_PATH",
    default_value = "db/"
    )]
    path: String,
}

#[derive(Clone, Debug)]
pub struct Storage {
    db: Arc<DB>,
}

impl Storage {}
