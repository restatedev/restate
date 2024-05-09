// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{BufMut, BytesMut};
use bytestring::ByteString;
use codederror::CodedError;
use restate_core::cancellation_watcher;
use restate_core::metadata_store::{Precondition, VersionedValue};
use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, IoMode, Priority, RocksDb, RocksDbManager,
    RocksError,
};
use restate_types::arc_util::Updateable;
use restate_types::config::RocksDbOptions;
use restate_types::storage::{
    StorageCodec, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
};
use restate_types::Version;
use rocksdb::{BoundColumnFamily, Options, WriteBatch, WriteOptions, DB};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace};

pub type RequestSender = mpsc::Sender<MetadataStoreRequest>;
pub type RequestReceiver = mpsc::Receiver<MetadataStoreRequest>;

type Result<T> = std::result::Result<T, Error>;

const DB_NAME: &str = "local-metadata-store";
const KV_PAIRS: &str = "kv_pairs";

#[derive(Debug)]
pub enum MetadataStoreRequest {
    Get {
        key: ByteString,
        result_tx: oneshot::Sender<Result<Option<VersionedValue>>>,
    },
    GetVersion {
        key: ByteString,
        result_tx: oneshot::Sender<Result<Option<Version>>>,
    },
    Put {
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
        result_tx: oneshot::Sender<Result<()>>,
    },
    Delete {
        key: ByteString,
        precondition: Precondition,
        result_tx: oneshot::Sender<Result<()>>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] rocksdb::Error),
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] RocksError),
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("encode error: {0}")]
    Encode(#[from] StorageEncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] StorageDecodeError),
}

impl Error {
    fn kv_pair_exists() -> Self {
        Error::FailedPrecondition("key-value pair already exists".to_owned())
    }

    fn version_mismatch(expected: Version, actual: Option<Version>) -> Self {
        Error::FailedPrecondition(format!(
            "Expected version '{}' but found version '{:?}'",
            expected, actual
        ))
    }
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error("failed opening rocksdb: {0}")]
    #[code(unknown)]
    RocksDB(#[from] RocksError),
}

/// Single node metadata store which stores the key value pairs in RocksDB.
///
/// In order to avoid issues arising from concurrency, we run the metadata
/// store in a single thread.
pub struct LocalMetadataStore {
    db: Arc<DB>,
    rocksdb: Arc<RocksDb>,
    opts: Box<dyn Updateable<RocksDbOptions> + Send + 'static>,
    request_rx: RequestReceiver,
    buffer: BytesMut,

    // for creating other senders
    request_tx: RequestSender,
}

impl LocalMetadataStore {
    pub fn new<F, V>(
        data_dir: impl AsRef<Path>,
        request_queue_length: usize,
        rocksdb_options: F,
    ) -> std::result::Result<Self, BuildError>
    where
        F: Fn() -> V,
        V: Updateable<RocksDbOptions> + Send + 'static,
    {
        let (request_tx, request_rx) = mpsc::channel(request_queue_length);

        let db_name = DbName::new(DB_NAME);
        let db_manager = RocksDbManager::get();
        let cfs = vec![CfName::new(KV_PAIRS)];
        let db_spec = DbSpecBuilder::new(
            db_name.clone(),
            data_dir.as_ref().to_path_buf(),
            Options::default(),
        )
        .add_cf_pattern(CfPrefixPattern::ANY, |opts| opts)
        .ensure_column_families(cfs)
        .build_as_db();

        let db = db_manager.open_db(rocksdb_options(), db_spec)?;
        let rocksdb = db_manager
            .get_db(db_name)
            .expect("metadata store db is open");

        Ok(Self {
            db,
            rocksdb,
            opts: Box::new(rocksdb_options()),
            buffer: BytesMut::default(),
            request_rx,
            request_tx,
        })
    }

    fn write_options(&mut self) -> WriteOptions {
        let rocks_db_options = self.opts.load();
        let mut write_opts = WriteOptions::default();

        write_opts.disable_wal(rocks_db_options.rocksdb_disable_wal());

        if !rocks_db_options.rocksdb_disable_wal() {
            // always sync if we have wal enabled
            write_opts.set_sync(true);
        }

        write_opts
    }

    pub fn request_sender(&self) -> RequestSender {
        self.request_tx.clone()
    }

    pub async fn run(mut self) {
        debug!("Running LocalMetadataStore");

        loop {
            tokio::select! {
                request = self.request_rx.recv() => {
                    let request = request.expect("receiver should not be closed since we own one clone.");
                    self.handle_request(request).await;
                }
                _ = cancellation_watcher() => {
                    break;
                },
            }
        }

        debug!("Stopped LocalMetadataStore");
    }

    fn kv_cf_handle(&self) -> Arc<BoundColumnFamily> {
        self.db
            .cf_handle(KV_PAIRS)
            .expect("KV_PAIRS column family exists")
    }

    async fn handle_request(&mut self, request: MetadataStoreRequest) {
        trace!("Handle request '{:?}'", request);

        match request {
            MetadataStoreRequest::Get { key, result_tx } => {
                let result = self.get(&key);
                Self::log_error(&result, "Get");
                let _ = result_tx.send(result);
            }
            MetadataStoreRequest::GetVersion { key, result_tx } => {
                let result = self.get_version(&key);
                Self::log_error(&result, "GetVersion");
                let _ = result_tx.send(result);
            }
            MetadataStoreRequest::Put {
                key,
                value,
                precondition,
                result_tx,
            } => {
                let result = self.put(&key, &value, precondition).await;
                Self::log_error(&result, "Put");
                let _ = result_tx.send(result);
            }
            MetadataStoreRequest::Delete {
                key,
                precondition,
                result_tx,
            } => {
                let result = self.delete(&key, precondition);
                Self::log_error(&result, "Delete");
                let _ = result_tx.send(result);
            }
        };
    }

    fn get(&self, key: &ByteString) -> Result<Option<VersionedValue>> {
        let cf_handle = self.kv_cf_handle();
        let slice = self.db.get_pinned_cf(&cf_handle, key)?;

        if let Some(bytes) = slice {
            Ok(Some(Self::decode(bytes)?))
        } else {
            Ok(None)
        }
    }

    fn get_version(&self, key: &ByteString) -> Result<Option<Version>> {
        let cf_handle = self.kv_cf_handle();
        let slice = self.db.get_pinned_cf(&cf_handle, key)?;

        if let Some(bytes) = slice {
            // todo only deserialize the version part
            let versioned_value = Self::decode::<VersionedValue>(bytes)?;
            Ok(Some(versioned_value.version))
        } else {
            Ok(None)
        }
    }

    async fn put(
        &mut self,
        key: &ByteString,
        value: &VersionedValue,
        precondition: Precondition,
    ) -> Result<()> {
        match precondition {
            Precondition::None => Ok(self.write_versioned_kv_pair(key, value).await?),
            Precondition::DoesNotExist => {
                let current_version = self.get_version(key)?;
                if current_version.is_none() {
                    Ok(self.write_versioned_kv_pair(key, value).await?)
                } else {
                    Err(Error::kv_pair_exists())
                }
            }
            Precondition::MatchesVersion(version) => {
                let current_version = self.get_version(key)?;
                if current_version == Some(version) {
                    Ok(self.write_versioned_kv_pair(key, value).await?)
                } else {
                    Err(Error::version_mismatch(version, current_version))
                }
            }
        }
    }

    async fn write_versioned_kv_pair(
        &mut self,
        key: &ByteString,
        value: &VersionedValue,
    ) -> Result<()> {
        self.buffer.clear();
        Self::encode(value, &mut self.buffer)?;

        let write_options = self.write_options();
        let cf_handle = self.kv_cf_handle();
        let mut wb = WriteBatch::default();
        wb.put_cf(&cf_handle, key, self.buffer.as_ref());
        Ok(self
            .rocksdb
            .write_batch(Priority::High, IoMode::default(), write_options, wb)
            .await?)
    }

    fn delete(&mut self, key: &ByteString, precondition: Precondition) -> Result<()> {
        match precondition {
            Precondition::None => self.delete_kv_pair(key),
            // this condition does not really make sense for the delete operation
            Precondition::DoesNotExist => {
                let current_version = self.get_version(key)?;

                if current_version.is_none() {
                    // nothing to do
                    Ok(())
                } else {
                    Err(Error::kv_pair_exists())
                }
            }
            Precondition::MatchesVersion(version) => {
                let current_version = self.get_version(key)?;

                if current_version == Some(version) {
                    self.delete_kv_pair(key)
                } else {
                    Err(Error::version_mismatch(version, current_version))
                }
            }
        }
    }

    fn delete_kv_pair(&mut self, key: &ByteString) -> Result<()> {
        let write_options = self.write_options();
        self.db
            .delete_cf_opt(&self.kv_cf_handle(), key, &write_options)
            .map_err(Into::into)
    }

    fn encode<T: StorageEncode, B: BufMut>(value: T, buf: &mut B) -> Result<()> {
        StorageCodec::encode(value, buf)?;
        Ok(())
    }

    fn decode<T: StorageDecode>(buf: impl AsRef<[u8]>) -> Result<T> {
        let value = StorageCodec::decode(&mut buf.as_ref())?;
        Ok(value)
    }

    fn log_error<T>(result: &Result<T>, request: &str) {
        if let Err(err) = &result {
            debug!("failed to process request '{}': '{}'", request, err)
        }
    }
}
