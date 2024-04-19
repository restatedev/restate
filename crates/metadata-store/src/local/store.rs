// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use bytestring::ByteString;
use codederror::CodedError;
use restate_core::cancellation_watcher;
use restate_core::metadata_store::{Precondition, VersionedValue};
use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, Owner, RocksDbManager, RocksError,
};
use restate_types::arc_util::Updateable;
use restate_types::config::RocksDbOptions;
use restate_types::errors::GenericError;
use restate_types::Version;
use rocksdb::{BoundColumnFamily, Options, WriteOptions, DB};
use serde::de::DeserializeOwned;
use serde::Serialize;
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
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("codec error: {0}")]
    Codec(GenericError),
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
    write_opts: WriteOptions,
    request_rx: RequestReceiver,

    // for creating other senders
    request_tx: RequestSender,
}

impl LocalMetadataStore {
    pub fn new(
        data_dir: impl AsRef<Path>,
        request_queue_length: usize,
        rocksdb_options: impl Updateable<RocksDbOptions> + Send + 'static,
    ) -> std::result::Result<Self, BuildError> {
        let (request_tx, request_rx) = mpsc::channel(request_queue_length);

        let db_manager = RocksDbManager::get();
        let cfs = vec![CfName::new(KV_PAIRS)];
        let db_spec = DbSpecBuilder::new(
            DbName::new(DB_NAME),
            Owner::MetadataStore,
            data_dir.as_ref().to_path_buf(),
            Options::default(),
        )
        .add_cf_pattern(CfPrefixPattern::ANY, |opts| opts)
        .ensure_column_families(cfs)
        .build_as_db();

        let db = db_manager.open_db(rocksdb_options, db_spec)?;

        Ok(Self {
            db,
            write_opts: Self::default_write_options(),
            request_rx,
            request_tx,
        })
    }

    fn default_write_options() -> WriteOptions {
        let mut write_opts = WriteOptions::default();

        // make sure that we write to wal and sync to disc
        write_opts.disable_wal(false);
        write_opts.set_sync(true);

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

    async fn handle_request(&self, request: MetadataStoreRequest) {
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
                let result = self.put(&key, &value, precondition);
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

    fn put(
        &self,
        key: &ByteString,
        value: &VersionedValue,
        precondition: Precondition,
    ) -> Result<()> {
        match precondition {
            Precondition::None => Ok(self.write_versioned_kv_pair(key, value)?),
            Precondition::DoesNotExist => {
                let current_version = self.get_version(key)?;
                if current_version.is_none() {
                    Ok(self.write_versioned_kv_pair(key, value)?)
                } else {
                    Err(Error::kv_pair_exists())
                }
            }
            Precondition::MatchesVersion(version) => {
                let current_version = self.get_version(key)?;
                if current_version == Some(version) {
                    Ok(self.write_versioned_kv_pair(key, value)?)
                } else {
                    Err(Error::version_mismatch(version, current_version))
                }
            }
        }
    }

    fn write_versioned_kv_pair(&self, key: &ByteString, value: &VersionedValue) -> Result<()> {
        let cf_handle = self.kv_cf_handle();
        let versioned_value = Self::encode(value)?;
        self.db
            .put_cf_opt(&cf_handle, key, versioned_value, &self.write_opts)?;
        Ok(())
    }

    fn delete(&self, key: &ByteString, precondition: Precondition) -> Result<()> {
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

    fn delete_kv_pair(&self, key: &ByteString) -> Result<()> {
        self.db
            .delete_cf_opt(&self.kv_cf_handle(), key, &self.write_opts)
            .map_err(Into::into)
    }

    fn encode<T: Serialize>(value: T) -> Result<Bytes> {
        // todo: Add version information
        bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map(Into::into)
            .map_err(|err| Error::Codec(err.into()))
    }

    fn decode<T: DeserializeOwned>(bytes: impl AsRef<[u8]>) -> Result<T> {
        // todo: Add version information
        bincode::serde::decode_from_slice(bytes.as_ref(), bincode::config::standard())
            .map(|(value, _)| value)
            .map_err(|err| Error::Codec(err.into()))
    }

    fn log_error<T>(result: &Result<T>, request: &str) {
        if let Err(err) = &result {
            debug!("failed to process request '{}': '{}'", request, err)
        }
    }
}
