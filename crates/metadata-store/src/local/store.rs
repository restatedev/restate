// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BytesMut;
use bytestring::ByteString;
use restate_core::metadata_store::{Precondition, VersionedValue};
use restate_core::{cancellation_watcher, ShutdownError};
use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, IoMode, Priority, RocksDb, RocksDbManager,
    RocksError,
};
use restate_types::config::{MetadataStoreOptions, RocksDbOptions};
use restate_types::errors::GenericError;
use restate_types::live::BoxedLiveLoad;
use restate_types::logs::metadata::Logs;
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY,
};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::storage::{
    StorageCodec, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
};
use restate_types::{Version, Versioned};
use rocksdb::{BoundColumnFamily, DBCompressionType, WriteBatch, WriteOptions, DB};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace};

pub type RequestSender = mpsc::Sender<MetadataStoreRequest>;
pub type RequestReceiver = mpsc::Receiver<MetadataStoreRequest>;

pub type ProvisionSender = mpsc::Sender<ProvisionMetadataStoreRequest>;
pub type ProvisionReceiver = mpsc::Receiver<ProvisionMetadataStoreRequest>;

type Result<T, E = Error> = std::result::Result<T, E>;

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
            "Expected version '{expected}' but found version '{actual:?}'"
        ))
    }
}

/// Single node metadata store which stores the key value pairs in RocksDB.
///
/// In order to avoid issues arising from concurrency, we run the metadata
/// store in a single thread.
pub struct LocalMetadataStore {
    db: Arc<DB>,
    rocksdb: Arc<RocksDb>,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    request_rx: RequestReceiver,
    provision_rx: ProvisionReceiver,
    buffer: BytesMut,

    // for creating other senders
    request_tx: RequestSender,
    provision_tx: ProvisionSender,
}

impl LocalMetadataStore {
    pub async fn create(
        options: &MetadataStoreOptions,
        updateable_rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    ) -> std::result::Result<Self, RocksError> {
        let (request_tx, request_rx) = mpsc::channel(options.request_queue_length());
        let (provision_tx, provision_rx) = mpsc::channel(1);

        let db_name = DbName::new(DB_NAME);
        let db_manager = RocksDbManager::get();
        let cfs = vec![CfName::new(KV_PAIRS)];
        let db_spec = DbSpecBuilder::new(db_name.clone(), options.data_dir(), db_options(options))
            .add_cf_pattern(
                CfPrefixPattern::ANY,
                cf_options(options.rocksdb_memory_budget()),
            )
            .ensure_column_families(cfs)
            .build()
            .expect("valid spec");

        let db = db_manager
            .open_db(updateable_rocksdb_options.clone(), db_spec)
            .await?;
        let rocksdb = db_manager
            .get_db(db_name)
            .expect("metadata store db is open");

        Ok(Self {
            db,
            rocksdb,
            rocksdb_options: updateable_rocksdb_options,
            buffer: BytesMut::default(),
            request_rx,
            request_tx,
            provision_tx,
            provision_rx,
        })
    }

    fn write_options(&mut self) -> WriteOptions {
        let opts = self.rocksdb_options.live_load();
        let mut write_opts = WriteOptions::default();

        write_opts.disable_wal(opts.rocksdb_disable_wal());

        if !opts.rocksdb_disable_wal() {
            // always sync if we have wal enabled
            write_opts.set_sync(true);
        }

        write_opts
    }

    pub fn request_sender(&self) -> RequestSender {
        self.request_tx.clone()
    }

    pub fn provision_handle(&self) -> ProvisionHandle {
        ProvisionHandle {
            tx: self.provision_tx.clone(),
        }
    }

    pub async fn run(mut self) {
        debug!("Running LocalMetadataStore");

        loop {
            tokio::select! {
                request = self.request_rx.recv() => {
                    let request = request.expect("receiver should not be closed since we own one clone.");
                    self.handle_request(request).await;
                },
                provision_request = self.provision_rx.recv() => {
                    let provision_request = provision_request.expect("receiver should not be closed since we own one clone.");
                    self.handle_provision_request(provision_request).await;
                },
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

    async fn handle_provision_request(&mut self, provision_request: ProvisionMetadataStoreRequest) {
        trace!("Handle provision request");
        let (initial_nodes_configuration, initial_partition_table, initial_logs, response_tx) =
            provision_request.into_inner();

        match self
            .handle_provision_request_inner(
                initial_nodes_configuration,
                initial_partition_table,
                initial_logs,
            )
            .await
        {
            Ok(()) => {
                // if the receiver is gone, then the caller is no longer interested
                let _ = response_tx.send(Ok(()));
            }
            Err(err) => {
                debug!("Failed processing provision request: {err}");
                // if the receiver is gone, then the caller is no longer interested
                let _ = response_tx.send(Err(err));
            }
        }
    }

    async fn handle_provision_request_inner(
        &mut self,
        initial_nodes_configuration: NodesConfiguration,
        initial_partition_table: PartitionTable,
        initial_logs: Logs,
    ) -> Result<(), ProvisionError> {
        // 1. check whether we are already provisioned by checking for the nodes configuration
        if self
            .get(&NODES_CONFIG_KEY)
            .map_err(ProvisionError::internal)?
            .is_some()
        {
            return Err(ProvisionError::AlreadyProvisioned);
        }

        let serialized_partition_table = self
            .serialize_versioned_value(&initial_partition_table)
            .map_err(ProvisionError::internal)?;
        let serialized_logs = self
            .serialize_versioned_value(&initial_logs)
            .map_err(ProvisionError::internal)?;
        let serialized_nodes_configuration = self
            .serialize_versioned_value(&initial_nodes_configuration)
            .map_err(ProvisionError::internal)?;

        // 2. write the initial metadata
        self.put_initial_metadata(
            &serialized_partition_table,
            &serialized_logs,
            &serialized_nodes_configuration,
        )
        .await
        .map_err(ProvisionError::internal)?;

        Ok(())
    }

    async fn put_initial_metadata(
        &mut self,
        serialized_partition_table: &VersionedValue,
        serialized_logs: &VersionedValue,
        serialized_nodes_configuration: &VersionedValue,
    ) -> Result<()> {
        // todo replace with multi put once supported

        // It's very important that we put the different metadata values under their correct keys.
        // Otherwise, client's won't find these values.

        // It can happen that a provision attempt failed before. In this case, we might have
        // written the partition table or logs before. Therefore, we allow to overwrite them
        // here because we know that the provisioning was not successful (no nodes configuration).
        self.put(
            &PARTITION_TABLE_KEY,
            serialized_partition_table,
            Precondition::None,
        )
        .await?;
        self.put(&BIFROST_CONFIG_KEY, serialized_logs, Precondition::None)
            .await?;

        // Extra safety to not allow the nodes configuration to be overwritten even though we
        // checked before that it does not exist.
        self.put(
            &NODES_CONFIG_KEY,
            serialized_nodes_configuration,
            Precondition::DoesNotExist,
        )
        .await?;

        Ok(())
    }

    fn serialize_versioned_value<T: Versioned + StorageEncode>(
        &mut self,
        value: &T,
    ) -> Result<VersionedValue> {
        let serialized_value = StorageCodec::encode_and_split(value, &mut self.buffer)?.freeze();
        Ok(VersionedValue::new(value.version(), serialized_value))
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
            .write_batch(
                "local-metadata-write-batch",
                Priority::High,
                IoMode::default(),
                write_options,
                wb,
            )
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

    fn encode<T: StorageEncode>(value: &T, buf: &mut BytesMut) -> Result<()> {
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

#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Internal(GenericError),
    #[error("already provisioned")]
    AlreadyProvisioned,
}

impl ProvisionError {
    fn internal(err: impl Into<GenericError>) -> Self {
        ProvisionError::Internal(err.into())
    }
}

pub struct ProvisionMetadataStoreRequest {
    initial_nodes_configuration: NodesConfiguration,
    initial_partition_table: PartitionTable,
    initial_logs: Logs,
    response_tx: oneshot::Sender<Result<(), ProvisionError>>,
}

impl ProvisionMetadataStoreRequest {
    fn into_inner(
        self,
    ) -> (
        NodesConfiguration,
        PartitionTable,
        Logs,
        oneshot::Sender<Result<(), ProvisionError>>,
    ) {
        (
            self.initial_nodes_configuration,
            self.initial_partition_table,
            self.initial_logs,
            self.response_tx,
        )
    }
}

pub struct ProvisionHandle {
    tx: mpsc::Sender<ProvisionMetadataStoreRequest>,
}

impl ProvisionHandle {
    pub async fn provision(
        &self,
        initial_nodes_configuration: NodesConfiguration,
        initial_partition_table: PartitionTable,
        initial_logs: Logs,
    ) -> Result<(), ProvisionError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .send(ProvisionMetadataStoreRequest {
                initial_partition_table,
                initial_logs,
                initial_nodes_configuration,
                response_tx,
            })
            .await
            .map_err(|_| ProvisionError::Shutdown(ShutdownError))?;

        response_rx
            .await
            .map_err(|_| ProvisionError::Shutdown(ShutdownError))?
    }
}

fn db_options(_options: &MetadataStoreOptions) -> rocksdb::Options {
    rocksdb::Options::default()
}

fn cf_options(
    memory_budget: usize,
) -> impl Fn(rocksdb::Options) -> rocksdb::Options + Send + Sync + 'static {
    move |mut opts| {
        set_memory_related_opts(&mut opts, memory_budget);
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        opts.set_num_levels(3);

        opts.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Zstd,
        ]);

        //
        opts
    }
}

fn set_memory_related_opts(opts: &mut rocksdb::Options, memtables_budget: usize) {
    // We set the budget to allow 1 mutable + 3 immutable.
    opts.set_write_buffer_size(memtables_budget / 4);

    // merge 2 memtables when flushing to L0
    opts.set_min_write_buffer_number_to_merge(2);
    opts.set_max_write_buffer_number(4);
    // start flushing L0->L1 as soon as possible. each file on level0 is
    // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
    // memtable_memory_budget.
    opts.set_level_zero_file_num_compaction_trigger(2);
    // doesn't really matter much, but we don't want to create too many files
    opts.set_target_file_size_base(memtables_budget as u64 / 8);
    // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
    opts.set_max_bytes_for_level_base(memtables_budget as u64);
}
