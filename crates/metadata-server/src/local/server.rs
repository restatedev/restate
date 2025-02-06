// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::grpc::handler::MetadataServerHandler;
use crate::grpc::metadata_server_svc_server::MetadataServerSvcServer;
use crate::{
    grpc, MetadataServer, MetadataStoreRequest, PreconditionViolation, RequestError,
    RequestReceiver,
};
use bytes::BytesMut;
use bytestring::ByteString;
use restate_core::cancellation_watcher;
use restate_core::metadata_store::{serialize_value, Precondition, VersionedValue};
use restate_core::network::NetworkServerBuilder;
use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, IoMode, Priority, RocksDb, RocksDbManager,
    RocksError,
};
use restate_types::config::{Configuration, MetadataServerOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration};
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode};
use restate_types::Version;
use rocksdb::{BoundColumnFamily, DBCompressionType, WriteBatch, WriteOptions, DB};
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::codec::CompressionEncoding;
use tracing::{debug, info, trace};

const DB_NAME: &str = "local-metadata-store";
const KV_PAIRS: &str = "kv_pairs";

/// Single node metadata store which stores the key value pairs in RocksDB.
///
/// In order to avoid issues arising from concurrency, we run the metadata
/// store in a single thread.
pub struct LocalMetadataServer {
    db: Arc<DB>,
    rocksdb: Arc<RocksDb>,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    request_rx: RequestReceiver,
    buffer: BytesMut,
    health_status: HealthStatus<MetadataServerStatus>,
}

impl LocalMetadataServer {
    pub async fn create(
        options: &MetadataServerOptions,
        updateable_rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
        health_status: HealthStatus<MetadataServerStatus>,
        server_builder: &mut NetworkServerBuilder,
    ) -> Result<Self, RocksError> {
        health_status.update(MetadataServerStatus::StartingUp);
        let (request_tx, request_rx) = mpsc::channel(options.request_queue_length());

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

        server_builder.register_grpc_service(
            MetadataServerSvcServer::new(MetadataServerHandler::new(request_tx, None, None))
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
            grpc::FILE_DESCRIPTOR_SET,
        );

        Ok(Self {
            db,
            rocksdb,
            rocksdb_options: updateable_rocksdb_options,
            buffer: BytesMut::default(),
            health_status,
            request_rx,
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

    pub async fn run(mut self) {
        debug!("Running LocalMetadataStore");
        self.health_status.update(MetadataServerStatus::Member);

        // Only needed if we resume from a Restate version that has not properly set the
        // MetadataServerState to Member in the NodesConfiguration.
        if let Err(err) = self.patch_metadata_server_state().await {
            info!("Failed to patch MetadataServerState: {err}");
        }

        loop {
            tokio::select! {
                Some(request) = self.request_rx.recv() => {
                    self.handle_request(request).await;
                },
                _ = cancellation_watcher() => {
                    break;
                },
            }
        }

        self.health_status.update(MetadataServerStatus::Unknown);

        debug!("Stopped LocalMetadataStore");
    }

    async fn patch_metadata_server_state(&mut self) -> anyhow::Result<()> {
        let value = self.get(&NODES_CONFIG_KEY)?;

        if value.is_none() {
            // nothing to patch
            return Ok(());
        };

        let value = value.unwrap();
        let mut bytes = value.value.as_ref();

        let mut nodes_configuration = StorageCodec::decode::<NodesConfiguration, _>(&mut bytes)?;

        if let Some(node_config) =
            nodes_configuration.find_node_by_name(Configuration::pinned().common.node_name())
        {
            if matches!(
                node_config.metadata_server_config.metadata_server_state,
                MetadataServerState::Member
            ) {
                // nothing to patch
                return Ok(());
            }

            let mut new_node_config = node_config.clone();
            new_node_config.metadata_server_config.metadata_server_state =
                MetadataServerState::Member;

            nodes_configuration.upsert_node(new_node_config);
            nodes_configuration.increment_version();

            let new_nodes_configuration = serialize_value(&nodes_configuration)?;

            self.put(
                &NODES_CONFIG_KEY,
                &new_nodes_configuration,
                Precondition::MatchesVersion(value.version),
            )
            .await?;

            info!("Successfully patched MetadataServerState in the NodesConfiguration.");
        }

        Ok(())
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

    fn get(&self, key: &ByteString) -> Result<Option<VersionedValue>, RequestError> {
        let cf_handle = self.kv_cf_handle();
        let slice = self
            .db
            .get_pinned_cf(&cf_handle, key)
            .map_err(|err| RequestError::Internal(err.into()))?;

        if let Some(bytes) = slice {
            Ok(Some(Self::decode(bytes)?))
        } else {
            Ok(None)
        }
    }

    fn get_version(&self, key: &ByteString) -> Result<Option<Version>, RequestError> {
        let cf_handle = self.kv_cf_handle();
        let slice = self
            .db
            .get_pinned_cf(&cf_handle, key)
            .map_err(|err| RequestError::Internal(err.into()))?;

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
    ) -> Result<(), RequestError> {
        match precondition {
            Precondition::None => Ok(self.write_versioned_kv_pair(key, value).await?),
            Precondition::DoesNotExist => {
                let current_version = self.get_version(key)?;
                if current_version.is_none() {
                    Ok(self.write_versioned_kv_pair(key, value).await?)
                } else {
                    Err(PreconditionViolation::kv_pair_exists())?
                }
            }
            Precondition::MatchesVersion(version) => {
                let current_version = self.get_version(key)?;
                if current_version == Some(version) {
                    Ok(self.write_versioned_kv_pair(key, value).await?)
                } else {
                    Err(PreconditionViolation::version_mismatch(
                        version,
                        current_version,
                    ))?
                }
            }
        }
    }

    async fn write_versioned_kv_pair(
        &mut self,
        key: &ByteString,
        value: &VersionedValue,
    ) -> Result<(), RequestError> {
        self.buffer.clear();
        Self::encode(value, &mut self.buffer)?;

        let write_options = self.write_options();
        let cf_handle = self.kv_cf_handle();
        let mut wb = WriteBatch::default();
        wb.put_cf(&cf_handle, key, self.buffer.as_ref());
        self.rocksdb
            .write_batch(
                "local-metadata-write-batch",
                Priority::High,
                IoMode::default(),
                write_options,
                wb,
            )
            .await
            .map_err(|err| RequestError::Internal(err.into()))
    }

    fn delete(&mut self, key: &ByteString, precondition: Precondition) -> Result<(), RequestError> {
        match precondition {
            Precondition::None => self.delete_kv_pair(key),
            // this condition does not really make sense for the delete operation
            Precondition::DoesNotExist => {
                let current_version = self.get_version(key)?;

                if current_version.is_none() {
                    // nothing to do
                    Ok(())
                } else {
                    Err(PreconditionViolation::kv_pair_exists())?
                }
            }
            Precondition::MatchesVersion(version) => {
                let current_version = self.get_version(key)?;

                if current_version == Some(version) {
                    self.delete_kv_pair(key)
                } else {
                    Err(PreconditionViolation::version_mismatch(
                        version,
                        current_version,
                    ))?
                }
            }
        }
    }

    fn delete_kv_pair(&mut self, key: &ByteString) -> Result<(), RequestError> {
        let write_options = self.write_options();
        self.db
            .delete_cf_opt(&self.kv_cf_handle(), key, &write_options)
            .map_err(|err| RequestError::Internal(err.into()))
    }

    fn encode<T: StorageEncode>(value: &T, buf: &mut BytesMut) -> Result<(), RequestError> {
        StorageCodec::encode(value, buf)?;
        Ok(())
    }

    fn decode<T: StorageDecode>(buf: impl AsRef<[u8]>) -> Result<T, RequestError> {
        let value = StorageCodec::decode(&mut buf.as_ref())?;
        Ok(value)
    }

    fn log_error<T>(result: &Result<T, RequestError>, request: &str) {
        if let Err(err) = &result {
            debug!("failed to process request '{}': '{}'", request, err)
        }
    }
}

#[async_trait::async_trait]
impl MetadataServer for LocalMetadataServer {
    async fn run(self) -> anyhow::Result<()> {
        self.run().await;
        Ok(())
    }
}

pub fn db_options(_options: &MetadataServerOptions) -> rocksdb::Options {
    rocksdb::Options::default()
}

pub fn cf_options(
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

pub fn set_memory_related_opts(opts: &mut rocksdb::Options, memtables_budget: usize) {
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
