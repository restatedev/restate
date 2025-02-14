// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::local::storage::RocksDbStorage;
use crate::{MetadataServer, MetadataStoreRequest, RequestError, RequestReceiver, RequestSender};
use bytestring::ByteString;
use restate_core::metadata_store::{
    serialize_value, MetadataStoreClient, Precondition, ProvisionedMetadataStore, ReadError,
    VersionedValue, WriteError,
};
use restate_core::{cancellation_watcher, MetadataWriter, ShutdownError};
use restate_rocksdb::RocksError;
use restate_types::config::{Configuration, MetadataServerOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration};
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::storage::{StorageCodec, StorageDecodeError, StorageEncodeError};
use restate_types::{PlainNodeId, Version};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace};

/// Single node metadata store which stores the key value pairs in RocksDB.
///
/// In order to avoid issues arising from concurrency, we run the metadata
/// store in a single thread.
pub struct LocalMetadataServer {
    storage: RocksDbStorage,
    request_tx: RequestSender,
    request_rx: RequestReceiver,
    health_status: HealthStatus<MetadataServerStatus>,
}

impl LocalMetadataServer {
    pub async fn create(
        options: &MetadataServerOptions,
        updateable_rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
        health_status: HealthStatus<MetadataServerStatus>,
    ) -> Result<Self, RocksError> {
        health_status.update(MetadataServerStatus::StartingUp);
        let (request_tx, request_rx) = mpsc::channel(options.request_queue_length());

        let storage = RocksDbStorage::create(options, updateable_rocksdb_options).await?;

        Ok(Self {
            storage,
            health_status,
            request_tx,
            request_rx,
        })
    }

    pub fn client(&self) -> MetadataStoreClient {
        MetadataStoreClient::new(
            LocalMetadataStoreClient::new(self.request_tx.clone()),
            Some(
                Configuration::pinned()
                    .common
                    .metadata_client
                    .backoff_policy
                    .clone(),
            ),
        )
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        debug!("Running LocalMetadataStore");
        self.health_status.update(MetadataServerStatus::Member);

        migrate_nodes_configuration(&mut self.storage).await?;

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

        Ok(())
    }

    async fn handle_request(&mut self, request: MetadataStoreRequest) {
        trace!("Handle request '{:?}'", request);

        match request {
            MetadataStoreRequest::Get { key, result_tx } => {
                let result = self.storage.get(&key);
                Self::log_error(&result, "Get");
                let _ = result_tx.send(result);
            }
            MetadataStoreRequest::GetVersion { key, result_tx } => {
                let result = self.storage.get_version(&key);
                Self::log_error(&result, "GetVersion");
                let _ = result_tx.send(result);
            }
            MetadataStoreRequest::Put {
                key,
                value,
                precondition,
                result_tx,
            } => {
                let result = self.storage.put(&key, &value, precondition).await;
                Self::log_error(&result, "Put");
                let _ = result_tx.send(result);
            }
            MetadataStoreRequest::Delete {
                key,
                precondition,
                result_tx,
            } => {
                let result = self.storage.delete(&key, precondition);
                Self::log_error(&result, "Delete");
                let _ = result_tx.send(result);
            }
        };
    }

    fn log_error<T>(result: &Result<T, RequestError>, request: &str) {
        if let Err(err) = &result {
            debug!("failed to process request '{}': '{}'", request, err)
        }
    }
}

#[async_trait::async_trait]
impl MetadataServer for LocalMetadataServer {
    async fn run(self, _metadata_writer: Option<MetadataWriter>) -> anyhow::Result<()> {
        self.run().await.map_err(Into::into)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("failed reading/writing: {0}")]
    ReadWrite(#[from] RequestError),
    #[error("encode error: {0}")]
    Encode(#[from] StorageEncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] StorageDecodeError),
    #[error("cannot auto migrate a multi node cluster which uses the local metadata server")]
    MultiNodeCluster,
}

pub async fn migrate_nodes_configuration(
    storage: &mut RocksDbStorage,
) -> Result<(), MigrationError> {
    let value = storage.get(&NODES_CONFIG_KEY)?;

    if value.is_none() {
        // nothing to migrate
        return Ok(());
    };

    let value = value.unwrap();
    let mut bytes = value.value.as_ref();

    let mut nodes_configuration = StorageCodec::decode::<NodesConfiguration, _>(&mut bytes)?;

    let mut modified = false;

    if let Some(node_config) =
        nodes_configuration.find_node_by_name(Configuration::pinned().common.node_name())
    {
        // Only needed if we resume from a Restate version that has not properly set the
        // MetadataServerState to Member in the NodesConfiguration.
        if !matches!(
            node_config.metadata_server_config.metadata_server_state,
            MetadataServerState::Member
        ) {
            info!(
                "Setting MetadataServerState to Member in NodesConfiguration for node {}",
                node_config.name
            );
            let mut new_node_config = node_config.clone();
            new_node_config.metadata_server_config.metadata_server_state =
                MetadataServerState::Member;

            nodes_configuration.upsert_node(new_node_config);
            modified = true;
        }
    }

    // If we have a node-id 0 in our NodesConfiguration, then we need to migrate it to another
    // value since node-id 0 is a reserved value now.
    let zero = PlainNodeId::new(0);
    if let Ok(node_config) = nodes_configuration.find_node_by_id(zero) {
        if nodes_configuration.len() > 1 {
            return Err(MigrationError::MultiNodeCluster);
        }

        assert_eq!(node_config.name, Configuration::pinned().node_name(), "The only known node of this cluster is {} but my node name is {}. This indicates that my node name was changed after an initial provisioning of the node", node_config.name, Configuration::pinned().node_name());

        let plain_node_id_to_migrate_to =
            if let Some(force_node_id) = Configuration::pinned().common.force_node_id {
                assert_ne!(
                    force_node_id, zero,
                    "It should no longer be allowed to force the node id to 0"
                );
                force_node_id
            } else {
                PlainNodeId::MIN
            };

        let mut new_node_config = node_config.clone();
        new_node_config.current_generation = plain_node_id_to_migrate_to
            .with_generation(node_config.current_generation.generation());
        info!(
            "Migrating node id of node '{}' from '{}' to '{}'",
            node_config.name, node_config.current_generation, new_node_config.current_generation
        );

        nodes_configuration.remove_node_unchecked(node_config.current_generation);
        nodes_configuration.upsert_node(new_node_config);
        modified = true;
    }

    if modified {
        nodes_configuration.increment_version();

        let new_nodes_configuration = serialize_value(&nodes_configuration)?;

        storage
            .put(
                &NODES_CONFIG_KEY,
                &new_nodes_configuration,
                Precondition::MatchesVersion(value.version),
            )
            .await?;

        info!("Successfully completed NodesConfiguration migration");
    }

    Ok(())
}

struct LocalMetadataStoreClient {
    request_tx: RequestSender,
}

impl LocalMetadataStoreClient {
    fn new(request_tx: RequestSender) -> Self {
        Self { request_tx }
    }
}

#[async_trait::async_trait]
impl ProvisionedMetadataStore for LocalMetadataStoreClient {
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let (result_tx, result_rx) = oneshot::channel();
        let get_request = MetadataStoreRequest::Get { key, result_tx };
        self.request_tx
            .send(get_request)
            .await
            .map_err(|_| ReadError::terminal(ShutdownError))?;

        result_rx
            .await
            .map_err(|_| ReadError::terminal(ShutdownError))?
            .map_err(ReadError::other)
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let (result_tx, result_rx) = oneshot::channel();
        let get_version_request = MetadataStoreRequest::GetVersion { key, result_tx };
        self.request_tx
            .send(get_version_request)
            .await
            .map_err(|_| ReadError::terminal(ShutdownError))?;

        result_rx
            .await
            .map_err(|_| ReadError::terminal(ShutdownError))?
            .map_err(ReadError::other)
    }

    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let (result_tx, result_rx) = oneshot::channel();
        let put_request = MetadataStoreRequest::Put {
            key,
            value,
            precondition,
            result_tx,
        };
        self.request_tx
            .send(put_request)
            .await
            .map_err(|_| WriteError::terminal(ShutdownError))?;

        result_rx
            .await
            .map_err(|_| WriteError::terminal(ShutdownError))?
            .map_err(WriteError::from)
    }

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let (result_tx, result_rx) = oneshot::channel();
        let delete_request = MetadataStoreRequest::Delete {
            key,
            precondition,
            result_tx,
        };
        self.request_tx
            .send(delete_request)
            .await
            .map_err(|_| WriteError::terminal(ShutdownError))?;

        result_rx
            .await
            .map_err(|_| WriteError::terminal(ShutdownError))?
            .map_err(WriteError::from)
    }
}

impl From<RequestError> for WriteError {
    fn from(value: RequestError) -> Self {
        match value {
            err @ (RequestError::Internal(_)
            | RequestError::Unavailable(_, _)
            | RequestError::InvalidArgument(_)) => WriteError::other(err),
            RequestError::FailedPrecondition(err) => {
                WriteError::FailedPrecondition(err.to_string())
            }
            err @ (RequestError::Encode(_) | RequestError::Decode(_)) => {
                WriteError::Codec(err.into())
            }
        }
    }
}
