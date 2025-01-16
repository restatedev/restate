// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod grpc;
pub mod grpc_svc;
mod kv_memory_storage;
pub mod local;
mod network;
pub mod omnipaxos;
pub mod raft;
mod util;

use crate::grpc::handler::MetadataStoreHandler;
use crate::grpc_svc::metadata_store_svc_server::MetadataStoreSvcServer;
use ::omnipaxos::util::NodeId;
use assert2::let_assert;
use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use restate_core::metadata_store::VersionedValue;
pub use restate_core::metadata_store::{
    MetadataStoreClient, Precondition, ReadError, ReadModifyWriteError, WriteError,
};
use restate_core::network::NetworkServerBuilder;
use restate_core::{MetadataWriter, ShutdownError};
use restate_types::config::{
    Configuration, MetadataStoreKind, MetadataStoreOptions, RocksDbOptions,
};
use restate_types::errors::GenericError;
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::net::AdvertisedAddress;
use restate_types::nodes_config::{
    LogServerConfig, MetadataStoreConfig, MetadataStoreState, NodeConfig, NodesConfiguration,
};
use restate_types::protobuf::common::MetadataStoreStatus;
use restate_types::storage::{StorageCodec, StorageDecodeError, StorageEncodeError};
use restate_types::{flexbuffers_storage_encode_decode, GenerationalNodeId, PlainNodeId, Version};
use std::fmt::{Display, Formatter};
use std::future::Future;
use tokio::sync::{mpsc, oneshot, watch};
use ulid::Ulid;

pub type BoxedMetadataStoreService = Box<dyn MetadataStoreService>;

pub type RequestSender = mpsc::Sender<MetadataStoreRequest>;
pub type RequestReceiver = mpsc::Receiver<MetadataStoreRequest>;

pub type ProvisionSender = mpsc::Sender<ProvisionRequest>;
pub type ProvisionReceiver = mpsc::Receiver<ProvisionRequest>;

type StatusWatch = watch::Receiver<MetadataStoreSummary>;
type StatusSender = watch::Sender<MetadataStoreSummary>;

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error("internal error: {0}")]
    Internal(GenericError),
    #[error("service currently unavailable: {0}")]
    Unavailable(GenericError),
    #[error("failed precondition: {0}")]
    FailedPrecondition(#[from] PreconditionViolation),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("encode error: {0}")]
    Encode(#[from] StorageEncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] StorageDecodeError),
}

#[derive(Debug, thiserror::Error)]
pub enum PreconditionViolation {
    #[error("key-value pair already exists")]
    Exists,
    #[error("expected version '{expected}' but found version '{actual:?}'")]
    VersionMismatch {
        expected: Version,
        actual: Option<Version>,
    },
}

impl PreconditionViolation {
    fn kv_pair_exists() -> Self {
        PreconditionViolation::Exists
    }

    fn version_mismatch(expected: Version, actual: Option<Version>) -> Self {
        PreconditionViolation::VersionMismatch { expected, actual }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    #[error("failed provisioning: {0}")]
    Internal(GenericError),
}

#[derive(Debug, thiserror::Error)]
#[error("invalid nodes configuration: {0}")]
pub struct InvalidConfiguration(String);

#[async_trait::async_trait]
pub trait MetadataStoreServiceBoxed {
    async fn run_boxed(self: Box<Self>) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl<T: MetadataStoreService> MetadataStoreServiceBoxed for T {
    async fn run_boxed(self: Box<Self>) -> anyhow::Result<()> {
        (*self).run().await
    }
}

#[async_trait::async_trait]
pub trait MetadataStoreService: MetadataStoreServiceBoxed + Send {
    async fn run(self) -> anyhow::Result<()>;

    fn boxed(self) -> BoxedMetadataStoreService
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[async_trait::async_trait]
impl<T: MetadataStoreService + ?Sized> MetadataStoreService for Box<T> {
    async fn run(self) -> anyhow::Result<()> {
        self.run_boxed().await
    }
}

#[derive(Debug)]
pub enum MetadataStoreRequest {
    Get {
        key: ByteString,
        result_tx: oneshot::Sender<Result<Option<VersionedValue>, RequestError>>,
    },
    GetVersion {
        key: ByteString,
        result_tx: oneshot::Sender<Result<Option<Version>, RequestError>>,
    },
    Put {
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
    Delete {
        key: ByteString,
        precondition: Precondition,
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
}

#[derive(Debug)]
pub struct ProvisionRequest {
    nodes_configuration: NodesConfiguration,
    result_tx: oneshot::Sender<Result<bool, ProvisionError>>,
}

trait MetadataStoreBackend {
    /// Create a request sender for this backend.
    fn request_sender(&self) -> RequestSender;

    /// Create a provision sender for this backend.
    fn provision_sender(&self) -> Option<ProvisionSender>;

    /// Create a status watch for this backend.
    fn status_watch(&self) -> Option<StatusWatch>;

    /// Run the metadata store backend
    fn run(self) -> impl Future<Output = anyhow::Result<()>> + Send + 'static;
}

struct MetadataStoreRunner<S> {
    store: S,
}

impl<S> MetadataStoreRunner<S>
where
    S: MetadataStoreBackend,
{
    pub fn new(store: S, server_builder: &mut NetworkServerBuilder) -> Self {
        server_builder.register_grpc_service(
            MetadataStoreSvcServer::new(MetadataStoreHandler::new(
                store.request_sender(),
                store.provision_sender(),
                store.status_watch(),
            )),
            grpc_svc::FILE_DESCRIPTOR_SET,
        );

        Self { store }
    }
}

#[async_trait::async_trait]
impl<S> MetadataStoreService for MetadataStoreRunner<S>
where
    S: MetadataStoreBackend + Send,
{
    async fn run(self) -> anyhow::Result<()> {
        let MetadataStoreRunner { store } = self;

        store.run().await?;

        Ok(())
    }
}

pub async fn create_metadata_store(
    metadata_store_options: &MetadataStoreOptions,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    health_status: HealthStatus<MetadataStoreStatus>,
    metadata_writer: Option<MetadataWriter>,
    server_builder: &mut NetworkServerBuilder,
) -> anyhow::Result<BoxedMetadataStoreService> {
    match metadata_store_options.kind {
        MetadataStoreKind::Local => local::create_store(
            metadata_store_options,
            rocksdb_options,
            health_status,
            server_builder,
        )
        .await
        .map_err(anyhow::Error::from)
        .map(|store| store.boxed()),
        MetadataStoreKind::Raft(ref raft_options) => raft::create_store(
            raft_options,
            rocksdb_options,
            health_status,
            metadata_writer,
            server_builder,
        )
        .await
        .map_err(anyhow::Error::from)
        .map(|store| store.boxed()),
        MetadataStoreKind::Omnipaxos => omnipaxos::create_store(
            rocksdb_options,
            health_status,
            metadata_writer,
            server_builder,
        )
        .await
        .map_err(anyhow::Error::from)
        .map(|store| store.boxed()),
    }
}
impl MetadataStoreRequest {
    fn split_request(self) -> (Callback, Request) {
        let (request_kind, callback_kind) = match self {
            MetadataStoreRequest::Get { key, result_tx } => {
                (RequestKind::Get { key }, CallbackKind::Get { result_tx })
            }
            MetadataStoreRequest::GetVersion { key, result_tx } => (
                RequestKind::GetVersion { key },
                CallbackKind::GetVersion { result_tx },
            ),
            MetadataStoreRequest::Put {
                key,
                value,
                precondition,
                result_tx,
            } => (
                RequestKind::Put {
                    key,
                    value,
                    precondition,
                },
                CallbackKind::Put { result_tx },
            ),
            MetadataStoreRequest::Delete {
                key,
                precondition,
                result_tx,
            } => (
                RequestKind::Delete { key, precondition },
                CallbackKind::Delete { result_tx },
            ),
        };

        let request_id = Ulid::new();

        let callback = Callback {
            request_id,
            kind: callback_kind,
        };

        let request = Request {
            request_id,
            kind: request_kind,
        };

        (callback, request)
    }
}

struct Callback {
    request_id: Ulid,
    kind: CallbackKind,
}

impl Callback {
    fn fail(self, err: impl Into<RequestError>) {
        match self.kind {
            CallbackKind::Get { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            CallbackKind::GetVersion { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            CallbackKind::Put { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            CallbackKind::Delete { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
        };
    }

    fn complete_get(self, result: Option<VersionedValue>) {
        let_assert!(
            CallbackKind::Get { result_tx } = self.kind,
            "expected 'Get' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(Ok(result));
    }

    fn complete_get_version(self, result: Option<Version>) {
        let_assert!(
            CallbackKind::GetVersion { result_tx } = self.kind,
            "expected 'GetVersion' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(Ok(result));
    }

    fn complete_put(self, result: Result<(), RequestError>) {
        let_assert!(
            CallbackKind::Put { result_tx } = self.kind,
            "expected 'Put' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(result);
    }

    fn complete_delete(self, result: Result<(), RequestError>) {
        let_assert!(
            CallbackKind::Delete { result_tx } = self.kind,
            "expected 'Delete' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(result);
    }
}

enum CallbackKind {
    Get {
        result_tx: oneshot::Sender<Result<Option<VersionedValue>, RequestError>>,
    },
    GetVersion {
        result_tx: oneshot::Sender<Result<Option<Version>, RequestError>>,
    },
    Put {
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
    Delete {
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Request {
    request_id: Ulid,
    kind: RequestKind,
}

flexbuffers_storage_encode_decode!(Request);

impl Request {
    pub fn new(kind: RequestKind) -> Self {
        Request {
            request_id: Ulid::new(),
            kind,
        }
    }

    fn encode_to_vec(&self) -> Result<Vec<u8>, StorageEncodeError> {
        let mut buffer = BytesMut::new();
        // todo: Removing support for BufMut requires an extra copy from BytesMut to Vec :-(
        StorageCodec::encode(self, &mut buffer)?;
        Ok(buffer.to_vec())
    }

    fn decode_from_bytes(mut bytes: Bytes) -> Result<Self, StorageDecodeError> {
        StorageCodec::decode::<Request, _>(&mut bytes)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum RequestKind {
    Get {
        key: ByteString,
    },
    GetVersion {
        key: ByteString,
    },
    Put {
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    },
    Delete {
        key: ByteString,
        precondition: Precondition,
    },
}

type JoinClusterSender = mpsc::Sender<JoinClusterRequest>;
type JoinClusterReceiver = mpsc::Receiver<JoinClusterRequest>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KnownLeader {
    node_id: PlainNodeId,
    address: AdvertisedAddress,
}

#[derive(Debug, thiserror::Error)]
enum JoinClusterError {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("cannot accept new members since I am not active.")]
    NotActive(Option<KnownLeader>),
    #[error("cannot accept new members since I am not the leader.")]
    NotLeader(Option<KnownLeader>),
    #[error("invalid omni paxos configuration: {0}")]
    ConfigError(String),
    #[error("pending reconfiguration")]
    PendingReconfiguration,
    #[error("received a concurrent join request for node id '{0}'")]
    ConcurrentRequest(PlainNodeId),
    #[error("internal error: {0}")]
    Internal(String),
}

struct JoinClusterRequest {
    node_id: u64,
    storage_id: u64,
    response_tx: oneshot::Sender<Result<JoinClusterResponse, JoinClusterError>>,
}

impl JoinClusterRequest {
    fn into_inner(
        self,
    ) -> (
        oneshot::Sender<Result<JoinClusterResponse, JoinClusterError>>,
        u64,
        u64,
    ) {
        (self.response_tx, self.node_id, self.storage_id)
    }
}

struct JoinClusterResponse {
    log_prefix: Bytes,
    metadata_store_config: Bytes,
}

#[derive(Debug)]
struct JoinClusterHandle {
    join_cluster_tx: JoinClusterSender,
}

impl JoinClusterHandle {
    pub fn new(join_cluster_tx: JoinClusterSender) -> Self {
        JoinClusterHandle { join_cluster_tx }
    }

    pub async fn join_cluster(
        &self,
        node_id: u64,
        storage_id: u64,
    ) -> Result<JoinClusterResponse, JoinClusterError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.join_cluster_tx
            .send(JoinClusterRequest {
                node_id,
                storage_id,
                response_tx,
            })
            .await
            .map_err(|_| ShutdownError)?;

        response_rx.await.map_err(|_| ShutdownError)?
    }
}

/// Identifier to detect the loss of a disk.
type StorageId = u64;

#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    prost_dto::IntoProst,
    prost_dto::FromProst,
)]
#[prost(target = "crate::grpc_svc::MemberId")]
pub struct MemberId {
    node_id: NodeId,
    storage_id: StorageId,
}

impl MemberId {
    fn new(node_id: NodeId, storage_id: StorageId) -> Self {
        MemberId {
            node_id,
            storage_id,
        }
    }
}

impl Display for MemberId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plain_node_id = PlainNodeId::new(
            u32::try_from(self.node_id).expect("node ids should be derived from PlainNodeIds"),
        );
        write!(f, "{}:{:x}", plain_node_id, self.storage_id & 0x0ffff)
    }
}

/// Status summary of the metadata store.
#[derive(Clone, Debug, Default)]
enum MetadataStoreSummary {
    #[default]
    Starting,
    Provisioning,
    Passive,
    Active {
        leader: Option<MemberId>,
        configuration: MetadataStoreConfiguration,
    },
}

#[derive(Clone, Debug, prost_dto::IntoProst, prost_dto::FromProst)]
#[prost(target = "crate::grpc_svc::MetadataStoreConfiguration")]
struct MetadataStoreConfiguration {
    id: u32,
    members: Vec<MemberId>,
}

/// Ensures that the initial nodes configuration contains the current node and has the right
/// [`MetadataStoreState`] set.
fn prepare_initial_nodes_configuration(
    configuration: &Configuration,
    configuration_id: u32,
    nodes_configuration: &mut NodesConfiguration,
) -> Result<PlainNodeId, InvalidConfiguration> {
    let plain_node_id = if let Some(node_config) =
        nodes_configuration.find_node_by_name(configuration.common.node_name())
    {
        if let Some(force_node_id) = configuration.common.force_node_id {
            if force_node_id != node_config.current_generation.as_plain() {
                return Err(InvalidConfiguration(format!(
                    "nodes configuration has wrong plain node id; expected: {}, actual: {}",
                    force_node_id,
                    node_config.current_generation.as_plain()
                )));
            }
        }

        let restate_node_id = node_config.current_generation.as_plain();

        let mut node_config = node_config.clone();
        node_config.metadata_store_config.metadata_store_state =
            MetadataStoreState::Active(configuration_id);

        nodes_configuration.upsert_node(node_config);

        restate_node_id
    } else {
        // give precedence to the force node id
        let current_generation = configuration
            .common
            .force_node_id
            .map(|node_id| node_id.with_generation(1))
            .unwrap_or_else(|| {
                nodes_configuration
                    .max_plain_node_id()
                    .map(|node_id| node_id.next().with_generation(1))
                    .unwrap_or(GenerationalNodeId::INITIAL_NODE_ID)
            });

        let metadata_store_config = MetadataStoreConfig {
            metadata_store_state: MetadataStoreState::Active(configuration_id),
        };

        let node_config = NodeConfig::new(
            configuration.common.node_name().to_owned(),
            current_generation,
            configuration.common.location().clone(),
            configuration.common.advertised_address.clone(),
            configuration.common.roles,
            LogServerConfig::default(),
            metadata_store_config,
        );

        nodes_configuration.upsert_node(node_config);

        current_generation.as_plain()
    };

    Ok(plain_node_id)
}
