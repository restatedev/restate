// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate core;

pub mod grpc;
mod metric_definitions;
pub mod raft;

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;
use prost::Message;
use raft_proto::eraftpb::Snapshot;
use restate_types::net::listener::AddressBook;
use tokio::sync::{mpsc, oneshot, watch};
use tonic::Status;
use ulid::Ulid;

use restate_core::network::NetworkServerBuilder;
use restate_core::{MetadataWriter, ShutdownError, TaskCenter};
use restate_metadata_providers::replicated::{KnownLeader, create_replicated_metadata_client};
use restate_metadata_server_grpc::{MetadataServerConfiguration, grpc as protobuf};
pub use restate_metadata_store::{
    MetadataStoreClient, ReadError, ReadModifyWriteError, WriteError,
};
use restate_types::config::{Configuration, MetadataClientKind};
use restate_types::errors::{ConversionError, GenericError, MaybeRetryableError};
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::metadata::{Precondition, VersionedValue};
use restate_types::nodes_config::{
    ClusterFingerprint, MetadataServerConfig, MetadataServerState, NodeConfig, NodesConfiguration,
    Role,
};
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::storage::{StorageDecodeError, StorageEncodeError};
use restate_types::{GenerationalNodeId, PlainNodeId, Version};

use crate::raft::RaftMetadataServer;

pub type BoxedMetadataServer = Box<dyn MetadataServer>;

pub type RequestSender = mpsc::Sender<MetadataStoreRequest>;
pub type RequestReceiver = mpsc::Receiver<MetadataStoreRequest>;

pub type ProvisionSender = mpsc::Sender<ProvisionRequest>;
pub type ProvisionReceiver = mpsc::Receiver<ProvisionRequest>;

type StatusWatch = watch::Receiver<MetadataServerSummary>;
type StatusSender = watch::Sender<MetadataServerSummary>;

type MetadataCommandSender = mpsc::Sender<MetadataCommand>;
type MetadataCommandReceiver = mpsc::Receiver<MetadataCommand>;

pub const KNOWN_LEADER_KEY: &str = "x-restate-known-leader";

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error("internal error: {0}")]
    Internal(GenericError),
    #[error("service currently unavailable: {0}")]
    Unavailable(GenericError, Option<KnownLeader>),
    #[error("failed precondition: {0}")]
    FailedPrecondition(#[from] PreconditionViolation),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("encode error: {0}")]
    Encode(#[from] StorageEncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] StorageDecodeError),
    #[error("rejecting request because it seems to target a different cluster: {0}")]
    ClusterIdentityMismatch(String),
}

impl MaybeRetryableError for RequestError {
    fn retryable(&self) -> bool {
        match self {
            RequestError::Internal(_) => false,
            RequestError::Unavailable(_, _) => true,
            RequestError::FailedPrecondition(_) => false,
            RequestError::InvalidArgument(_) => false,
            RequestError::Encode(_) => false,
            RequestError::Decode(_) => false,
            RequestError::ClusterIdentityMismatch(_) => false,
        }
    }
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

#[derive(Debug, thiserror::Error)]
enum JoinError {
    #[error("rpc failed: status: {}, message: {}", _0.code(), _0.message())]
    Rpc(Box<Status>, Option<KnownLeader>),
    #[error("other error: {0}")]
    Other(GenericError),
}

#[async_trait::async_trait]
pub trait MetadataServerBoxed {
    async fn run_boxed(
        self: Box<Self>,
        metadata_writer: Option<MetadataWriter>,
    ) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl<T: MetadataServer> MetadataServerBoxed for T {
    async fn run_boxed(
        self: Box<Self>,
        metadata_writer: Option<MetadataWriter>,
    ) -> anyhow::Result<()> {
        (*self).run(metadata_writer).await
    }
}

#[async_trait::async_trait]
pub trait MetadataServer: MetadataServerBoxed + Send {
    async fn run(self, metadata_writer: Option<MetadataWriter>) -> anyhow::Result<()>;

    fn boxed(self) -> BoxedMetadataServer
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[async_trait::async_trait]
impl<T: MetadataServer + ?Sized> MetadataServer for Box<T> {
    async fn run(self, metadata_writer: Option<MetadataWriter>) -> anyhow::Result<()> {
        self.run_boxed(metadata_writer).await
    }
}

/// Identifies the cluster for request validation.
/// During normal operation, fingerprint is used.
/// During bootstrapping when fingerprint is unavailable, cluster_name can be used as fallback.
#[derive(Debug, Clone, Default)]
pub struct ClusterIdentity {
    pub fingerprint: Option<ClusterFingerprint>,
    pub cluster_name: Option<String>,
}

#[derive(Debug)]
pub enum MetadataStoreRequest {
    Get {
        key: ByteString,
        cluster_identity: ClusterIdentity,
        result_tx: oneshot::Sender<Result<Option<VersionedValue>, RequestError>>,
    },
    GetVersion {
        key: ByteString,
        cluster_identity: ClusterIdentity,
        result_tx: oneshot::Sender<Result<Option<Version>, RequestError>>,
    },
    Put {
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
        cluster_identity: ClusterIdentity,
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
    Delete {
        key: ByteString,
        precondition: Precondition,
        cluster_identity: ClusterIdentity,
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
}

#[derive(Debug)]
pub struct ProvisionRequest {
    nodes_configuration: NodesConfiguration,
    result_tx: oneshot::Sender<Result<bool, ProvisionError>>,
}

pub async fn create_metadata_server_and_client(
    mut config: Live<Configuration>,
    health_status: HealthStatus<MetadataServerStatus>,
    server_builder: &mut NetworkServerBuilder,
    address_book: &AddressBook,
) -> anyhow::Result<(BoxedMetadataServer, MetadataStoreClient)> {
    metric_definitions::describe_metrics();
    let config = config.live_load();
    RaftMetadataServer::create(health_status, server_builder)
        .await
        .map_err(anyhow::Error::from)
        .map(|server| {
            let metadata_client_options = config.common.metadata_client.clone();
            let backoff_policy = metadata_client_options.backoff_policy.clone();
            let_assert!(
                MetadataClientKind::Replicated { mut addresses } =
                    config.common.metadata_client.kind.clone()
            );
            // make sure we include our own address in the list of addresses if we are hosting
            // a metadata server ourselves.
            if config.common.roles.contains(Role::MetadataServer) {
                addresses.push(config.common.advertised_address(address_book));
            }
            (
                server.boxed(),
                create_replicated_metadata_client(
                    addresses,
                    Some(backoff_policy),
                    Arc::new(metadata_client_options),
                ),
            )
        })
}

impl MetadataStoreRequest {
    fn into_request(self) -> (Request, ClusterIdentity) {
        let request_id = Ulid::new();

        match self {
            MetadataStoreRequest::Get {
                key,
                result_tx,
                cluster_identity,
            } => (
                Request::ReadOnly(ReadOnlyRequest {
                    request_id,
                    kind: ReadOnlyRequestKind::Get { key, result_tx },
                }),
                cluster_identity,
            ),
            MetadataStoreRequest::GetVersion {
                key,
                result_tx,
                cluster_identity,
            } => (
                Request::ReadOnly(ReadOnlyRequest {
                    request_id,
                    kind: ReadOnlyRequestKind::GetVersion { key, result_tx },
                }),
                cluster_identity,
            ),
            MetadataStoreRequest::Put {
                key,
                value,
                precondition,
                result_tx,
                cluster_identity,
            } => {
                let request = WriteRequest {
                    request_id,
                    kind: RequestKind::Put {
                        key,
                        value,
                        precondition,
                    },
                };
                let callback = Callback {
                    request_id,
                    kind: CallbackKind::Put { result_tx },
                };

                (Request::Write { callback, request }, cluster_identity)
            }
            MetadataStoreRequest::Delete {
                key,
                precondition,
                result_tx,
                cluster_identity,
            } => {
                let request = WriteRequest {
                    request_id,
                    kind: RequestKind::Delete { key, precondition },
                };
                let callback = Callback {
                    request_id,
                    kind: CallbackKind::Delete { result_tx },
                };
                (Request::Write { request, callback }, cluster_identity)
            }
        }
    }
}

#[derive(derive_more::Debug)]
enum Request {
    ReadOnly(ReadOnlyRequest),
    Write {
        #[debug(skip)]
        callback: Callback,
        request: WriteRequest,
    },
}

impl Request {
    fn fail(self, err: impl Into<RequestError>) {
        match self {
            Request::ReadOnly(read_only_request) => {
                read_only_request.fail(err);
            }
            Request::Write { callback, .. } => {
                callback.fail(err);
            }
        }
    }
}

struct Callback {
    request_id: Ulid,
    kind: CallbackKind,
}

impl Callback {
    fn fail(self, err: impl Into<RequestError>) {
        match self.kind {
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
    Put {
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
    Delete {
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct WriteRequest {
    request_id: Ulid,
    kind: RequestKind,
}

impl WriteRequest {
    fn key(&self) -> &ByteString {
        match &self.kind {
            RequestKind::Put { key, .. } => key,
            RequestKind::Delete { key, .. } => key,
        }
    }

    fn precondition(&self) -> Precondition {
        match &self.kind {
            RequestKind::Put { precondition, .. } => *precondition,
            RequestKind::Delete { precondition, .. } => *precondition,
        }
    }

    fn encode_to_vec(self) -> Result<Vec<u8>, StorageEncodeError> {
        let request = protobuf::WriteRequest::from(self);
        Ok(request.encode_to_vec())
    }

    fn decode_from_bytes(bytes: Bytes) -> Result<Self, StorageDecodeError> {
        let result = protobuf::WriteRequest::decode(bytes)
            .map_err(|err| StorageDecodeError::DecodeValue(err.into()))?;
        result
            .try_into()
            .map_err(|err: ConversionError| StorageDecodeError::DecodeValue(err.into()))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum RequestKind {
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

#[derive(Debug)]
struct ReadOnlyRequest {
    request_id: Ulid,
    kind: ReadOnlyRequestKind,
}

impl ReadOnlyRequest {
    fn fail(self, err: impl Into<RequestError>) {
        match self.kind {
            ReadOnlyRequestKind::Get { result_tx, .. } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            ReadOnlyRequestKind::GetVersion { result_tx, .. } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
        };
    }
}

#[derive(derive_more::Debug)]
enum ReadOnlyRequestKind {
    Get {
        key: ByteString,
        #[debug(skip)]
        result_tx: oneshot::Sender<Result<Option<VersionedValue>, RequestError>>,
    },
    GetVersion {
        key: ByteString,
        #[debug(skip)]
        result_tx: oneshot::Sender<Result<Option<Version>, RequestError>>,
    },
}

type JoinClusterSender = mpsc::Sender<JoinClusterRequest>;
type JoinClusterReceiver = mpsc::Receiver<JoinClusterRequest>;

#[derive(Debug, thiserror::Error)]
enum JoinClusterError {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("cannot accept new members since I am not a member.")]
    NotMember(Option<KnownLeader>),
    #[error("cannot accept new members since I am not the leader.")]
    NotLeader(Option<KnownLeader>),
    #[error("pending reconfiguration")]
    PendingReconfiguration,
    #[error("received a concurrent join request for node id '{0}'")]
    ConcurrentRequest(PlainNodeId),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("join request was dropped")]
    ProposalDropped,
    #[error("unknown node '{0}'")]
    UnknownNode(PlainNodeId),
    #[error("node '{0}' does not have the 'metadata-server' role")]
    InvalidRole(PlainNodeId),
    #[error("rejecting join request because node seems to belong to a different cluster: {0}")]
    ClusterIdentityMismatch(String),
}

type JoinClusterResponseSender = oneshot::Sender<Result<Version, JoinClusterError>>;

struct JoinClusterRequest {
    member_id: MemberId,
    cluster_identity: ClusterIdentity,
    response_tx: JoinClusterResponseSender,
}

impl JoinClusterRequest {
    fn into_inner(self) -> (JoinClusterResponseSender, MemberId, ClusterIdentity) {
        (self.response_tx, self.member_id, self.cluster_identity)
    }
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
        member_id: MemberId,
        cluster_identity: ClusterIdentity,
    ) -> Result<Version, JoinClusterError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.join_cluster_tx
            .send(JoinClusterRequest {
                member_id,
                cluster_identity,
                response_tx,
            })
            .await
            .map_err(|_| ShutdownError)?;

        response_rx.await.map_err(|_| ShutdownError)?
    }
}

type CreatedAtMillis = i64;

/// A member of the replicated metadata store is identified by its `node_id` and a durable
/// timestamp when it was first started.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MemberId {
    node_id: PlainNodeId,
    created_at_millis: CreatedAtMillis,
}

impl MemberId {
    pub fn new(node_id: PlainNodeId, created_at_millis: CreatedAtMillis) -> Self {
        MemberId {
            node_id,
            created_at_millis,
        }
    }
}

impl Display for MemberId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:x}", self.node_id, self.created_at_millis & 0x0ffff)
    }
}

/// Status summary of the metadata server.
#[derive(Clone, Debug, Default)]
enum MetadataServerSummary {
    #[default]
    Starting,
    Provisioning,
    Standby,
    Member {
        leader: Option<PlainNodeId>,
        configuration: MetadataServerConfiguration,
        raft: RaftSummary,
        snapshot: Option<SnapshotSummary>,
    },
}

#[derive(Clone, Debug, prost_dto::IntoProst, prost_dto::FromProst)]
#[prost(target = "restate_metadata_server_grpc::grpc::RaftSummary")]
struct RaftSummary {
    term: u64,
    committed: u64,
    applied: u64,
    first_index: u64,
    last_index: u64,
}

#[derive(Clone, Debug, prost_dto::IntoProst, prost_dto::FromProst)]
#[prost(target = "restate_metadata_server_grpc::grpc::SnapshotSummary")]
struct SnapshotSummary {
    index: u64,
    // size in bytes
    size: u64,
}

impl SnapshotSummary {
    fn from_snapshot(snapshot: &Snapshot) -> Self {
        SnapshotSummary {
            size: u64::try_from(snapshot.get_data().len()).expect("snapshot size to fit into u64"),
            index: snapshot.get_metadata().get_index(),
        }
    }
}

/// Ensures that the initial nodes configuration contains the current node and sets this node to be
/// a [`MetadataServerState::Member`] because it is the seed node for the metadata store cluster.
fn nodes_configuration_for_metadata_cluster_seed(
    configuration: &Configuration,
    nodes_configuration: &mut NodesConfiguration,
) -> Result<PlainNodeId, InvalidConfiguration> {
    let plain_node_id = if let Some(node_config) =
        nodes_configuration.find_node_by_name(configuration.common.node_name())
    {
        if let Some(force_node_id) = configuration.common.force_node_id
            && force_node_id != node_config.current_generation.as_plain()
        {
            return Err(InvalidConfiguration(format!(
                "nodes configuration has wrong plain node id; expected: {}, actual: {}",
                force_node_id,
                node_config.current_generation.as_plain()
            )));
        }

        let restate_node_id = node_config.current_generation.as_plain();

        let mut node_config = node_config.clone();
        node_config.metadata_server_config.metadata_server_state = MetadataServerState::Member;

        nodes_configuration.upsert_node(node_config);

        restate_node_id
    } else {
        // We have to add ourselves to the NodesConfiguration because we aren't part of it yet.

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

        let metadata_server_config = MetadataServerConfig {
            metadata_server_state: MetadataServerState::Provisioning,
        };

        let advertised_address = TaskCenter::with_current(|tc| {
            configuration.common.advertised_address(tc.address_book())
        });

        let node_config = NodeConfig::builder()
            .name(configuration.common.node_name().to_owned())
            .current_generation(current_generation)
            .location(configuration.common.location().clone())
            .address(advertised_address)
            .roles(configuration.common.roles)
            .metadata_server_config(metadata_server_config)
            .build();

        nodes_configuration.upsert_node(node_config);

        current_generation.as_plain()
    };

    Ok(plain_node_id)
}

type RemoveNodeResponseSender = oneshot::Sender<Result<(), MetadataCommandError>>;

#[derive(Debug)]
enum MetadataCommand {
    AddNode(oneshot::Sender<Result<(), MetadataCommandError>>),
    RemoveNode {
        plain_node_id: PlainNodeId,
        created_at_millis: Option<CreatedAtMillis>,
        response_tx: RemoveNodeResponseSender,
    },
}

impl MetadataCommand {
    fn fail(self, err: impl Into<MetadataCommandError>) {
        match self {
            MetadataCommand::AddNode(response_tx) => {
                // if receiver is gone, then it is no longer interested
                let _ = response_tx.send(Err(err.into()));
            }
            MetadataCommand::RemoveNode { response_tx, .. } => {
                // if receiver is gone, then it is no longer interested
                let _ = response_tx.send(Err(err.into()));
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum MetadataCommandError {
    #[error("service currently unavailable: {0}")]
    Unavailable(String),
    #[error("command needs to be processed by the metadata cluster leader")]
    NotLeader(Option<KnownLeader>),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("failed to add node: {0}")]
    AddNode(#[from] AddNodeError),
    #[error("failed to remove node: {0}")]
    RemoveNode(#[from] RemoveNodeError),
}

#[derive(Debug, thiserror::Error)]
enum AddNodeError {
    #[error(
        "node needs to join the Restate cluster first before becoming a metadata cluster member"
    )]
    NotReadyToJoin,
    #[error("cannot add node because it is still a member of the metadata cluster")]
    StillMember,
}

#[derive(Debug, thiserror::Error)]
enum RemoveNodeError {
    #[error("node '{0}' is not a member")]
    NotMember(MemberId),
    #[error("node '{0}' is not a member")]
    NotMemberPlainNodeId(PlainNodeId),
    #[error("pending reconfiguration, try at a later point")]
    PendingReconfiguration,
    #[error("unknown node '{0}'")]
    UnknownNode(PlainNodeId),
    #[error("concurrent remove request for node '{0}'")]
    ConcurrentRequest(PlainNodeId),
    #[error("cannot remove the only member '{0}' of the metadata cluster")]
    OnlyMember(MemberId),
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(any(test, feature = "test-util"))]
pub mod tests {
    use restate_types::{Version, Versioned, flexbuffers_storage_encode_decode};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
    pub struct Value {
        pub version: Version,
        pub value: u32,
    }

    impl Default for Value {
        fn default() -> Self {
            Self {
                version: Version::MIN,
                value: Default::default(),
            }
        }
    }

    impl Value {
        pub fn new(value: u32) -> Self {
            Value {
                value,
                ..Value::default()
            }
        }

        pub fn next_version(mut self) -> Self {
            self.version = self.version.next();
            self
        }
    }

    impl Versioned for Value {
        fn version(&self) -> Version {
            self.version
        }
    }

    flexbuffers_storage_encode_decode!(Value);
}
