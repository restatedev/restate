// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use arc_swap::access::Access;
use tonic::codec::CompressionEncoding;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

use restate_types::PlainNodeId;
use restate_types::config::NetworkingOptions;
use restate_types::net::connect_opts::GrpcConnectionOptions;
use restate_types::nodes_config::ClusterFingerprint;

use super::MetadataServerNetworkSvcServer;
use crate::raft::network::connection_manager::ConnectionError;
use crate::raft::network::grpc_svc::metadata_server_network_svc_server::MetadataServerNetworkSvc;
use crate::raft::network::grpc_svc::{JoinClusterRequest, JoinClusterResponse};
use crate::raft::network::{
    CLUSTER_FINGERPRINT_METADATA_KEY, CLUSTER_NAME_METADATA_KEY, ConnectionManager, NetworkMessage,
    PEER_METADATA_KEY, grpc_svc,
};
use crate::{ClusterIdentity, JoinClusterError, JoinClusterHandle, MemberId};

#[derive(Debug)]
pub struct MetadataServerNetworkHandler<M> {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<M>>>,
    join_cluster_handle: Option<JoinClusterHandle>,
}

impl<M> MetadataServerNetworkHandler<M> {
    pub fn new(
        connection_manager: Arc<ArcSwapOption<ConnectionManager<M>>>,
        join_cluster_handle: Option<JoinClusterHandle>,
    ) -> Self {
        Self {
            connection_manager,
            join_cluster_handle,
        }
    }

    pub fn into_server(self, config: &NetworkingOptions) -> MetadataServerNetworkSvcServer<Self> {
        let server = MetadataServerNetworkSvcServer::new(self)
            .max_decoding_message_size(config.message_size_limit().get())
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip);
        if config.disable_compression {
            server
        } else {
            // note: the order of those calls defines the priority
            // deflate/gzip has significantly higher CPU overhead according to our CPU profiling,
            // so we prefer zstd over gzip.
            server
                .send_compressed(CompressionEncoding::Zstd)
                .send_compressed(CompressionEncoding::Gzip)
        }
    }
}

#[async_trait::async_trait]
impl<M> MetadataServerNetworkSvc for MetadataServerNetworkHandler<M>
where
    M: NetworkMessage + Send + 'static,
{
    type ConnectToStream = BoxStream<grpc_svc::NetworkMessage>;

    async fn connect_to(
        &self,
        request: Request<Streaming<grpc_svc::NetworkMessage>>,
    ) -> Result<Response<Self::ConnectToStream>, Status> {
        if let Some(connection_manager) = self.connection_manager.load().as_ref() {
            let peer_metadata =
                request
                    .metadata()
                    .get(PEER_METADATA_KEY)
                    .ok_or(Status::invalid_argument(format!(
                        "'{PEER_METADATA_KEY}' is missing"
                    )))?;
            let peer = PlainNodeId::from_str(
                peer_metadata
                    .to_str()
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
            )
            .map_err(|err| Status::invalid_argument(err.to_string()))?;

            // Extract optional cluster fingerprint from metadata
            let cluster_fingerprint = request
                .metadata()
                .get(CLUSTER_FINGERPRINT_METADATA_KEY)
                .map(|v| {
                    v.to_str()
                        .map_err(|err| Status::invalid_argument(err.to_string()))?
                        .parse::<u64>()
                        .map_err(|err| Status::invalid_argument(err.to_string()))
                })
                .transpose()?
                .map(ClusterFingerprint::try_from)
                .transpose()
                .map_err(|err| Status::invalid_argument(err.to_string()))?;

            // Extract optional cluster name from metadata
            let cluster_name: Option<String> = request
                .metadata()
                .get(CLUSTER_NAME_METADATA_KEY)
                .map(|v| {
                    v.to_str()
                        .map(|s| s.to_owned())
                        .map_err(|err| Status::invalid_argument(err.to_string()))
                })
                .transpose()?;

            let outgoing_rx = connection_manager.accept_connection(
                peer,
                cluster_fingerprint,
                cluster_name.as_deref(),
                request.into_inner(),
            )?;
            Ok(Response::new(outgoing_rx))
        } else {
            Err(Status::unavailable(
                "The metadata store instance has not been fully initialized yet.",
            ))
        }
    }

    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterResponse>, Status> {
        if let Some(join_handle) = self.join_cluster_handle.as_ref() {
            let request = request.into_inner();

            // Convert the optional fingerprint, if it is 0 (invalid) then we treat it as None
            let cluster_identity = ClusterIdentity {
                fingerprint: ClusterFingerprint::try_from(request.cluster_fingerprint).ok(),
                cluster_name: request.cluster_name,
            };

            let nodes_config_version = join_handle
                .join_cluster(
                    MemberId::new(
                        PlainNodeId::from(request.node_id),
                        request.created_at_millis,
                    ),
                    cluster_identity,
                )
                .await?;

            Ok(Response::new(JoinClusterResponse {
                nodes_config_version: Some(nodes_config_version.into()),
            }))
        } else {
            Err(Status::unimplemented(
                "The metadata store does not support joining of other nodes",
            ))
        }
    }
}

impl From<JoinClusterError> for Status {
    fn from(err: JoinClusterError) -> Self {
        match &err {
            JoinClusterError::Shutdown(_) => Status::aborted(err.to_string()),
            JoinClusterError::NotMember(known_leader) => {
                let mut status = Status::failed_precondition(err.to_string());

                if let Some(known_leader) = known_leader {
                    known_leader.add_to_status(&mut status);
                }

                status
            }
            JoinClusterError::NotLeader(known_leader) => {
                let mut status = Status::unavailable(err.to_string());

                if let Some(known_leader) = known_leader {
                    known_leader.add_to_status(&mut status);
                }

                status
            }
            JoinClusterError::PendingReconfiguration => Status::unavailable(err.to_string()),
            JoinClusterError::ConcurrentRequest(_) => Status::aborted(err.to_string()),
            JoinClusterError::Internal(_) | JoinClusterError::ProposalDropped => {
                Status::internal(err.to_string())
            }
            JoinClusterError::UnknownNode(_) | JoinClusterError::InvalidRole(_) => {
                Status::invalid_argument(err.to_string())
            }
            JoinClusterError::ClusterIdentityMismatch(_) => {
                Status::permission_denied(err.to_string())
            }
        }
    }
}

impl From<ConnectionError> for Status {
    fn from(value: ConnectionError) -> Self {
        match value {
            ConnectionError::Internal(err) => Status::internal(err),
            ConnectionError::Shutdown(err) => Status::aborted(err.to_string()),
            ConnectionError::ClusterFingerprintMismatch | ConnectionError::ClusterNameMismatch => {
                Status::permission_denied(value.to_string())
            }
        }
    }
}
