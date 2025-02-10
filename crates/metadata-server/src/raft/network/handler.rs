// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::raft::network::connection_manager::ConnectionError;
use crate::raft::network::grpc_svc::metadata_server_network_svc_server::MetadataServerNetworkSvc;
use crate::raft::network::grpc_svc::JoinClusterRequest;
use crate::raft::network::{grpc_svc, ConnectionManager, NetworkMessage, PEER_METADATA_KEY};
use crate::{JoinClusterError, JoinClusterHandle, MemberId};
use arc_swap::access::Access;
use arc_swap::ArcSwapOption;
use restate_types::PlainNodeId;
use std::str::FromStr;
use std::sync::Arc;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

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
                        "'{}' is missing",
                        PEER_METADATA_KEY
                    )))?;
            let peer = PlainNodeId::from_str(
                peer_metadata
                    .to_str()
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
            )
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
            let outgoing_rx = connection_manager.accept_connection(peer, request.into_inner())?;
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
    ) -> Result<Response<()>, Status> {
        if let Some(join_handle) = self.join_cluster_handle.as_ref() {
            let request = request.into_inner();
            join_handle
                .join_cluster(MemberId::new(
                    PlainNodeId::from(request.node_id),
                    request.created_at_millis,
                ))
                .await?;

            Ok(Response::new(()))
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
            JoinClusterError::UnknownNode(_)
            | JoinClusterError::InvalidRole(_)
            | JoinClusterError::Standby(_) => Status::invalid_argument(err.to_string()),
        }
    }
}

impl From<ConnectionError> for Status {
    fn from(value: ConnectionError) -> Self {
        match value {
            ConnectionError::Internal(err) => Status::internal(err),
            ConnectionError::Shutdown(err) => Status::aborted(err.to_string()),
        }
    }
}
