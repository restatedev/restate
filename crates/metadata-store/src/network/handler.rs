// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::network::connection_manager::ConnectionError;
use crate::network::grpc_svc::metadata_store_network_svc_server::MetadataStoreNetworkSvc;
use crate::network::grpc_svc::{JoinClusterRequest, JoinClusterResponse};
use crate::network::{grpc_svc, ConnectionManager, NetworkMessage};
use crate::{JoinClusterError, JoinClusterHandle};
use arc_swap::access::Access;
use arc_swap::ArcSwapOption;
use std::str::FromStr;
use std::sync::Arc;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

pub const PEER_METADATA_KEY: &str = "x-restate-metadata-store-peer";
pub const KNOWN_LEADER_KEY: &str = "x-restate-known-leader";

#[derive(Debug)]
pub struct MetadataStoreNetworkHandler<M> {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<M>>>,
    join_cluster_handle: Option<JoinClusterHandle>,
}

impl<M> MetadataStoreNetworkHandler<M> {
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
impl<M> MetadataStoreNetworkSvc for MetadataStoreNetworkHandler<M>
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
            let peer = u64::from_str(
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
    ) -> Result<Response<JoinClusterResponse>, Status> {
        if let Some(join_handle) = self.join_cluster_handle.as_ref() {
            let request = request.into_inner();
            let join_result = join_handle
                .join_cluster(request.node_id, request.storage_id)
                .await?;

            Ok(Response::new(JoinClusterResponse {
                metadata_store_config: join_result.metadata_store_config,
                log_prefix: join_result.log_prefix,
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
            JoinClusterError::NotActive(known_leader) => {
                let mut status = Status::failed_precondition(err.to_string());

                if let Some(known_leader) = known_leader {
                    status.metadata_mut().insert(
                        KNOWN_LEADER_KEY,
                        serde_json::to_string(known_leader)
                            .expect("KnownLeader to be serializable")
                            .parse()
                            .expect("to be valid metadata"),
                    );
                }

                status
            }
            JoinClusterError::NotLeader(known_leader) => {
                let mut status = Status::unavailable(err.to_string());

                if let Some(known_leader) = known_leader {
                    status.metadata_mut().insert(
                        KNOWN_LEADER_KEY,
                        serde_json::to_string(known_leader)
                            .expect("KnownLeader to be serializable")
                            .parse()
                            .expect("to be valid metadata"),
                    );
                }

                status
            }
            JoinClusterError::ConfigError(_) => Status::internal(err.to_string()),
            JoinClusterError::PendingReconfiguration => Status::unavailable(err.to_string()),
            JoinClusterError::ConcurrentRequest(_) => Status::aborted(err.to_string()),
            JoinClusterError::Internal(_) => Status::internal(err.to_string()),
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
