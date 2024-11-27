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
use crate::network::grpc_svc::NetworkMessage;
use crate::network::ConnectionManager;
use std::str::FromStr;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

pub const PEER_METADATA_KEY: &str = "x-restate-metadata-store-peer";

#[derive(Debug)]
pub struct MetadataStoreNetworkHandler {
    connection_manager: ConnectionManager,
}

impl MetadataStoreNetworkHandler {
    pub fn new(connection_manager: ConnectionManager) -> Self {
        Self { connection_manager }
    }
}

#[async_trait::async_trait]
impl MetadataStoreNetworkSvc for MetadataStoreNetworkHandler {
    type ConnectToStream = BoxStream<NetworkMessage>;

    async fn connect_to(
        &self,
        request: Request<Streaming<NetworkMessage>>,
    ) -> Result<Response<Self::ConnectToStream>, Status> {
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
        let outgoing_rx = self
            .connection_manager
            .accept_connection(peer, request.into_inner())?;
        Ok(Response::new(outgoing_rx))
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
