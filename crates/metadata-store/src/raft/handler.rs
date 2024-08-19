// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::raft::connection_manager::{ConnectionError, ConnectionManager};
use crate::raft::grpc_svc::raft_metadata_store_svc_server::RaftMetadataStoreSvc;
use crate::raft::grpc_svc::RaftMessage;
use std::str::FromStr;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

pub const RAFT_PEER_METADATA_KEY: &str = "x-restate-raft-peer";

#[derive(Debug)]
pub struct RaftMetadataStoreHandler {
    connection_manager: ConnectionManager,
}

impl RaftMetadataStoreHandler {
    pub fn new(connection_manager: ConnectionManager) -> Self {
        Self { connection_manager }
    }
}

#[async_trait::async_trait]
impl RaftMetadataStoreSvc for RaftMetadataStoreHandler {
    type RaftStream = BoxStream<RaftMessage>;

    async fn raft(
        &self,
        request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<Self::RaftStream>, Status> {
        let raft_peer_metadata =
            request
                .metadata()
                .get(RAFT_PEER_METADATA_KEY)
                .ok_or(Status::invalid_argument(format!(
                    "'{}' is missing",
                    RAFT_PEER_METADATA_KEY
                )))?;
        let raft_peer = u64::from_str(
            raft_peer_metadata
                .to_str()
                .map_err(|err| Status::invalid_argument(err.to_string()))?,
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let outgoing_rx = self
            .connection_manager
            .accept_connection(raft_peer, request.into_inner())?;
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
