// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use restate_types::net::RpcRequest;
use restate_types::{GenerationalNodeId, NodeId};

use tokio::time::Instant;

use super::connection::OwnedSendPermit;
use super::{
    ConnectError, Connection, ConnectionClosed, ConnectionManager, LazyConnection,
    MessageSendError, NetworkSender, RpcError, Swimlane,
};
use super::{GrpcConnector, TransportConnect};

/// Access to node-to-node networking infrastructure.
pub struct Networking<T> {
    connections: ConnectionManager,
    connector: T,
}

impl<T: Clone> Clone for Networking<T> {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            connector: self.connector.clone(),
        }
    }
}

impl Networking<GrpcConnector> {
    pub fn with_grpc_connector() -> Self {
        Self {
            connections: ConnectionManager::default(),
            connector: GrpcConnector::default(),
        }
    }
}

#[cfg(feature = "test-util")]
/// used for testing. Accepts connections but can't establish new connections
impl Networking<super::FailingConnector> {
    pub fn new_incoming_only() -> Self {
        Self {
            connections: ConnectionManager::default(),
            connector: super::FailingConnector,
        }
    }
}

impl<T: TransportConnect> Networking<T> {
    pub fn with_connector(connector: T) -> Self {
        Self {
            connector,
            connections: ConnectionManager::default(),
        }
    }

    pub fn connection_manager(&self) -> &ConnectionManager {
        &self.connections
    }
}

impl<T: TransportConnect> NetworkSender for Networking<T> {
    /// Get a connection to a peer node
    async fn get_connection<N: Into<NodeId>>(
        &self,
        node_id: N,
        swimlane: Swimlane,
    ) -> Result<Connection, ConnectError> {
        self.connections
            .get_or_connect(node_id, swimlane, &self.connector)
            .await
    }

    fn lazy_connect(
        &self,
        node_id: GenerationalNodeId,
        swimlane: Swimlane,
        buffer_size: usize,
        auto_reconnect: bool,
    ) -> LazyConnection {
        LazyConnection::create(node_id, self.clone(), swimlane, buffer_size, auto_reconnect)
    }

    /// Acquire an owned send permit for a node
    async fn reserve_owned<N: Into<NodeId>>(
        &self,
        node_id: N,
        swimlane: Swimlane,
    ) -> Option<OwnedSendPermit> {
        let connection = self
            .connections
            .get_or_connect(node_id, swimlane, &self.connector)
            .await
            .ok()?;
        connection.reserve_owned().await
    }

    /// Call an RPC method on a peer node
    async fn call_rpc<M, N>(
        &self,
        node_id: N,
        swimlane: Swimlane,
        msg: M,
        sort_code: Option<u64>,
        timeout: Option<Duration>,
    ) -> Result<M::Response, RpcError>
    where
        M: RpcRequest,
        N: Into<NodeId> + Send,
    {
        let start = Instant::now();
        let op = async {
            let connection = self
                .connections
                .get_or_connect(node_id, swimlane, &self.connector)
                .await
                .map_err(MessageSendError::from)?;
            let permit = match connection.reserve().await {
                None => return Err(ConnectionClosed.into()),
                Some(permit) => permit,
            };
            let reply = permit
                .send_rpc(msg, sort_code)
                .map_err(MessageSendError::from)?;
            Ok(reply.await?)
        };

        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, op)
                .await
                .map_err(|_| RpcError::Timeout(start.elapsed()))?,
            None => op.await,
        }
    }
}
