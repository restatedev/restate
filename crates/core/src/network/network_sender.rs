// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use restate_types::NodeId;
use restate_types::net::RpcRequest;

// re-export Swimlane from protobuf
use super::connection::OwnedSendPermit;
use super::{ConnectError, Connection, DiscoveryError, LazyConnection, RpcError};
pub use crate::network::protobuf::network::Swimlane;

/// Send NetworkMessage to nodes
pub trait NetworkSender: Clone + Send + Sync + 'static {
    /// Get a connection to a peer node
    fn get_connection<N>(
        &self,
        node_id: N,
        swimlane: Swimlane,
    ) -> impl std::future::Future<Output = Result<Connection, ConnectError>> + Send
    where
        N: Into<NodeId> + Send;

    /// Gets a connection to a peer node, but does not block if the connection is not ready.
    ///
    /// The connection is established in the background
    fn lazy_connect<N>(
        &self,
        node_id: N,
        swimlane: Swimlane,
        buffer_size: usize,
        auto_reconnect: bool,
    ) -> Result<LazyConnection, DiscoveryError>
    where
        N: Into<NodeId> + Send;

    /// Acquire an owned send permit for a node
    fn reserve_owned<N>(
        &self,
        node_id: N,
        swimlane: Swimlane,
    ) -> impl std::future::Future<Output = Option<OwnedSendPermit>> + Send
    where
        N: Into<NodeId> + Send;

    /// Call an RPC method on a peer node
    fn call_rpc<M, N>(
        &self,
        node_id: N,
        swimlane: Swimlane,
        message: M,
        sort_code: Option<u64>,
        timeout: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<M::Response, RpcError>> + Send
    where
        M: RpcRequest,
        N: Into<NodeId> + Send;
}
