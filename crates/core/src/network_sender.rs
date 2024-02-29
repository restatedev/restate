// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use restate_node_protocol::NetworkMessage;
use restate_types::nodes_config::NodesConfigError;
use restate_types::NodeId;

use crate::ShutdownError;

#[derive(Debug, thiserror::Error)]
pub enum NetworkSendError {
    #[error("Unknown node: {0}")]
    UnknownNode(#[from] NodesConfigError),
    #[error("Operation aborted, node is shutting down")]
    Shutdown(#[from] ShutdownError),
    #[error("OldPeerGeneration: {0}")]
    OldPeerGeneration(String),
    #[error("Cannot send messages to this node: {0}")]
    Unavailable(String),
}

/// Send NetworkMessage to nodes
#[async_trait]
pub trait NetworkSender: Send + Sync {
    /// Send a message to a peer node. Order of messages is not guaranteed since underlying
    /// implementations might load balance message writes across multiple connections or re-order
    /// messages in-flight based on priority. If ordered delivery is required, then use
    /// [`restate_network::ConnectionSender`] instead.
    ///
    /// Establishing connections is handled internally with basic retries for straight-forward
    /// failures.
    ///
    /// If `to` is a NodeID with generation, then it's guaranteed that messages will be sent to
    /// this particular generation, otherwise, it'll be routed to the latest generation available
    /// in nodes configuration at the time of the call. This might return
    /// [[`NetworkSendError::OldPeerGeneration`]] if the node is is not the latest generation.
    ///
    /// It returns Ok(()) when the message is:
    /// - Successfully serialized to the wire format based on the negotiated protocol
    /// - Serialized message was enqueued on the send buffer of the socket
    ///
    /// That means that this is not a guarantee that the message has been sent
    /// over the network or that the peer have received it.
    async fn send(&self, to: NodeId, message: &NetworkMessage) -> Result<(), NetworkSendError>;
}
