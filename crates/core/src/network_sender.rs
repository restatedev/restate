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

/// Access to node-to-node networking infrastructure
#[async_trait]
pub trait NetworkSender: Send + Sync {
    async fn send(&self, to: NodeId, message: &NetworkMessage) -> Result<(), NetworkSendError>;
}
