// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_node_protocol::CodecError;
use restate_types::nodes_config::NodesConfigError;

use crate::ShutdownError;

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("codec error: {0}")]
    CodecError(#[from] CodecError),
    #[error("target not registered: {0}")]
    NotRegisteredTarget(String),
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkSendError {
    #[error("unknown node: {0}")]
    UnknownNode(#[from] NodesConfigError),
    #[error("operation aborted, node is shutting down")]
    Shutdown(#[from] ShutdownError),
    #[error("oldPeerGeneration: {0}")]
    OldPeerGeneration(String),
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("peer is not connected")]
    ConnectionClosed,
    #[error("cannot send messages to this node: {0}")]
    Unavailable(String),
}
