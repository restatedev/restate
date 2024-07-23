// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::net::{CodecError, MIN_SUPPORTED_PROTOCOL_VERSION};
use restate_types::nodes_config::NodesConfigError;
use restate_types::NodeId;

use crate::{ShutdownError, SyncError};

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("codec error: {0}")]
    CodecError(#[from] CodecError),
    #[error("target not registered: {0}")]
    NotRegisteredTarget(String),
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("unknown node: {0}")]
    UnknownNode(#[from] NodesConfigError),
    #[error("operation aborted, node is shutting down")]
    Shutdown(#[from] ShutdownError),
    #[error("node {0} address is bad: {1}")]
    BadNodeAddress(NodeId, http_0_2::Error),
    #[error("timeout: {0}")]
    Timeout(&'static str),
    #[error("protocol error: {0}")]
    ProtocolError(#[from] ProtocolError),
    #[error("cannot connect: {} {}", tonic_0_10::Status::code(.0), tonic_0_10::Status::message(.0))]
    ConnectError(#[from] tonic_0_10::Status),
    #[error("new node generation exists: {0}")]
    OldPeerGeneration(String),
    #[error("peer is not connected")]
    ConnectionClosed,
    #[error("cannot send messages to this node: {0}")]
    Unavailable(String),
    #[error("failed syncing metadata: {0}")]
    Metadata(#[from] SyncError),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("handshake failed: {0}")]
    HandshakeFailed(&'static str),
    #[error("handshake timeout: {0}")]
    HandshakeTimeout(&'static str),
    #[error("peer dropped connection")]
    PeerDropped,
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("grpc error: {0}")]
    GrpcError(#[from] tonic_0_10::Status),
    #[error(
        "peer has unsupported protocol version {0}, minimum supported is '{}'",
        MIN_SUPPORTED_PROTOCOL_VERSION as i32
    )]
    UnsupportedVersion(i32),
}

impl From<ProtocolError> for tonic_0_10::Status {
    fn from(value: ProtocolError) -> Self {
        match value {
            ProtocolError::HandshakeFailed(e) => tonic_0_10::Status::invalid_argument(e),
            ProtocolError::HandshakeTimeout(e) => tonic_0_10::Status::deadline_exceeded(e),
            ProtocolError::PeerDropped => tonic_0_10::Status::cancelled("peer dropped"),
            ProtocolError::Codec(e) => tonic_0_10::Status::internal(e.to_string()),
            ProtocolError::UnsupportedVersion(_) => {
                tonic_0_10::Status::invalid_argument(value.to_string())
            }
            ProtocolError::GrpcError(s) => s,
        }
    }
}

impl From<NetworkError> for tonic_0_10::Status {
    fn from(value: NetworkError) -> Self {
        match value {
            NetworkError::Shutdown(_) => tonic_0_10::Status::unavailable(value.to_string()),
            NetworkError::ProtocolError(e) => e.into(),
            NetworkError::Timeout(e) => tonic_0_10::Status::deadline_exceeded(e),
            NetworkError::OldPeerGeneration(e) => tonic_0_10::Status::already_exists(e),
            NetworkError::ConnectError(s) => s,
            e => tonic_0_10::Status::internal(e.to_string()),
        }
    }
}
