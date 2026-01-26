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

use restate_types::net::codec::EncodeError;
use tonic::Code;

use restate_types::NodeId;
use restate_types::net::{CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION};
use restate_types::nodes_config::NodesConfigError;

use crate::ShutdownError;

use super::protobuf::network::{rpc_reply, watch_update};

#[derive(Debug, thiserror::Error)]
#[error("connection closed")]
pub struct ConnectionClosed;

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("target service has not started serving requests yet")]
    ServiceNotReady,
    #[error("target service has stopped serving requests")]
    ServiceStopped,
    #[error("target service has no capacity")]
    CapacityExceeded,
    #[error("target service was not found")]
    ServiceNotFound,
    #[error("message was unrecognized")]
    MessageUnrecognized,
}

impl From<RouterError> for rpc_reply::Status {
    fn from(value: RouterError) -> Self {
        match value {
            RouterError::ServiceNotReady => Self::ServiceNotReady,
            RouterError::ServiceStopped => Self::ServiceStopped,
            RouterError::CapacityExceeded => Self::LoadShedding,
            RouterError::ServiceNotFound => Self::ServiceNotFound,
            RouterError::MessageUnrecognized => Self::MessageUnrecognized,
        }
    }
}

/// A type to communicate rpc processing error to the caller
///
/// This is used by service handlers to report back that they are not able to process
/// the request. Note that this is only a subset of the errors in the network protocol
/// to ensure that the handler adheres to the contract of the rpc protocol.
///
/// The rest of the errors are emitted by the message routing infrastructure.
pub enum Verdict {
    /// The target service is known but the message type is not recognized by the handler
    MessageUnrecognized,
    /// The sort code does not translate into a valid service target
    SortCodeNotFound,
    /// The service decided to drop the request due to load shedding
    /// Note that the service **must not** process this message. The status
    /// returns gives the caller the guarantee that it's safe to retry the request
    /// without causing any duplication.
    LoadShedding,
    /// Message type was recognized but payload failed
    /// to decode.
    InvalidPayload,
}

impl From<Verdict> for rpc_reply::Status {
    fn from(value: Verdict) -> Self {
        match value {
            Verdict::MessageUnrecognized => Self::MessageUnrecognized,
            Verdict::SortCodeNotFound => Self::SortCodeNotFound,
            Verdict::LoadShedding => Self::LoadShedding,
            Verdict::InvalidPayload => Self::InvalidPayload,
        }
    }
}

impl From<Verdict> for watch_update::Start {
    fn from(value: Verdict) -> Self {
        match value {
            Verdict::MessageUnrecognized => Self::MessageUnrecognized,
            Verdict::SortCodeNotFound => Self::SortCodeNotFound,
            Verdict::LoadShedding => Self::LoadShedding,
            Verdict::InvalidPayload => Self::InvalidPayload,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("operation aborted, node is shutting down")]
    Shutdown(#[from] ShutdownError),
    #[error("exceeded deadline after spending {0:?}")]
    Timeout(Duration),
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    #[error(transparent)]
    ConnectionClosed(#[from] ConnectionClosed),
    #[error("cannot send messages to this node: {0}")]
    ConnectionFailed(String),
}

impl From<ConnectError> for NetworkError {
    fn from(value: ConnectError) -> Self {
        match value {
            ConnectError::Handshake(_) => NetworkError::ConnectionFailed(value.to_string()),
            ConnectError::Throttled(_) => NetworkError::ConnectionFailed(value.to_string()),
            ConnectError::Transport(e) => NetworkError::ConnectionFailed(e),
            ConnectError::Discovery(e) => NetworkError::Discovery(e),
            ConnectError::Shutdown(e) => NetworkError::Shutdown(e),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum DiscoveryError {
    #[error("node {0} was shut down or removed")]
    NodeIsGone(NodeId),
    #[error("node {0} was not found in config")]
    UnknownNodeId(NodeId),
}

#[derive(Debug, thiserror::Error)]
pub enum MessageSendError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Encoding(#[from] EncodeError),
    #[error(transparent)]
    ConnectionClosed(#[from] ConnectionClosed),
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum ConnectError {
    #[error(transparent)]
    Handshake(#[from] HandshakeError),
    #[error("connect throttled; {0:?} left in throttling window")]
    Throttled(Duration),
    #[error("transport error: {0}")]
    Transport(String),
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

impl From<tonic::Status> for ConnectError {
    fn from(value: tonic::Status) -> Self {
        match value.code() {
            Code::Ok => unreachable!(),
            Code::DeadlineExceeded => {
                Self::Handshake(HandshakeError::Timeout(value.message().to_owned()))
            }
            _ => Self::Handshake(HandshakeError::Failed(value.message().to_owned())),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum HandshakeError {
    #[error("handshake failed: {0}")]
    Failed(String),
    #[error("handshake timeout: {0}")]
    Timeout(String),
    #[error("peer dropped connection during handshake")]
    PeerDropped,
    #[error(
        "peer has unsupported protocol version {0}, supported versions are '[{min}:{max}]'",
        min = MIN_SUPPORTED_PROTOCOL_VERSION as i32,
        max = CURRENT_PROTOCOL_VERSION as i32
    )]
    UnsupportedVersion(i32),
}

impl From<HandshakeError> for tonic::Status {
    fn from(value: HandshakeError) -> Self {
        match value {
            HandshakeError::Failed(e) => tonic::Status::invalid_argument(e),
            HandshakeError::Timeout(e) => tonic::Status::deadline_exceeded(e),
            HandshakeError::PeerDropped => tonic::Status::cancelled("peer dropped"),
            HandshakeError::UnsupportedVersion(_) => {
                tonic::Status::invalid_argument(value.to_string())
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AcceptError {
    #[error("this node has not acquired a node ID yet")]
    NotReady,
    #[error(transparent)]
    Handshake(#[from] HandshakeError),
    #[error(transparent)]
    NodesConfig(#[from] NodesConfigError),
    #[error("new node generation exists: {0}")]
    OldPeerGeneration(String),
    #[error("node was previously observed as shutting down")]
    PreviouslyShutdown,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

impl From<AcceptError> for tonic::Status {
    fn from(value: AcceptError) -> Self {
        match value {
            e @ AcceptError::NotReady => tonic::Status::unavailable(e.to_string()),
            AcceptError::Handshake(handshake) => handshake.into(),
            AcceptError::OldPeerGeneration(e) => tonic::Status::already_exists(e),
            AcceptError::PreviouslyShutdown => tonic::Status::already_exists(value.to_string()),
            AcceptError::NodesConfig(err @ NodesConfigError::GenerationMismatch { .. }) => {
                tonic::Status::failed_precondition(err.to_string())
            }
            AcceptError::NodesConfig(err) => tonic::Status::invalid_argument(err.to_string()),
            AcceptError::Shutdown(e) => tonic::Status::aborted(e.to_string()),
        }
    }
}
