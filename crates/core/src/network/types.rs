// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Poll, ready};
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use tokio::sync::oneshot;

use restate_types::net::{AdvertisedAddress, ProtocolVersion, RpcResponse};
use restate_types::{GenerationalNodeId, Version};

use super::protobuf::network::{Header, rpc_reply};
use super::{ConnectError, ConnectionClosed};

/// Address of a peer in the network. It can be a specific node or an anonymous peer.
#[derive(Debug, Clone, Copy, Eq, derive_more::IsVariant, derive_more::Display)]
pub enum PeerAddress {
    #[display("{_0}")]
    ServerNode(GenerationalNodeId),
    #[display("Anonymous")]
    Anonymous,
}

impl PartialEq for PeerAddress {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ServerNode(l0), Self::ServerNode(r0)) => l0 == r0,
            // anonymous peers are not comparable (partial equivalence)
            _ => false,
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, derive_more::Display)]
pub enum Destination {
    Address(AdvertisedAddress),
    Node(GenerationalNodeId),
}

#[derive(Clone, Copy, Default, derive_more::Debug)]
pub struct PeerMetadataVersion {
    #[debug("{}", logs.unwrap_or(Version::INVALID))]
    pub logs: Option<Version>,
    #[debug("{}", logs.unwrap_or(Version::INVALID))]
    pub nodes_config: Option<Version>,
    #[debug("{}", logs.unwrap_or(Version::INVALID))]
    pub partition_table: Option<Version>,
    #[debug("{}", logs.unwrap_or(Version::INVALID))]
    pub schema: Option<Version>,
}

impl From<Header> for PeerMetadataVersion {
    fn from(value: Header) -> Self {
        Self {
            logs: value.my_logs_version.map(Version::from),
            nodes_config: value.my_nodes_config_version.map(Version::from),
            partition_table: value.my_partition_table_version.map(Version::from),
            schema: value.my_schema_version.map(Version::from),
        }
    }
}

pub enum RawRpcReply {
    Success(Bytes),
    Error(RpcReplyError),
}

/// A token to receive RPC reply on
///
/// NOTE: This token is _also_ used to track your interest in sending the request **in addition** to
/// receiving the reply. Dropping this token will cancel sending the request if it was still queued
/// in egress stream. If the request was sent already, we'll simply ignore the response if it
/// arrives.
///
/// If this is not the behaviour you want, then perhaps the message should also
/// be defined as unary.
pub struct ReplyRx<O: RpcResponse> {
    protocol_version: ProtocolVersion,
    inner: oneshot::Receiver<RawRpcReply>,
    _marker: PhantomData<O>,
}

impl<O: RpcResponse> ReplyRx<O> {
    pub fn new(protocol_version: ProtocolVersion) -> (RpcReplyTx, Self) {
        let (reply_sender, inner) = oneshot::channel();
        (
            reply_sender,
            Self {
                protocol_version,
                inner,
                _marker: PhantomData,
            },
        )
    }
}

impl<O: RpcResponse + Unpin> Future for ReplyRx<O> {
    type Output = Result<O, RpcReplyError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match ready!(self.inner.poll_unpin(cx)) {
            Ok(RawRpcReply::Success(payload)) => {
                // todo: Should we carry the context from response header? Consult Azmy/Jack or
                // research.
                Poll::Ready(Ok(O::decode(payload, self.protocol_version)))
            }
            Ok(RawRpcReply::Error(err)) => Poll::Ready(Err(err)),
            // the send end has been dropped. This means that the connection was closed before we
            // were able to register this request in the reply tracker.
            //
            // Note that we register the
            Err(_) => Poll::Ready(Err(RpcReplyError::NotSent)),
        }
    }
}

/// Internal sender used to ship the response back to the caller.
///
/// It's life is bound to the user holding on to the ReplyRx. This is the _dual_ of ReplyRx.
pub(super) type RpcReplyTx = oneshot::Sender<RawRpcReply>;

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error(transparent)]
    ConnectionClosed(#[from] ConnectionClosed),
    #[error(transparent)]
    Send(#[from] ConnectError),
    #[error(transparent)]
    Receive(#[from] RpcReplyError),
    #[error("timed out")]
    Timeout(Duration),
}

#[derive(Debug, thiserror::Error)]
pub enum RpcReplyError {
    /// receiver will not respond to this request, it might have already processed it.
    #[error("peer respond to this RPC request with unrecognised error reason (status={0})")]
    Unknown(i32),
    /// The peer might have processed this request
    #[error("the peer dropped the reply port for this request")]
    Dropped,
    /// It's safe to assume that the request was not sent to the peer.
    #[error("the connection was dropped before sending the request")]
    NotSent,
    /// Connection was closed while waiting for reply, the request has (probably) been sent.
    #[error(transparent)]
    ConnectionClosed(ConnectionClosed),
    /// target service was not found or not registered at destination node
    #[error("the requested rpc service is not registered or has been stopped at peer")]
    ServiceNotFound,
    #[error("the requested rpc service did not start accepting requests yet")]
    ServiceNotReady,
    #[error("the requested rpc service stopped processing messages")]
    ServiceStopped,
    #[error("the requested rpc service didn't recognize this message")]
    MessageUnrecognized,
    #[error("peer dropped the request due to back-pressure")]
    LoadShedding,
    #[error(
        "the target has the rpc service but it has rejected serving the request for the supplied sort code"
    )]
    SortCodeNotFound,
}

impl RpcReplyError {
    /// Whether the peer have _processed_ the request or not. If this returns false, it's
    /// guaranteed that the peer have not processed the request.
    pub fn maybe_processed(&self) -> bool {
        match self {
            RpcReplyError::Unknown(_)
            | RpcReplyError::Dropped
            | RpcReplyError::ConnectionClosed(_) => true,
            RpcReplyError::ServiceNotFound
            | RpcReplyError::NotSent
            | RpcReplyError::LoadShedding
            | RpcReplyError::ServiceStopped
            | RpcReplyError::MessageUnrecognized
            | RpcReplyError::ServiceNotReady
            | RpcReplyError::SortCodeNotFound => false,
        }
    }
}

// i32 representation of the enum rpc_reply::Status
impl From<i32> for RpcReplyError {
    fn from(value: i32) -> Self {
        let status =
            rpc_reply::Status::try_from(value).map_err(|unknown| RpcReplyError::Unknown(unknown.0));
        match status {
            Ok(rpc_reply::Status::Unknown) => RpcReplyError::Unknown(0),
            Ok(rpc_reply::Status::ServiceNotFound) => RpcReplyError::ServiceNotFound,
            Ok(rpc_reply::Status::SortCodeNotFound) => RpcReplyError::SortCodeNotFound,
            Ok(rpc_reply::Status::Dropped) => RpcReplyError::Dropped,
            Ok(rpc_reply::Status::LoadShedding) => RpcReplyError::LoadShedding,
            Ok(rpc_reply::Status::ServiceStopped) => RpcReplyError::ServiceStopped,
            Ok(rpc_reply::Status::ServiceNotReady) => RpcReplyError::ServiceNotReady,
            Ok(rpc_reply::Status::MessageUnrecognized) => RpcReplyError::MessageUnrecognized,
            Err(err) => err,
        }
    }
}
