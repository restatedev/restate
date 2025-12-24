// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tracing::{Span, debug, info, trace, warn};

use restate_types::GenerationalNodeId;
use restate_types::net::ProtocolVersion;
use restate_types::net::codec::{EncodeError, WireEncode};
use restate_types::net::{RpcRequest, Service, ServiceTag, UnaryMessage};

use super::io::{EgressMessage, SendToken, Sent};
use super::{ConnectError, DiscoveryError, NetworkSender, RawRpcReply, ReplyRx, Swimlane};
use crate::TaskCenterFutureExt;
use crate::network::HandshakeError;

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "connection closed")
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum TrySendError<T> {
    /// The connection's buffer is full and it cannot accept more messages
    Full(T),
    /// The connection is permanently closed and cannot be used anymore
    Closed(T),
}

impl<T> TrySendError<T> {
    /// Consume the `TrySendError`, returning the unsent value.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(val) => val,
            TrySendError::Closed(val) => val,
        }
    }
}

impl<T> std::fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            TrySendError::Full(..) => "Full(..)".fmt(f),
            TrySendError::Closed(..) => "Closed(..)".fmt(f),
        }
    }
}

impl<T> std::fmt::Display for TrySendError<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            fmt,
            "{}",
            match self {
                TrySendError::Full(..) => "no available capacity",
                TrySendError::Closed(..) => "connection closed",
            }
        )
    }
}

impl<T> std::error::Error for TrySendError<T> {}
impl<T> std::error::Error for SendError<T> {}

/// A connection to a generational peer which connects in the background
///
/// This sender will buffer messages while waiting for the connection to be established,
/// and will drop all messages if the connection is closed. This connection is pinned to a specific
/// node generation and will not reconnect to a different generation. If a new generation preempts
/// the current one, the connection will be closed and all enqueued messages will be dropped.
///
/// You can use `auto_reconnect` to enable background re-connection, but it's important to
/// note that reconnections will not happen in preemption cases or if we believe that the node is
/// gone for good (e.g. we observed its shutdown)
///
/// The connection can be cheaply cloned and all clones will share the same connection and backing
/// buffer.
#[derive(Clone)]
pub struct LazyConnection {
    node_id: GenerationalNodeId,
    is_connected: Arc<AtomicBool>,
    sender: mpsc::Sender<OutgoingOp>,
}

impl LazyConnection {
    /// Creates a new connection to a generational peer
    ///
    /// `auto_reconnect` sets whether to reconnect automatically if the connection is lost. Note that the connection
    /// will not be retried if the connection to this node id not possible any more.
    ///
    /// Note that `buffer_size` is an additional buffer on top of the connection's buffer. The
    /// buffer is used to queue messages while waiting for the connection to be established.
    #[must_use]
    pub fn create<T: NetworkSender>(
        node_id: GenerationalNodeId,
        network_sender: T,
        swimlane: Swimlane,
        buffer_size: usize,
        auto_reconnect: bool,
    ) -> Self {
        let (sender, msg_rx) = mpsc::channel(buffer_size);
        let is_connected = Arc::new(AtomicBool::new(false));
        let conn = ConnectionTask {
            auto_reconnect,
            peer: node_id,
            swimlane,
            is_connected: is_connected.clone(),
            network_sender,
            msg_rx,
        };

        tokio::spawn(conn.run().in_current_tc());
        Self {
            node_id,
            is_connected,
            sender,
        }
    }

    #[must_use]
    pub fn node_id(&self) -> &GenerationalNodeId {
        &self.node_id
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    #[must_use]
    pub fn has_capacity(&self) -> bool {
        self.is_connected() && self.sender.capacity() > 0
    }

    #[must_use]
    pub fn is_connected(&self) -> bool {
        !self.is_closed() && self.is_connected.load(Ordering::Relaxed)
    }

    /// Sends a unary message over this sender
    ///
    /// This never blocks. It will return an error if the buffer is full or if the
    /// connection is closed.
    ///
    /// Note that you don't need to hold the `SendToken` token around, if you don't care
    /// about send acknowledgements, you simply drop/ignore it.
    pub fn try_send_unary<M: UnaryMessage>(
        &self,
        message: M,
        sort_code: Option<u64>,
    ) -> Result<SendToken, TrySendError<M>> {
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => return Err(TrySendError::Full(message)),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(TrySendError::Closed(message)),
        };

        let (notifier, send_token) = Sent::create();
        permit.send(OutgoingOp::Unary {
            notifier,
            message: Box::new(message),
            service_tag: M::Service::TAG,
            msg_type: M::TYPE,
            sort_code,
        });
        Ok(send_token)
    }

    /// Sends rpc call over this connection
    ///
    /// This never blocks. It will return an error if the buffer is full or if the
    /// connection is closed.
    ///
    /// Sends an rpc call over this connection or fail immediately if the connection is terminally
    /// closed or the buffer is full. If you drop `ReplyRx`, the message _might_ not get sent if it
    /// was not shipped to the peer yet.
    pub fn try_send_rpc<M: RpcRequest>(
        &self,
        message: M,
        sort_code: Option<u64>,
    ) -> Result<ReplyRx<M::Response>, TrySendError<M>> {
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => return Err(TrySendError::Full(message)),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(TrySendError::Closed(message)),
        };

        let (reply_sender, reply) = ReplyRx::new();
        permit.send(OutgoingOp::CallRpc {
            reply_sender,
            message: Box::new(message),
            service_tag: M::Service::TAG,
            msg_type: M::TYPE,
            sort_code,
            span: Span::current(),
        });

        Ok(reply)
    }

    /// Sends a unary message over this sender.
    ///
    /// Note that you don't need to hold the `SendToken` token around, if you don't care
    /// about send acknowledgements, you simply drop/ignore it.
    pub async fn send_unary<M: UnaryMessage>(
        &self,
        message: M,
        sort_code: Option<u64>,
    ) -> Result<SendToken, SendError<M>> {
        let permit = match self.sender.reserve().await {
            Ok(permit) => permit,
            Err(mpsc::error::SendError(())) => return Err(SendError(message)),
        };

        let (notifier, send_token) = Sent::create();
        permit.send(OutgoingOp::Unary {
            notifier,
            message: Box::new(message),
            service_tag: M::Service::TAG,
            msg_type: M::TYPE,
            sort_code,
        });
        Ok(send_token)
    }

    /// Sends an rpc call over this connection
    ///
    /// If you drop `ReplyRx`, the message _might_ not get sent if it
    /// was not shipped to the peer yet.
    pub async fn send_rpc<M: RpcRequest>(
        &self,
        message: M,
        sort_code: Option<u64>,
    ) -> Result<ReplyRx<M::Response>, SendError<M>> {
        let permit = match self.sender.reserve().await {
            Ok(permit) => permit,
            Err(mpsc::error::SendError(())) => return Err(SendError(message)),
        };

        let (reply_sender, reply) = ReplyRx::new();
        permit.send(OutgoingOp::CallRpc {
            reply_sender,
            message: Box::new(message),
            service_tag: M::Service::TAG,
            msg_type: M::TYPE,
            sort_code,
            span: Span::current(),
        });

        Ok(reply)
    }
}

enum OutgoingOp {
    CallRpc {
        reply_sender: oneshot::Sender<RawRpcReply>,
        message: Box<dyn WireEncode + Send>,
        service_tag: ServiceTag,
        msg_type: &'static str,
        sort_code: Option<u64>,
        span: Span,
    },
    Unary {
        notifier: Sent,
        message: Box<dyn WireEncode + Send>,
        service_tag: ServiceTag,
        msg_type: &'static str,
        sort_code: Option<u64>,
    },
}

impl OutgoingOp {
    fn into_egress_message(
        self,
        protocol_version: ProtocolVersion,
    ) -> Result<EgressMessage, EncodeError> {
        match self {
            OutgoingOp::CallRpc {
                reply_sender,
                message,
                service_tag,
                msg_type,
                sort_code,
                span,
            } => {
                let payload = message.encode_to_bytes(protocol_version)?;
                Ok(EgressMessage::RpcCall {
                    payload,
                    reply_sender,
                    span: Some(span),
                    sort_code,
                    service_tag,
                    msg_type,
                    version: protocol_version,
                    #[cfg(feature = "test-util")]
                    header: None,
                })
            }
            OutgoingOp::Unary {
                notifier,
                message,
                service_tag,
                msg_type,
                sort_code,
            } => {
                let payload = message.encode_to_bytes(protocol_version)?;
                Ok(EgressMessage::Unary {
                    service_tag,
                    msg_type,
                    sort_code,
                    payload,
                    version: protocol_version,
                    notifier,
                    #[cfg(feature = "test-util")]
                    header: None,
                })
            }
        }
    }
}

struct ConnectionTask<T> {
    auto_reconnect: bool,
    peer: GenerationalNodeId,
    swimlane: Swimlane,
    is_connected: Arc<AtomicBool>,
    network_sender: T,
    msg_rx: mpsc::Receiver<OutgoingOp>,
}

impl<T: NetworkSender> ConnectionTask<T> {
    async fn run(self) {
        let Self {
            auto_reconnect,
            peer,
            swimlane,
            is_connected,
            network_sender,
            mut msg_rx,
        } = self;
        let mut inflight_msg: Option<OutgoingOp> = None;
        'connect: loop {
            if msg_rx.is_closed() {
                return;
            }
            is_connected.store(false, Ordering::Relaxed);
            // If this fails with unrecoverable error, we don't try and reconnect, instead we update the
            // state and return.
            let connection = match network_sender.get_connection(peer, swimlane).await {
                Ok(connection) => connection,
                // We fail immediately if we are not allowed to reconnect anyway.
                Err(err) if !auto_reconnect => {
                    info!(%swimlane, %peer, "Connection failed and we were not asked to reconnect: {}", err);
                    // don't bother if we were asked to not retry.
                    return;
                }
                // on failure, we should see throttled in the subsequent attempt
                Err(err @ ConnectError::Transport(_)) => {
                    trace!(%swimlane, %peer, "Cannot connect, will retry: {}", err);
                    continue 'connect;
                }
                // on failure, we should see throttled in the subsequent attempt
                Err(err @ ConnectError::Throttled(dur)) => {
                    trace!(%swimlane, %peer, "Cannot connect, will retry later: {}", err);
                    // Should we wait the whole duration? probably not, as we might be able to
                    // connect earlier. This can happen if that peer connected to us with a
                    // bidirectional connection, or if failure detector decided that this node is
                    // now reachable. Therefore, we divide the duration by 2.
                    tokio::time::sleep(dur / 2).await;
                    continue 'connect;
                }
                Err(
                    // We don't know this node-id yet, but we might in the future, keep retrying.
                    err @ ConnectError::Discovery(DiscoveryError::UnknownNodeId(_)),
                ) => {
                    trace!(%swimlane, %peer, "Cannot to connect, will retry: {}", err);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue 'connect;
                }
                Err(
                    err @ ConnectError::Handshake(HandshakeError::Timeout(_))
                    | err @ ConnectError::Handshake(HandshakeError::Failed(_))
                    | err @ ConnectError::Handshake(HandshakeError::PeerDropped),
                ) => {
                    // Handshake::Failed is a bit of a wildcard, but we assume that it's safer to
                    // retry than to give up. Situations like connecting to the node before it
                    // acquires its own node id is retryable, but other might not be. The essence
                    // of the problem comes from the translation of grpc status errors that can
                    // include a wide variety of errors.
                    trace!(%swimlane, %peer, "Cannot to connect, will retry: {}", err);
                    continue 'connect;
                }
                // Terminal errors
                Err(ConnectError::Shutdown(_)) => {
                    debug!(%swimlane, %peer, "Cannot connect, we are shutting down");
                    return;
                }
                Err(err @ ConnectError::Handshake(HandshakeError::UnsupportedVersion(_))) => {
                    // We assume here that the peer cannot fix this problem without a software
                    // upgrade, which implies future a higher generation number is necessary.
                    warn!(%swimlane, %peer, "Cannot connect, giving up: {}", err);
                    return;
                }
                Err(err @ ConnectError::Discovery(DiscoveryError::NodeIsGone(_))) => {
                    //  Peer is not coming back, it's gone!
                    info!(%swimlane, %peer, "Cannot connect, giving up: {}", err);
                    return;
                }
            };

            // Connection established, we can send messages
            is_connected.store(true, Ordering::Relaxed);

            loop {
                // If we have a message that we already pulled from the queue, then we should try and
                // send it first.
                let msg = match inflight_msg.take() {
                    Some(msg) => msg,
                    None => match msg_rx.recv().await {
                        Some(msg) => msg,
                        None => {
                            // all senders were dropped, stop the task.
                            return;
                        }
                    },
                };

                // Note that we only try to secure a send permit after we have an actual message to
                // send. If we don't do this, we risk holding a permit indefinitely (if no messages
                // need to be sent) and this will block the connection from being closed cleanly.
                let Some(permit) = connection.reserve().await else {
                    // Connection was lost, reconnect
                    if auto_reconnect {
                        trace!(%swimlane, %peer, "Connection lost to {}, will reconnect", peer);
                        // keep the message so we can send it after we reconnect
                        inflight_msg = Some(msg);
                        continue 'connect;
                    } else {
                        trace!(%swimlane, %peer, "Connection lost to {}", peer);
                        return;
                    }
                };

                // Ship it!
                match msg.into_egress_message(connection.protocol_version) {
                    Ok(msg) => permit.send(msg),
                    Err(err) => {
                        // on error, the notifier token is dropped. The behaviour that the caller
                        // observes is identical to the message being Dropped.
                        debug!(%swimlane, %peer, "Failed to send message: {}", err);
                    }
                }
            }
        }
    }
}
