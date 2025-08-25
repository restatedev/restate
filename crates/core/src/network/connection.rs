// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod throttle;

use restate_types::net::codec::EncodeError;
// re-export
pub use throttle::ConnectThrottle;

use std::sync::Arc;

use metrics::counter;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::Span;
use tracing::debug;

use restate_types::GenerationalNodeId;
use restate_types::config::Configuration;
use restate_types::net::ProtocolVersion;
use restate_types::net::RpcRequest;
use restate_types::net::UnaryMessage;

use crate::Metadata;
use crate::TaskId;
use crate::TaskKind;
use crate::network::PeerMetadataVersion;
use crate::network::metric_definitions::NETWORK_CONNECTION_CREATED;

use super::ConnectError;
use super::ConnectionClosed;
use super::ConnectionDirection;
use super::Destination;
use super::HandshakeError;
use super::MessageRouter;
use super::ReplyRx;
use super::Swimlane;
use super::TransportConnect;
use super::handshake::wait_for_welcome;
use super::io::ConnectionReactor;
use super::io::EgressStream;
use super::io::SendToken;
use super::io::{DrainReason, EgressMessage, EgressSender};
use super::protobuf::network::Header;
use super::protobuf::network::Hello;
use super::tracking::ConnectionTracking;

/// A permit to send a single message over a connection
///
/// While holding the permit, the connection cannot gracefully drain until this
/// permit is dropped. However, the connection can still be dropped if it was terminated
/// by the peer.
pub struct OwnedSendPermit {
    protocol_version: ProtocolVersion,
    permit: mpsc::OwnedPermit<EgressMessage>,
}

/// A permit to send a single message over a connection
///
/// While holding the permit, the the connection cannot gracefully drain until this
/// permit is dropped. However, the connection can still be dropped if it was terminated
/// by the peer.
pub struct SendPermit<'a> {
    protocol_version: ProtocolVersion,
    permit: mpsc::Permit<'a, EgressMessage>,
}

impl SendPermit<'_> {
    /// Send a raw egress message over this permit
    pub fn send(self, msg: EgressMessage) {
        self.permit.send(msg);
    }

    // -- Helpers --

    /// Sends a unary message over this permit.
    pub fn send_unary<M: UnaryMessage>(
        self,
        message: M,
        sort_code: Option<u64>,
    ) -> Result<SendToken, EncodeError> {
        let (msg, token) =
            EgressMessage::make_unary_message(message, sort_code, self.protocol_version)?;
        self.permit.send(msg);
        Ok(token)
    }

    /// Sends an rpc call over this permit.
    pub fn send_rpc<M: RpcRequest>(
        self,
        message: M,
        sort_code: Option<u64>,
    ) -> Result<ReplyRx<M::Response>, EncodeError> {
        let (msg, reply_token) =
            EgressMessage::make_rpc_message(message, sort_code, self.protocol_version)?;
        self.permit.send(msg);

        Ok(reply_token)
    }

    #[cfg(feature = "test-util")]
    pub fn send_rpc_with_header<M: RpcRequest>(
        self,
        message: M,
        sort_code: Option<u64>,
        header: Header,
    ) -> Result<ReplyRx<M::Response>, EncodeError> {
        let (msg, reply_token) = EgressMessage::make_rpc_message_with_header(
            message,
            sort_code,
            self.protocol_version,
            header,
        )?;
        self.permit.send(msg);

        Ok(reply_token)
    }
}

impl OwnedSendPermit {
    /// Sends a unary message over this permit.
    pub fn send_unary<M: UnaryMessage>(
        self,
        message: M,
        sort_code: Option<u64>,
    ) -> Result<SendToken, EncodeError> {
        let (msg, token) =
            EgressMessage::make_unary_message(message, sort_code, self.protocol_version)?;
        self.permit.send(msg);
        Ok(token)
    }

    /// Sends an rpc call over this permit.
    pub fn send_rpc<M: RpcRequest>(
        self,
        message: M,
        sort_code: Option<u64>,
    ) -> Result<ReplyRx<M::Response>, EncodeError> {
        let (msg, reply_token) =
            EgressMessage::make_rpc_message(message, sort_code, self.protocol_version)?;
        self.permit.send(msg);

        Ok(reply_token)
    }
}

/// A single streaming connection with a channel to the peer. A connection can be
/// opened by either ends of the connection and has no direction. Any connection
/// can be used to send or receive from a peer.
#[derive(Clone, derive_more::Debug)]
pub struct Connection {
    pub(crate) peer: GenerationalNodeId,
    pub(crate) protocol_version: ProtocolVersion,
    #[debug(skip)]
    pub(crate) sender: EgressSender,
    pub(crate) swimlane: Swimlane,
    pub(crate) created: Instant,
}

impl Connection {
    pub(crate) fn new(
        peer: GenerationalNodeId,
        protocol_version: ProtocolVersion,
        swimlane: Swimlane,
        sender: EgressSender,
    ) -> Self {
        Self {
            peer,
            protocol_version,
            sender,
            swimlane,
            created: Instant::now(),
        }
    }

    /// Starts a new _dedicated_ connection to a destination
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        destination: Destination,
        swimlane: Swimlane,
        transport_connector: impl TransportConnect,
        direction: ConnectionDirection,
        task_kind: TaskKind,
        router: Arc<MessageRouter>,
        conn_tracker: impl ConnectionTracking + Send + Sync + 'static,
        is_dedicated: bool,
    ) -> Result<(Self, TaskId), ConnectError> {
        ConnectThrottle::may_connect(&destination)?;
        Self::force_connect(
            destination.clone(),
            swimlane,
            transport_connector,
            direction,
            task_kind,
            router,
            conn_tracker,
            is_dedicated,
        )
        .await
    }

    /// Does not check for throttling before attempting a connection.
    #[allow(clippy::too_many_arguments)]
    pub async fn force_connect(
        destination: Destination,
        swimlane: Swimlane,
        transport_connector: impl TransportConnect,
        direction: ConnectionDirection,
        task_kind: TaskKind,
        router: Arc<MessageRouter>,
        conn_tracker: impl ConnectionTracking + Send + Sync + 'static,
        is_dedicated: bool,
    ) -> Result<(Self, TaskId), ConnectError> {
        let result = Self::connect_inner(
            destination.clone(),
            swimlane,
            transport_connector,
            direction,
            task_kind,
            router,
            conn_tracker,
            is_dedicated,
        )
        .await;

        ConnectThrottle::note_connect_status(&destination, result.is_ok());
        match result {
            Err(ref e) => {
                debug!(%direction, %swimlane, "Couldn't connect to {}: {}", destination, e);
            }
            Ok((_, task_id)) => {
                debug!(%direction, %swimlane, %task_id, "Connection established to {}", destination);
            }
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_inner(
        destination: Destination,
        swimlane: Swimlane,
        transport_connector: impl TransportConnect,
        direction: ConnectionDirection,
        task_kind: TaskKind,
        router: Arc<MessageRouter>,
        conn_tracker: impl ConnectionTracking + Send + Sync + 'static,
        is_dedicated: bool,
    ) -> Result<(Self, TaskId), ConnectError> {
        Self::connect_inner_with_node_id(
            None,
            destination,
            swimlane,
            transport_connector,
            direction,
            task_kind,
            router,
            conn_tracker,
            is_dedicated,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn connect_inner_with_node_id(
        my_node_id: Option<GenerationalNodeId>,
        destination: Destination,
        swimlane: Swimlane,
        transport_connector: impl TransportConnect,
        direction: ConnectionDirection,
        task_kind: TaskKind,
        router: Arc<MessageRouter>,
        conn_tracker: impl ConnectionTracking + Send + Sync + 'static,
        is_dedicated: bool,
    ) -> Result<(Self, TaskId), ConnectError> {
        let metadata = Metadata::current();
        let my_node_id = my_node_id.or_else(|| metadata.my_node_id_opt());
        let nodes_config = metadata.nodes_config_snapshot();
        let cluster_name = nodes_config.cluster_name().to_owned();
        let cluster_fingerprint = nodes_config.cluster_fingerprint();

        let (tx, egress, shared) = if let Swimlane::Gossip = swimlane {
            // For gossip swimlane, we need those connections to be as tight as possible. Buffering
            // too much outside the socket buffers isn't helpful as it increases the chances of
            // falsely believing that our messages are going through.
            EgressStream::with_capacity(1)
        } else {
            EgressStream::create()
        };

        // perform handshake.
        shared
            .unbounded_send(EgressMessage::Message(
                Header::default(),
                Hello::new(
                    my_node_id,
                    cluster_name,
                    cluster_fingerprint,
                    direction,
                    swimlane,
                )
                .into(),
                Some(Span::current()),
            ))
            .unwrap();

        // Establish the connection
        let mut incoming = transport_connector
            .connect(&destination, swimlane, egress)
            .await?;

        // finish the handshake
        let (header, welcome) = wait_for_welcome(
            &mut incoming,
            Configuration::pinned().networking.handshake_timeout.into(),
        )
        .await?;

        let protocol_version = welcome.protocol_version();

        // this should not happen if the peer follows the correct protocol negotiation
        if !protocol_version.is_supported() {
            return Err(HandshakeError::UnsupportedVersion(protocol_version.into()).into());
        }

        // sanity checks
        // In this version, we don't allow anonymous connections.
        let peer_node_id: GenerationalNodeId = welcome
            .my_node_id
            .ok_or(HandshakeError::Failed(
                "Peer must set my_node_id in Welcome message".to_owned(),
            ))?
            .into();

        // we expect the node to identify itself as the same NodeId we think we are connecting to.
        if let Destination::Node(destination_node_id) = destination
            && peer_node_id != destination_node_id
        {
            // Node claims that it's someone else!
            return Err(HandshakeError::Failed(
                "Node returned an unexpected GenerationalNodeId in Welcome message.".to_owned(),
            )
            .into());
        }

        let connection = Connection::new(peer_node_id, protocol_version, swimlane, tx);

        // if peer cannot respect our hello intent of direction, we are okay with registering
        let is_bidi = matches!(
            welcome.direction_ack(),
            ConnectionDirection::Unknown | ConnectionDirection::Bidirectional
        );
        // a connection is not considered dedicated if it's bidirectional
        let is_dedicated = is_dedicated && !is_bidi;
        let peer_metadata = PeerMetadataVersion::from(header);

        let reactor =
            ConnectionReactor::new(connection.clone(), shared, Some(peer_metadata), router);

        let task_id = reactor.start(task_kind, conn_tracker, is_dedicated, incoming)?;

        counter!(NETWORK_CONNECTION_CREATED, "direction" => "outgoing", "swimlane" => swimlane.as_str_name()).increment(1);
        Ok((connection, task_id))
    }

    /// The node id at the other end of this connection
    pub const fn peer(&self) -> GenerationalNodeId {
        self.peer
    }

    /// The current negotiated protocol version of the connection
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Resolves when the connection is closed
    pub fn closed(&self) -> impl std::future::Future<Output = ()> + Send + Sync + 'static {
        let sender = self.sender.clone();
        async move { sender.closed().await }
    }

    /// Starts a drain of this stream. Enqueued messages will be sent before
    /// terminating but no new messages will be accepted after the connection processes
    /// the drain reason signal. Returns `ConnectionClosed` if the connection is draining
    /// or if it has already been closed.
    pub async fn drain(self, reason: DrainReason) -> Result<(), ConnectionClosed> {
        self.sender.close(reason).await
    }

    /// Allocates capacity to send one message on this connection. If connection is closed, this
    /// returns None.
    #[must_use]
    pub async fn reserve(&self) -> Option<SendPermit<'_>> {
        let permit = self.sender.reserve().await.ok()?;
        Some(SendPermit {
            permit,
            protocol_version: self.protocol_version,
        })
    }

    #[must_use]
    pub async fn reserve_owned(&self) -> Option<OwnedSendPermit> {
        let permit = self.sender.clone().reserve_owned().await.ok()?;
        Some(OwnedSendPermit {
            permit,
            protocol_version: self.protocol_version,
        })
    }

    /// Tries to allocate capacity to send one message on this connection.
    /// Returns None if the connection was closed or is at capacity.
    #[must_use]
    pub fn try_reserve_owned(&self) -> Option<OwnedSendPermit> {
        let permit = self.sender.clone().try_reserve_owned()?;

        Some(OwnedSendPermit {
            permit,
            protocol_version: self.protocol_version,
        })
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.sender.same_channel(&other.sender)
    }
}

#[cfg(feature = "test-util")]
pub mod test_util {
    use super::*;

    use restate_types::GenerationalNodeId;

    use crate::network::Swimlane;

    // For testing
    //
    // Used to simulate remote connection.
    #[derive(derive_more::Debug)]
    pub struct MockPeerConnection {
        /// The Id of the node that this connection represents
        pub my_node_id: GenerationalNodeId,
        pub swimlane: Swimlane,
        pub reactor_task_id: TaskId,
        pub conn: Connection,
    }

    impl MockPeerConnection {
        pub fn new(
            my_node_id: GenerationalNodeId,
            swimlane: Swimlane,
            reactor_task_id: TaskId,
            conn: Connection,
        ) -> Self {
            Self {
                my_node_id,
                swimlane,
                reactor_task_id,
                conn,
            }
        }
        pub fn into_inner(self) -> Connection {
            self.conn
        }
    }
}
