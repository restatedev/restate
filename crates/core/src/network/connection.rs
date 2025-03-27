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

// re-export
pub use throttle::ConnectThrottle;
use tracing::debug;

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::Span;

use restate_types::GenerationalNodeId;
use restate_types::config::Configuration;
use restate_types::net::ProtocolVersion;
use restate_types::net::codec::Targeted;
use restate_types::net::codec::WireEncode;

use crate::Metadata;
use crate::TaskId;
use crate::TaskKind;
use crate::network::PeerMetadataVersion;

use super::ConnectError;
use super::ConnectionClosed;
use super::ConnectionDirection;
use super::Destination;
use super::Handler;
use super::HandshakeError;
use super::NetworkError;
use super::Outgoing;
use super::PeerAddress;
use super::TransportConnect;
use super::generate_msg_id;
use super::handshake::wait_for_welcome;
use super::io::ConnectionReactor;
use super::io::EgressStream;
use super::io::UnboundedEgressSender;
use super::io::WeakUnboundedEgressSender;
use super::io::{DrainReason, EgressMessage, EgressSender};
use super::metric_definitions::OUTGOING_CONNECTION;
use super::protobuf::network::Header;
use super::protobuf::network::Hello;
use super::protobuf::network::message;
use super::protobuf::network::message::Body;
use super::tracking::ConnectionTracking;
use super::tracking::PeerRouting;

pub struct OwnedSendPermit<M> {
    protocol_version: ProtocolVersion,
    permit: mpsc::OwnedPermit<EgressMessage>,
    _phantom: std::marker::PhantomData<M>,
}

pub struct SendPermit<'a, M> {
    protocol_version: ProtocolVersion,
    permit: mpsc::Permit<'a, EgressMessage>,
    _phantom: std::marker::PhantomData<M>,
}

impl<M> SendPermit<'_, M>
where
    M: WireEncode + Targeted,
{
    /// Sends a message over this permit.
    ///
    /// Note that sending messages over this permit won't use the peer information nor the connection
    /// associated with the message.
    pub fn send<S>(self, message: Outgoing<M, S>) {
        let header = Header {
            msg_id: message.msg_id(),
            in_response_to: message.in_response_to(),
            ..Default::default()
        };

        let target = M::TARGET.into();
        let payload = message.into_body().encode_to_bytes(self.protocol_version);

        let body = Body::Encoded(message::BinaryMessage { target, payload });
        self.permit
            .send(EgressMessage::Message(header, body, Some(Span::current())));
    }
}

impl<M> OwnedSendPermit<M>
where
    M: WireEncode + Targeted,
{
    /// Sends a message over this permit.
    ///
    /// Note that sending messages over this permit won't use the peer information nor the connection
    /// associated with the message.
    pub fn send<S>(self, message: Outgoing<M, S>) {
        let header = Header {
            msg_id: message.msg_id(),
            in_response_to: message.in_response_to(),
            ..Default::default()
        };

        let target = M::TARGET.into();
        let payload = message.into_body().encode_to_bytes(self.protocol_version);

        let body = Body::Encoded(message::BinaryMessage { target, payload });
        self.permit
            .send(EgressMessage::Message(header, body, Some(Span::current())));
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
    pub(crate) created: Instant,
}

impl Connection {
    pub(crate) fn new(
        peer: GenerationalNodeId,
        protocol_version: ProtocolVersion,
        sender: EgressSender,
    ) -> Self {
        Self {
            peer,
            protocol_version,
            sender,
            created: Instant::now(),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new_closed(peer: GenerationalNodeId) -> Self {
        Self {
            peer,
            protocol_version: Default::default(),
            sender: EgressSender::new_closed(),
            created: Instant::now(),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new_fake(
        peer: GenerationalNodeId,
        protocol_version: ProtocolVersion,
        capacity: usize,
    ) -> (
        Self,
        super::io::UnboundedEgressSender,
        super::io::EgressStream,
        super::io::DropEgressStream,
    ) {
        use super::io::EgressStream;

        let (sender, unbounded_sender, egress, drop_egress) = EgressStream::create(capacity);
        (
            Self::new(peer, protocol_version, sender),
            unbounded_sender,
            egress,
            drop_egress,
        )
    }

    /// Starts a new connection to a destination
    pub async fn connect(
        destination: Destination,
        transport_connector: impl TransportConnect,
        direction: ConnectionDirection,
        task_kind: TaskKind,
        router: impl Handler + Sync + 'static,
        conn_tracker: impl ConnectionTracking + Send + Sync + 'static,
        peer_router: impl PeerRouting + Clone + Send + Sync + 'static,
    ) -> Result<(Connection, TaskId), ConnectError> {
        ConnectThrottle::may_connect(&destination)?;
        Self::force_connect(
            destination.clone(),
            transport_connector,
            direction,
            task_kind,
            router,
            conn_tracker,
            peer_router,
        )
        .await
    }

    /// Does not check for throttling before attempting a connection.
    pub async fn force_connect(
        destination: Destination,
        transport_connector: impl TransportConnect,
        direction: ConnectionDirection,
        task_kind: TaskKind,
        router: impl Handler + Sync + 'static,
        conn_tracker: impl ConnectionTracking + Send + Sync + 'static,
        peer_router: impl PeerRouting + Clone + Send + Sync + 'static,
    ) -> Result<(Connection, TaskId), ConnectError> {
        let result = Self::connect_inner(
            destination.clone(),
            transport_connector,
            direction,
            task_kind,
            router,
            conn_tracker,
            peer_router,
        )
        .await;

        ConnectThrottle::note_connect_status(&destination, result.is_ok());
        match result {
            Err(ref e) => {
                debug!(%direction, "Couldn't connect to {}: {}", destination, e);
            }
            Ok((_, task_id)) => {
                debug!(%direction, %task_id, "Connection established to {}", destination);
            }
        }
        result
    }

    async fn connect_inner(
        destination: Destination,
        transport_connector: impl TransportConnect,
        direction: ConnectionDirection,
        task_kind: TaskKind,
        router: impl Handler + Sync + 'static,
        conn_tracker: impl ConnectionTracking + Send + Sync + 'static,
        peer_router: impl PeerRouting + Clone + Send + Sync + 'static,
    ) -> Result<(Connection, TaskId), ConnectError> {
        let metadata = Metadata::current();
        let my_node_id = metadata.my_node_id_opt();
        let nodes_config = metadata.nodes_config_snapshot();
        let cluster_name = nodes_config.cluster_name().to_owned();

        let (tx, unbounded_sender, egress, drop_egress) = EgressStream::create(
            Configuration::pinned()
                .networking
                .outbound_queue_length
                .get(),
        );

        // perform handshake.
        unbounded_sender
            .unbounded_send(EgressMessage::Message(
                Header::default(),
                Hello::new(my_node_id, cluster_name, direction).into(),
                Some(Span::current()),
            ))
            .unwrap();

        // Establish the connection
        let mut incoming = transport_connector.connect(&destination, egress).await?;

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
        if let Destination::Node(destination_node_id) = destination {
            if peer_node_id != destination_node_id {
                // Node claims that it's someone else!
                return Err(HandshakeError::Failed(
                    "Node returned an unexpected GenerationalNodeId in Welcome message.".to_owned(),
                )
                .into());
            }
        }

        let connection = Connection::new(peer_node_id, protocol_version, tx);

        // if peer cannot respect our hello intent of direction, we are okay with registering
        let should_register = matches!(
            welcome.direction_ack(),
            ConnectionDirection::Unknown
                | ConnectionDirection::Bidirectional
                | ConnectionDirection::Forward
        );
        let peer_metadata = PeerMetadataVersion::from(header.clone());

        let reactor = ConnectionReactor::new(
            connection.clone(),
            unbounded_sender,
            drop_egress,
            Some(peer_metadata),
        );

        let task_id = reactor.start(
            task_kind,
            router,
            conn_tracker,
            peer_router,
            incoming,
            should_register,
        )?;

        OUTGOING_CONNECTION.increment(1);
        Ok((connection, task_id))
    }

    /// The node id at the other end of this connection
    pub fn peer(&self) -> GenerationalNodeId {
        self.peer
    }

    /// The current negotiated protocol version of the connection
    pub fn protocol_version(&self) -> ProtocolVersion {
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
    pub async fn reserve<M>(&self) -> Option<SendPermit<'_, M>> {
        let permit = self.sender.reserve().await.ok()?;
        Some(SendPermit {
            permit,
            protocol_version: self.protocol_version,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Allocates capacity to send one message on this connection within a timeout.
    /// If connection is closed, this returns Ok(None).
    pub async fn reserve_timeout<M>(
        &self,
        timeout: Duration,
    ) -> Result<OwnedSendPermit<M>, NetworkError> {
        let start = Instant::now();
        let permit = tokio::time::timeout(timeout, self.sender.clone().reserve_owned())
            .await
            .map_err(|_| NetworkError::Timeout(start.elapsed()))?
            .map_err(|_| NetworkError::ConnectionClosed(ConnectionClosed))?;
        Ok(OwnedSendPermit {
            permit,
            protocol_version: self.protocol_version,
            _phantom: std::marker::PhantomData,
        })
    }

    pub async fn reserve_owned<M>(&self) -> Option<OwnedSendPermit<M>> {
        let permit = self.sender.clone().reserve_owned().await.ok()?;
        Some(OwnedSendPermit {
            permit,
            protocol_version: self.protocol_version,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Tries to allocate capacity to send one message on this connection.
    /// Returns None if the connection was closed or is at capacity.
    pub fn try_reserve_owned<M>(&self) -> Option<OwnedSendPermit<M>> {
        let permit = self.sender.clone().try_reserve_owned()?;

        Some(OwnedSendPermit {
            permit,
            protocol_version: self.protocol_version,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.sender.same_channel(&other.sender)
    }
}

/// A weak connection that can be used to send system-level messages to peers.
/// This should **not** be used for other purposes.
#[derive(Clone, derive_more::Debug)]
pub struct UnboundedConnectionRef {
    pub(crate) peer: PeerAddress,
    pub(crate) protocol_version: ProtocolVersion,
    #[debug(skip)]
    pub(crate) sender: WeakUnboundedEgressSender,
}

impl UnboundedConnectionRef {
    pub fn new(
        peer: PeerAddress,
        protocol_version: ProtocolVersion,
        sender: &UnboundedEgressSender,
    ) -> Self {
        Self {
            peer,
            protocol_version,
            sender: sender.downgrade(),
        }
    }

    pub fn peer(&self) -> &PeerAddress {
        &self.peer
    }

    /// Encodes and sends the message on the unbounded channel to peer
    pub fn encode_and_send<M>(&self, msg: M) -> Result<(), ConnectionClosed>
    where
        M: WireEncode + Targeted,
    {
        // no need to serialize if the connection is already closed
        let Some(sender) = self.sender.upgrade() else {
            return Err(ConnectionClosed);
        };

        let header = Header {
            // for compatibility with protocol V1
            msg_id: generate_msg_id(),
            ..Default::default()
        };

        let target = M::TARGET.into();
        let payload = msg.encode_to_bytes(self.protocol_version);

        let body = Body::Encoded(message::BinaryMessage { target, payload });
        sender.unbounded_send(EgressMessage::Message(header, body, Some(Span::current())))
    }
}

#[cfg(any(test, feature = "test-util"))]
pub mod test_util {
    use super::*;

    use async_trait::async_trait;
    use futures::StreamExt;
    use futures::stream::BoxStream;
    use tokio::sync::mpsc;
    use tracing::info;
    use tracing::warn;

    use restate_types::net::ProtocolVersion;
    use restate_types::net::codec::Targeted;
    use restate_types::net::codec::WireEncode;
    use restate_types::nodes_config::NodesConfiguration;
    use restate_types::{GenerationalNodeId, Version};

    use crate::TaskCenter;
    use crate::TaskHandle;
    use crate::TaskKind;
    use crate::cancellation_watcher;
    use crate::network::ConnectionManager;
    use crate::network::Handler;
    use crate::network::HandshakeError;
    use crate::network::Incoming;
    use crate::network::MessageHandler;
    use crate::network::MessageRouterBuilder;
    use crate::network::PeerMetadataVersion;
    use crate::network::RouterError;
    use crate::network::handshake::negotiate_protocol_version;
    use crate::network::handshake::wait_for_hello;
    use crate::network::io::DropEgressStream;
    use crate::network::io::EgressStream;
    use crate::network::io::UnboundedEgressSender;
    use crate::network::protobuf::network::ConnectionDirection;
    use crate::network::protobuf::network::Header;
    use crate::network::protobuf::network::Hello;
    use crate::network::protobuf::network::Message;
    use crate::network::protobuf::network::Welcome;
    use crate::network::protobuf::network::message;
    use crate::network::protobuf::network::message::BinaryMessage;
    use crate::network::protobuf::network::message::Body;

    // For testing
    //
    // Used to simulate incoming connection. Gives control to reading and writing messages.
    //
    // Sending messages on this connection simulates a remote machine sending messages to our
    // connection manager. Sending means "incoming messages". The recv_stream on the other hand
    // can be used to read responses that we sent back.
    #[derive(derive_more::Debug)]
    pub struct MockPeerConnection {
        /// The Id of the node that this connection represents
        pub my_node_id: GenerationalNodeId,
        /// The Id of the node we are connected to
        pub(crate) peer: GenerationalNodeId,
        pub protocol_version: ProtocolVersion,
        #[debug(skip)]
        pub(crate) sender: EgressSender,
        #[debug(skip)]
        pub(crate) unbounded_sender: UnboundedEgressSender,
        pub created: Instant,

        #[debug(skip)]
        pub recv_stream: BoxStream<'static, Message>,
        #[debug(skip)]
        drop_egress: DropEgressStream,
    }

    impl MockPeerConnection {
        /// must run in task-center
        pub async fn connect(
            from_node_id: GenerationalNodeId,
            my_node_config_version: Version,
            my_cluster_name: String,
            connection_manager: &ConnectionManager,
            message_buffer: usize,
        ) -> anyhow::Result<Self> {
            let (sender, unbounded_sender, incoming, drop_egress) =
                EgressStream::create(message_buffer);

            let hello = Hello::new(
                Some(from_node_id),
                my_cluster_name,
                ConnectionDirection::Bidirectional,
            );
            let header = Header {
                my_nodes_config_version: Some(my_node_config_version.into()),
                msg_id: crate::network::generate_msg_id(),
                ..Default::default()
            };
            unbounded_sender.unbounded_send(EgressMessage::Message(header, hello.into(), None))?;

            let created = Instant::now();
            let mut recv_stream = connection_manager
                .accept_incoming_connection(incoming)
                .await?;
            let msg = recv_stream
                .next()
                .await
                .ok_or(anyhow::anyhow!("expected welcome message"))?;
            let welcome = match msg.body {
                Some(message::Body::Welcome(welcome)) => welcome,
                _ => anyhow::bail!("unexpected message, we expect Welcome instead"),
            };

            let peer: GenerationalNodeId =
                welcome.my_node_id.expect("peer node id must be set").into();

            Ok(Self {
                my_node_id: from_node_id,
                peer,
                protocol_version: welcome.protocol_version(),
                sender,
                unbounded_sender,
                recv_stream: Box::pin(recv_stream),
                created,
                drop_egress,
            })
        }

        /// fails only if receiver is terminated (connection terminated)
        pub async fn send_raw<M>(&self, message: M, header: Header) -> anyhow::Result<()>
        where
            M: WireEncode + Targeted,
        {
            let target = M::TARGET.into();
            let payload = message.encode_to_bytes(self.protocol_version);
            let body = Body::Encoded(message::BinaryMessage { target, payload });

            let message = Message {
                header: Some(header),
                body: Some(body),
            };

            self.sender.send(EgressMessage::RawMessage(message)).await?;

            Ok(())
        }

        pub async fn reserve<M>(&self) -> Option<SendPermit<'_, M>> {
            let permit = self.sender.reserve().await.ok()?;
            Some(SendPermit {
                permit,
                protocol_version: self.protocol_version,
                _phantom: std::marker::PhantomData,
            })
        }

        pub async fn reserve_owned<M>(self) -> Option<OwnedSendPermit<M>> {
            let permit = self.sender.reserve_owned().await.ok()?;
            Some(OwnedSendPermit {
                permit,
                protocol_version: self.protocol_version,
                _phantom: std::marker::PhantomData,
            })
        }

        /// Allows you to use utilities in Connection
        /// Reminder: Sending on this connection will cause message to arrive as incoming to the node
        /// we are connected to.
        pub fn to_owned_connection(&self) -> Connection {
            Connection {
                peer: self.peer,
                protocol_version: self.protocol_version,
                sender: self.sender.clone(),
                created: self.created,
            }
        }

        // Allow for messages received on this connection to be processed by a given message handler.
        pub fn process_with_message_handler<H: MessageHandler + Send + Sync + 'static>(
            self,
            handler: H,
        ) -> anyhow::Result<(Connection, TaskHandle<anyhow::Result<()>>)> {
            let mut router = MessageRouterBuilder::default();
            router.add_message_handler(handler);
            let router = router.build();
            self.process_with_message_router(router)
        }

        // Allow for messages received on this connection to be processed by a given message router.
        // A task will be created that takes ownership of the receive stream. Stopping the task will
        // drop the receive stream (simulates connection loss).
        pub fn process_with_message_router<R: Handler + 'static>(
            self,
            router: R,
        ) -> anyhow::Result<(Connection, TaskHandle<anyhow::Result<()>>)> {
            let Self {
                my_node_id,
                peer,
                protocol_version,
                sender,
                unbounded_sender,
                created,
                recv_stream,
                drop_egress,
            } = self;

            let connection = Connection {
                peer,
                protocol_version,
                sender,
                created,
            };

            let message_processor = MessageProcessor {
                my_node_id,
                router,
                connection: connection.clone(),
                tx: unbounded_sender,
                recv_stream,
            };
            let handle = TaskCenter::spawn_unmanaged(
                TaskKind::ConnectionReactor,
                "test-message-processor",
                async move { message_processor.run(drop_egress).await },
            )?;
            Ok((connection, handle))
        }

        // Allow for messages received on this connection to be forwarded to the supplied sender.
        pub fn forward_to_sender(
            self,
            sender: mpsc::Sender<(GenerationalNodeId, Incoming<BinaryMessage>)>,
        ) -> anyhow::Result<(Connection, TaskHandle<anyhow::Result<()>>)> {
            let handler = ForwardingHandler {
                my_node_id: self.my_node_id,
                inner_sender: sender,
            };

            self.process_with_message_router(handler)
        }
    }

    // Represents a partially connected peer connection in test environment. A connection must be
    // handshaken in order to be converted into MockPeerConnection.
    // This is used to represent an outgoing connection from (`peer` to `my_node_id`)
    #[derive(derive_more::Debug)]
    pub struct PartialPeerConnection {
        /// The Id of the node that this connection represents
        pub my_node_id: GenerationalNodeId,
        /// The Id of the node id that started this connection
        pub(crate) peer: GenerationalNodeId,
        #[debug(skip)]
        pub(crate) sender: EgressSender,
        #[debug(skip)]
        pub(crate) unbounded_sender: UnboundedEgressSender,
        pub created: Instant,

        #[debug(skip)]
        pub recv_stream: BoxStream<'static, Message>,
        #[debug(skip)]
        pub(crate) drop_egress: DropEgressStream,
    }

    impl PartialPeerConnection {
        // todo(asoli): replace implementation with body of accept_incoming_connection to unify
        // handshake validations
        pub async fn handshake(
            self,
            nodes_config: &NodesConfiguration,
        ) -> anyhow::Result<MockPeerConnection> {
            let Self {
                my_node_id,
                peer,
                sender,
                unbounded_sender,
                created,
                mut recv_stream,
                drop_egress,
            } = self;
            let temp_stream = recv_stream.by_ref();
            let (header, hello) =
                wait_for_hello(temp_stream, std::time::Duration::from_millis(500)).await?;

            // NodeId **must** be generational at this layer
            let _peer_node_id = hello.my_node_id.ok_or(HandshakeError::Failed(
                "NodeId is not set in the Hello message".to_owned(),
            ))?;

            // Are we both from the same cluster?
            if hello.cluster_name != nodes_config.cluster_name() {
                return Err(HandshakeError::Failed("cluster name mismatch".to_owned()).into());
            }

            let selected_protocol_version = negotiate_protocol_version(&hello)?;

            // Enqueue the welcome message
            let welcome = Welcome::new(my_node_id, selected_protocol_version, hello.direction());

            let header = Header::new(
                nodes_config.version(),
                None,
                None,
                None,
                crate::network::generate_msg_id(),
                Some(header.msg_id),
            );
            unbounded_sender.unbounded_send(EgressMessage::Message(
                header,
                welcome.into(),
                None,
            ))?;

            Ok(MockPeerConnection {
                my_node_id,
                peer,
                protocol_version: selected_protocol_version,
                sender,
                unbounded_sender,
                created,
                recv_stream,
                drop_egress,
            })
        }
    }

    pub struct ForwardingHandler {
        my_node_id: GenerationalNodeId,
        inner_sender: mpsc::Sender<(GenerationalNodeId, Incoming<BinaryMessage>)>,
    }

    impl ForwardingHandler {
        pub fn new(
            my_node_id: GenerationalNodeId,
            inner_sender: mpsc::Sender<(GenerationalNodeId, Incoming<BinaryMessage>)>,
        ) -> Self {
            Self {
                my_node_id,
                inner_sender,
            }
        }
    }

    #[async_trait]
    impl Handler for ForwardingHandler {
        async fn call(
            &self,
            message: Incoming<BinaryMessage>,
            _protocol_version: ProtocolVersion,
        ) -> Result<(), RouterError> {
            if self
                .inner_sender
                .send((self.my_node_id, message))
                .await
                .is_err()
            {
                warn!("Failed to send message to inner sender, connection is closed");
            }
            Ok(())
        }
    }

    struct MessageProcessor<R> {
        my_node_id: GenerationalNodeId,
        router: R,
        connection: Connection,
        tx: UnboundedEgressSender,
        recv_stream: BoxStream<'static, Message>,
    }

    impl<R: Handler> MessageProcessor<R> {
        async fn run(mut self, _drop_egress: DropEgressStream) -> anyhow::Result<()> {
            let mut cancel = std::pin::pin!(cancellation_watcher());
            loop {
                tokio::select! {
                    _ = &mut cancel => {
                        info!("Message processor cancelled for node {}", self.my_node_id);
                        break;
                    }
                    maybe_msg = self.recv_stream.next() => {
                        let Some(msg) = maybe_msg else {
                            info!("Terminating message processor because connection sender is dropped for node {}", self.my_node_id);
                            break;
                        };
                        //  header is required on all messages
                        let Some(header) = msg.header else {
                            self.tx.unbounded_drain(DrainReason::CodecError(
                                "Header is missing on message".to_owned(),
                            ));
                            break;
                        };

                        // body are not allowed to be empty.
                        let Some(body) = msg.body else {
                            self.tx
                                .unbounded_drain(DrainReason::CodecError("Body is missing on message".to_owned()));
                            break;
                        };

                        // Welcome and hello are not allowed after handshake
                        if body.is_welcome() || body.is_hello() {
                            self.tx.unbounded_drain(DrainReason::CodecError(
                                "Hello/Welcome are not allowed after handshake".to_string(),
                            ));
                            break;
                        };

                        // If it's a control signal, we terminate the connection
                        if let message::Body::ConnectionControl(ctrl_msg) = &body {
                            info!(
                                "Terminating connection based on signal from peer: {:?} {}",
                                ctrl_msg.signal(),
                                ctrl_msg.message
                            );
                            break;
                        }


                        self.route_message(header, body).await?;
                    }
                }
            }
            Ok(())
        }

        async fn route_message(&mut self, header: Header, body: Body) -> anyhow::Result<()> {
            match body.try_as_binary_body(self.connection.protocol_version) {
                Ok(msg) => {
                    if let Err(e) = self
                        .router
                        .call(
                            Incoming::from_parts(
                                msg,
                                self.connection.clone(),
                                header.msg_id,
                                header.in_response_to,
                                PeerMetadataVersion::from(header),
                            ),
                            self.connection.protocol_version,
                        )
                        .await
                    {
                        warn!("Error processing message: {:?}", e);
                    }
                }
                Err(error) => {
                    // terminate the stream
                    info!("Error processing message, reporting error to peer: {error}");
                    self.tx
                        .unbounded_drain(DrainReason::CodecError(error.to_string()));
                }
            }
            Ok(())
        }
    }
}
