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
use std::sync::Weak;
use std::time::Duration;

use enum_map::{enum_map, EnumMap};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;
use tracing::{debug, trace};

use restate_types::net::codec::Targeted;
use restate_types::net::codec::{serialize_message, WireEncode};
use restate_types::net::metadata::MetadataKind;
use restate_types::net::ProtocolVersion;
use restate_types::protobuf::node::message;
use restate_types::protobuf::node::Header;
use restate_types::protobuf::node::Message;
use restate_types::{GenerationalNodeId, Version};

use super::NetworkError;
use super::Outgoing;
use crate::Metadata;

pub struct OwnedSendPermit<M> {
    _protocol_version: ProtocolVersion,
    _permit: mpsc::OwnedPermit<Message>,
    _phantom: std::marker::PhantomData<M>,
}

pub struct SendPermit<'a, M> {
    protocol_version: ProtocolVersion,
    permit: mpsc::Permit<'a, Message>,
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
    pub fn send<S>(self, message: Outgoing<M, S>, metadata: &Metadata) {
        let metadata_versions = HeaderMetadataVersions::from_metadata(metadata);
        self.send_with_versions(message, metadata_versions);
    }

    fn send_with_versions<S>(
        self,
        message: Outgoing<M, S>,
        metadata_versions: HeaderMetadataVersions,
    ) {
        let header = Header::new(
            metadata_versions[MetadataKind::NodesConfiguration]
                .expect("nodes configuration version must be set"),
            metadata_versions[MetadataKind::Logs],
            metadata_versions[MetadataKind::Schema],
            metadata_versions[MetadataKind::PartitionTable],
            message.msg_id(),
            message.in_response_to(),
        );
        let body = serialize_message(message.into_body(), self.protocol_version)
            .expect("message encoding infallible");
        self.send_raw(Message::new(header, body));
    }
}

impl<M> SendPermit<'_, M> {
    /// Sends a raw pre-serialized message over this permit.
    ///
    /// Note that sending messages over this permit won't use the peer information nor the connection
    /// associated with the message.
    pub(crate) fn send_raw(self, raw_message: Message) {
        self.permit.send(raw_message);
    }
}

/// A single streaming connection with a channel to the peer. A connection can be
/// opened by either ends of the connection and has no direction. Any connection
/// can be used to send or receive from a peer.
///
/// The primary owner of a connection is the running reactor, all other components
/// should hold a `WeakConnection` if access to a certain connection is
/// needed.
pub struct OwnedConnection {
    pub(crate) peer: GenerationalNodeId,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) sender: mpsc::Sender<Message>,
    pub(crate) created: Instant,
}

impl OwnedConnection {
    pub(crate) fn new(
        peer: GenerationalNodeId,
        protocol_version: ProtocolVersion,
        sender: mpsc::Sender<Message>,
    ) -> Self {
        Self {
            peer,
            protocol_version,
            sender,
            created: Instant::now(),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new_fake(
        peer: GenerationalNodeId,
        protocol_version: ProtocolVersion,
        sender: mpsc::Sender<Message>,
    ) -> Arc<Self> {
        Arc::new(Self::new(peer, protocol_version, sender))
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

    /// Best-effort delivery of signals on the connection.
    pub fn send_control_frame(&self, control: message::ConnectionControl) {
        let signal = control.signal();
        let msg = Message {
            header: None,
            body: Some(control.into()),
        };

        debug!(?msg, "Sending control frame to peer");
        if self.sender.try_send(msg).is_ok() {
            trace!(?signal, "Control frame was written to connection");
        }
    }

    /// A handle that sends messages through that connection. This hides the
    /// wire protocol from the user and guarantees order of messages.
    pub fn downgrade(self: &Arc<Self>) -> WeakConnection {
        WeakConnection {
            peer: self.peer,
            connection: Arc::downgrade(self),
        }
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
    ) -> Result<SendPermit<'_, M>, NetworkError> {
        let start = Instant::now();
        let permit = tokio::time::timeout(timeout, self.sender.reserve())
            .await
            .map_err(|_| NetworkError::Timeout(start.elapsed()))?
            .map_err(|_| NetworkError::ConnectionClosed(self.peer))?;
        Ok(SendPermit {
            permit,
            protocol_version: self.protocol_version,
            _phantom: std::marker::PhantomData,
        })
    }

    pub async fn reserve_owned<M>(self) -> Option<OwnedSendPermit<M>> {
        let permit = self.sender.reserve_owned().await.ok()?;
        Some(OwnedSendPermit {
            _permit: permit,
            _protocol_version: self.protocol_version,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Tries to allocate capacity to send one message on this connection. If there is no capacity,
    /// it will fail with [`NetworkError::Full`]. If connection is closed it returns [`NetworkError::ConnectionClosed`]
    pub fn try_reserve<M>(&self) -> Result<SendPermit<'_, M>, NetworkError> {
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(TrySendError::Full(_)) => return Err(NetworkError::Full),
            Err(TrySendError::Closed(_)) => return Err(NetworkError::ConnectionClosed(self.peer)),
        };

        Ok(SendPermit {
            permit,
            protocol_version: self.protocol_version,
            _phantom: std::marker::PhantomData,
        })
    }
}

#[derive(derive_more::Index)]
pub(crate) struct HeaderMetadataVersions {
    #[index]
    versions: EnumMap<MetadataKind, Option<Version>>,
}

impl Default for HeaderMetadataVersions {
    // Used primarily in tests
    fn default() -> Self {
        let versions = enum_map! {
            MetadataKind::NodesConfiguration => Some(Version::MIN),
            MetadataKind::Schema => None,
            MetadataKind::Logs => None,
            MetadataKind::PartitionTable => None,
        };
        Self { versions }
    }
}

impl HeaderMetadataVersions {
    pub fn from_metadata(metadata: &Metadata) -> Self {
        let versions = enum_map! {
            MetadataKind::NodesConfiguration => Some(metadata.nodes_config_version()),
            MetadataKind::Schema => Some(metadata.schema_version()),
            MetadataKind::Logs => Some(metadata.logs_version()),
            MetadataKind::PartitionTable => Some(metadata.partition_table_version()),
        };
        Self { versions }
    }
}

impl PartialEq for OwnedConnection {
    fn eq(&self, other: &Self) -> bool {
        self.sender.same_channel(&other.sender)
    }
}

/// A handle to send messages through a connection. It's safe to hold and clone objects of this
/// even if the connection has been dropped. Cheap to clone.
#[derive(Clone, Debug)]
pub struct WeakConnection {
    pub(crate) peer: GenerationalNodeId,
    pub(crate) connection: Weak<OwnedConnection>,
}

static_assertions::assert_impl_all!(WeakConnection: Send, Sync);

impl WeakConnection {
    pub fn new_closed(peer: GenerationalNodeId) -> Self {
        Self {
            peer,
            connection: Weak::new(),
        }
    }

    /// The node id at the other end of this connection
    pub fn peer(&self) -> GenerationalNodeId {
        self.peer
    }

    pub fn is_closed(&self) -> bool {
        self.connection
            .upgrade()
            .map(|c| c.is_closed())
            .unwrap_or(true)
    }

    /// Resolves when the connection is closed
    pub fn closed(&self) -> impl std::future::Future<Output = ()> + Send + Sync + 'static {
        let weak_connection = self.connection.clone();
        async move {
            let Some(connection) = weak_connection.upgrade() else {
                return;
            };
            connection.closed().await
        }
    }
}

impl PartialEq for WeakConnection {
    fn eq(&self, other: &Self) -> bool {
        self.connection.ptr_eq(&other.connection)
    }
}

#[cfg(any(test, feature = "test-util"))]
pub mod test_util {
    use super::*;

    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TrySendError;
    use tokio_stream::wrappers::ReceiverStream;
    use tracing::info;
    use tracing::warn;

    use restate_types::net::codec::MessageBodyExt;
    use restate_types::net::codec::Targeted;
    use restate_types::net::codec::{serialize_message, WireEncode};
    use restate_types::net::CodecError;
    use restate_types::net::ProtocolVersion;
    use restate_types::nodes_config::NodesConfiguration;
    use restate_types::protobuf::node::message;
    use restate_types::protobuf::node::message::BinaryMessage;
    use restate_types::protobuf::node::message::Body;
    use restate_types::protobuf::node::message::ConnectionControl;
    use restate_types::protobuf::node::Header;
    use restate_types::protobuf::node::Hello;
    use restate_types::protobuf::node::Message;
    use restate_types::protobuf::node::Welcome;
    use restate_types::{GenerationalNodeId, Version};

    use crate::cancellation_watcher;
    use crate::network::handshake::negotiate_protocol_version;
    use crate::network::handshake::wait_for_hello;
    use crate::network::ConnectionManager;
    use crate::network::Handler;
    use crate::network::Incoming;
    use crate::network::MessageHandler;
    use crate::network::MessageRouterBuilder;
    use crate::network::NetworkError;
    use crate::network::PeerMetadataVersion;
    use crate::network::ProtocolError;
    use crate::network::TransportConnect;
    use crate::TaskCenter;
    use crate::TaskHandle;
    use crate::TaskKind;

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
        pub sender: mpsc::Sender<Message>,
        pub created: Instant,

        #[debug(skip)]
        pub recv_stream: BoxStream<'static, Message>,
    }

    impl MockPeerConnection {
        /// must run in task-center
        pub async fn connect<T: TransportConnect>(
            from_node_id: GenerationalNodeId,
            my_node_config_version: Version,
            my_cluster_name: String,
            connection_manager: &ConnectionManager<T>,
            message_buffer: usize,
        ) -> anyhow::Result<Self> {
            let (sender, rx) = mpsc::channel(message_buffer);
            let incoming = ReceiverStream::new(rx).map(Ok);

            let hello = Hello::new(from_node_id, my_cluster_name);
            let hello = Message::new(
                Header::new(
                    my_node_config_version,
                    None,
                    None,
                    None,
                    crate::network::generate_msg_id(),
                    None,
                ),
                hello,
            );
            sender.send(hello).await?;

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
                recv_stream: Box::pin(recv_stream),
                created,
            })
        }

        /// fails only if receiver is terminated (connection terminated)
        pub async fn send_raw<M>(&self, message: M, header: Header) -> anyhow::Result<()>
        where
            M: WireEncode + Targeted,
        {
            let body = serialize_message(message, self.protocol_version).expect("serde unfallible");
            let message = Message::new(header, body);

            self.sender.send(message).await?;

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
                _permit: permit,
                _protocol_version: self.protocol_version,
                _phantom: std::marker::PhantomData,
            })
        }

        /// Tries to allocate capacity to send one message on this connection. If there is no capacity,
        /// it will fail with [`NetworkError::Full`]. If connection is closed it returns [`NetworkError::ConnectionClosed`]
        pub fn try_reserve<M>(&self) -> Result<SendPermit<'_, M>, NetworkError> {
            let permit = match self.sender.try_reserve() {
                Ok(permit) => permit,
                Err(TrySendError::Full(_)) => return Err(NetworkError::Full),
                Err(TrySendError::Closed(_)) => {
                    return Err(NetworkError::ConnectionClosed(self.peer))
                }
            };

            Ok(SendPermit {
                permit,
                protocol_version: self.protocol_version,
                _phantom: std::marker::PhantomData,
            })
        }

        /// Allows you to use utilities in OwnedConnection or WeakConnection.
        /// Reminder: Sending on this connection will cause message to arrive as incoming to the node
        /// we are connected to.
        pub fn to_owned_connection(&self) -> OwnedConnection {
            OwnedConnection {
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
        ) -> anyhow::Result<(WeakConnection, TaskHandle<anyhow::Result<()>>)> {
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
        ) -> anyhow::Result<(WeakConnection, TaskHandle<anyhow::Result<()>>)> {
            let Self {
                my_node_id,
                peer,
                protocol_version,
                sender,
                created,
                recv_stream,
            } = self;

            let connection = Arc::new(OwnedConnection {
                peer,
                protocol_version,
                sender,
                created,
            });

            let weak = connection.downgrade();
            let message_processor = MessageProcessor {
                my_node_id,
                router,
                connection,
                recv_stream,
            };
            let handle = TaskCenter::spawn_unmanaged(
                TaskKind::ConnectionReactor,
                "test-message-processor",
                async move { message_processor.run().await },
            )?;
            Ok((weak, handle))
        }

        // Allow for messages received on this connection to be forwarded to the supplied sender.
        pub fn forward_to_sender(
            self,
            sender: mpsc::Sender<(GenerationalNodeId, Incoming<BinaryMessage>)>,
        ) -> anyhow::Result<(WeakConnection, TaskHandle<anyhow::Result<()>>)> {
            let handler = ForwardingHandler {
                my_node_id: self.my_node_id,
                inner_sender: sender,
            };

            self.process_with_message_router(handler)
        }
    }

    // Prepresents a partially connected peer connection in test environment. A connection must be
    // handshaken in order to be converted into MockPeerConnection.
    //
    // This is used to represent an outgoing connection from (`peer` to `my_node_id`)
    #[derive(derive_more::Debug)]
    pub struct PartialPeerConnection {
        /// The Id of the node that this connection represents
        pub my_node_id: GenerationalNodeId,
        /// The Id of the node id that started this connection
        pub(crate) peer: GenerationalNodeId,
        pub sender: mpsc::Sender<Message>,
        pub created: Instant,

        #[debug(skip)]
        pub recv_stream: BoxStream<'static, Message>,
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
                created,
                mut recv_stream,
            } = self;
            let temp_stream = recv_stream.by_ref();
            let (header, hello) = wait_for_hello(
                &mut temp_stream.map(Ok),
                std::time::Duration::from_millis(500),
            )
            .await?;

            // NodeId **must** be generational at this layer
            let _peer_node_id = hello.my_node_id.ok_or(ProtocolError::HandshakeFailed(
                "NodeId is not set in the Hello message",
            ))?;

            // Are we both from the same cluster?
            if hello.cluster_name != nodes_config.cluster_name() {
                return Err(ProtocolError::HandshakeFailed("cluster name mismatch").into());
            }

            let selected_protocol_version = negotiate_protocol_version(&hello)?;

            // Enqueue the welcome message
            let welcome = Welcome::new(my_node_id, selected_protocol_version);

            let welcome = Message::new(
                Header::new(
                    nodes_config.version(),
                    None,
                    None,
                    None,
                    crate::network::generate_msg_id(),
                    Some(header.msg_id),
                ),
                welcome,
            );
            sender.try_send(welcome)?;

            Ok(MockPeerConnection {
                my_node_id,
                peer,
                protocol_version: selected_protocol_version,
                sender,
                created,
                recv_stream,
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
        type Error = CodecError;

        async fn call(
            &self,
            message: Incoming<BinaryMessage>,
            _protocol_version: ProtocolVersion,
        ) -> Result<(), Self::Error> {
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
        connection: Arc<OwnedConnection>,
        recv_stream: BoxStream<'static, Message>,
    }

    impl<R: Handler> MessageProcessor<R> {
        async fn run(mut self) -> anyhow::Result<()> {
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
                            self.connection.send_control_frame(ConnectionControl::codec_error(
                                "Header is missing on message",
                            ));
                            break;
                        };

                        // body are not allowed to be empty.
                        let Some(body) = msg.body else {
                            self.connection
                                .send_control_frame(ConnectionControl::codec_error("Body is missing on message"));
                            break;
                        };

                        // Welcome and hello are not allowed after handshake
                        if body.is_welcome() || body.is_hello() {
                            self.connection.send_control_frame(ConnectionControl::codec_error(
                                "Hello/Welcome are not allowed after handshake",
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
                                self.connection.downgrade(),
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
                Err(status) => {
                    // terminate the stream
                    info!("Error processing message, reporting error to peer: {status}");
                    self.connection
                        .send_control_frame(ConnectionControl::codec_error(status.to_string()));
                }
            }
            Ok(())
        }
    }
}
