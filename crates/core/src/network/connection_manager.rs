// Copyright (c) 2024-2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Instant;

use enum_map::EnumMap;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, trace, warn, Instrument, Span};

use restate_types::config::NetworkingOptions;
use restate_types::net::codec::MessageBodyExt;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::protobuf::node::message::{self, ConnectionControl};
use restate_types::protobuf::node::{Header, Hello, Message, Welcome};
use restate_types::{GenerationalNodeId, NodeId, PlainNodeId, Version};

use super::connection::{OwnedConnection, WeakConnection};
use super::error::{NetworkError, ProtocolError};
use super::handshake::wait_for_welcome;
use super::metric_definitions::{
    self, CONNECTION_DROPPED, INCOMING_CONNECTION, MESSAGE_PROCESSING_DURATION, MESSAGE_RECEIVED,
    ONGOING_DRAIN, OUTGOING_CONNECTION,
};
use super::transport_connector::TransportConnect;
use super::{Handler, MessageRouter};
use crate::metadata::Urgency;
use crate::network::handshake::{negotiate_protocol_version, wait_for_hello};
use crate::network::{Incoming, PeerMetadataVersion};
use crate::Metadata;
use crate::{cancellation_watcher, current_task_id, task_center, TaskId, TaskKind};

struct ConnectionManagerInner {
    router: MessageRouter,
    connections: HashMap<TaskId, Weak<OwnedConnection>>,
    connection_by_gen_id: HashMap<GenerationalNodeId, Vec<Weak<OwnedConnection>>>,
    /// This tracks the max generation we observed from connection attempts regardless of our nodes
    /// configuration. We cannot accept connections from nodes older than ones we have observed
    /// already.
    observed_generations: HashMap<PlainNodeId, u32>,
}

impl ConnectionManagerInner {
    fn drop_connection(&mut self, task_id: TaskId) {
        self.connections.remove(&task_id);
    }

    fn cleanup_stale_connections(&mut self, peer_node_id: &GenerationalNodeId) {
        if let Some(connections) = self.connection_by_gen_id.get_mut(peer_node_id) {
            connections.retain(|c| c.upgrade().is_some_and(|c| !c.is_closed()));
        }
    }

    fn get_random_connection(
        &self,
        peer_node_id: &GenerationalNodeId,
    ) -> Option<Arc<OwnedConnection>> {
        self.connection_by_gen_id
            .get(peer_node_id)
            .and_then(|connections| connections.choose(&mut rand::thread_rng())?.upgrade())
    }
}

impl Default for ConnectionManagerInner {
    fn default() -> Self {
        metric_definitions::describe_metrics();
        Self {
            router: MessageRouter::default(),
            connections: HashMap::default(),
            connection_by_gen_id: HashMap::default(),
            observed_generations: HashMap::default(),
        }
    }
}

pub struct ConnectionManager<T> {
    inner: Arc<Mutex<ConnectionManagerInner>>,
    networking_options: NetworkingOptions,
    transport_connector: Arc<T>,
    metadata: Metadata,
}

impl<T> Clone for ConnectionManager<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            networking_options: self.networking_options.clone(),
            transport_connector: self.transport_connector.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
/// used for testing. Accepts connections but can't establish new connections
impl ConnectionManager<super::FailingConnector> {
    pub fn new_incoming_only(metadata: Metadata) -> Self {
        let inner = Arc::new(Mutex::new(ConnectionManagerInner::default()));

        Self {
            metadata,
            inner,
            transport_connector: Arc::new(super::FailingConnector::default()),
            networking_options: NetworkingOptions::default(),
        }
    }
}

impl<T: TransportConnect> ConnectionManager<T> {
    /// Creates the connection manager.
    pub fn new(
        metadata: Metadata,
        transport_connector: Arc<T>,
        networking_options: NetworkingOptions,
    ) -> Self {
        let inner = Arc::new(Mutex::new(ConnectionManagerInner::default()));

        Self {
            metadata,
            inner,
            transport_connector,
            networking_options,
        }
    }
    /// Updates the message router. Note that this only impacts new connections.
    /// In general, this should be called once on application start after
    /// initializing all message handlers.
    pub fn set_message_router(&self, router: MessageRouter) {
        self.inner.lock().router = router;
    }

    /// Accept a new incoming connection stream and register a network reactor task for it.
    pub async fn accept_incoming_connection<S>(
        &self,
        mut incoming: S,
    ) -> Result<impl Stream<Item = Message> + Unpin + Send + 'static, NetworkError>
    where
        S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send + 'static,
    {
        // Perform the handshake inline before creating the reactor. This allows
        // us to naturally push-back on stream creation by piggybacking on the
        // concurrency limit of the server + the handshake timeout.
        //
        // How do we handshake a new connection?
        // ---------------------------------
        // Client -> Server: Hello
        // Server -> Client: Ok(Stream)
        // Server => Client: Welcome
        // Handshake completed. Hello/Welcome messages are not allowed after this point.
        //
        // A client must send the Hello message within a preconfigured time-window as the very
        // first message in the stream. The client **must** send the Hello message _before_ waiting
        // for the response stream to be created. This allows the server to only create the
        // control stream if the client can successfully negotiate with the server. The server can
        // decide to abort the stream if it's not interested in connections coming from this node
        // without allocating any resources.
        //
        // Timeouts are used in both ways (expectation to receive Hello/Welcome within a time
        // window) to avoid dangling resources by misbehaving peers or under sever load conditions.
        // The client can retry with an exponential backoff on handshake timeout.
        debug!("Accepting incoming connection");
        let (header, hello) = wait_for_hello(
            &mut incoming,
            self.networking_options.handshake_timeout.into(),
        )
        .await?;
        let nodes_config = self.metadata.nodes_config_ref();
        let my_node_id = self.metadata.my_node_id();
        // NodeId **must** be generational at this layer
        let peer_node_id = hello.my_node_id.ok_or(ProtocolError::HandshakeFailed(
            "NodeId is not set in the Hello message",
        ))?;

        if peer_node_id.generation() == 0 {
            return Err(
                ProtocolError::HandshakeFailed("NodeId has invalid generation number").into(),
            );
        }

        let peer_node_id = NodeId::from(peer_node_id)
            .as_generational()
            .expect("peer node is generational");

        // Sanity check. Nodes must not connect to themselves from other generations.
        if my_node_id.as_plain() == peer_node_id.as_plain() && peer_node_id != my_node_id {
            // My node ID but different generations!
            return Err(ProtocolError::HandshakeFailed(
                "cannot accept a connection to the same NodeID from a different generation",
            )
            .into());
        }

        // Are we both from the same cluster?
        if hello.cluster_name != nodes_config.cluster_name() {
            return Err(ProtocolError::HandshakeFailed("cluster name mismatch").into());
        }

        let selected_protocol_version = negotiate_protocol_version(&hello)?;
        debug!(
            "Negotiated protocol version {:?} with client",
            selected_protocol_version
        );

        self.verify_node_id(peer_node_id, header, &nodes_config)?;

        let (tx, output_stream) =
            mpsc::channel(self.networking_options.outbound_queue_length.into());
        let output_stream = ReceiverStream::new(output_stream);
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

        tx.try_send(welcome)
            .expect("channel accept Welcome message");
        let connection = OwnedConnection::new(peer_node_id, selected_protocol_version, tx);

        INCOMING_CONNECTION.increment(1);
        // Register the connection.
        let _ = self.start_connection_reactor(connection, incoming)?;

        // Our output stream, i.e. responses.
        Ok(output_stream)
    }

    /// Always attempts to create a new connection with peer
    pub async fn enforce_new_connection(
        &self,
        node_id: GenerationalNodeId,
    ) -> Result<WeakConnection, NetworkError> {
        let connection = self.connect(node_id).await?;
        Ok(connection.downgrade())
    }

    /// Gets an existing connection or creates a new one if no active connection exists. If
    /// multiple connections already exist, it returns a random one.
    pub async fn get_or_connect(
        &self,
        node_id: GenerationalNodeId,
    ) -> Result<Arc<OwnedConnection>, NetworkError> {
        // find a connection by node_id
        let maybe_connection: Option<Arc<OwnedConnection>> = {
            let guard = self.inner.lock();
            guard.get_random_connection(&node_id)
            // lock is dropped.
        };

        if let Some(connection) = maybe_connection {
            return Ok(connection);
        }
        // We have no connection. We attempt to create a new connection.
        self.connect(node_id).await
    }

    async fn connect(
        &self,
        node_id: GenerationalNodeId,
    ) -> Result<Arc<OwnedConnection>, NetworkError> {
        if node_id == self.metadata.my_node_id() {
            return self.connect_loopback();
        }

        let my_node_id = self.metadata.my_node_id();
        let nodes_config = self.metadata.nodes_config_snapshot();
        let cluster_name = nodes_config.cluster_name().to_owned();

        let (tx, output_stream) =
            mpsc::channel(self.networking_options.outbound_queue_length.into());
        let output_stream = ReceiverStream::new(output_stream);
        let hello = Hello::new(my_node_id, cluster_name);

        // perform handshake.
        let hello = Message::new(
            Header::new(
                self.metadata.nodes_config_version(),
                None,
                None,
                None,
                super::generate_msg_id(),
                None,
            ),
            hello,
        );

        // Prime the channel with the hello message before connecting.
        tx.send(hello).await.expect("Channel accept hello message");

        // Establish the connection
        let mut incoming = self
            .transport_connector
            .connect(node_id, &nodes_config, output_stream)
            .await?;
        // finish the handshake
        let (_header, welcome) = wait_for_welcome(
            &mut incoming,
            self.networking_options.handshake_timeout.into(),
        )
        .await?;
        let protocol_version = welcome.protocol_version();

        if !protocol_version.is_supported() {
            return Err(ProtocolError::UnsupportedVersion(protocol_version.into()).into());
        }

        // sanity checks
        let peer_node_id: NodeId = welcome
            .my_node_id
            .ok_or(ProtocolError::HandshakeFailed(
                "Peer must set my_node_id in Welcome message",
            ))?
            .into();

        // we expect the node to identify itself as the same NodeId
        // we think we are connecting to
        if peer_node_id != node_id {
            // Node claims that it's someone else!
            return Err(ProtocolError::HandshakeFailed(
                "Node returned an unexpected NodeId in Welcome message.",
            )
            .into());
        }

        let connection = OwnedConnection::new(
            peer_node_id
                .as_generational()
                .expect("must be generational id"),
            protocol_version,
            tx,
        );

        OUTGOING_CONNECTION.increment(1);
        self.start_connection_reactor(connection, incoming)
    }

    fn connect_loopback(&self) -> Result<Arc<OwnedConnection>, NetworkError> {
        let (tx, rx) = mpsc::channel(self.networking_options.outbound_queue_length.into());
        let connection = OwnedConnection::new(
            self.metadata.my_node_id(),
            restate_types::net::CURRENT_PROTOCOL_VERSION,
            tx,
        );

        let incoming = ReceiverStream::new(rx).map(Ok);
        self.start_connection_reactor(connection, incoming)
    }

    fn verify_node_id(
        &self,
        peer_node_id: GenerationalNodeId,
        header: Header,
        nodes_config: &NodesConfiguration,
    ) -> Result<(), NetworkError> {
        if let Err(e) = nodes_config.find_node_by_id(peer_node_id) {
            // If nodeId is unrecognized and peer is at higher nodes configuration version,
            // then we have to update our NodesConfiguration
            if let Some(other_nodes_config_version) = header.my_nodes_config_version.map(Into::into)
            {
                let peer_is_in_the_future = other_nodes_config_version > nodes_config.version();

                if peer_is_in_the_future {
                    self.metadata.notify_observed_version(
                        MetadataKind::NodesConfiguration,
                        other_nodes_config_version,
                        None,
                        Urgency::High,
                    );
                    debug!("Remote node '{}' with newer nodes configuration '{}' tried to connect. Trying to fetch newer version before accepting connection.", peer_node_id, other_nodes_config_version);
                } else {
                    info!("Unknown remote node '{}' tried to connect to cluster. Rejecting connection.", peer_node_id);
                }
            } else {
                info!("Unknown remote node '{}' w/o specifying its node configuration tried to connect to cluster. Rejecting connection.", peer_node_id);
            }

            return Err(NetworkError::UnknownNode(e));
        }

        Ok(())
    }

    fn start_connection_reactor<S>(
        &self,
        connection: OwnedConnection,
        incoming: S,
    ) -> Result<Arc<OwnedConnection>, NetworkError>
    where
        S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send + 'static,
    {
        // Lock is held, don't perform expensive or async operations here.

        // If we have a connection with an older generation, we request to drop it.
        // However, more than one connection with the same generation is allowed.
        let mut _cleanup = false;
        let mut guard = self.inner.lock();
        let known_generation = guard
            .observed_generations
            .get(&connection.peer.as_plain())
            .copied()
            .unwrap_or(connection.peer.generation());

        if known_generation > connection.peer.generation() {
            // This peer is _older_ than the one we have seen in the past, we cannot accept
            // this connection. We terminate the stream immediately.
            return Err(NetworkError::OldPeerGeneration(format!(
                "newer generation '{}' has been observed",
                NodeId::new_generational(connection.peer.id(), known_generation)
            )));
        }
        if known_generation < connection.peer.generation() {
            // We have observed newer generation of the same node.
            // TODO: Terminate old node's connection by cancelling its reactor task,
            // and continue with this connection.
            _cleanup = true;
        }
        // update observed generation
        guard
            .observed_generations
            .insert(connection.peer.as_plain(), connection.peer.generation());

        let connection = Arc::new(connection);
        let peer_node_id = connection.peer;
        let connection_weak = Arc::downgrade(&connection);
        let span = tracing::error_span!(parent: None, "network-reactor",
            task_id = tracing::field::Empty,
            peer_node_id = %peer_node_id,
            protocol_version = ?connection.protocol_version() as i32,
        );
        let router = guard.router.clone();

        let task_id = task_center().spawn_child(
            TaskKind::ConnectionReactor,
            "network-connection-reactor",
            None,
            run_reactor(
                self.inner.clone(),
                connection.clone(),
                router,
                incoming,
                self.metadata.clone(),
            )
            .instrument(span),
        )?;
        if peer_node_id != self.metadata.my_node_id() {
            debug!(
                peer_node_id = %peer_node_id,
                task_id = %task_id,
                "Incoming connection accepted from node {}", peer_node_id
            );
        }
        // Reactor has already started by now.

        guard.connections.insert(task_id, connection_weak.clone());
        // clean up old connections
        guard.cleanup_stale_connections(&peer_node_id);
        // Add this connection.
        guard
            .connection_by_gen_id
            .entry(peer_node_id)
            .or_default()
            .push(connection_weak);
        Ok(connection)
    }
}

async fn run_reactor<S>(
    connection_manager: Arc<Mutex<ConnectionManagerInner>>,
    connection: Arc<OwnedConnection>,
    router: MessageRouter,
    mut incoming: S,
    metadata: Metadata,
) -> anyhow::Result<()>
where
    S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send,
{
    Span::current().record(
        "task_id",
        tracing::field::display(current_task_id().unwrap()),
    );
    let mut cancellation = std::pin::pin!(cancellation_watcher());
    let mut seen_versions = MetadataVersions::default();

    // Receive loop
    loop {
        // read a message from the stream
        let msg = tokio::select! {
            biased;
            _ = &mut cancellation => {
                connection.send_control_frame(ConnectionControl::shutdown());
                break;
            },
            msg = incoming.next() => {
                match msg {
                    Some(Ok(msg)) => { msg }
                    Some(Err(status)) => {
                        // stream has terminated.
                        info!("Error received: {status}, terminating stream");
                        break;
                    }
                    None => {
                        // stream has terminated cleanly.
                        break;
                    }
                }
            }
        };

        MESSAGE_RECEIVED.increment(1);
        let processing_started = Instant::now();

        //  header is required on all messages
        let Some(header) = msg.header else {
            connection.send_control_frame(ConnectionControl::codec_error(
                "Header is missing on message",
            ));
            break;
        };

        seen_versions
            .update(
                header.my_nodes_config_version.map(Into::into),
                header.my_partition_table_version.map(Into::into),
                header.my_schema_version.map(Into::into),
                header.my_logs_version.map(Into::into),
            )
            .into_iter()
            .for_each(|(kind, version)| {
                if let Some(version) = version {
                    metadata.notify_observed_version(
                        kind,
                        version,
                        Some(connection.downgrade()),
                        Urgency::Normal,
                    );
                }
            });

        // body are not allowed to be empty.
        let Some(body) = msg.body else {
            connection
                .send_control_frame(ConnectionControl::codec_error("Body is missing on message"));
            break;
        };

        // Welcome and hello are not allowed after handshake
        if body.is_welcome() || body.is_hello() {
            connection.send_control_frame(ConnectionControl::codec_error(
                "Hello/Welcome are not allowed after handshake",
            ));
            break;
        };

        // if it's a control signal, handle it, otherwise, route with message router.
        if let message::Body::ConnectionControl(ctrl_msg) = &body {
            // do something
            info!(
                "Terminating connection based on signal from peer: {:?} {}",
                ctrl_msg.signal(),
                ctrl_msg.message
            );
            break;
        }

        match body.try_as_binary_body(connection.protocol_version) {
            Ok(msg) => {
                if let Err(e) = router
                    .call(
                        Incoming::from_parts(
                            msg,
                            connection.downgrade(),
                            header.msg_id,
                            header.in_response_to,
                            PeerMetadataVersion::from(header),
                        ),
                        connection.protocol_version,
                    )
                    .await
                {
                    warn!("Error processing message: {:?}", e);
                }
                MESSAGE_PROCESSING_DURATION.record(processing_started.elapsed());
            }
            Err(status) => {
                // terminate the stream
                info!("Error processing message, reporting error to peer: {status}");
                MESSAGE_PROCESSING_DURATION.record(processing_started.elapsed());
                connection.send_control_frame(ConnectionControl::codec_error(status.to_string()));
                break;
            }
        }
    }

    // remove from active set
    ONGOING_DRAIN.increment(1.0);
    on_connection_draining(&connection, &connection_manager);
    let protocol_version = connection.protocol_version;
    let peer_node_id = connection.peer;
    let connection_created_at = connection.created;
    // dropping the connection since it's the owner of sender stream.
    drop(connection);

    let drain_start = std::time::Instant::now();
    trace!("Draining connection");
    let mut drain_counter = 0;
    // Draining of incoming queue
    while let Some(Ok(msg)) = incoming.next().await {
        // ignore malformed messages
        let Some(header) = msg.header else {
            continue;
        };
        if let Some(body) = msg.body {
            // we ignore non-deserializable messages (serde errors, or control signals in drain)
            if let Ok(msg) = body.try_as_binary_body(protocol_version) {
                drain_counter += 1;
                if let Err(e) = router
                    .call(
                        Incoming::from_parts(
                            msg,
                            // This is a dying connection, don't pass it down.
                            WeakConnection::new_closed(peer_node_id),
                            header.msg_id,
                            header.in_response_to,
                            PeerMetadataVersion::from(header),
                        ),
                        protocol_version,
                    )
                    .await
                {
                    debug!(
                        "Error processing message while draining connection: {:?}",
                        e
                    );
                }
            }
        }
    }

    // We should also terminate response stream. This happens automatically when
    // the sender is dropped
    on_connection_terminated(&connection_manager);
    ONGOING_DRAIN.decrement(1.0);
    CONNECTION_DROPPED.increment(1);
    debug!(
        "Connection terminated, drained {} messages in {:?}, total connection age is {:?}",
        drain_counter,
        drain_start.elapsed(),
        connection_created_at.elapsed()
    );
    Ok(())
}

fn on_connection_draining(
    connection: &OwnedConnection,
    inner_manager: &Mutex<ConnectionManagerInner>,
) {
    let mut guard = inner_manager.lock();
    if let Some(connections) = guard.connection_by_gen_id.get_mut(&connection.peer) {
        // Remove this connection from connections map to reduce the chance
        // of picking it up as connection.
        connections.retain(|c| {
            c.upgrade()
                .map(|c| c.as_ref() != connection)
                .unwrap_or_default()
        })
    }
}

fn on_connection_terminated(inner_manager: &Mutex<ConnectionManagerInner>) {
    let task_id = current_task_id().expect("TaskId is set");
    let mut guard = inner_manager.lock();
    guard.drop_connection(task_id);
}

#[derive(Debug, Clone, PartialEq, derive_more::Index, derive_more::IndexMut)]
pub struct MetadataVersions {
    #[index]
    versions: EnumMap<MetadataKind, Version>,
}

impl Default for MetadataVersions {
    fn default() -> Self {
        Self {
            versions: EnumMap::from_fn(|_| Version::INVALID),
        }
    }
}

impl MetadataVersions {
    pub fn update(
        &mut self,
        nodes_config_version: Option<Version>,
        partition_table_version: Option<Version>,
        schema_version: Option<Version>,
        logs_version: Option<Version>,
    ) -> EnumMap<MetadataKind, Option<Version>> {
        let mut result = EnumMap::default();
        result[MetadataKind::NodesConfiguration] =
            self.update_internal(MetadataKind::NodesConfiguration, nodes_config_version);
        result[MetadataKind::PartitionTable] =
            self.update_internal(MetadataKind::PartitionTable, partition_table_version);
        result[MetadataKind::Schema] = self.update_internal(MetadataKind::Schema, schema_version);
        result[MetadataKind::Logs] = self.update_internal(MetadataKind::Logs, logs_version);

        result
    }

    fn update_internal(
        &mut self,
        metadata_kind: MetadataKind,
        version: Option<Version>,
    ) -> Option<Version> {
        if let Some(version) = version {
            if version > self.versions[metadata_kind] {
                self.versions[metadata_kind] = version;
                return Some(version);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;
    use test_log::test;
    use tokio::sync::mpsc;

    use restate_test_util::{assert_eq, let_assert};
    use restate_types::net::codec::WireDecode;
    use restate_types::net::metadata::{GetMetadataRequest, MetadataMessage};
    use restate_types::net::partition_processor_manager::GetProcessorsState;
    use restate_types::net::{
        AdvertisedAddress, ProtocolVersion, CURRENT_PROTOCOL_VERSION,
        MIN_SUPPORTED_PROTOCOL_VERSION,
    };
    use restate_types::nodes_config::{
        LogServerConfig, NodeConfig, NodesConfigError, NodesConfiguration, Role,
    };
    use restate_types::protobuf::node::message::Body;
    use restate_types::protobuf::node::{Header, Hello};
    use restate_types::Version;

    use crate::network::MockPeerConnection;
    use crate::{TestCoreEnv, TestCoreEnvBuilder};

    // Test handshake with a client
    #[tokio::test]
    async fn test_hello_welcome_handshake() -> Result<()> {
        let test_setup = TestCoreEnv::create_with_single_node(1, 1).await;
        test_setup
            .tc
            .run_in_scope("test", None, async {
                let metadata = crate::metadata();
                let connections = ConnectionManager::new_incoming_only(metadata.clone());

                let _mock_connection = MockPeerConnection::connect(
                    GenerationalNodeId::new(1, 1),
                    metadata.nodes_config_version(),
                    metadata.nodes_config_ref().cluster_name().to_owned(),
                    &connections,
                    10,
                )
                .await
                .unwrap();

                Ok(())
            })
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn test_hello_welcome_timeout() -> Result<()> {
        let test_setup = TestCoreEnv::create_with_single_node(1, 1).await;
        let metadata = test_setup.metadata;
        let net_opts = NetworkingOptions::default();
        test_setup
            .tc
            .run_in_scope("test", None, async {
                let (_tx, rx) = mpsc::channel(1);
                let connections = ConnectionManager::new_incoming_only(metadata);

                let start = tokio::time::Instant::now();
                let incoming = ReceiverStream::new(rx);
                let resp = connections.accept_incoming_connection(incoming).await;
                assert!(resp.is_err());
                assert!(matches!(
                    resp,
                    Err(NetworkError::ProtocolError(
                        ProtocolError::HandshakeTimeout(_)
                    ))
                ));
                assert!(start.elapsed() >= net_opts.handshake_timeout.into());
                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn test_bad_handshake() -> Result<()> {
        let test_setup = TestCoreEnv::create_with_single_node(1, 1).await;
        let metadata = test_setup.metadata;
        test_setup
            .tc
            .run_in_scope("test", None, async {
                let (tx, rx) = mpsc::channel(1);
                let my_node_id = metadata.my_node_id();

                // unsupported protocol version
                let hello = Hello {
                    min_protocol_version: ProtocolVersion::Unknown.into(),
                    max_protocol_version: ProtocolVersion::Unknown.into(),
                    my_node_id: Some(my_node_id.into()),
                    cluster_name: metadata.nodes_config_ref().cluster_name().to_owned(),
                };
                let hello = Message::new(
                    Header::new(
                        metadata.nodes_config_version(),
                        None,
                        None,
                        None,
                        crate::network::generate_msg_id(),
                        None,
                    ),
                    hello,
                );
                tx.send(Ok(hello))
                    .await
                    .expect("Channel accept hello message");

                let connections = ConnectionManager::new_incoming_only(metadata.clone());
                let incoming = ReceiverStream::new(rx);
                let resp = connections.accept_incoming_connection(incoming).await;
                assert!(resp.is_err());
                assert!(matches!(
                    resp,
                    Err(NetworkError::ProtocolError(
                        ProtocolError::UnsupportedVersion(proto_version)
                    )) if proto_version == ProtocolVersion::Unknown as i32
                ));

                // cluster name mismatch
                let (tx, rx) = mpsc::channel(1);
                let my_node_id = metadata.my_node_id();
                let hello = Hello {
                    min_protocol_version: MIN_SUPPORTED_PROTOCOL_VERSION.into(),
                    max_protocol_version: CURRENT_PROTOCOL_VERSION.into(),
                    my_node_id: Some(my_node_id.into()),
                    cluster_name: "Random-cluster".to_owned(),
                };
                let hello = Message::new(
                    Header::new(
                        metadata.nodes_config_version(),
                        None,
                        None,
                        None,
                        crate::network::generate_msg_id(),
                        None,
                    ),
                    hello,
                );
                tx.send(Ok(hello)).await?;

                let connections = ConnectionManager::new_incoming_only(metadata.clone());
                let incoming = ReceiverStream::new(rx);
                let err = connections
                    .accept_incoming_connection(incoming)
                    .await
                    .err()
                    .unwrap();
                assert!(matches!(
                    err,
                    NetworkError::ProtocolError(ProtocolError::HandshakeFailed(
                        "cluster name mismatch"
                    ))
                ));
                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn test_node_generation() -> Result<()> {
        let test_setup = TestCoreEnv::create_with_single_node(1, 2).await;
        let metadata = test_setup.metadata;
        test_setup
            .tc
            .run_in_scope("test", None, async {
                let (tx, rx) = mpsc::channel(1);
                let mut my_node_id = metadata.my_node_id();
                assert_eq!(2, my_node_id.generation());
                my_node_id.bump_generation();

                // newer generation
                let hello = Hello::new(
                    my_node_id,
                    metadata.nodes_config_ref().cluster_name().to_owned(),
                );
                let hello = Message::new(
                    Header::new(
                        metadata.nodes_config_version(),
                        None,
                        None,
                        None,
                        crate::network::generate_msg_id(),
                        None,
                    ),
                    hello,
                );
                tx.send(Ok(hello))
                    .await
                    .expect("Channel accept hello message");

                let connections = ConnectionManager::new_incoming_only(metadata.clone());

                let incoming = ReceiverStream::new(rx);
                let err = connections
                    .accept_incoming_connection(incoming)
                    .await
                    .err()
                    .unwrap();

                assert!(matches!(
                    err,
                    NetworkError::ProtocolError(ProtocolError::HandshakeFailed(
                        "cannot accept a connection to the same NodeID from a different generation",
                    ))
                ));

                // Unrecognized node Id
                let (tx, rx) = mpsc::channel(1);
                let my_node_id = GenerationalNodeId::new(55, 2);

                let hello = Hello::new(
                    my_node_id,
                    metadata.nodes_config_ref().cluster_name().to_owned(),
                );
                let hello = Message::new(
                    Header::new(
                        metadata.nodes_config_version(),
                        None,
                        None,
                        None,
                        crate::network::generate_msg_id(),
                        None,
                    ),
                    hello,
                );
                tx.send(Ok(hello))
                    .await
                    .expect("Channel accept hello message");

                let connections = ConnectionManager::new_incoming_only(metadata);

                let incoming = ReceiverStream::new(rx);
                let err = connections
                    .accept_incoming_connection(incoming)
                    .await
                    .err()
                    .unwrap();
                assert!(matches!(
                    err,
                    NetworkError::UnknownNode(NodesConfigError::UnknownNodeId(_))
                ));
                Ok(())
            })
            .await
    }

    #[test(tokio::test(start_paused = true))]
    async fn fetching_metadata_updates_through_message_headers() -> Result<()> {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());

        let node_id = GenerationalNodeId::new(42, 42);
        let node_config = NodeConfig::new(
            "42".to_owned(),
            node_id,
            AdvertisedAddress::Uds("foobar1".into()),
            Role::Worker.into(),
            LogServerConfig::default(),
        );
        nodes_config.upsert_node(node_config);

        let test_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_nodes_config(nodes_config)
            .build()
            .await;

        test_env
            .tc
            .run_in_scope("test", None, async {
                let metadata = crate::metadata();

                let mut connection = MockPeerConnection::connect(
                    node_id,
                    metadata.nodes_config_version(),
                    metadata.nodes_config_ref().cluster_name().to_string(),
                    test_env.networking.connection_manager(),
                    10,
                )
                .await
                .into_test_result()?;

                let request = GetProcessorsState {};
                let partition_table_version = metadata.partition_table_version().next();
                let header = Header::new(
                    metadata.nodes_config_version(),
                    None,
                    None,
                    Some(partition_table_version),
                    crate::network::generate_msg_id(),
                    None,
                );

                connection
                    .send_raw(request, header)
                    .await
                    .into_test_result()?;

                // we expect the request to go throught he existing open connection to my node
                let message = connection.recv_stream.next().await.expect("some message");
                assert_get_metadata_request(
                    message,
                    connection.protocol_version,
                    MetadataKind::PartitionTable,
                    partition_table_version,
                );

                Ok(())
            })
            .await
    }

    fn assert_get_metadata_request(
        message: Message,
        protocol_version: ProtocolVersion,
        metadata_kind: MetadataKind,
        version: Version,
    ) {
        let metadata_message =
            decode_metadata_message(message, protocol_version).expect("valid message");
        assert_that!(
            metadata_message,
            pat!(MetadataMessage::GetMetadataRequest(pat!(
                GetMetadataRequest {
                    metadata_kind: eq(metadata_kind),
                    min_version: eq(Some(version))
                }
            )))
        );
    }

    fn decode_metadata_message(
        message: Message,
        protocol_version: ProtocolVersion,
    ) -> Result<MetadataMessage> {
        let_assert!(Some(Body::Encoded(mut binary_message)) = message.body);

        let metadata_message =
            MetadataMessage::decode(&mut binary_message.payload, protocol_version)?;
        Ok(metadata_message)
    }
}
