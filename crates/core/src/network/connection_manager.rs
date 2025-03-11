// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Weak};
use std::time::Duration;

use ahash::HashMap;
use enum_map::EnumMap;
use futures::future::OptionFuture;
use futures::{Stream, StreamExt};
use metrics::{counter, histogram};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use parking_lot::Mutex;
use tokio::time::{Instant, timeout};
use tracing::{Instrument, Span, debug, info, instrument, trace, warn};

use restate_futures_util::overdue::OverdueLoggingExt;
use restate_types::config::Configuration;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::{GenerationalNodeId, Merge, NodeId, PlainNodeId, Version};

use super::connection::{OwnedConnection, WeakConnection};
use super::error::{NetworkError, ProtocolError};
use super::handshake::wait_for_welcome;
use super::io::{
    DrainReason, DropEgressStream, EgressMessage, EgressStream, UnboundedEgressSender,
};
use super::metric_definitions::{
    self, CONNECTION_DROPPED, INCOMING_CONNECTION, OUTGOING_CONNECTION,
};
use super::protobuf::network::{Header, Hello, Message, Welcome, message::Body, message::Signal};
use super::transport_connector::TransportConnect;
use super::{Handler, MessageRouter};
use crate::metadata::Urgency;
use crate::network::handshake::{negotiate_protocol_version, wait_for_hello};
use crate::network::metric_definitions::{
    NETWORK_MESSAGE_PROCESSING_DURATION, NETWORK_MESSAGE_RECEIVED, NETWORK_MESSAGE_RECEIVED_BYTES,
};
use crate::network::{Incoming, PeerMetadataVersion};
use crate::{Metadata, TaskCenter, TaskContext, TaskId, TaskKind, my_node_id};

#[derive(Copy, Clone, PartialOrd, PartialEq, Default)]
struct GenStatus {
    generation: u32,
    // a gone node is a node that has told us that it's shutting down. We don't expect to be able
    // to connect to this node in the future unless a higher generation node shows up.
    gone: bool,
}

impl GenStatus {
    fn new(generation: u32) -> Self {
        Self {
            generation,
            gone: false,
        }
    }
}

impl Merge for GenStatus {
    fn merge(&mut self, other: Self) -> bool {
        if other.generation > self.generation {
            self.generation = other.generation;
            self.gone = other.gone;
            true
        } else if other.generation == self.generation && self.gone {
            false
        } else if other.generation == self.generation && !self.gone {
            self.gone.merge(other.gone)
        } else {
            false
        }
    }
}

struct ConnectionManagerInner {
    router: MessageRouter,
    connections: HashMap<TaskId, Weak<OwnedConnection>>,
    connection_by_gen_id: HashMap<GenerationalNodeId, Vec<Weak<OwnedConnection>>>,
    /// This tracks the max generation we observed from connection attempts regardless of our nodes
    /// configuration. We cannot accept connections from nodes older than ones we have observed
    /// already.
    observed_generations: HashMap<PlainNodeId, GenStatus>,
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
        target_concurrency: usize,
    ) -> Option<Arc<OwnedConnection>> {
        use rand::prelude::IndexedRandom;
        self.connection_by_gen_id
            .get(peer_node_id)
            .and_then(|connections| {
                // Suggest we create new connection if the number
                // of connections is below the target
                if connections.len() >= target_concurrency {
                    connections.choose(&mut rand::rng())?.upgrade()
                } else {
                    None
                }
            })
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

#[derive(Clone, Default)]
pub struct ConnectionManager {
    inner: Arc<Mutex<ConnectionManagerInner>>,
}

impl ConnectionManager {
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
    ) -> Result<EgressStream, NetworkError>
    where
        S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send + 'static,
    {
        let metadata = Metadata::current();
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
            Configuration::pinned().networking.handshake_timeout.into(),
        )
        .await?;
        let nodes_config = metadata.nodes_config_ref();
        let my_node_id = metadata.my_node_id();
        // NodeId **must** be generational at this layer, we may support accepting connections from
        // anonymous nodes in the future. When this happens, this restate-server release will not
        // be compatible with it.
        let peer_node_id = hello.my_node_id.ok_or(ProtocolError::HandshakeFailed(
            "GenerationalNodeId is not set in the Hello message",
        ))?;

        // we don't allow node-id 0 in this version.
        if peer_node_id.id == 0 {
            return Err(ProtocolError::HandshakeFailed("Peer cannot have node Id of 0").into());
        }

        if peer_node_id.generation == 0 {
            return Err(
                ProtocolError::HandshakeFailed("NodeId has invalid generation number").into(),
            );
        }

        // convert to our internal type
        let peer_node_id = GenerationalNodeId::from(peer_node_id);

        // Sanity check. Nodes must not connect to themselves from other generations.
        if my_node_id.is_same_but_different(&peer_node_id) {
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

        self.verify_node_id(peer_node_id, &header, &nodes_config, &metadata)?;

        let (sender, unbounded_sender, egress, drop_egress) = EgressStream::create(
            Configuration::pinned()
                .networking
                .outbound_queue_length
                .get(),
        );
        // Enqueue the welcome message
        let welcome = Welcome::new(my_node_id, selected_protocol_version);
        unbounded_sender
            .unbounded_send(EgressMessage::Message(
                Header::default(),
                welcome.into(),
                None,
            ))
            .map_err(|_| ProtocolError::PeerDropped)?;
        let connection = OwnedConnection::new(peer_node_id, selected_protocol_version, sender);

        INCOMING_CONNECTION.increment(1);
        // Register the connection.
        let _ =
            self.start_connection_reactor(connection, unbounded_sender, incoming, drop_egress)?;

        // Our output stream, i.e. responses.
        Ok(egress)
    }

    /// Gets an existing connection or creates a new one if no active connection exists. If
    /// multiple connections already exist, it returns a random one.
    pub async fn get_or_connect<C>(
        &self,
        node_id: GenerationalNodeId,
        transport_connector: &C,
    ) -> Result<Arc<OwnedConnection>, NetworkError>
    where
        C: TransportConnect,
    {
        // fail fast if we are connecting to our previous self
        if my_node_id().is_same_but_different(&node_id) {
            return Err(NetworkError::NodeIsGone(node_id));
        }

        // find a connection by node_id
        let maybe_connection: Option<Arc<OwnedConnection>> = {
            let guard = self.inner.lock();
            guard.get_random_connection(
                &node_id,
                Configuration::pinned()
                    .networking
                    .num_concurrent_connections(),
            )
            // lock is dropped.
        };

        if let Some(connection) = maybe_connection {
            return Ok(connection);
        }

        let is_gone = {
            self.inner
                .lock()
                .observed_generations
                .get(&node_id.as_plain())
                .map(|status| {
                    node_id.generation() < status.generation
                        || (node_id.generation() == status.generation && status.gone)
                })
                .unwrap_or(false)
            // lock is dropped.
        };

        if is_gone {
            return Err(NetworkError::NodeIsGone(node_id));
        }
        // We have no connection. We attempt to create a new connection.
        self.connect(node_id, transport_connector).await
    }

    async fn connect<C>(
        &self,
        node_id: GenerationalNodeId,
        transport_connector: &C,
    ) -> Result<Arc<OwnedConnection>, NetworkError>
    where
        C: TransportConnect,
    {
        let metadata = Metadata::current();
        if node_id == metadata.my_node_id() {
            return self.connect_loopback();
        }

        let my_node_id = metadata.my_node_id();
        let nodes_config = metadata.nodes_config_snapshot();
        let cluster_name = nodes_config.cluster_name().to_owned();

        let (tx, unbounded_sender, egress, drop_egress) = EgressStream::create(
            Configuration::pinned()
                .networking
                .outbound_queue_length
                .get(),
        );

        // perform handshake.
        let hello = Hello::new(my_node_id, cluster_name);
        // Prime the channel with the hello message before connecting.
        unbounded_sender
            .unbounded_send(EgressMessage::Message(
                Header::default(),
                hello.into(),
                None,
            ))
            .expect("egress channel must be open");

        // Establish the connection
        let mut incoming = transport_connector
            .connect(node_id, &nodes_config, egress)
            .await?;
        // finish the handshake
        let (_header, welcome) = wait_for_welcome(
            &mut incoming,
            Configuration::pinned().networking.handshake_timeout.into(),
        )
        .await?;
        let protocol_version = welcome.protocol_version();

        if !protocol_version.is_supported() {
            return Err(ProtocolError::UnsupportedVersion(protocol_version.into()).into());
        }

        // sanity checks
        // In this version, we don't allow anonymous connections.
        let peer_node_id: GenerationalNodeId = welcome
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
                "Node returned an unexpected GenerationalNodeId in Welcome message.",
            )
            .into());
        }

        let connection = OwnedConnection::new(peer_node_id, protocol_version, tx);

        OUTGOING_CONNECTION.increment(1);
        self.start_connection_reactor(connection, unbounded_sender, incoming, drop_egress)
    }

    #[instrument(skip_all)]
    fn connect_loopback(&self) -> Result<Arc<OwnedConnection>, NetworkError> {
        trace!("Creating an express path connection to self");
        let (tx, unbounded_sender, egress, drop_egress) = EgressStream::create(
            Configuration::pinned()
                .networking
                .outbound_queue_length
                .get(),
        );
        let connection = OwnedConnection::new(
            my_node_id(),
            restate_types::net::CURRENT_PROTOCOL_VERSION,
            tx,
        );

        self.start_connection_reactor(connection, unbounded_sender, egress.map(Ok), drop_egress)
    }

    fn verify_node_id(
        &self,
        peer_node_id: GenerationalNodeId,
        header: &Header,
        nodes_config: &NodesConfiguration,
        metadata: &Metadata,
    ) -> Result<(), NetworkError> {
        if let Err(e) = nodes_config.find_node_by_id(peer_node_id) {
            // If nodeId is unrecognized and peer is at higher nodes configuration version,
            // then we have to update our NodesConfiguration
            if let Some(other_nodes_config_version) = header.my_nodes_config_version.map(Into::into)
            {
                let peer_is_in_the_future = other_nodes_config_version > nodes_config.version();

                if peer_is_in_the_future {
                    metadata.notify_observed_version(
                        MetadataKind::NodesConfiguration,
                        other_nodes_config_version,
                        None,
                        Urgency::High,
                    );
                    debug!(
                        "Remote node '{}' with newer nodes configuration '{}' tried to connect. Trying to fetch newer version before accepting connection.",
                        peer_node_id, other_nodes_config_version
                    );
                } else {
                    info!(
                        "Unknown remote node '{}' tried to connect to cluster. Rejecting connection.",
                        peer_node_id
                    );
                }
            } else {
                info!(
                    "Unknown remote node '{}' w/o specifying its node configuration tried to connect to cluster. Rejecting connection.",
                    peer_node_id
                );
            }

            return Err(NetworkError::UnknownNode(e));
        }

        Ok(())
    }

    fn start_connection_reactor<S>(
        &self,
        connection: OwnedConnection,
        unbounded_sender: UnboundedEgressSender,
        incoming: S,
        drop_egress: DropEgressStream,
    ) -> Result<Arc<OwnedConnection>, NetworkError>
    where
        S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send + 'static,
    {
        // Lock is held, don't perform expensive or async operations here.

        // If we have a connection with an older generation, we request to drop it.
        // However, more than one connection with the same generation is allowed.
        let mut _cleanup = false;
        let mut guard = self.inner.lock();
        let known_status = guard
            .observed_generations
            .get(&connection.peer.as_plain())
            .copied()
            .unwrap_or(GenStatus::new(connection.peer.generation()));

        if known_status.generation > connection.peer.generation() {
            // This peer is _older_ than the one we have seen in the past, we cannot accept
            // this connection. We terminate the stream immediately.
            return Err(NetworkError::OldPeerGeneration(format!(
                "newer generation '{}' has been observed",
                NodeId::new_generational(connection.peer.id(), known_status.generation)
            )));
        }

        if known_status.generation == connection.peer.generation() && known_status.gone {
            // This peer was observed to have shutdown before. We cannot accept new connections from this peer.
            return Err(NetworkError::NodeIsGone(connection.peer));
        }

        if known_status.generation < connection.peer.generation() {
            // We have observed newer generation of the same node.
            // TODO: Terminate old node's connection by cancelling its reactor task,
            // and continue with this connection.
            _cleanup = true;
        }
        // update observed generation
        let new_status = GenStatus::new(connection.peer.generation());
        guard
            .observed_generations
            .entry(connection.peer.as_plain())
            .and_modify(|status| {
                status.merge(new_status);
            })
            .or_insert(new_status);

        let connection = Arc::new(connection);
        let peer_node_id = connection.peer;
        let connection_weak = Arc::downgrade(&connection);
        let span = tracing::error_span!(parent: None, "network-reactor",
            task_id = tracing::field::Empty,
            peer = %peer_node_id,
        );
        let router = guard.router.clone();

        let task_id = TaskCenter::spawn_child(
            TaskKind::ConnectionReactor,
            "network-connection-reactor",
            run_reactor(
                self.inner.clone(),
                connection.clone(),
                unbounded_sender,
                router,
                incoming,
                drop_egress,
            )
            .instrument(span),
        )?;
        if peer_node_id != my_node_id() {
            debug!(
                peer = %peer_node_id,
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
    tx: UnboundedEgressSender,
    router: MessageRouter,
    mut incoming: S,
    drop_egress: DropEgressStream,
) -> anyhow::Result<()>
where
    S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send,
{
    let current_task = TaskContext::current();
    let metadata = Metadata::current();
    Span::current().record("task_id", tracing::field::display(current_task.id()));
    let mut cancellation = std::pin::pin!(current_task.cancellation_token().cancelled());
    let mut seen_versions = MetadataVersions::default();

    let context_propagator = TraceContextPropagator::default();
    let mut needs_drain = false;
    let mut is_peer_shutting_down = false;
    // Receive loop
    loop {
        // read a message from the stream
        let msg = tokio::select! {
            biased;
            _ = &mut cancellation => {
                if TaskCenter::is_shutdown_requested() {
                    // We want to make the distinction between whether we are terminating the
                    // connection, or whether the node is shutting down.
                    tx.unbounded_drain(DrainReason::Shutdown);
                } else {
                    tx.unbounded_drain(DrainReason::ConnectionDrain);
                }
                // we only drain the connection if we were the initiators of the termination
                needs_drain = true;
                break;
            },
            msg = incoming.next() => {
                match msg {
                    Some(Ok(msg)) => { msg }
                    Some(Err(status)) => {
                        // stream has terminated.
                        info!("Error received: {status}, terminating stream");
                        tx.unbounded_drain(DrainReason::ConnectionDrain);
                        break;
                    }
                    None => {
                        // peer terminated the connection
                        // stream has terminated cleanly.
                        needs_drain = true;
                        tx.unbounded_drain(DrainReason::ConnectionDrain);
                        break;
                    }
                }
            }
        };

        let processing_started = Instant::now();

        // body are not allowed to be empty.
        let Some(body) = msg.body else {
            tx.unbounded_drain(DrainReason::CodecError(
                "Body is missing on message".to_owned(),
            ));
            break;
        };

        // Welcome and hello are not allowed after handshake
        if body.is_welcome() || body.is_hello() {
            tx.unbounded_drain(DrainReason::CodecError(
                "Hello/Welcome are not allowed after handshake".to_owned(),
            ));
            break;
        };

        // if it's a control signal, handle it, otherwise, route with message router.
        if let Body::ConnectionControl(ctrl_msg) = &body {
            // do something
            info!(
                "Terminating connection based on signal from {}: {}",
                connection.peer(),
                ctrl_msg.message
            );
            if ctrl_msg.signal() == Signal::Shutdown {
                tx.unbounded_drain(DrainReason::ConnectionDrain);
                is_peer_shutting_down = true;
                needs_drain = true;
            }
            break;
        }

        //  header is required on all messages
        let Some(header) = msg.header else {
            tx.unbounded_drain(DrainReason::CodecError(
                "Header is missing on message".to_owned(),
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

        let encoded_len = body.encoded_len();
        match body.try_as_binary_body(connection.protocol_version) {
            Ok(msg) => {
                let target = msg.target();

                let parent_context = header
                    .span_context
                    .as_ref()
                    .map(|span_ctx| context_propagator.extract(span_ctx));

                // unconstrained: We want to avoid yielding if the message router has capacity,
                // this is to improve tail latency of message processing. We still give tokio
                // a yielding point when reading the next message but it would be excessive to
                // introduce more than one yielding point in this reactor loop.
                if let Err(e) = tokio::task::unconstrained(
                    router.call(
                        Incoming::from_parts(
                            msg,
                            connection.downgrade(),
                            header.msg_id,
                            header.in_response_to,
                            PeerMetadataVersion::from(header),
                        )
                        .with_parent_context(parent_context),
                        connection.protocol_version,
                    ),
                )
                .await
                {
                    warn!(
                        target = target.as_str_name(),
                        "Error processing message: {e}"
                    );
                }
                histogram!(NETWORK_MESSAGE_PROCESSING_DURATION, "target" => target.as_str_name())
                    .record(processing_started.elapsed());
                counter!(NETWORK_MESSAGE_RECEIVED, "target" => target.as_str_name()).increment(1);
                counter!(NETWORK_MESSAGE_RECEIVED_BYTES, "target" => target.as_str_name())
                    .increment(encoded_len as u64);
                trace!(
                    target = target.as_str_name(),
                    "Processed message in {:?}",
                    processing_started.elapsed()
                );
            }
            Err(status) => {
                // terminate the stream
                warn!("Error processing message, reporting error to peer: {status}",);
                tx.unbounded_drain(DrainReason::CodecError(status.to_string()));
                break;
            }
        }
    }

    // remove from active set
    on_connection_draining(&connection, &connection_manager, is_peer_shutting_down);
    let protocol_version = connection.protocol_version;
    let peer_node_id = connection.peer;
    let connection_created_at = connection.created;

    let drain_start = std::time::Instant::now();
    let mut drain_counter = 0;
    let mut tx = Some(tx);
    let mut drop_egress = Some(drop_egress);

    // dropping the connection since it's the owner of sender stream.
    drop(connection);
    if needs_drain {
        let mut drain_timeout = std::pin::pin!(tokio::time::sleep(Duration::from_secs(15)));
        debug!("Draining connection");
        loop {
            tokio::select! {
                _ = &mut drain_timeout => {
                    // drain timed out.
                    debug!("Drain timed out, closing connection");
                    drop_egress.take();
                    drop(incoming);
                    break;
                },
                msg = incoming.next() => {
                    let Some(Ok(msg)) = msg else {
                        break;
                    };
                    // Draining of incoming queue
                    if let Some(Body::ConnectionControl(ctrl_msg)) = &msg.body {
                        trace!(
                            "Received signal from {} while draining: {}",
                            peer_node_id,
                            ctrl_msg.signal()
                        );
                        // No more requests/unary messages will arrive. Reactor can be closed if we don't
                        // have waiting responses
                        match ctrl_msg.signal() {
                            Signal::RequestStreamDrained => {
                                // No more requests coming, but rpc responses might still arrive.
                                // We don't need tx anymore. We'll not create future responder
                                // tasks.
                                tx.take();
                            }
                            Signal::ResponseStreamDrained => {
                                // No more requests coming, but rpc requests might still arrive.
                                // Peer will terminate its sender stream once if drained its two
                                // egress streams.
                            }
                            _ => {}
                        }
                        continue;
                    }
                    // ignore malformed messages
                    let Some(header) = msg.header else {
                        continue;
                    };
                    if let Some(body) = msg.body {
                        // we ignore non-deserializable messages (serde errors, or control signals in drain)
                        if let Ok(msg) = body.try_as_binary_body(protocol_version) {
                            drain_counter += 1;
                            let parent_context = header
                                .span_context
                                .as_ref()
                                .map(|span_ctx| context_propagator.extract(span_ctx));

                            if let Err(e) = router
                                .call(
                                    Incoming::from_parts(
                                        msg,
                                        // This is a dying connection, don't pass it down.
                                        WeakConnection::new_closed(peer_node_id),
                                        header.msg_id,
                                        header.in_response_to,
                                        PeerMetadataVersion::from(header),
                                    )
                                    .with_parent_context(parent_context),
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
                },
            }
        }
    }
    // in case we didn't drain, we still need to drop the response stream
    tx.take();

    // Wait for egress to fully drain
    if timeout(
        Duration::from_secs(5),
        OptionFuture::from(drop_egress)
            .log_slow_after(
                Duration::from_secs(2),
                tracing::Level::INFO,
                "Waiting for connection's egress to drain",
            )
            .with_overdue(Duration::from_secs(3), tracing::Level::WARN),
    )
    .await
    .is_err()
    {
        info!("Connection's egress has taken too long to drain, will drop");
    }
    on_connection_terminated(&connection_manager);
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
    is_peer_shutting_down: bool,
) {
    let mut guard = inner_manager.lock();
    if let Some(connections) = guard.connection_by_gen_id.get_mut(&connection.peer) {
        // Remove this connection from connections map to reduce the chance
        // of picking it up as connection.
        connections.retain(|c| {
            c.upgrade()
                .map(|c| c.as_ref() != connection)
                .unwrap_or_default()
        });
    }
    if is_peer_shutting_down {
        let mut new_status = GenStatus::new(connection.peer.generation());
        new_status.gone = true;
        guard
            .observed_generations
            .entry(connection.peer.as_plain())
            .and_modify(|status| {
                status.merge(new_status);
            })
            .or_insert(new_status);
    }
}

fn on_connection_terminated(inner_manager: &Mutex<ConnectionManagerInner>) {
    let mut guard = inner_manager.lock();
    guard.drop_connection(TaskContext::with_current(|ctx| ctx.id()));
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

    use futures::stream;
    use googletest::IntoTestResult;
    use googletest::prelude::*;
    use test_log::test;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use restate_test_util::{assert_eq, let_assert};
    use restate_types::Version;
    use restate_types::config::NetworkingOptions;
    use restate_types::locality::NodeLocation;
    use restate_types::net::codec::WireDecode;
    use restate_types::net::metadata::{GetMetadataRequest, MetadataMessage};
    use restate_types::net::node::GetNodeState;
    use restate_types::net::{
        AdvertisedAddress, CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION,
        ProtocolVersion,
    };
    use restate_types::nodes_config::{
        LogServerConfig, MetadataServerConfig, NodeConfig, NodesConfigError, NodesConfiguration,
        Role,
    };

    use crate::network::MockPeerConnection;
    use crate::network::protobuf::network::message::Body;
    use crate::network::protobuf::network::{Header, Hello};
    use crate::{self as restate_core, TestCoreEnv, TestCoreEnvBuilder};

    // Test handshake with a client
    #[restate_core::test]
    async fn test_hello_welcome_handshake() -> Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;
        let metadata = Metadata::current();
        let connections = ConnectionManager::default();

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
    }

    #[restate_core::test(start_paused = true)]
    async fn test_hello_welcome_timeout() -> Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;
        let net_opts = NetworkingOptions::default();
        let connections = ConnectionManager::default();

        let start = tokio::time::Instant::now();
        let resp = connections
            .accept_incoming_connection(stream::pending())
            .await;
        assert!(resp.is_err());
        assert!(matches!(
            resp,
            Err(NetworkError::ProtocolError(
                ProtocolError::HandshakeTimeout(_)
            ))
        ));
        assert!(&start.elapsed() >= net_opts.handshake_timeout.as_ref());
        Ok(())
    }

    #[restate_core::test]
    async fn test_bad_handshake() -> Result<()> {
        let test_setup = TestCoreEnv::create_with_single_node(1, 1).await;
        let metadata = test_setup.metadata;
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

        let connections = ConnectionManager::default();
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

        let connections = ConnectionManager::default();
        let incoming = ReceiverStream::new(rx);
        let err = connections
            .accept_incoming_connection(incoming)
            .await
            .err()
            .unwrap();
        assert!(matches!(
            err,
            NetworkError::ProtocolError(ProtocolError::HandshakeFailed("cluster name mismatch"))
        ));
        Ok(())
    }

    #[restate_core::test]
    async fn test_node_generation() -> Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 2).await;
        let metadata = Metadata::current();
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

        let connections = ConnectionManager::default();

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

        let connections = ConnectionManager::default();

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
    }

    #[test(restate_core::test(start_paused = true))]
    async fn fetching_metadata_updates_through_message_headers() -> Result<()> {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());

        let node_id = GenerationalNodeId::new(42, 42);
        let node_config = NodeConfig::new(
            "42".to_owned(),
            node_id,
            NodeLocation::default(),
            AdvertisedAddress::Uds("foobar1".into()),
            Role::Worker.into(),
            LogServerConfig::default(),
            MetadataServerConfig::default(),
        );
        nodes_config.upsert_node(node_config);

        let test_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_nodes_config(nodes_config)
            .build()
            .await;

        let metadata = Metadata::current();

        let mut connection = MockPeerConnection::connect(
            node_id,
            metadata.nodes_config_version(),
            metadata.nodes_config_ref().cluster_name().to_string(),
            test_env.networking.connection_manager(),
            10,
        )
        .await
        .into_test_result()?;

        let request = GetNodeState {};
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

        // we expect the request to go through he existing open connection to my node
        let message = connection.recv_stream.next().await.expect("some message");
        assert_get_metadata_request(
            message,
            connection.protocol_version,
            MetadataKind::PartitionTable,
            partition_table_version,
        );

        Ok(())
    }

    fn assert_get_metadata_request(
        message: Message,
        protocol_version: ProtocolVersion,
        metadata_kind: MetadataKind,
        version: Version,
    ) {
        let metadata_message = decode_metadata_message(message, protocol_version);
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
    ) -> MetadataMessage {
        let_assert!(Some(Body::Encoded(mut binary_message)) = message.body);

        MetadataMessage::decode(&mut binary_message.payload, protocol_version)
    }
}
