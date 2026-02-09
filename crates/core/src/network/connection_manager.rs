// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map;
use std::sync::Arc;

use ahash::HashMap;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, Stream};
use metrics::counter;
use parking_lot::Mutex;
use tracing::{debug, info, trace};

use restate_types::config::Configuration;
use restate_types::nodes_config::{ClusterFingerprint, NodesConfigError, NodesConfiguration};
use restate_types::{GenerationalNodeId, Merge, NodeId, PlainNodeId};

use super::Swimlane;
use super::connection::Connection;
use super::io::{ConnectionReactor, EgressMessage, EgressStream};
use super::protobuf::network::ConnectionDirection;
use super::protobuf::network::{Message, Welcome};
use super::tracking::ConnectionTracking;
use super::transport_connector::{TransportConnect, find_node};
use super::{
    AcceptError, ConnectError, Destination, DiscoveryError, HandshakeError, MessageRouter,
};
use crate::network::PeerMetadataVersion;
use crate::network::connection::ConnectThrottle;
use crate::network::handshake::{negotiate_protocol_version, wait_for_hello};
use crate::network::metric_definitions::{NETWORK_CONNECTION_CREATED, NETWORK_CONNECTION_DROPPED};
use crate::{Metadata, TaskId, TaskKind, my_node_id};

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
    router: Arc<MessageRouter>,
    connection_by_gen_id: HashMap<(GenerationalNodeId, Swimlane), Connection>,
    /// This tracks the max generation we observed from connection attempts regardless of our nodes
    /// configuration. We cannot accept connections from nodes older than ones we have observed
    /// already.
    observed_generations: HashMap<PlainNodeId, GenStatus>,
}

impl ConnectionManagerInner {
    fn get_connection(
        &self,
        peer_node_id: GenerationalNodeId,
        swimlane: Swimlane,
    ) -> Option<Connection> {
        self.connection_by_gen_id
            .get(&(peer_node_id, swimlane))
            .cloned()
    }

    fn create_loopback_connection(
        &mut self,
        swimlane: Swimlane,
        connection_manager: ConnectionManager,
    ) -> Result<Connection, ConnectError> {
        let (tx, egress, shared) = EgressStream::create_loopback();
        let connection = Connection::new(
            my_node_id(),
            restate_types::net::CURRENT_PROTOCOL_VERSION,
            swimlane,
            tx,
        );

        let reactor = ConnectionReactor::new(connection.clone(), shared, None, self.router.clone());

        let previous = self.register(connection.clone());

        let task_id = match reactor.start(TaskKind::LocalReactor, connection_manager, false, egress)
        {
            Ok(task_id) => task_id,
            Err(e) => {
                self.deregister(&connection);
                // put back the previous connection
                if let Some(existing) = previous {
                    self.register(existing);
                }
                return Err(e.into());
            }
        };

        debug!(%swimlane, %task_id, "express path connection reactor started");

        Ok(connection)
    }

    fn can_discover(&self, node_id: GenerationalNodeId) -> Result<(), DiscoveryError> {
        if self
            .observed_generations
            .get(&node_id.as_plain())
            .map(|status| {
                node_id.generation() < status.generation
                    || (node_id.generation() == status.generation && status.gone)
            })
            .unwrap_or(false)
        {
            return Err(DiscoveryError::NodeIsGone(node_id.into()));
        }

        Ok(())
    }

    /// returns existing connection if there was any
    fn register(&mut self, connection: Connection) -> Option<Connection> {
        self.connection_by_gen_id
            .insert((connection.peer, connection.swimlane), connection)
    }

    fn deregister(&mut self, connection: &Connection) {
        let peer = connection.peer;
        let swimlane = connection.swimlane;
        trace!(%swimlane, "connection was unregistered {}", peer);
        match self.connection_by_gen_id.entry((peer, swimlane)) {
            hash_map::Entry::Occupied(c) if c.get() == connection => {
                c.remove();
            }
            _ => {}
        }
    }

    fn update_generation_or_preempt(
        &mut self,
        nodes_config: &NodesConfiguration,
        peer_node_id: GenerationalNodeId,
    ) -> Result<(), AcceptError> {
        let known_status = self
            .observed_generations
            .get(&peer_node_id.as_plain())
            .copied()
            .unwrap_or(GenStatus::new(peer_node_id.generation()));

        if known_status.generation > peer_node_id.generation() {
            // This peer is _older_ than the one we have seen in the past, we cannot accept
            // this connection. We terminate the stream immediately.
            return Err(AcceptError::OldPeerGeneration(format!(
                "newer generation '{}' has been observed",
                NodeId::new_generational(peer_node_id.id(), known_status.generation)
            )));
        }

        if known_status.generation == peer_node_id.generation() && known_status.gone {
            // This peer was observed to have shutdown before. We cannot accept new connections from this peer.
            return Err(AcceptError::PreviouslyShutdown);
        }

        if let Err(e) = nodes_config.find_node_by_id(peer_node_id) {
            match e {
                NodesConfigError::UnknownNodeId(_) => {}
                NodesConfigError::Deleted(_) => return Err(AcceptError::PreviouslyShutdown),
                NodesConfigError::GenerationMismatch { expected, found } => {
                    if found.is_newer_than(expected) {
                        return Err(AcceptError::OldPeerGeneration(format!(
                            "newer generation '{found}' has been observed"
                        )));
                    }
                }
            }
        }

        // todo: if we have connections with an older generation, we request to drop it.
        // However, more than one connection with the same generation is allowed.
        // if known_status.generation < connection.peer.generation() {
        // todo: Terminate old node's connections
        // }

        // update observed generation
        let new_status = GenStatus::new(peer_node_id.generation());
        self.observed_generations
            .entry(peer_node_id.as_plain())
            .and_modify(|status| {
                status.merge(new_status);
            })
            .or_insert(new_status);
        Ok(())
    }
}

impl Default for ConnectionManagerInner {
    fn default() -> Self {
        super::metric_definitions::describe_metrics();
        Self {
            router: Arc::new(MessageRouter::default()),
            connection_by_gen_id: HashMap::default(),
            observed_generations: HashMap::default(),
        }
    }
}

/// A future of an in-flight connection attempt.
type SharedConnectionAttempt =
    Shared<BoxFuture<'static, Result<(Connection, TaskId), ConnectError>>>;

#[derive(Clone, Default)]
pub struct ConnectionManager {
    inner: Arc<Mutex<ConnectionManagerInner>>,
    in_flight_connects: Arc<Mutex<HashMap<(Destination, Swimlane), SharedConnectionAttempt>>>,
}

impl ConnectionManager {
    /// Updates the message router. Note that this only impacts new connections.
    /// In general, this should be called once on application start after
    /// initializing all message handlers.
    pub fn set_message_router(&self, router: MessageRouter) {
        self.inner.lock().router = Arc::new(router);
    }

    /// Accept a new incoming connection stream and register a network reactor task for it.
    pub async fn accept_incoming_connection<S>(
        &self,
        mut incoming: S,
    ) -> Result<EgressStream, AcceptError>
    where
        S: Stream<Item = Message> + Unpin + Send + 'static,
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
        let (header, hello) = wait_for_hello(
            &mut incoming,
            Configuration::pinned().networking.handshake_timeout.into(),
        )
        .await?;

        // temporary until we allow connections to be established before we acquire a node id.
        let Some(my_node_id) = metadata.my_node_id_opt() else {
            return Err(AcceptError::NotReady);
        };

        let should_register = matches!(
            hello.direction(),
            ConnectionDirection::Unknown
                | ConnectionDirection::Bidirectional
                | ConnectionDirection::Reverse
        );

        // -- Lock is held
        let mut guard = self.inner.lock();

        let nodes_config = metadata.nodes_config_ref();

        // check cluster fingerprint if it's set on the hello message *and* our nodes config has it
        // set as well. Only validate if BOTH sides have a fingerprint.
        if let Ok(incoming_fingerprint) = ClusterFingerprint::try_from(hello.cluster_fingerprint)
            && let Some(expected_fingerprint) = nodes_config.cluster_fingerprint()
            && incoming_fingerprint != expected_fingerprint
        {
            return Err(HandshakeError::Failed("cluster fingerprint mismatch".to_owned()).into());
        }

        // NodeId **must** be generational at this layer, we may support accepting connections from
        // anonymous nodes in the future. When this happens, this restate-server release will not
        // be compatible with it.
        let peer_node_id = hello.my_node_id.ok_or_else(|| {
            HandshakeError::Failed("GenerationalNodeId is not set in the Hello message".to_owned())
        })?;

        // we don't allow node-id 0 in this version.
        if peer_node_id.id == 0 {
            return Err(HandshakeError::Failed("peer cannot have node Id of 0".to_owned()).into());
        }

        if peer_node_id.generation == 0 {
            return Err(
                HandshakeError::Failed("NodeId has invalid generation number".to_owned()).into(),
            );
        }

        // convert to our internal type
        let peer_node_id = GenerationalNodeId::from(peer_node_id);

        // Sanity check. Nodes must not connect to themselves from other generations.
        if my_node_id.is_same_but_different(&peer_node_id) {
            // My node ID but different generations!
            return Err(HandshakeError::Failed(
                "cannot accept a connection to the same NodeID from a different generation"
                    .to_owned(),
            )
            .into());
        }

        // Are we both from the same cluster?
        if hello.cluster_name != nodes_config.cluster_name() {
            return Err(HandshakeError::Failed("cluster name mismatch".to_owned()).into());
        }

        let selected_protocol_version = negotiate_protocol_version(&hello)?;
        debug!(
            "Negotiated protocol version {:?} with client",
            selected_protocol_version
        );

        let (sender, egress, shared) = EgressStream::create();

        guard.update_generation_or_preempt(&nodes_config, peer_node_id)?;

        let peer_metadata = PeerMetadataVersion::from(header);

        // Enqueue the welcome message
        let welcome = Welcome::new(my_node_id, selected_protocol_version, hello.direction());
        shared
            .unbounded_send(EgressMessage::Message(welcome.into()))
            .map_err(|_| HandshakeError::PeerDropped)?;
        let connection = Connection::new(
            peer_node_id,
            selected_protocol_version,
            hello.swimlane(),
            sender,
        );

        // Register the connection.
        let router = guard.router.clone();

        let existing_connection = if should_register {
            guard.register(connection.clone())
        } else {
            None
        };

        let reactor =
            ConnectionReactor::new(connection.clone(), shared, Some(peer_metadata), router);

        let task_id = match reactor.start(
            TaskKind::ConnectionReactor,
            self.clone(),
            !should_register,
            incoming,
        ) {
            Ok(task_id) => task_id,
            Err(e) => {
                guard.deregister(&connection);
                // put back the previous connection
                if let Some(existing) = existing_connection {
                    guard.register(existing);
                }
                return Err(e.into());
            }
        };

        debug!(
            direction = %hello.direction(),
            task_id = %task_id,
            peer = %peer_node_id,
            swimlane = %hello.swimlane(),
            "Incoming connection accepted from node {}", peer_node_id
        );

        counter!(NETWORK_CONNECTION_CREATED, "direction" => "incoming", "swimlane" => hello.swimlane().as_str_name()).increment(1);

        // Our output stream, i.e. responses.
        Ok(egress)
    }

    /// Create a fake connection simulating another node connecting to this server.
    ///
    /// The connection can be used to send and receive RPC messages to this server.
    /// The message router passed here will be used to route messages going from this server to the
    /// fake server on the other end of this connection.
    #[cfg(feature = "test-util")]
    pub async fn accept_fake_server_connection(
        &self,
        node_id: GenerationalNodeId,
        swimlane: Swimlane,
        direction: ConnectionDirection,
        message_router: Option<Arc<MessageRouter>>,
    ) -> Result<super::MockPeerConnection, ConnectError> {
        use crate::network::tracking::NoopTracker;
        use crate::network::{MockPeerConnection, PassthroughConnector};

        let transport = PassthroughConnector(self.clone());
        let (conn, task_id) = Connection::connect_inner_with_node_id(
            Some(node_id),
            Destination::Address(restate_types::net::address::AdvertisedAddress::new_uds(
                "/tmp/fake".into(),
            )),
            swimlane,
            transport,
            direction,
            TaskKind::RemoteConnectionReactor,
            message_router.unwrap_or_default(),
            NoopTracker,
            true,
        )
        .await?;

        Ok(MockPeerConnection {
            my_node_id: node_id,
            swimlane,
            reactor_task_id: task_id,
            conn,
        })
    }

    /// Gets an existing connection or creates a new one if no active connection exists. If
    /// multiple connections already exist, it returns a random one.
    pub async fn get_or_connect<C>(
        &self,
        node_id: impl Into<NodeId>,
        swimlane: Swimlane,
        transport_connector: &C,
    ) -> Result<Connection, ConnectError>
    where
        C: TransportConnect,
    {
        let my_node_id_opt = Metadata::with_current(|m| m.my_node_id_opt());
        let node_id = node_id.into();
        // find latest generation if this is not generational node id
        let node_id = match node_id.as_generational() {
            Some(id) => id,
            None => {
                find_node(
                    &Metadata::with_current(|metadata| metadata.nodes_config_ref()),
                    node_id,
                )?
                .current_generation
            }
        };

        // fail fast if we are connecting to our previous self
        if my_node_id_opt.is_some_and(|m| m.is_same_but_different(&node_id)) {
            return Err(DiscoveryError::NodeIsGone(node_id.into()).into());
        }

        let router = {
            // -- Lock held
            let mut guard = self.inner.lock();

            // find a connection by node_id
            if let Some(connection) = guard.get_connection(node_id, swimlane) {
                return Ok(connection);
            }

            if my_node_id_opt.is_some_and(|my_node| my_node == node_id) {
                return guard.create_loopback_connection(swimlane, self.clone());
            }

            // fail if the node is seen as gone before
            guard.can_discover(node_id)?;
            guard.router.clone()
        };

        // We have no connection. We attempt to create a new connection or latch onto an
        // existing attempt.
        self.create_shared_connection(
            Destination::Node(node_id),
            swimlane,
            router,
            transport_connector,
        )
        .await
    }

    async fn create_shared_connection<C>(
        &self,
        dest: Destination,
        swimlane: Swimlane,
        router: Arc<MessageRouter>,
        transport_connector: &C,
    ) -> Result<Connection, ConnectError>
    where
        C: TransportConnect,
    {
        // 1) Check if there's already a future in flight for this address
        let maybe_connect = {
            let mut in_flight = self.in_flight_connects.lock();

            // If yes, clone that shared future so multiple callers coalesce
            if let Some(existing) = in_flight.get(&(dest.clone(), swimlane)) {
                existing.clone()
            } else {
                // 2) Not in flight. Check the throttle window for recent failures
                ConnectThrottle::may_connect(&dest)?;

                // 3) If we pass the throttle check, create a brand-new future
                let fut = {
                    let dest = dest.clone();
                    let transport_connector = transport_connector.clone();
                    let conn_tracker = self.clone();
                    async move {
                        // Gossip swimlane is always bidirectional. If a node A connects to node B
                        // via the gossip swimlane, there is no need to establish a connection in
                        // the other direction. If this is proving to be problematic, it's okay to
                        // revert back to using Forward. Such a change will be backwards
                        // compatible.
                        let direction = if swimlane == Swimlane::Gossip {
                            ConnectionDirection::Bidirectional
                        } else {
                            ConnectionDirection::Forward
                        };
                        // Actually attempt/force to connect. We checked throttling before creating this
                        // future.
                        Connection::force_connect(
                            dest,
                            swimlane,
                            transport_connector,
                            direction,
                            TaskKind::ConnectionReactor,
                            router,
                            conn_tracker,
                            // connection-manager managed connections are not dedicated
                            false,
                        )
                        .await
                    }
                    .boxed()
                    .shared()
                };

                // Put it in the map so other concurrent callers share the same future
                in_flight.insert((dest.clone(), swimlane), fut.clone());
                fut
            }
        };

        // 4) Await the shared future (outside the lock)
        let maybe_connection = maybe_connect.await;

        // 5) Remove the completed future so subsequent calls can attempt a fresh connect
        let mut in_flight = self.in_flight_connects.lock();
        in_flight.remove(&(dest, swimlane));

        Ok(maybe_connection?.0)
    }
}

impl ConnectionTracking for ConnectionManager {
    fn connection_draining(&self, conn: &Connection) {
        trace!(
            swimlane = %conn.swimlane,
            "Connection started draining: {}", conn.peer);
        self.inner.lock().deregister(conn);
    }

    fn connection_created(&self, conn: &Connection, is_dedicated: bool) {
        if !is_dedicated {
            self.inner.lock().register(conn.clone());
        }
        trace!(
            swimlane = %conn.swimlane,
            "Connection reactor started: {}", conn.peer);
    }

    fn connection_dropped(&self, conn: &Connection) {
        debug!(
            swimlane = %conn.swimlane,
            "Connection terminated, connection lived for {:?}",
            conn.created.elapsed()
        );
        self.inner.lock().deregister(conn);
        counter!(NETWORK_CONNECTION_DROPPED).increment(1);
    }

    fn notify_peer_shutdown(&self, node_id: GenerationalNodeId) {
        let mut guard = self.inner.lock();
        // current status
        let is_already_gone = guard
            .observed_generations
            .get(&node_id.as_plain())
            .map(|status| {
                node_id.generation() < status.generation
                    || (node_id.generation() == status.generation && status.gone)
            })
            .unwrap_or(false);
        let mut new_status = GenStatus::new(node_id.generation());
        new_status.gone = true;
        guard
            .observed_generations
            .entry(node_id.as_plain())
            .and_modify(|status| {
                status.merge(new_status);
            })
            .or_insert(new_status);
        if !is_already_gone {
            info!(peer = %node_id, "Peer shutdown notification received");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream;
    use googletest::prelude::*;
    use test_log::test;
    use tokio::sync::mpsc;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;

    use restate_test_util::assert_eq;
    use restate_types::RestateVersion;
    use restate_types::Version;
    use restate_types::config::NetworkingOptions;
    use restate_types::net::address::AdvertisedAddress;
    use restate_types::net::metadata::GetMetadataRequest;
    use restate_types::net::metadata::MetadataKind;
    use restate_types::net::metadata::MetadataManagerService;
    use restate_types::net::node::GetNodeState;
    use restate_types::net::{
        CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION, ProtocolVersion,
    };
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};

    use restate_memory::MemoryPool;

    use crate::network::BackPressureMode;
    use crate::network::MessageRouterBuilder;
    use crate::network::ServiceMessage;
    use crate::network::Swimlane;
    use crate::network::protobuf::network::{Header, Hello};
    use crate::{self as restate_core, TestCoreEnv, TestCoreEnvBuilder};

    // Test handshake with a client
    #[restate_core::test]
    async fn test_hello_welcome_handshake() -> Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;
        let connections = ConnectionManager::default();

        let _mock_connection = connections
            .accept_fake_server_connection(
                GenerationalNodeId::new(1, 1),
                Swimlane::default(),
                ConnectionDirection::Forward,
                None,
            )
            .await?;

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
            Err(AcceptError::Handshake(HandshakeError::Timeout(_)))
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
            cluster_fingerprint: metadata
                .nodes_config_ref()
                .cluster_fingerprint()
                .map_or(0, |f| f.to_u64()),
            direction: ConnectionDirection::Bidirectional.into(),
            swimlane: Swimlane::default().into(),
        };
        let hello = Message::new(Header::default(), hello);
        tx.send(hello).await.expect("Channel accept hello message");

        let connections = ConnectionManager::default();
        let incoming = ReceiverStream::new(rx);
        let resp = connections.accept_incoming_connection(incoming).await;
        assert!(resp.is_err());
        assert!(matches!(
            resp,
            Err(AcceptError::Handshake(
                HandshakeError::UnsupportedVersion(proto_version)
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
            cluster_fingerprint: metadata
                .nodes_config_ref()
                .cluster_fingerprint()
                .map_or(0, |f| f.to_u64()),
            direction: ConnectionDirection::Bidirectional.into(),
            swimlane: Swimlane::default().into(),
        };
        let hello = Message::new(Header::default(), hello);
        tx.send(hello).await?;

        let connections = ConnectionManager::default();
        let incoming = ReceiverStream::new(rx);
        let err = connections
            .accept_incoming_connection(incoming)
            .await
            .err()
            .unwrap();
        assert_that!(
            err,
            pat!(AcceptError::Handshake(pat!(HandshakeError::Failed(eq(
                "cluster name mismatch"
            )))))
        );

        // cluster fingerprint mismatch
        let (tx, rx) = mpsc::channel(1);
        let my_node_id = metadata.my_node_id();
        let hello = Hello {
            min_protocol_version: MIN_SUPPORTED_PROTOCOL_VERSION.into(),
            max_protocol_version: CURRENT_PROTOCOL_VERSION.into(),
            my_node_id: Some(my_node_id.into()),
            cluster_name: "Random-cluster".to_owned(),
            cluster_fingerprint: 42,
            direction: ConnectionDirection::Bidirectional.into(),
            swimlane: Swimlane::default().into(),
        };
        let hello = Message::new(Header::default(), hello);
        tx.send(hello).await?;

        let connections = ConnectionManager::default();
        let incoming = ReceiverStream::new(rx);
        let err = connections
            .accept_incoming_connection(incoming)
            .await
            .err()
            .unwrap();
        assert_that!(
            err,
            pat!(AcceptError::Handshake(pat!(HandshakeError::Failed(eq(
                "cluster fingerprint mismatch"
            )))))
        );
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
            Some(my_node_id),
            metadata.nodes_config_ref().cluster_name().to_owned(),
            metadata.nodes_config_ref().cluster_fingerprint(),
            ConnectionDirection::Bidirectional,
            Swimlane::default(),
        );
        let hello = Message::new(Header::default(), hello);
        tx.send(hello).await.expect("Channel accept hello message");

        let connections = ConnectionManager::default();

        let incoming = ReceiverStream::new(rx);
        let err = connections
            .accept_incoming_connection(incoming)
            .await
            .err()
            .unwrap();

        assert_that!(
            err,
            pat!(AcceptError::Handshake(pat!(HandshakeError::Failed(eq(
                "cannot accept a connection to the same NodeID from a different generation",
            )))))
        );
        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn fetching_metadata_updates_through_message_headers() -> Result<()> {
        let mut nodes_config = NodesConfiguration::new_for_testing();

        let node_id = GenerationalNodeId::new(42, 42);
        let node_config = NodeConfig::builder()
            .name("42".to_owned())
            .current_generation(node_id)
            .address(AdvertisedAddress::default())
            .roles(Role::Worker.into())
            .binary_version(RestateVersion::current())
            .build();
        nodes_config.upsert_node(node_config);

        let test_env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_nodes_config(nodes_config)
            .build()
            .await;

        let metadata = Metadata::current();

        let mut incoming_router = MessageRouterBuilder::default();
        // Original: buffer_size=10, BackPressureMode::Lossy (from metadata manager)
        let pool = MemoryPool::new(10 * 1024); // 10KB
        let metadata_manager_rx = incoming_router
            .register_service::<MetadataManagerService>(pool, BackPressureMode::Lossy);

        let mut metadata_manager_rx = metadata_manager_rx.start();

        let connection = test_env
            .networking
            .connection_manager()
            .accept_fake_server_connection(
                node_id,
                Swimlane::default(),
                ConnectionDirection::Forward,
                Some(Arc::new(incoming_router.build())),
            )
            .await?;

        let request = GetNodeState::default();
        let partition_table_version = metadata.partition_table_version().next();
        let header = Header {
            my_nodes_config_version: metadata.nodes_config_version().into(),
            my_partition_table_version: partition_table_version.into(),
            ..Default::default()
        };

        let permit = connection.conn.reserve().await.unwrap();

        let rx = permit.send_rpc_with_header(request, None, header).unwrap();
        let _result = rx.await;

        // we expect the request to go through he existing open connection to my node
        let message = metadata_manager_rx.next().await.expect("some message");
        assert_get_metadata_request(
            message,
            MetadataKind::PartitionTable,
            partition_table_version,
        );

        Ok(())
    }

    fn assert_get_metadata_request(
        message: ServiceMessage<MetadataManagerService>,
        metadata_kind: MetadataKind,
        version: Version,
    ) {
        let ServiceMessage::Rpc(message) = message else {
            panic!("Expected a RPC message");
        };

        let message = message.into_typed::<GetMetadataRequest>().into_body();

        assert_that!(
            message,
            pat!(GetMetadataRequest {
                metadata_kind: eq(metadata_kind),
                min_version: eq(Some(version))
            })
        );
    }
}
