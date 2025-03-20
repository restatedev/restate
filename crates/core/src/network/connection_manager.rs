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

use ahash::HashMap;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, Stream};
use parking_lot::Mutex;
use tracing::{debug, info, instrument, trace, warn};

use restate_types::config::Configuration;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{NodesConfigError, NodesConfiguration};
use restate_types::{GenerationalNodeId, Merge, NodeId, PlainNodeId};

use super::connection::Connection;
use super::io::{
    ConnectionReactor, DropEgressStream, EgressMessage, EgressStream, UnboundedEgressSender,
};
use super::metric_definitions::{self, CONNECTION_DROPPED, INCOMING_CONNECTION};
use super::protobuf::network::ConnectionDirection;
use super::protobuf::network::{Header, Message, Welcome};
use super::tracking::{ConnectionTracking, PeerRouting};
use super::transport_connector::{TransportConnect, find_node};
use super::{
    AcceptError, ConnectError, Destination, DiscoveryError, HandshakeError, MessageRouter,
};
use crate::metadata::Urgency;
use crate::network::connection::ConnectThrottle;
use crate::network::handshake::{negotiate_protocol_version, wait_for_hello};
use crate::{Metadata, ShutdownError, TaskId, TaskKind, my_node_id};

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
    connection_by_gen_id: HashMap<GenerationalNodeId, Vec<Connection>>,
    /// This tracks the max generation we observed from connection attempts regardless of our nodes
    /// configuration. We cannot accept connections from nodes older than ones we have observed
    /// already.
    observed_generations: HashMap<PlainNodeId, GenStatus>,
}

impl ConnectionManagerInner {
    fn get_random_connection(
        &self,
        peer_node_id: &GenerationalNodeId,
        target_concurrency: usize,
    ) -> Option<Connection> {
        use rand::prelude::IndexedRandom;
        self.connection_by_gen_id
            .get(peer_node_id)
            .and_then(|connections| {
                // Suggest we create new connection if the number
                // of connections is below the target
                if connections.len() >= target_concurrency {
                    connections.choose(&mut rand::rng()).cloned()
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
    in_flight_connects: Arc<Mutex<HashMap<Destination, SharedConnectionAttempt>>>,
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
        let peer_node_id = hello.my_node_id.ok_or(HandshakeError::Failed(
            "GenerationalNodeId is not set in the Hello message".to_owned(),
        ))?;

        // we don't allow node-id 0 in this version.
        if peer_node_id.id == 0 {
            return Err(HandshakeError::Failed("Peer cannot have node Id of 0".to_owned()).into());
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

        self.verify_node_id(peer_node_id, &header, &nodes_config, &metadata)?;

        let (sender, unbounded_sender, egress, drop_egress) = EgressStream::create(
            Configuration::pinned()
                .networking
                .outbound_queue_length
                .get(),
        );

        self.update_generation_or_preempt(peer_node_id)?;

        // Enqueue the welcome message
        let welcome = Welcome::new(my_node_id, selected_protocol_version, hello.direction());
        unbounded_sender
            .unbounded_send(EgressMessage::Message(
                Header::default(),
                welcome.into(),
                None,
            ))
            .map_err(|_| HandshakeError::PeerDropped)?;
        let connection = Connection::new(peer_node_id, selected_protocol_version, sender);

        let should_register = matches!(
            hello.direction(),
            ConnectionDirection::Unknown
                | ConnectionDirection::Bidirectional
                | ConnectionDirection::Reverse
        );

        // Register the connection.
        let task_id = self.start_connection_reactor(
            TaskKind::ConnectionReactor,
            connection,
            unbounded_sender,
            incoming,
            drop_egress,
            should_register,
        )?;

        info!(
            direction_at_peer = %hello.direction(),
            task_id = %task_id,
            peer = %peer_node_id,
            "Incoming connection accepted from node {}", peer_node_id
        );

        INCOMING_CONNECTION.increment(1);

        // Our output stream, i.e. responses.
        Ok(egress)
    }

    /// Gets an existing connection or creates a new one if no active connection exists. If
    /// multiple connections already exist, it returns a random one.
    pub async fn get_or_connect<C>(
        &self,
        node_id: impl Into<NodeId>,
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

        // find a connection by node_id
        if let Some(connection) = self.inner.lock().get_random_connection(
            &node_id,
            Configuration::pinned()
                .networking
                .num_concurrent_connections(),
        ) {
            return Ok(connection);
        }

        if my_node_id_opt.is_some_and(|my_node| my_node == node_id) {
            return self.connect_loopback();
        }

        if self
            .inner
            .lock()
            .observed_generations
            .get(&node_id.as_plain())
            .map(|status| {
                node_id.generation() < status.generation
                    || (node_id.generation() == status.generation && status.gone)
            })
            .unwrap_or(false)
        {
            return Err(DiscoveryError::NodeIsGone(node_id.into()).into());
        }

        // We have no connection. We attempt to create a new connection or latch onto an
        // existing attempt.
        self.create_forward_shared_connection(Destination::Node(node_id), transport_connector)
            .await
    }

    async fn create_forward_shared_connection<C>(
        &self,
        dest: Destination,
        transport_connector: &C,
    ) -> Result<Connection, ConnectError>
    where
        C: TransportConnect,
    {
        // 1) Check if there's already a future in flight for this address
        let maybe_connect = {
            let mut in_flight = self.in_flight_connects.lock();

            // If yes, clone that shared future so multiple callers coalesce
            if let Some(existing) = in_flight.get(&dest) {
                existing.clone()
            } else {
                // 2) Not in flight. Check the throttle window for recent failures
                ConnectThrottle::may_connect(&dest)?;

                // 3) If we pass the throttle check, create a brand-new future
                let fut = {
                    let dest = dest.clone();
                    let transport_connector = transport_connector.clone();
                    let router = self.inner.lock().router.clone();
                    let conn_tracker = self.clone();
                    let peer_router = self.clone();
                    async move {
                        // Actually attempt/force to connect. We checked throttling before creating this
                        // future.
                        Connection::force_connect(
                            dest,
                            transport_connector,
                            ConnectionDirection::Forward,
                            TaskKind::ConnectionReactor,
                            router,
                            conn_tracker,
                            peer_router,
                        )
                        .await
                    }
                    .boxed()
                    .shared()
                };

                // Put it in the map so other concurrent callers share the same future
                in_flight.insert(dest.clone(), fut.clone());
                fut
            }
        };

        // 4) Await the shared future (outside the lock)
        let maybe_connection = maybe_connect.await;

        // 5) Remove the completed future so subsequent calls can attempt a fresh connect
        let mut in_flight = self.in_flight_connects.lock();
        in_flight.remove(&dest);

        Ok(maybe_connection?.0)
    }

    #[instrument(skip_all)]
    fn connect_loopback(&self) -> Result<Connection, ConnectError> {
        debug!("Creating an express path connection to self");
        let (tx, unbounded_sender, egress, drop_egress) = EgressStream::create(
            Configuration::pinned()
                .networking
                .outbound_queue_length
                .get(),
        );
        let connection = Connection::new(
            my_node_id(),
            restate_types::net::CURRENT_PROTOCOL_VERSION,
            tx,
        );

        self.start_connection_reactor(
            TaskKind::LocalReactor,
            connection.clone(),
            unbounded_sender,
            egress,
            drop_egress,
            // loopback is always registered
            true,
        )?;

        Ok(connection)
    }

    fn verify_node_id(
        &self,
        peer_node_id: GenerationalNodeId,
        header: &Header,
        nodes_config: &NodesConfiguration,
        metadata: &Metadata,
    ) -> Result<(), NodesConfigError> {
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

            return Err(e);
        }

        Ok(())
    }

    fn update_generation_or_preempt(
        &self,
        peer_node_id: GenerationalNodeId,
    ) -> Result<(), AcceptError> {
        // Lock is held, don't perform expensive or async operations here.
        let mut guard = self.inner.lock();
        let known_status = guard
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

        // todo: if we have connections with an older generation, we request to drop it.
        // However, more than one connection with the same generation is allowed.
        // if known_status.generation < connection.peer.generation() {
        // todo: Terminate old node's connections
        // }

        // update observed generation
        let new_status = GenStatus::new(peer_node_id.generation());
        guard
            .observed_generations
            .entry(peer_node_id.as_plain())
            .and_modify(|status| {
                status.merge(new_status);
            })
            .or_insert(new_status);
        Ok(())
    }

    fn start_connection_reactor<S>(
        &self,
        task_kind: TaskKind,
        connection: Connection,
        unbounded_sender: UnboundedEgressSender,
        incoming: S,
        drop_egress: DropEgressStream,
        should_register: bool,
    ) -> Result<TaskId, ShutdownError>
    where
        S: Stream<Item = Message> + Unpin + Send + 'static,
    {
        let router = self.inner.lock().router.clone();

        let reactor = ConnectionReactor::new(connection, unbounded_sender, drop_egress);

        let task_id = reactor.start(
            task_kind,
            router,
            self.clone(),
            self.clone(),
            incoming,
            should_register,
        )?;

        Ok(task_id)
    }
}

impl PeerRouting for ConnectionManager {
    fn register(&self, connection: &Connection) {
        let peer = connection.peer;
        let mut guard = self.inner.lock();
        guard
            .connection_by_gen_id
            .entry(connection.peer)
            .or_default()
            .push(connection.clone());
        trace!("connection to {} was registered", peer);
    }

    fn deregister(&self, connection: &Connection) {
        let mut guard = self.inner.lock();
        let peer = connection.peer;
        trace!("connection reactor was deregistered {}", peer);
        if let Some(connections) = guard.connection_by_gen_id.get_mut(&peer) {
            let len_before = connections.len();
            connections.retain(|c| c != connection && !c.is_closed());
            if len_before > 0 && connections.is_empty() {
                info!(%peer, "All registered connections to this node were terminated");
            }
        }
    }
}

impl ConnectionTracking for ConnectionManager {
    fn connection_created(&self, conn: &Connection) {
        trace!("Connection reactor started: {}", conn.peer);
    }

    fn connection_dropped(&self, conn: &Connection) {
        info!(
            "Connection terminated, connection lived for {:?}",
            conn.created.elapsed()
        );
        CONNECTION_DROPPED.increment(1);
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
    use tokio_stream::StreamExt;

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
            direction: ConnectionDirection::Bidirectional.into(),
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
            direction: ConnectionDirection::Bidirectional.into(),
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
            ConnectionDirection::Bidirectional,
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

        // Unrecognized node Id
        let (tx, rx) = mpsc::channel(1);
        let my_node_id = GenerationalNodeId::new(55, 2);

        let hello = Hello::new(
            Some(my_node_id),
            metadata.nodes_config_ref().cluster_name().to_owned(),
            ConnectionDirection::Bidirectional,
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
            pat!(AcceptError::NodesConfig(pat!(
                NodesConfigError::UnknownNodeId(_)
            )))
        );
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
