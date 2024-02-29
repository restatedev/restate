// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{hash_map, HashMap};
use std::sync::{Arc, Mutex, Weak};

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, info, Instrument, Span};

use restate_core::metadata;
use restate_core::{cancellation_watcher, current_task_id, task_center, TaskId, TaskKind};
use restate_node_protocol::node::message::{self, ConnectionControl};
use restate_node_protocol::node::{Header, Hello, Message, Welcome};
use restate_node_protocol::MessageEnvelope;
use restate_node_services::node_svc::node_svc_client::NodeSvcClient;
use restate_types::nodes_config::AdvertisedAddress;
use restate_types::{GenerationalNodeId, NodeId, PlainNodeId};

use super::codec::deserialize_message;
use super::connection::{Connection, ConnectionSender};
use super::handshake::{negotiate_protocol_version, wait_for_hello, wait_for_welcome};
use super::message_router::MessageRouter;
use crate::error::{NetworkError, ProtocolError};
use crate::utils::create_grpc_channel_from_network_address;

#[derive(Default)]
struct ConnectionManagerInner {
    connections: HashMap<TaskId, Weak<Connection>>,
    connection_by_gen_id: HashMap<GenerationalNodeId, Vec<Weak<Connection>>>,
    observed_generations: HashMap<PlainNodeId, u32>,
    channel_cache: HashMap<AdvertisedAddress, Channel>,
}

impl ConnectionManagerInner {
    fn drop_connection(&mut self, task_id: TaskId) {
        self.connections.remove(&task_id);
    }

    fn cleanup_stale_connections(&mut self, peer_node_id: &GenerationalNodeId) {
        if let Some(connections) = self.connection_by_gen_id.get_mut(peer_node_id) {
            connections.retain(|c| c.upgrade().is_some());
        }
    }

    fn get_random_connection(&self, peer_node_id: &GenerationalNodeId) -> Option<Arc<Connection>> {
        self.connection_by_gen_id
            .get(peer_node_id)
            .and_then(|connections| connections.choose(&mut rand::thread_rng())?.upgrade())
    }
}

#[derive(Clone, Default)]
pub struct ConnectionManager {
    inner: Arc<Mutex<ConnectionManagerInner>>,
    router: Arc<MessageRouter>,
}

impl ConnectionManager {
    pub fn router(&self) -> &MessageRouter {
        &self.router
    }

    pub async fn accept_incoming_connection<S>(
        &self,
        mut incoming: S,
    ) -> Result<BoxStream<'static, Result<Message, tonic::Status>>, NetworkError>
    where
        S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send + 'static,
    {
        // perform the handshake inline. This allows us to naturally push-back on stream creation
        // by piggybacking on the concurrency limit of the server + the handshake timeout.
        //
        // How do we handshake a new connection?
        // ---------------------------------
        //
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

        info!("Accepting incoming connection");
        let (header, hello) = wait_for_hello(&mut incoming).await?;
        // NodeId **must** be generational
        let peer_node_id = hello
            .my_node_id
            .clone()
            .ok_or(ProtocolError::HandshakeFailed(
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
        let my_node_id = metadata().my_node_id();
        if my_node_id.as_plain() == peer_node_id.as_plain() && peer_node_id != my_node_id {
            // Same node ID but different generations!
            return Err(ProtocolError::HandshakeFailed(
                "cannot accept a connection to the same NodeID from a different generation",
            )
            .into());
        }

        let metadata = metadata();

        // Are we both on the correct cluster?
        if hello.cluster_name != metadata.nodes_config().cluster_name() {
            return Err(ProtocolError::HandshakeFailed("Cluster name mismatch").into());
        }

        let selected_protocol_version = negotiate_protocol_version(&hello)?;
        debug!(
            "Negotiated protocol version {:?} with client",
            selected_protocol_version
        );

        // TODO: Validate that the node Id recognized in nodes configuration if the peer configuration
        // version is at or lower than our configuration.

        // If node Id is unrecognized and peer is at higher nodes configuration version, issue a
        // sync to the higher version and wait for version sync.
        let peer_is_in_the_future = header
            .my_nodes_config_version
            .is_some_and(|v| v.value > metadata.nodes_config_version().into());

        match metadata.nodes_config().find_node_by_id(peer_node_id) {
            Ok(_) => {}
            Err(e) if peer_is_in_the_future => {
                // todo: notify metadata about higher version.
                // and wait for the new version to sync up, then retry the lookup.
                // let _ = self
                //     .metadata
                //     .wait_for_version(
                //         MetadataKind::NodesConfiguration,
                //         header.my_nodes_config_version.unwrap().into(),
                //     )
                //     .await?;
                return Err(NetworkError::UnknownNode(e));
            }
            Err(e) => {
                return Err(NetworkError::UnknownNode(e));
            }
        };

        let (tx, rx) = mpsc::channel(1);
        // Enqueue the welcome message
        let welcome = Welcome::new(metadata.my_node_id(), selected_protocol_version);

        let node_config_version = metadata.nodes_config_version();
        let welcome = Message::new(Header::new(node_config_version), welcome);

        tx.try_send(welcome)
            .expect("channel accept Welcome message");

        let connection = Connection::new(peer_node_id, selected_protocol_version, tx);
        // Register the connection.
        let _ = self.start_connection_reactor(connection, incoming)?;
        // For uniformity with outbound connections, we map all responses to Ok, we never rely on
        // sending tonic::Status errors explicitly. We use ConnectionControl frames to communicate
        // errors and/or drop the stream when necessary.
        let transformed = ReceiverStream::new(rx).map(Ok);

        Ok(Box::pin(transformed))
    }

    pub async fn enforced_new_node_sender(
        &self,
        node_id: GenerationalNodeId,
    ) -> Result<ConnectionSender, NetworkError> {
        let connection = self.connect(node_id).await?;
        Ok(connection.sender())
    }

    pub async fn get_node_sender(
        &self,
        node_id: GenerationalNodeId,
    ) -> Result<ConnectionSender, NetworkError> {
        // find a connection by node_id
        let maybe_connection: Option<Arc<Connection>> = {
            let guard = self.inner.lock().unwrap();
            guard.get_random_connection(&node_id)
            // lock is dropped.
        };

        if let Some(connection) = maybe_connection {
            return Ok(connection.sender());
        }
        // We have no connection, or the connection we picked is stale. We attempt to create a
        // new connection anyway.
        let connection = self.connect(node_id).await?;
        Ok(connection.sender())
    }

    async fn connect(&self, node_id: GenerationalNodeId) -> Result<Arc<Connection>, NetworkError> {
        let address = metadata()
            .nodes_config()
            .find_node_by_id(node_id)?
            .address
            .clone();

        info!("Attempting to connect to node {} at {}", node_id, address);
        // Do we have a channel in cache for this address?
        let channel = {
            let mut guard = self.inner.lock().unwrap();
            if let hash_map::Entry::Vacant(entry) = guard.channel_cache.entry(address.clone()) {
                let channel = create_grpc_channel_from_network_address(address.clone())
                    .map_err(|e| NetworkError::BadNodeAddress(node_id.into(), e))?;
                entry.insert(channel.clone());
                channel
            } else {
                guard.channel_cache.get(&address).unwrap().clone()
            }
        };

        self.connect_with_channel(node_id, channel).await
    }

    // Left here for future use. This allows the node to connect to itself and bypass the
    // networking stack.
    #[cfg(test)]
    fn _connect_loopback(
        &self,
        node_id: GenerationalNodeId,
    ) -> Result<Arc<Connection>, NetworkError> {
        let (tx, rx) = mpsc::channel(1);
        let connection = Connection::new(
            node_id,
            restate_node_protocol::common::CURRENT_PROTOCOL_VERSION,
            tx,
        );

        let transformed = ReceiverStream::new(rx).map(Ok);
        let incoming = Box::pin(transformed);
        self.start_connection_reactor(connection, incoming)
    }

    async fn connect_with_channel(
        &self,
        node_id: GenerationalNodeId,
        channel: Channel,
    ) -> Result<Arc<Connection>, NetworkError> {
        let mut client = NodeSvcClient::new(channel);
        let nodes_config = metadata().nodes_config();
        let cluster_name = nodes_config.cluster_name();

        let (tx, rx) = mpsc::channel(1);
        let hello = Hello::new(metadata().my_node_id(), cluster_name.to_owned());

        // perform handshake.
        let hello = Message::new(Header::new(nodes_config.version()), hello);

        // Prime the channel with the hello message before connecting.
        tx.send(hello).await.expect("Channel accept hello message");

        let incoming = client
            .create_connection(ReceiverStream::new(rx))
            .await?
            .into_inner();

        let mut transformed = incoming.map(|x| x.map_err(ProtocolError::from));
        // finish the handshake
        let (_header, welcome) = wait_for_welcome(&mut transformed).await?;
        let protocol_version = welcome.protocol_version();

        if !protocol_version.is_supported() {
            return Err(ProtocolError::UnsupportedVersion(protocol_version.into()).into());
        }

        // sanity check, we should expect the node to have the same node configuration id
        let peer_node_id: NodeId = welcome
            .my_node_id
            .ok_or(ProtocolError::HandshakeFailed(
                "Peer must set my_node_id in Welcome message",
            ))?
            .into();

        if peer_node_id != node_id {
            // Node claims that it's someone else!
            return Err(ProtocolError::HandshakeFailed(
                "Node returned an unexpected NodeId in Welcome message.",
            )
            .into());
        }

        let connection = Connection::new(
            peer_node_id.as_generational().unwrap(),
            protocol_version,
            tx,
        );

        self.start_connection_reactor(connection, transformed)
    }

    fn start_connection_reactor<S>(
        &self,
        connection: Connection,
        incoming: S,
    ) -> Result<Arc<Connection>, NetworkError>
    where
        S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send + 'static,
    {
        // If we have a connection with an older generation, we request to drop it.
        // However, more than one connection with the same generation is allowed.
        let mut _cleanup = false;
        let mut guard = self.inner.lock().unwrap();
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
            // todo: Terminate old node's connection. but continue.
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

        let task_id = task_center().spawn_child(
            TaskKind::ConnectionReactor,
            "network-connection-reactor",
            None,
            run_reactor(
                self.inner.clone(),
                connection.clone(),
                self.router.clone(),
                incoming,
            )
            .instrument(span),
        )?;

        guard.connections.insert(task_id, connection_weak.clone());
        // clean up old connections
        guard.cleanup_stale_connections(&peer_node_id);
        // Add this connection.
        guard
            .connection_by_gen_id
            .entry(peer_node_id)
            .or_default()
            .push(connection_weak);
        info!("Incoming connection accepted from node {}", peer_node_id);
        Ok(connection)
    }
}

async fn run_reactor<S>(
    connection_manager: Arc<Mutex<ConnectionManagerInner>>,
    connection: Arc<Connection>,
    router: Arc<MessageRouter>,
    mut incoming: S,
) -> anyhow::Result<()>
where
    S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send,
{
    Span::current().record(
        "task_id",
        tracing::field::display(current_task_id().unwrap()),
    );
    // Receive loop
    loop {
        // read a message from the stream
        let msg = tokio::select! {
            biased;
            _ = cancellation_watcher() => {
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

        // header is optional on non-hello messages.
        if let Some(_header) = msg.header {
            // todo: if header contains newer config or metadata versions, notify metadata().
        };

        // body are not allowed to be empty.
        let Some(body) = msg.body else {
            connection
                .send_control_frame(ConnectionControl::codec_error("Body is missing on message"));
            break;
        };

        // Welcome and hello are not allowed
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

        match deserialize_message(body, connection.protocol_version) {
            Ok(msg) => {
                // route message based on type.
                let envelope = MessageEnvelope::new(connection.peer, msg);
                router.route_message(envelope).await;
            }
            Err(status) => {
                // terminate the stream
                info!("Error processing message, reporting error to peer: {status}");
                connection.send_control_frame(ConnectionControl::codec_error(status.to_string()));
                break;
            }
        }
    }

    // remove from active set
    on_connection_draining(connection.cid, connection.peer, &connection_manager);
    let protocol_version = connection.protocol_version;
    let peer_node_id = connection.peer;
    let connection_created_at = connection.created;
    // dropping the connection since it's the owner of sender stream.
    drop(connection);

    let drain_start = std::time::Instant::now();
    info!("Draining connection");
    let mut drain_counter = 0;
    // Draining of incoming queue
    while let Some(Ok(msg)) = incoming.next().await {
        if let Some(body) = msg.body {
            // we ignore non-deserializable messages (serde errors, or control signals in drain)
            if let Ok(msg) = deserialize_message(body, protocol_version) {
                drain_counter += 1;
                // route message based on type.
                let envelope = MessageEnvelope::new(peer_node_id, msg);
                router.route_message(envelope).await;
            }
        }
    }

    // We should also terminate response stream. This happens automatically when
    // the sender is dropped
    on_connection_terminated(&connection_manager);
    info!(
        "Connection terminated, drained {} messages in {:?}, total connection age is {:?}",
        drain_counter,
        drain_start.elapsed(),
        connection_created_at.elapsed()
    );
    Ok(())
}

fn on_connection_draining(
    cid: u64,
    peer_node_id: GenerationalNodeId,
    inner_manager: &Mutex<ConnectionManagerInner>,
) {
    let mut guard = inner_manager.lock().unwrap();
    if let Some(connections) = guard.connection_by_gen_id.get_mut(&peer_node_id) {
        // Remove this connection from connections map to reduce the chance
        // of picking it up as connection.
        connections.retain(|c| {
            c.upgrade()
                .map(|connection| connection.cid != cid)
                .unwrap_or_default()
        })
    }
}

fn on_connection_terminated(inner_manager: &Mutex<ConnectionManagerInner>) {
    let task_id = current_task_id().expect("TaskId is not set");
    let mut guard = inner_manager.lock().unwrap();
    guard.drop_connection(task_id);
}

#[cfg(test)]
mod tests {
    use crate::v2::handshake::HANDSHAKE_TIMEOUT;

    use super::*;

    use googletest::prelude::*;

    use restate_core::TestCoreEnv;
    use restate_node_protocol::node::message;
    use restate_node_protocol::{
        common::ProtocolVersion, CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION,
    };
    use restate_test_util::assert_eq;
    use restate_types::nodes_config::NodesConfigError;

    // Test handshake with a client
    #[tokio::test]
    async fn test_hello_welcome_handshake() -> Result<()> {
        let test_setup = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        test_setup
            .tc
            .run_in_scope("test", None, async {
                let metadata = restate_core::metadata();
                let (tx, rx) = mpsc::channel(1);
                let connections = ConnectionManager::default();

                let hello = Hello::new(
                    metadata.my_node_id(),
                    metadata.nodes_config().cluster_name().to_owned(),
                );
                let hello = Message::new(Header::new(metadata.nodes_config_version()), hello);
                tx.send(Ok(hello))
                    .await
                    .expect("Channel accept hello message");

                let incoming = ReceiverStream::new(rx);
                let mut output_stream = connections
                    .accept_incoming_connection(incoming)
                    .await
                    .expect("handshake");
                let msg = output_stream
                    .next()
                    .await
                    .expect("welcome message")
                    .expect("ok");
                let welcome = match msg.body {
                    Some(message::Body::Welcome(welcome)) => welcome,
                    _ => panic!("unexpected message"),
                };
                assert_eq!(welcome.my_node_id, Some(metadata.my_node_id().into()));
                Ok(())
            })
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn test_hello_welcome_timeout() -> Result<()> {
        let test_setup = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        test_setup
            .tc
            .run_in_scope("test", None, async {
                let (_tx, rx) = mpsc::channel(1);
                let connections = ConnectionManager::default();

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
                assert!(start.elapsed() >= HANDSHAKE_TIMEOUT);
                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn test_bad_handshake() -> Result<()> {
        let test_setup = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        test_setup
            .tc
            .run_in_scope("test", None, async {
                let metadata = restate_core::metadata();

                let (tx, rx) = mpsc::channel(1);
                let my_node_id = metadata.my_node_id();

                // unsupported protocol version
                let hello = Hello {
                    min_protocol_version: ProtocolVersion::Unknown.into(),
                    max_protocol_version: ProtocolVersion::Unknown.into(),
                    my_node_id: Some(my_node_id.into()),
                    cluster_name: metadata.nodes_config().cluster_name().to_owned(),
                };
                let hello = Message::new(Header::new(metadata.nodes_config_version()), hello);
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
                let hello = Message::new(Header::new(metadata.nodes_config_version()), hello);
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
                    NetworkError::ProtocolError(ProtocolError::HandshakeFailed(
                        "Cluster name mismatch"
                    ))
                ));
                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn test_node_generation() -> Result<()> {
        let test_setup = TestCoreEnv::create_with_mock_nodes_config(1, 2).await;
        test_setup
            .tc
            .run_in_scope("test", None, async {
                let metadata = restate_core::metadata();

                let (tx, rx) = mpsc::channel(1);
                let mut my_node_id = metadata.my_node_id();
                assert_eq!(2, my_node_id.generation());
                my_node_id.bump_generation();

                // newer generation
                let hello = Hello::new(
                    my_node_id,
                    metadata.nodes_config().cluster_name().to_owned(),
                );
                let hello = Message::new(Header::new(metadata.nodes_config_version()), hello);
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
                println!("{:?}", err);

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
                    metadata.nodes_config().cluster_name().to_owned(),
                );
                let hello = Message::new(Header::new(metadata.nodes_config_version()), hello);
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
            })
            .await
    }
}
