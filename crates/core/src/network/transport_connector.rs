// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use futures::Stream;

use restate_types::NodeId;
use restate_types::nodes_config::{NodeConfig, NodesConfigError, NodesConfiguration};

use super::protobuf::network::Message;
use super::{ConnectError, Destination, DiscoveryError, Swimlane};

/// Finds a node in nodes configuration by ID and maps the error to [`DiscoveryError`]
pub fn find_node(
    nodes_config: &NodesConfiguration,
    node_id: impl Into<NodeId>,
) -> Result<&NodeConfig, DiscoveryError> {
    match nodes_config.find_node_by_id(node_id) {
        Ok(node) => Ok(node),
        Err(NodesConfigError::Deleted(id)) => Err(DiscoveryError::NodeIsGone(id)),
        Err(NodesConfigError::UnknownNodeId(id)) => Err(DiscoveryError::UnknownNodeId(id)),
        Err(NodesConfigError::GenerationMismatch { expected, found }) => {
            if found.is_newer_than(expected) {
                Err(DiscoveryError::NodeIsGone(expected))
            } else {
                Err(DiscoveryError::UnknownNodeId(expected))
            }
        }
    }
}

pub trait TransportConnect: Clone + Send + Sync + 'static {
    fn connect(
        &self,
        destination: &Destination,
        swimlane: Swimlane,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError>,
    > + Send;
}

impl<T: TransportConnect> TransportConnect for std::sync::Arc<T> {
    fn connect(
        &self,
        destination: &Destination,
        swimlane: Swimlane,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError>,
    > + Send {
        (**self).connect(destination, swimlane, output_stream)
    }
}

#[cfg(feature = "test-util")]
pub mod test_util {
    use super::*;

    use std::sync::Arc;

    use ahash::HashMap;
    use futures::Stream;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc;
    use tracing::info;

    use restate_types::GenerationalNodeId;
    use restate_types::config::Configuration;

    use restate_memory::MemoryPool;

    use crate::network::MessageRouterBuilder;
    use crate::network::handshake::negotiate_protocol_version;
    use crate::network::io::{ConnectionReactor, EgressMessage, EgressStream};
    use crate::network::protobuf::network::{Message, Welcome};
    use crate::network::tracking::NoopTracker;
    use crate::network::{
        Connection, ConnectionManager, HandshakeError, MessageRouter, MockPeerConnection,
        PeerMetadataVersion, Swimlane,
    };
    use crate::{Metadata, TaskKind};

    pub struct MockConnector<F> {
        inner: Arc<Mutex<MockConnectorInner<F>>>,
        new_connection_sender: mpsc::UnboundedSender<MockPeerConnection>,
    }

    /// A minimal in-memory transport for tests that run peers in distinct task-center scopes.
    ///
    /// Unlike [`MockConnector`], the test registers each peer's already-built router explicitly.
    /// This lets a connection created in one task center deliver to a service receiver owned by
    /// another one without manufacturing a second copy of the remote node.
    #[derive(Clone, Default)]
    pub struct RoutedConnector {
        routers: Arc<Mutex<HashMap<GenerationalNodeId, Arc<MessageRouter>>>>,
    }

    impl RoutedConnector {
        pub async fn register_router(
            &self,
            node_id: GenerationalNodeId,
            router: Arc<MessageRouter>,
        ) {
            let old = self.routers.lock().await.insert(node_id, router);
            assert!(old.is_none(), "router for {node_id} was registered twice");
        }
    }

    impl<F> Clone for MockConnector<F> {
        fn clone(&self) -> Self {
            Self {
                inner: Arc::clone(&self.inner),
                new_connection_sender: self.new_connection_sender.clone(),
            }
        }
    }

    struct MockConnectorInner<F> {
        pub routers: HashMap<GenerationalNodeId, Arc<MessageRouter>>,
        pub router_factory: F,
    }

    #[cfg(any(test, feature = "test-util"))]
    impl<F> MockConnector<F>
    where
        F: FnMut(GenerationalNodeId, &mut MessageRouterBuilder) + Send + Sync + 'static,
    {
        pub fn new(router_factory: F) -> (Self, mpsc::UnboundedReceiver<MockPeerConnection>) {
            let (sender, receiver) = mpsc::unbounded_channel();
            (
                Self {
                    inner: Arc::new(Mutex::new(MockConnectorInner {
                        routers: Default::default(),
                        router_factory,
                    })),
                    new_connection_sender: sender,
                },
                receiver,
            )
        }
    }

    impl<F> TransportConnect for MockConnector<F>
    where
        F: FnMut(GenerationalNodeId, &mut MessageRouterBuilder) + Send + Sync + 'static,
    {
        async fn connect(
            &self,
            destination: &Destination,
            swimlane: Swimlane,
            mut output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
        ) -> Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError> {
            let &Destination::Node(peer_node_id) = destination else {
                unimplemented!("MockConnector only supports Destination::Node");
            };
            let mut guard = self.inner.lock().await;
            let router = if let Some(router) = guard.routers.get(&peer_node_id) {
                router.clone()
            } else {
                let mut router = MessageRouterBuilder::with_default_pool(MemoryPool::unlimited());
                (guard.router_factory)(peer_node_id, &mut router);
                let router = Arc::new(router.build());
                guard.routers.insert(peer_node_id, router.clone());
                router
            };

            let metadata = Metadata::current();
            let nodes_config = metadata.nodes_config_ref();
            let my_node_id = metadata.my_node_id();
            // validates that the node is known in the config
            let current_generation = find_node(&nodes_config, peer_node_id)?.current_generation;
            info!(
                "Attempting to fake a connection {}->{} and current_generation is {}",
                my_node_id, peer_node_id, current_generation
            );

            let (tx, egress, shared) = EgressStream::create_loopback();

            let handshake_timeout = Configuration::pinned().networking.handshake_timeout.into();
            let (header, hello) =
                crate::network::handshake::wait_for_hello(&mut output_stream, handshake_timeout)
                    .await?;

            // NodeId **must** be generational at this layer
            let _peer_node_id = hello.my_node_id.ok_or_else(|| {
                HandshakeError::Failed("NodeId is not set in the Hello message".to_owned())
            })?;

            // Are we both from the same cluster?
            if hello.cluster_name != nodes_config.cluster_name() {
                return Err(HandshakeError::Failed("cluster name mismatch".to_owned()).into());
            }
            let peer_metadata = PeerMetadataVersion::from(header);

            let selected_protocol_version = negotiate_protocol_version(&hello)?;

            // Enqueue the welcome message
            let welcome = Welcome::new(peer_node_id, selected_protocol_version, hello.direction());

            shared
                .unbounded_send(EgressMessage::Message(welcome.into()))
                .unwrap();

            let connection =
                Connection::new(my_node_id, selected_protocol_version, hello.swimlane(), tx);
            let reactor =
                ConnectionReactor::new(connection.clone(), shared, Some(peer_metadata), router)
                    .start(
                        TaskKind::RemoteConnectionReactor,
                        NoopTracker,
                        false,
                        output_stream,
                    )?;

            let connection = MockPeerConnection::new(peer_node_id, swimlane, reactor, connection);

            let _ = self.new_connection_sender.send(connection);
            Ok(egress)
        }
    }

    impl TransportConnect for RoutedConnector {
        async fn connect(
            &self,
            destination: &Destination,
            _swimlane: Swimlane,
            mut output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
        ) -> Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError> {
            let &Destination::Node(peer_node_id) = destination else {
                unimplemented!("RoutedConnector only supports Destination::Node");
            };
            let router = self
                .routers
                .lock()
                .await
                .get(&peer_node_id)
                .cloned()
                .ok_or_else(|| {
                    ConnectError::Transport(format!("no router registered for peer {peer_node_id}"))
                })?;

            let metadata = Metadata::current();
            let nodes_config = metadata.nodes_config_ref();
            let my_node_id = metadata.my_node_id();
            let current_generation = find_node(&nodes_config, peer_node_id)?.current_generation;
            info!(
                "Attempting to route an in-memory connection {}->{} and current_generation is {}",
                my_node_id, peer_node_id, current_generation
            );

            let (tx, egress, shared) = EgressStream::create_loopback();

            let (header, hello) = crate::network::handshake::wait_for_hello(
                &mut output_stream,
                Configuration::pinned().networking.handshake_timeout.into(),
            )
            .await?;

            let _peer_node_id = hello.my_node_id.ok_or_else(|| {
                HandshakeError::Failed("NodeId is not set in the Hello message".to_owned())
            })?;
            if hello.cluster_name != nodes_config.cluster_name() {
                return Err(HandshakeError::Failed("cluster name mismatch".to_owned()).into());
            }
            let peer_metadata = PeerMetadataVersion::from(header);
            let selected_protocol_version = negotiate_protocol_version(&hello)?;
            let welcome = Welcome::new(peer_node_id, selected_protocol_version, hello.direction());
            shared
                .unbounded_send(EgressMessage::Message(welcome.into()))
                .expect("connection egress must be open during the handshake");

            let connection =
                Connection::new(my_node_id, selected_protocol_version, hello.swimlane(), tx);
            ConnectionReactor::new(connection, shared, Some(peer_metadata), router).start(
                TaskKind::RemoteConnectionReactor,
                NoopTracker,
                false,
                output_stream,
            )?;

            Ok(egress)
        }
    }

    /// Transport that fails all outgoing connections
    #[derive(Default, Clone)]
    pub struct FailingConnector;

    impl TransportConnect for FailingConnector {
        async fn connect(
            &self,
            _destination: &Destination,
            _swimlane: Swimlane,
            _output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
        ) -> Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError> {
            Result::<futures::stream::Empty<_>, _>::Err(ConnectError::Transport(
                "Trying to connect using failing transport".to_string(),
            ))
        }
    }

    #[derive(Clone)]
    pub(in crate::network) struct PassthroughConnector(pub ConnectionManager);

    impl TransportConnect for PassthroughConnector {
        async fn connect(
            &self,
            _destination: &Destination,
            _swimlane: Swimlane,
            output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
        ) -> Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError> {
            let output = self
                .0
                .accept_incoming_connection(output_stream)
                .await
                .map_err(|err| ConnectError::Transport(err.to_string()))?;
            Ok(output)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use tokio::time::{Duration, timeout};
    use tokio_stream::StreamExt;

    use restate_memory::MemoryPool;
    use restate_types::net::node::{Gossip, GossipFlags, GossipService};
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, RestateVersion};

    use super::test_util::RoutedConnector;
    use crate::network::{BackPressureMode, MessageRouterBuilder, ServiceMessage, Swimlane};
    use crate::{TaskCenterBuilder, TestCoreEnvBuilder, network::NetworkSender};

    const A: GenerationalNodeId = GenerationalNodeId::new(1, 1);
    const B: GenerationalNodeId = GenerationalNodeId::new(2, 1);

    #[crate::test(flavor = "multi_thread", worker_threads = 2)]
    async fn routed_connector_delivers_gossip_to_an_existing_peer_router() -> Result<()> {
        let task_center = TaskCenterBuilder::default_for_tests()
            .build()?
            .into_handle();
        let result = tokio::task::block_in_place(|| {
            task_center.block_on(async {
                let connector = RoutedConnector::default();
                let mut router_builder =
                    MessageRouterBuilder::with_default_pool(MemoryPool::unlimited());
                let mut gossip_rx = router_builder
                    .register_service::<GossipService>(BackPressureMode::Lossy)
                    .start();
                connector
                    .register_router(B, Arc::new(router_builder.build()))
                    .await;

                let env = TestCoreEnvBuilder::with_transport_connector(connector)
                    .set_my_node_id(A)
                    .set_nodes_config(nodes_config())
                    .build()
                    .await;

                let connection = env
                    .networking
                    .get_connection(B, Swimlane::default())
                    .await?;
                let token = connection
                    .reserve()
                    .await
                    .expect("routed connection is open")
                    .send_unary(test_gossip(), None)?;
                token.await?;

                let ServiceMessage::Unary(message) =
                    timeout(Duration::from_secs(1), gossip_rx.next())
                        .await?
                        .expect("routed gossip was delivered")
                else {
                    panic!("expected a unary gossip message");
                };
                let Ok(message) = message.try_into_typed::<Gossip>() else {
                    panic!("routed gossip had an unexpected payload type");
                };
                let message = message.into_body();
                assert!(message.flags.contains(GossipFlags::Special));

                Ok(())
            })
        });
        task_center.cancel_tasks(None, None).await;
        result
    }

    fn nodes_config() -> NodesConfiguration {
        let mut nodes_config = NodesConfiguration::new_for_testing();
        for node_id in [A, B] {
            nodes_config.upsert_node(
                NodeConfig::builder()
                    .name(format!("node-{node_id}"))
                    .current_generation(node_id)
                    .address(Default::default())
                    .roles(Role::Admin | Role::Worker)
                    .binary_version(RestateVersion::current())
                    .build(),
            );
        }
        nodes_config
    }

    fn test_gossip() -> Gossip {
        Gossip {
            instance_ts: MillisSinceEpoch::now(),
            sent_at: MillisSinceEpoch::now(),
            flags: GossipFlags::Special,
            nodes: Vec::new(),
            partitions: Vec::new(),
        }
    }
}
