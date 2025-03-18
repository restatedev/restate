// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_types::nodes_config::{NodeConfig, NodesConfigError, NodesConfiguration};
use restate_types::{GenerationalNodeId, NodeId};

use super::protobuf::network::Message;
use super::{ConnectError, DiscoveryError};

/// Finds a node in nodes configuration by ID and maps the error to [`DiscoveryError`]
pub fn find_node(
    nodes_config: &NodesConfiguration,
    node_id: impl Into<NodeId>,
) -> Result<&NodeConfig, DiscoveryError> {
    match nodes_config.find_node_by_id(node_id) {
        Ok(node_config) => Ok(node_config),
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
        node_id: GenerationalNodeId,
        nodes_config: &NodesConfiguration,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError>,
    > + Send;
}

impl<T: TransportConnect> TransportConnect for std::sync::Arc<T> {
    fn connect(
        &self,
        node_id: GenerationalNodeId,
        nodes_config: &NodesConfiguration,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError>,
    > + Send {
        (**self).connect(node_id, nodes_config, output_stream)
    }
}

#[cfg(any(test, feature = "test-util"))]
pub mod test_util {
    use super::*;

    use std::sync::Arc;

    use futures::{Stream, StreamExt};
    use parking_lot::Mutex;
    use tokio::sync::mpsc;
    use tokio::time::Instant;
    use tracing::info;

    use restate_types::GenerationalNodeId;
    use restate_types::nodes_config::NodesConfiguration;

    use crate::network::io::EgressStream;
    use crate::network::protobuf::network::Message;
    use crate::network::protobuf::network::message::BinaryMessage;
    use crate::network::{Connection, Incoming, MockPeerConnection, PartialPeerConnection};
    use crate::{TaskCenter, TaskHandle, TaskKind, my_node_id};

    #[derive(Clone)]
    pub struct MockConnector {
        pub sendbuf: usize,
        pub new_connection_sender: mpsc::UnboundedSender<MockPeerConnection>,
    }

    #[cfg(any(test, feature = "test-util"))]
    impl MockConnector {
        pub fn new(sendbuf: usize) -> (Self, mpsc::UnboundedReceiver<MockPeerConnection>) {
            let (new_connection_sender, rx) = mpsc::unbounded_channel();
            (
                Self {
                    sendbuf,
                    new_connection_sender,
                },
                rx,
            )
        }
    }

    impl TransportConnect for MockConnector {
        async fn connect(
            &self,
            node_id: GenerationalNodeId,
            nodes_config: &NodesConfiguration,
            output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
        ) -> Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError> {
            // validates that the node is known in the config
            let current_generation = find_node(nodes_config, node_id)?.current_generation;
            info!(
                "Attempting to fake a connection to node {} and current_generation is {}",
                node_id, current_generation
            );

            let (sender, unbounded_sender, egress, drop_egress) =
                EgressStream::create(self.sendbuf);

            let peer_connection = PartialPeerConnection {
                my_node_id: node_id,
                peer: my_node_id(),
                sender,
                unbounded_sender,
                recv_stream: output_stream.boxed(),
                created: Instant::now(),
                drop_egress,
            };

            let peer_connection = peer_connection.handshake(nodes_config).await.unwrap();

            if self.new_connection_sender.send(peer_connection).is_err() {
                // receiver has closed, cannot accept connections
                return Err(ConnectError::Transport(format!(
                    "MockConnector has been terminated, cannot connect to {node_id}"
                )));
            }
            Ok(egress)
        }
    }

    /// Accepts all connections, performs handshake and sends all received messages to a single
    /// stream
    pub struct MessageCollectorMockConnector {
        pub mock_connector: MockConnector,
        pub tasks: Mutex<Vec<(Connection, TaskHandle<anyhow::Result<()>>)>>,
    }

    impl MessageCollectorMockConnector {
        pub fn new(
            sendbuf: usize,
            sender: mpsc::Sender<(GenerationalNodeId, Incoming<BinaryMessage>)>,
        ) -> Arc<Self> {
            let (mock_connector, mut new_connections) = MockConnector::new(sendbuf);
            let connector = Arc::new(Self {
                mock_connector,
                tasks: Default::default(),
            });

            // start acceptor
            TaskCenter::spawn(TaskKind::Disposable, "test-connection-acceptor", {
                let connector = connector.clone();
                async move {
                    while let Some(connection) = new_connections.recv().await {
                        let (connection, task) = connection.forward_to_sender(sender.clone())?;
                        connector.tasks.lock().push((connection, task));
                    }
                    Ok(())
                }
            })
            .unwrap();
            connector
        }
    }

    impl TransportConnect for Arc<MessageCollectorMockConnector> {
        async fn connect(
            &self,
            node_id: GenerationalNodeId,
            nodes_config: &NodesConfiguration,
            output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
        ) -> Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError> {
            self.mock_connector
                .connect(node_id, nodes_config, output_stream)
                .await
        }
    }

    /// Transport that fails all outgoing connections
    #[derive(Default, Clone)]
    pub struct FailingConnector;

    #[cfg(any(test, feature = "test-util"))]
    impl TransportConnect for FailingConnector {
        async fn connect(
            &self,
            _node_id: GenerationalNodeId,
            _nodes_config: &NodesConfiguration,
            _output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
        ) -> Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError> {
            Result::<futures::stream::Empty<_>, _>::Err(ConnectError::Transport(
                "Trying to connect using failing transport".to_string(),
            ))
        }
    }
}
