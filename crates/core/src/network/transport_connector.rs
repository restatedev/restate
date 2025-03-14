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

use restate_types::GenerationalNodeId;
use restate_types::nodes_config::NodesConfiguration;

use super::protobuf::network::Message;
use super::{NetworkError, ProtocolError};

pub trait TransportConnect: Clone + Send + Sync + 'static {
    fn connect(
        &self,
        node_id: GenerationalNodeId,
        nodes_config: &NodesConfiguration,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Message, ProtocolError>> + Send + Unpin + 'static,
            NetworkError,
        >,
    > + Send;
}

impl<T: TransportConnect> TransportConnect for std::sync::Arc<T> {
    fn connect(
        &self,
        node_id: GenerationalNodeId,
        nodes_config: &NodesConfiguration,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Message, ProtocolError>> + Send + Unpin + 'static,
            NetworkError,
        >,
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

    use super::{NetworkError, ProtocolError};
    use crate::network::io::EgressStream;
    use crate::network::protobuf::network::Message;
    use crate::network::protobuf::network::message::BinaryMessage;
    use crate::network::{Incoming, MockPeerConnection, PartialPeerConnection, WeakConnection};
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
        ) -> Result<
            impl Stream<Item = Result<Message, ProtocolError>> + Send + Unpin + 'static,
            NetworkError,
        > {
            // validates that the node is known in the config
            let current_generation = nodes_config.find_node_by_id(node_id)?.current_generation;
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
                return Err(NetworkError::Unavailable(format!(
                    "MockConnector has been terminated, cannot connect to {node_id}"
                )));
            }
            Ok(egress.map(Ok))
        }
    }

    /// Accepts all connections, performs handshake and sends all received messages to a single
    /// stream
    pub struct MessageCollectorMockConnector {
        pub mock_connector: MockConnector,
        pub tasks: Mutex<Vec<(WeakConnection, TaskHandle<anyhow::Result<()>>)>>,
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
        ) -> Result<
            impl Stream<Item = Result<Message, ProtocolError>> + Send + Unpin + 'static,
            NetworkError,
        > {
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
        ) -> Result<
            impl Stream<Item = Result<Message, ProtocolError>> + Send + Unpin + 'static,
            NetworkError,
        > {
            Result::<futures::stream::Empty<_>, _>::Err(NetworkError::ConnectError(
                tonic::Status::unavailable("Trying to connect using failing transport"),
            ))
        }
    }
}
