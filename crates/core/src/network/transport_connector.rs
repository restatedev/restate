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

use dashmap::DashMap;
use futures::{Stream, StreamExt};
use tonic::transport::Channel;
use tracing::trace;

use restate_types::config::NetworkingOptions;
use restate_types::net::AdvertisedAddress;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::protobuf::node::Message;
use restate_types::GenerationalNodeId;

use super::protobuf::core_node_svc::core_node_svc_client::CoreNodeSvcClient;
use super::{NetworkError, ProtocolError};
use crate::network::net_util::create_tonic_channel_from_advertised_address;

pub trait TransportConnect: Send + Sync + 'static {
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

pub struct GrpcConnector {
    networking_options: NetworkingOptions,
    channel_cache: DashMap<AdvertisedAddress, Channel>,
}

impl GrpcConnector {
    pub fn new(networking_options: NetworkingOptions) -> Self {
        Self {
            networking_options,
            channel_cache: DashMap::new(),
        }
    }
}

impl TransportConnect for GrpcConnector {
    async fn connect(
        &self,
        node_id: GenerationalNodeId,
        nodes_config: &NodesConfiguration,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> Result<
        impl Stream<Item = Result<Message, ProtocolError>> + Send + Unpin + 'static,
        NetworkError,
    > {
        let address = nodes_config.find_node_by_id(node_id)?.address.clone();

        trace!("Attempting to connect to node {} at {}", node_id, address);
        // Do we have a channel in cache for this address?
        let channel = match self.channel_cache.get(&address) {
            Some(channel) => channel.clone(),
            None => self
                .channel_cache
                .entry(address.clone())
                .or_insert_with(|| {
                    create_tonic_channel_from_advertised_address(address, &self.networking_options)
                })
                .clone(),
        };

        // Establish the connection
        let mut client = CoreNodeSvcClient::new(channel);
        let incoming = client.create_connection(output_stream).await?.into_inner();
        Ok(incoming.map(|x| x.map_err(ProtocolError::from)))
    }
}

#[cfg(any(test, feature = "test-util"))]
pub mod test_util {
    use super::*;

    use std::sync::Arc;
    use std::time::Instant;

    use futures::{Stream, StreamExt};
    use parking_lot::Mutex;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tracing::info;

    use restate_types::nodes_config::NodesConfiguration;
    use restate_types::protobuf::node::message::BinaryMessage;
    use restate_types::protobuf::node::Message;
    use restate_types::GenerationalNodeId;

    use super::{NetworkError, ProtocolError};
    use crate::network::{Incoming, MockPeerConnection, PartialPeerConnection, WeakConnection};
    use crate::{my_node_id, TaskCenter, TaskHandle, TaskKind};

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

            let (sender, rx) = mpsc::channel(self.sendbuf);

            let peer_connection = PartialPeerConnection {
                my_node_id: node_id,
                peer: my_node_id(),
                sender,
                recv_stream: output_stream.boxed(),
                created: Instant::now(),
            };

            let peer_connection = peer_connection.handshake(nodes_config).await.unwrap();

            if self.new_connection_sender.send(peer_connection).is_err() {
                // receiver has closed, cannot accept connections
                return Err(NetworkError::Unavailable(format!(
                    "MockConnector has been terminated, cannot connect to {node_id}"
                )));
            }
            let incoming = ReceiverStream::new(rx).map(Ok);
            Ok(incoming)
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

    impl TransportConnect for MessageCollectorMockConnector {
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
    #[derive(Default)]
    pub struct FailingConnector {}

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
