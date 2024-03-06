// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use restate_node_protocol::codec::{deserialize_message, serialize_message, Targeted, WireSerde};
use restate_node_protocol::metadata::MetadataKind;
use restate_node_protocol::node::{Header, Message};
use restate_node_protocol::CURRENT_PROTOCOL_VERSION;
use restate_types::nodes_config::{AdvertisedAddress, NodeConfig, NodesConfiguration, Role};
use restate_types::{GenerationalNodeId, NodeId, Version};
use tracing::info;

use crate::network::{
    Handler, MessageHandler, MessageRouter, MessageRouterBuilder, NetworkSendError, NetworkSender,
};
use crate::{cancellation_watcher, metadata, spawn_metadata_manager, ShutdownError, TaskId};
use crate::{Metadata, MetadataManager, MetadataWriter};
use crate::{TaskCenter, TaskCenterFactory};

#[derive(Clone, Default)]
pub struct MockNetworkSender {
    sender: Option<mpsc::UnboundedSender<(GenerationalNodeId, Message)>>,
}

impl NetworkSender for MockNetworkSender {
    async fn send<M>(&self, to: NodeId, message: &M) -> Result<(), NetworkSendError>
    where
        M: WireSerde + Targeted + Send + Sync,
    {
        let Some(sender) = &self.sender else {
            info!("Not sending message, mock sender is not configured");
            return Ok(());
        };

        let to = match to.as_generational() {
            Some(to) => to,
            None => match metadata().nodes_config().find_node_by_id(to) {
                Ok(node) => node.current_generation,
                Err(e) => return Err(NetworkSendError::UnknownNode(e)),
            },
        };

        let header = Header::new(metadata().nodes_config_version());
        let body = serialize_message(message, CURRENT_PROTOCOL_VERSION)?;
        sender
            .send((to, Message::new(header, body)))
            .map_err(|_| NetworkSendError::Shutdown(ShutdownError))?;
        Ok(())
    }
}

#[derive(Default)]
struct NetworkReceiver {
    router: Arc<RwLock<MessageRouter>>,
}

impl NetworkReceiver {
    async fn run(
        &self,
        mut receiver: mpsc::UnboundedReceiver<(GenerationalNodeId, Message)>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancellation_watcher() => {
                    break;
                }
                maybe_msg = receiver.recv() => {
                    let Some((from, msg)) = maybe_msg else {
                        break;
                    };
                    {
                    let guard = self.router.read().await;
                    self.route_message(from, msg, &guard).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn route_message(
        &self,
        peer: GenerationalNodeId,
        msg: Message,
        router: &MessageRouter,
    ) -> anyhow::Result<()> {
        let body = msg.body.expect("body must be set");
        let msg = deserialize_message(body, CURRENT_PROTOCOL_VERSION)?;
        router
            .call(peer, rand::random(), CURRENT_PROTOCOL_VERSION, msg)
            .await?;
        Ok(())
    }
}

impl MockNetworkSender {
    pub fn with_sender(sender: mpsc::UnboundedSender<(GenerationalNodeId, Message)>) -> Self {
        Self {
            sender: Some(sender),
        }
    }
}

// This might need to be moved to a better place in the future.
pub struct TestCoreEnv {
    pub tc: TaskCenter,
    pub metadata: Metadata,
    pub metadata_writer: MetadataWriter,
    pub network_sender: MockNetworkSender,
    pub network_task: TaskId,
    router: Arc<RwLock<MessageRouter>>,
}

impl TestCoreEnv {
    fn create_empty() -> Self {
        let tc = TaskCenterFactory::create(tokio::runtime::Handle::current());
        let (tx, rx) = mpsc::unbounded_channel();

        let network_sender = MockNetworkSender::with_sender(tx);
        let metadata_manager = MetadataManager::build(network_sender.clone());
        let metadata = metadata_manager.metadata();
        let router = MessageRouterBuilder::default().build();
        let router = Arc::new(RwLock::new(router));
        let metadata_writer = metadata_manager.writer();
        let network_receiver = NetworkReceiver {
            router: router.clone(),
        };
        tc.try_set_global_metadata(metadata.clone());
        spawn_metadata_manager(&tc, metadata_manager).expect("metadata manager should start");
        let network_task = Self::start_network(&tc, network_receiver, rx);

        Self {
            tc,
            metadata,
            metadata_writer,
            network_sender,
            network_task,
            router,
        }
    }

    fn start_network(
        tc: &TaskCenter,
        network_receiver: NetworkReceiver,
        rx: mpsc::UnboundedReceiver<(GenerationalNodeId, Message)>,
    ) -> TaskId {
        tc.spawn(
            crate::TaskKind::ConnectionReactor,
            "test-network-receiver",
            None,
            async move { network_receiver.run(rx).await },
        )
        .unwrap()
    }

    pub async fn create_with_nodes_config(
        nodes_config: NodesConfiguration,
        my_node_id: u32,
        generation: u32,
    ) -> Self {
        let env = Self::create_empty();
        env.metadata_writer.submit(nodes_config.clone());

        env.tc
            .run_in_scope("test-env", None, async {
                let _ = env
                    .metadata
                    .wait_for_version(MetadataKind::NodesConfiguration, Version::MIN)
                    .await
                    .unwrap();
            })
            .await;
        env.metadata_writer
            .set_my_node_id(GenerationalNodeId::new(my_node_id, generation));
        env
    }

    pub async fn create_with_mock_nodes_config(node_id: u32, generation: u32) -> Self {
        let nodes_config = create_mock_nodes_config(node_id, generation);
        Self::create_with_nodes_config(nodes_config, node_id, generation).await
    }

    pub async fn set_message_router<H>(&mut self, router: MessageRouter) {
        let mut guard = self.router.write().await;
        *guard = router;
    }

    /// Runs the system with this message handle and removes all other handlers from router. If
    /// more than one handler is needed, then use MessageRouterBuilder and assign the new router
    /// via set_message_router instead.
    pub async fn set_message_handler<H>(&mut self, handler: H)
    where
        H: MessageHandler + Send + Sync + 'static,
    {
        let mut mr_builder = MessageRouterBuilder::default();
        mr_builder.add_message_handler(handler);
        let router = mr_builder.build();
        let mut guard = self.router.write().await;
        *guard = router;
    }
}

pub fn create_mock_nodes_config(node_id: u32, generation: u32) -> NodesConfiguration {
    let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
    let address = AdvertisedAddress::from_str("http://127.0.0.1:5122/").unwrap();
    let node_id = GenerationalNodeId::new(node_id, generation);
    let roles = Role::Admin | Role::Worker;
    let my_node = NodeConfig::new(format!("MyNode-{}", node_id), node_id, address, roles);
    nodes_config.upsert_node(my_node);
    nodes_config
}
