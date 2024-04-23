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
use restate_types::logs::metadata::{create_static_metadata, ProviderKind};
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY,
};
use restate_types::net::AdvertisedAddress;
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::partition_table::FixedPartitionTable;
use restate_types::{GenerationalNodeId, NodeId, Version};
use tracing::info;

use crate::metadata_store::{MetadataStoreClient, Precondition};
use crate::network::{
    Handler, MessageHandler, MessageRouter, MessageRouterBuilder, NetworkSendError, NetworkSender,
};
use crate::{cancellation_watcher, metadata, spawn_metadata_manager, ShutdownError, TaskId};
use crate::{Metadata, MetadataManager, MetadataWriter};
use crate::{TaskCenter, TaskCenterBuilder};

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
    pub fn from_sender(sender: mpsc::UnboundedSender<(GenerationalNodeId, Message)>) -> Self {
        Self {
            sender: Some(sender),
        }
    }
    pub fn inner_sender(&self) -> Option<mpsc::UnboundedSender<(GenerationalNodeId, Message)>> {
        self.sender.clone()
    }
}

pub struct TestCoreEnvBuilder<N> {
    pub tc: TaskCenter,
    pub my_node_id: GenerationalNodeId,
    pub network_rx: Option<mpsc::UnboundedReceiver<(GenerationalNodeId, Message)>>,
    pub metadata_manager: MetadataManager<N>,
    pub metadata_writer: MetadataWriter,
    pub metadata: Metadata,
    pub nodes_config: NodesConfiguration,
    pub router_builder: MessageRouterBuilder,
    pub network_sender: N,
    pub partition_table: FixedPartitionTable,
    pub metadata_store_client: MetadataStoreClient,
}

impl TestCoreEnvBuilder<MockNetworkSender> {
    pub fn new_with_mock_network() -> TestCoreEnvBuilder<MockNetworkSender> {
        let (tx, rx) = mpsc::unbounded_channel();
        let network_sender = MockNetworkSender::from_sender(tx);

        TestCoreEnvBuilder::new_with_network_tx_rx(network_sender, Some(rx))
    }
}

impl<N> TestCoreEnvBuilder<N>
where
    N: NetworkSender + 'static,
{
    pub fn new(network_sender: N) -> Self {
        TestCoreEnvBuilder::new_with_network_tx_rx(network_sender, None)
    }

    fn new_with_network_tx_rx(
        network_sender: N,
        network_rx: Option<mpsc::UnboundedReceiver<(GenerationalNodeId, Message)>>,
    ) -> Self {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");

        let my_node_id = GenerationalNodeId::new(1, 1);
        let metadata_store_client = MetadataStoreClient::new_in_memory();
        let metadata_manager =
            MetadataManager::build(network_sender.clone(), metadata_store_client.clone());
        let metadata = metadata_manager.metadata();
        let metadata_writer = metadata_manager.writer();
        let router_builder = MessageRouterBuilder::default();
        let nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        let partition_table = FixedPartitionTable::new(Version::MIN, 10);
        tc.try_set_global_metadata(metadata.clone());
        TestCoreEnvBuilder {
            tc,
            my_node_id,
            network_rx,
            metadata_manager,
            metadata_writer,
            metadata,
            network_sender,
            nodes_config,
            router_builder,
            partition_table,
            metadata_store_client,
        }
    }

    pub fn with_nodes_config(mut self, nodes_config: NodesConfiguration) -> Self {
        self.nodes_config = nodes_config;
        self
    }

    pub fn with_partition_table(mut self, partition_table: FixedPartitionTable) -> Self {
        self.partition_table = partition_table;
        self
    }

    pub fn set_my_node_id(mut self, my_node_id: GenerationalNodeId) -> Self {
        self.my_node_id = my_node_id;
        self
    }

    pub fn add_mock_nodes_config(mut self) -> Self {
        self.nodes_config =
            create_mock_nodes_config(self.my_node_id.raw_id(), self.my_node_id.raw_generation());
        self
    }

    pub fn add_message_handler<H>(mut self, handler: H) -> Self
    where
        H: MessageHandler + Send + Sync + 'static,
    {
        self.router_builder.add_message_handler(handler);
        self
    }

    pub async fn build(mut self) -> TestCoreEnv<N> {
        self.metadata_manager
            .register_in_message_router(&mut self.router_builder);

        let router = Arc::new(RwLock::new(self.router_builder.build()));
        let metadata_manager_task = spawn_metadata_manager(&self.tc, self.metadata_manager)
            .expect("metadata manager should start");

        let network_task = match self.network_rx {
            Some(network_rx) => {
                let network_receiver = NetworkReceiver {
                    router: router.clone(),
                };
                let network_task = self
                    .tc
                    .spawn(
                        crate::TaskKind::ConnectionReactor,
                        "test-network-receiver",
                        None,
                        async move { network_receiver.run(network_rx).await },
                    )
                    .unwrap();
                Some(network_task)
            }
            None => None,
        };

        self.metadata_store_client
            .put(
                NODES_CONFIG_KEY.clone(),
                self.nodes_config.clone(),
                Precondition::None,
            )
            .await
            .expect("to store nodes config in metadata store");
        self.metadata_writer.submit(self.nodes_config.clone());

        // todo: Allow client to update logs configuration and remove this bit here.
        let logs = create_static_metadata(
            ProviderKind::InMemory,
            self.partition_table.num_partitions(),
        );
        self.metadata_store_client
            .put(BIFROST_CONFIG_KEY.clone(), logs.clone(), Precondition::None)
            .await
            .expect("to store bifrost config in metadata store");
        self.metadata_writer.submit(logs.clone());

        self.metadata_store_client
            .put(
                PARTITION_TABLE_KEY.clone(),
                self.partition_table.clone(),
                Precondition::None,
            )
            .await
            .expect("to store partition table in metadata store");
        self.metadata_writer.submit(self.partition_table);

        self.tc
            .run_in_scope("test-env", None, async {
                let _ = self
                    .metadata
                    .wait_for_version(
                        MetadataKind::NodesConfiguration,
                        self.nodes_config.version(),
                    )
                    .await
                    .unwrap();
            })
            .await;
        self.metadata_writer.set_my_node_id(self.my_node_id);

        TestCoreEnv {
            tc: self.tc,
            network_task,
            metadata: self.metadata,
            metadata_manager_task,
            metadata_writer: self.metadata_writer,
            network_sender: self.network_sender,
            router,
            metadata_store_client: self.metadata_store_client,
        }
    }
}

// This might need to be moved to a better place in the future.
pub struct TestCoreEnv<N> {
    pub tc: TaskCenter,
    pub metadata: Metadata,
    pub metadata_writer: MetadataWriter,
    pub network_sender: N,
    pub network_task: Option<TaskId>,
    pub metadata_manager_task: TaskId,
    pub router: Arc<RwLock<MessageRouter>>,
    pub metadata_store_client: MetadataStoreClient,
}

impl TestCoreEnv<MockNetworkSender> {
    pub async fn create_with_mock_nodes_config(node_id: u32, generation: u32) -> Self {
        TestCoreEnvBuilder::new_with_mock_network()
            .set_my_node_id(GenerationalNodeId::new(node_id, generation))
            .add_mock_nodes_config()
            .build()
            .await
    }
}

impl<N> TestCoreEnv<N>
where
    N: NetworkSender,
{
    pub async fn set_message_router<H>(&mut self, router: MessageRouter) {
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
