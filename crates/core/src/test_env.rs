// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::{Arc, Weak};

use tokio::sync::{mpsc, RwLock};
use tracing::info;

use restate_types::cluster_controller::{ReplicationStrategy, SchedulingPlan};
use restate_types::logs::metadata::{bootstrap_logs_metadata, ProviderKind};
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY, SCHEDULING_PLAN_KEY,
};
use restate_types::net::codec::{
    serialize_message, MessageBodyExt, Targeted, WireDecode, WireEncode,
};
use restate_types::net::metadata::MetadataKind;
use restate_types::net::AdvertisedAddress;
use restate_types::net::CURRENT_PROTOCOL_VERSION;
use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role};
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::node::{Header, Message};
use restate_types::{GenerationalNodeId, Version};

use crate::metadata_store::{MetadataStoreClient, Precondition};
use crate::network::{
    Connection, Handler, Incoming, MessageHandler, MessageRouter, MessageRouterBuilder,
    NetworkError, NetworkSendError, NetworkSender, Outgoing,
};
use crate::{
    cancellation_watcher, metadata, spawn_metadata_manager, MetadataBuilder, ShutdownError, TaskId,
};
use crate::{Metadata, MetadataManager, MetadataWriter};
use crate::{TaskCenter, TaskCenterBuilder};

#[derive(Clone)]
pub struct MockNetworkSender {
    sender: Option<mpsc::UnboundedSender<(GenerationalNodeId, Message)>>,
    metadata: Metadata,
}

impl MockNetworkSender {
    pub fn new(metadata: Metadata) -> Self {
        Self {
            sender: None,
            metadata,
        }
    }
}

impl NetworkSender for MockNetworkSender {
    async fn send<M>(&self, mut message: Outgoing<M>) -> Result<(), NetworkSendError<M>>
    where
        M: WireEncode + Targeted + Send + Sync,
    {
        let Some(sender) = &self.sender else {
            info!("Not sending message, mock sender is not configured");
            return Ok(());
        };

        if !message.peer().is_generational() {
            let current_generation = match self
                .metadata
                .nodes_config_ref()
                .find_node_by_id(message.peer())
            {
                Ok(node) => node.current_generation,
                Err(e) => return Err(NetworkSendError::new(message, NetworkError::UnknownNode(e))),
            };
            message.set_peer(current_generation);
        }

        let metadata = metadata();
        let header = Header::new(
            metadata.nodes_config_version(),
            None,
            None,
            None,
            message.msg_id(),
            message.in_response_to(),
        );
        let body = match serialize_message(message.body(), CURRENT_PROTOCOL_VERSION) {
            Ok(body) => body,
            Err(e) => {
                return Err(NetworkSendError::new(
                    message,
                    NetworkError::ProtocolError(e.into()),
                ))
            }
        };
        sender
            .send((
                message.peer().as_generational().unwrap(),
                Message::new(header, body),
            ))
            .map_err(|_| NetworkSendError::new(message, NetworkError::Shutdown(ShutdownError)))?;
        Ok(())
    }
}

#[derive(Default)]
struct NetworkReceiver {
    router: Arc<RwLock<MessageRouter>>,
}

impl NetworkReceiver {
    async fn run(
        self,
        my_node_id: GenerationalNodeId,
        mut receiver: mpsc::UnboundedReceiver<(GenerationalNodeId, Message)>,
    ) -> anyhow::Result<()> {
        let (reply_sender, mut reply_receiver) = mpsc::channel::<Message>(50);
        // NOTE: rpc replies will only work if and only if the RpcRouter is using the same router_builder as the service you
        // are trying to call.
        // In other words, response will not be routed back if the Target component is registered with a different router_builder
        // than the client you using to make the call.
        let connection = Connection::new_fake(my_node_id, CURRENT_PROTOCOL_VERSION, reply_sender);

        loop {
            tokio::select! {
                _ = cancellation_watcher() => {
                    break;
                }
                maybe_msg = reply_receiver.recv() => {
                    let Some(msg) = maybe_msg else {
                        break;
                    };

                    let guard = self.router.read().await;
                    self.route_message(my_node_id, msg, &guard, Weak::new()).await?;
                }
                maybe_msg = receiver.recv() => {
                    let Some((from, msg)) = maybe_msg else {
                        break;
                    };
                    {
                    let guard = self.router.read().await;
                    self.route_message(from, msg, &guard, Arc::downgrade(&connection)).await?;
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
        connection: Weak<Connection>,
    ) -> anyhow::Result<()> {
        let body = msg.body.expect("body must be set");
        let header = msg.header.expect("header must be set");

        let msg = Incoming::from_parts(
            peer,
            body.try_as_binary_body(CURRENT_PROTOCOL_VERSION)?,
            connection,
            header.msg_id,
            header.in_response_to,
        );
        router.call(msg, CURRENT_PROTOCOL_VERSION).await?;
        Ok(())
    }
}

impl MockNetworkSender {
    pub fn from_sender(
        sender: mpsc::UnboundedSender<(GenerationalNodeId, Message)>,
        metadata: Metadata,
    ) -> Self {
        Self {
            sender: Some(sender),
            metadata,
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
    pub provider_kind: ProviderKind,
    pub router_builder: MessageRouterBuilder,
    pub network_sender: N,
    pub partition_table: PartitionTable,
    pub scheduling_plan: SchedulingPlan,
    pub metadata_store_client: MetadataStoreClient,
}

impl TestCoreEnvBuilder<MockNetworkSender> {
    pub fn new_with_mock_network() -> TestCoreEnvBuilder<MockNetworkSender> {
        let (tx, rx) = mpsc::unbounded_channel();
        let metadata_builder = MetadataBuilder::default();
        let network_sender = MockNetworkSender::from_sender(tx, metadata_builder.to_metadata());

        TestCoreEnvBuilder::new_with_network_tx_rx(network_sender, Some(rx), metadata_builder)
    }
}

impl<N> TestCoreEnvBuilder<N>
where
    N: NetworkSender + 'static,
{
    pub fn new(network_sender: N, metadata_builder: MetadataBuilder) -> Self {
        TestCoreEnvBuilder::new_with_network_tx_rx(network_sender, None, metadata_builder)
    }

    fn new_with_network_tx_rx(
        network_sender: N,
        network_rx: Option<mpsc::UnboundedReceiver<(GenerationalNodeId, Message)>>,
        metadata_builder: MetadataBuilder,
    ) -> Self {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .ingress_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");

        let my_node_id = GenerationalNodeId::new(1, 1);
        let metadata_store_client = MetadataStoreClient::new_in_memory();
        let metadata = metadata_builder.to_metadata();
        let metadata_manager = MetadataManager::new(
            metadata_builder,
            network_sender.clone(),
            metadata_store_client.clone(),
        );
        let metadata_writer = metadata_manager.writer();
        let router_builder = MessageRouterBuilder::default();
        let nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 10);
        let scheduling_plan =
            SchedulingPlan::from(&partition_table, ReplicationStrategy::OnAllNodes);
        tc.try_set_global_metadata(metadata.clone());

        // Use memory-loglet as a default if in test-mode
        #[cfg(any(test, feature = "test-util"))]
        let provider_kind = ProviderKind::InMemory;
        #[cfg(not(any(test, feature = "test-util")))]
        let provider_kind = ProviderKind::Local;

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
            scheduling_plan,
            metadata_store_client,
            provider_kind,
        }
    }

    pub fn with_nodes_config(mut self, nodes_config: NodesConfiguration) -> Self {
        self.nodes_config = nodes_config;
        self
    }

    pub fn with_partition_table(mut self, partition_table: PartitionTable) -> Self {
        self.partition_table = partition_table;
        self
    }

    pub fn with_scheduling_plan(mut self, scheduling_plan: SchedulingPlan) -> Self {
        self.scheduling_plan = scheduling_plan;
        self
    }

    pub fn set_my_node_id(mut self, my_node_id: GenerationalNodeId) -> Self {
        self.my_node_id = my_node_id;
        self
    }

    pub fn set_provider_kind(mut self, provider_kind: ProviderKind) -> Self {
        self.provider_kind = provider_kind;
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
                        async move { network_receiver.run(self.my_node_id, network_rx).await },
                    )
                    .unwrap();
                Some(network_task)
            }
            None => None,
        };

        self.metadata_store_client
            .put(
                NODES_CONFIG_KEY.clone(),
                &self.nodes_config,
                Precondition::None,
            )
            .await
            .expect("to store nodes config in metadata store");
        self.metadata_writer.submit(self.nodes_config.clone());

        let logs =
            bootstrap_logs_metadata(self.provider_kind, self.partition_table.num_partitions());
        self.metadata_store_client
            .put(BIFROST_CONFIG_KEY.clone(), &logs, Precondition::None)
            .await
            .expect("to store bifrost config in metadata store");
        self.metadata_writer.submit(logs.clone());

        self.metadata_store_client
            .put(
                PARTITION_TABLE_KEY.clone(),
                &self.partition_table,
                Precondition::None,
            )
            .await
            .expect("to store partition table in metadata store");
        self.metadata_writer.submit(self.partition_table);

        self.metadata_store_client
            .put(
                SCHEDULING_PLAN_KEY.clone(),
                &self.scheduling_plan,
                Precondition::None,
            )
            .await
            .expect("sot store scheduling plan in metadata store");

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
    let my_node = NodeConfig::new(
        format!("MyNode-{}", node_id),
        node_id,
        address,
        roles,
        LogServerConfig::default(),
    );
    nodes_config.upsert_node(my_node);
    nodes_config
}

/// No-op message handler which simply drops the received messages. Useful if you don't want to
/// react to network messages.
pub struct NoOpMessageHandler<M> {
    phantom_data: PhantomData<M>,
}

impl<M> Default for NoOpMessageHandler<M> {
    fn default() -> Self {
        NoOpMessageHandler {
            phantom_data: PhantomData,
        }
    }
}

impl<M> MessageHandler for NoOpMessageHandler<M>
where
    M: WireDecode + Targeted + Send + Sync,
{
    type MessageType = M;

    async fn on_message(&self, _msg: Incoming<Self::MessageType>) {
        // no-op
    }
}
