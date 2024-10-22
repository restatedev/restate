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
use std::sync::Arc;

use futures::Stream;

use restate_types::cluster_controller::{ReplicationStrategy, SchedulingPlan};
use restate_types::config::NetworkingOptions;
use restate_types::logs::metadata::{bootstrap_logs_metadata, ProviderKind};
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY, SCHEDULING_PLAN_KEY,
};
use restate_types::net::codec::{Targeted, WireDecode};
use restate_types::net::metadata::MetadataKind;
use restate_types::net::AdvertisedAddress;
use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role};
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::node::Message;
use restate_types::{GenerationalNodeId, Version};

use crate::metadata_store::{MetadataStoreClient, Precondition};
use crate::network::{
    ConnectionManager, FailingConnector, Incoming, MessageHandler, MessageRouterBuilder,
    NetworkError, Networking, ProtocolError, TransportConnect,
};
use crate::{spawn_metadata_manager, MetadataBuilder, TaskId};
use crate::{Metadata, MetadataManager, MetadataWriter};
use crate::{TaskCenter, TaskCenterBuilder};

pub struct TestCoreEnvBuilder<T> {
    pub tc: TaskCenter,
    pub my_node_id: GenerationalNodeId,
    pub metadata_manager: MetadataManager<T>,
    pub metadata_writer: MetadataWriter,
    pub metadata: Metadata,
    pub networking: Networking<T>,
    pub nodes_config: NodesConfiguration,
    pub provider_kind: ProviderKind,
    pub router_builder: MessageRouterBuilder,
    pub partition_table: PartitionTable,
    pub scheduling_plan: SchedulingPlan,
    pub metadata_store_client: MetadataStoreClient,
}

impl TestCoreEnvBuilder<FailingConnector> {
    pub fn with_incoming_only_connector() -> Self {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .ingress_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let metadata_builder = MetadataBuilder::default();
        let net_opts = NetworkingOptions::default();
        let connection_manager =
            ConnectionManager::new_incoming_only(metadata_builder.to_metadata());
        let networking = Networking::with_connection_manager(
            metadata_builder.to_metadata(),
            net_opts,
            connection_manager,
        );

        TestCoreEnvBuilder::with_networking(tc, networking, metadata_builder)
    }
}
impl<T: TransportConnect> TestCoreEnvBuilder<T> {
    pub fn with_transport_connector(tc: TaskCenter, connector: Arc<T>) -> TestCoreEnvBuilder<T> {
        let metadata_builder = MetadataBuilder::default();
        let net_opts = NetworkingOptions::default();
        let connection_manager =
            ConnectionManager::new(metadata_builder.to_metadata(), connector, net_opts.clone());
        let networking = Networking::with_connection_manager(
            metadata_builder.to_metadata(),
            net_opts,
            connection_manager,
        );

        TestCoreEnvBuilder::with_networking(tc, networking, metadata_builder)
    }

    pub fn with_networking(
        tc: TaskCenter,
        networking: Networking<T>,
        metadata_builder: MetadataBuilder,
    ) -> Self {
        let my_node_id = GenerationalNodeId::new(1, 1);
        let metadata_store_client = MetadataStoreClient::new_in_memory();
        let metadata = metadata_builder.to_metadata();
        let metadata_manager = MetadataManager::new(
            metadata_builder,
            networking.clone(),
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
            metadata_manager,
            metadata_writer,
            metadata,
            networking,
            nodes_config,
            router_builder,
            partition_table,
            scheduling_plan,
            metadata_store_client,
            provider_kind,
        }
    }

    pub fn set_nodes_config(mut self, nodes_config: NodesConfiguration) -> Self {
        self.nodes_config = nodes_config;
        self
    }

    pub fn set_partition_table(mut self, partition_table: PartitionTable) -> Self {
        self.partition_table = partition_table;
        self
    }

    pub fn set_scheduling_plan(mut self, scheduling_plan: SchedulingPlan) -> Self {
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

    pub async fn build(mut self) -> TestCoreEnv<T> {
        self.metadata_manager
            .register_in_message_router(&mut self.router_builder);
        self.networking
            .connection_manager()
            .set_message_router(self.router_builder.build());

        let metadata_manager_task = spawn_metadata_manager(&self.tc, self.metadata_manager)
            .expect("metadata manager should start");

        self.metadata_store_client
            .put(
                NODES_CONFIG_KEY.clone(),
                &self.nodes_config,
                Precondition::None,
            )
            .await
            .expect("to store nodes config in metadata store");
        self.metadata_writer.submit(self.nodes_config.clone());

        let logs = bootstrap_logs_metadata(
            self.provider_kind,
            None,
            self.partition_table.num_partitions(),
        );
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
            metadata: self.metadata,
            metadata_manager_task,
            metadata_writer: self.metadata_writer,
            networking: self.networking,
            metadata_store_client: self.metadata_store_client,
        }
    }
}

// This might need to be moved to a better place in the future.
pub struct TestCoreEnv<T> {
    pub tc: TaskCenter,
    pub metadata: Metadata,
    pub metadata_writer: MetadataWriter,
    pub networking: Networking<T>,
    pub metadata_manager_task: TaskId,
    pub metadata_store_client: MetadataStoreClient,
}

impl TestCoreEnv<FailingConnector> {
    pub async fn create_with_single_node(node_id: u32, generation: u32) -> Self {
        TestCoreEnvBuilder::with_incoming_only_connector()
            .set_my_node_id(GenerationalNodeId::new(node_id, generation))
            .add_mock_nodes_config()
            .build()
            .await
    }
}

impl<T: TransportConnect> TestCoreEnv<T> {
    pub async fn accept_incoming_connection<S>(
        &self,
        incoming: S,
    ) -> Result<impl Stream<Item = Message> + Unpin + Send + 'static, NetworkError>
    where
        S: Stream<Item = Result<Message, ProtocolError>> + Unpin + Send + 'static,
    {
        self.networking
            .connection_manager()
            .accept_incoming_connection(incoming)
            .await
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
