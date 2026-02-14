// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use futures::Stream;

use restate_metadata_store::MetadataStoreClient;
use restate_types::logs::metadata::{ProviderKind, bootstrap_logs_metadata};
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY,
};
use restate_types::net::Service;
use restate_types::net::address::AdvertisedAddress;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::partition_table::PartitionTable;
use restate_types::{GenerationalNodeId, RestateVersion, Version};

use restate_memory::MemoryPool;

use crate::network::protobuf::network::Message;
use crate::network::{
    AcceptError, BackPressureMode, FailingConnector, Handler, MessageRouterBuilder, Networking,
    TransportConnect,
};
use crate::{Metadata, MetadataManager, MetadataWriter};
use crate::{MetadataBuilder, TaskId, spawn_metadata_manager};
use crate::{TaskCenter, TaskKind};

pub struct TestCoreEnvBuilder<T> {
    pub my_node_id: GenerationalNodeId,
    pub metadata_manager: MetadataManager,
    pub metadata_writer: MetadataWriter,
    pub metadata: Metadata,
    pub networking: Networking<T>,
    pub nodes_config: NodesConfiguration,
    pub provider_kind: ProviderKind,
    pub router_builder: MessageRouterBuilder,
    pub partition_table: PartitionTable,
    pub metadata_store_client: MetadataStoreClient,
}

impl TestCoreEnvBuilder<FailingConnector> {
    pub fn with_incoming_only_connector() -> Self {
        let networking = Networking::new_incoming_only();
        TestCoreEnvBuilder::with_networking(networking)
    }
}
impl<T: TransportConnect> TestCoreEnvBuilder<T> {
    pub fn with_transport_connector(connector: T) -> TestCoreEnvBuilder<T> {
        let networking = Networking::with_connector(connector);
        TestCoreEnvBuilder::with_networking(networking)
    }

    pub fn with_networking(networking: Networking<T>) -> Self {
        let my_node_id = GenerationalNodeId::new(1, 1);
        let metadata_store_client = MetadataStoreClient::new_in_memory();
        let metadata_builder = MetadataBuilder::default();
        let metadata = metadata_builder.to_metadata();
        let metadata_manager =
            MetadataManager::new(metadata_builder, metadata_store_client.clone());
        let metadata_writer = metadata_manager.writer();
        let router_builder = MessageRouterBuilder::with_default_pool(MemoryPool::unlimited());
        let nodes_config = NodesConfiguration::new_for_testing();
        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 10);
        TaskCenter::try_set_global_metadata(metadata.clone());

        // Use memory-loglet as a default if in test-mode
        #[cfg(any(test, feature = "test-util"))]
        let provider_kind = ProviderKind::InMemory;
        #[cfg(not(any(test, feature = "test-util")))]
        let provider_kind = ProviderKind::Local;

        TestCoreEnvBuilder {
            my_node_id,
            metadata_manager,
            metadata_writer,
            metadata,
            networking,
            nodes_config,
            router_builder,
            partition_table,
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
            create_mock_nodes_config(self.my_node_id.raw_id(), self.my_node_id.generation());
        self
    }

    pub fn register_buffered_service<H, S>(
        mut self,
        pool: MemoryPool,
        backpressure: BackPressureMode,
        handler: H,
    ) -> Self
    where
        H: Handler<Service = S> + Send + Sync + 'static,
        S: Service,
    {
        let buffered = self
            .router_builder
            .register_buffered_service_with_pool(pool, backpressure);
        buffered
            .start(TaskKind::NetworkMessageHandler, "service-handler", handler)
            .unwrap();
        self
    }

    pub async fn build(mut self) -> TestCoreEnv<T> {
        self.metadata_manager
            .register_in_message_router(&mut self.router_builder);
        self.networking
            .connection_manager()
            .set_message_router(self.router_builder.build());

        let metadata_manager_task =
            spawn_metadata_manager(self.metadata_manager).expect("metadata manager should start");

        self.metadata_store_client
            .put(
                NODES_CONFIG_KEY.clone(),
                &self.nodes_config,
                Precondition::None,
            )
            .await
            .expect("to store nodes config in metadata store");
        self.metadata_writer
            .submit(Arc::new(self.nodes_config.clone()));

        let logs = bootstrap_logs_metadata(
            self.provider_kind,
            None,
            self.partition_table.num_partitions(),
        );
        self.metadata_store_client
            .put(BIFROST_CONFIG_KEY.clone(), &logs, Precondition::None)
            .await
            .expect("to store bifrost config in metadata store");
        self.metadata_writer.submit(Arc::new(logs));

        self.metadata_store_client
            .put(
                PARTITION_TABLE_KEY.clone(),
                &self.partition_table,
                Precondition::None,
            )
            .await
            .expect("to store partition table in metadata store");
        self.metadata_writer.submit(Arc::new(self.partition_table));

        let _ = self
            .metadata
            .wait_for_version(
                MetadataKind::NodesConfiguration,
                self.nodes_config.version(),
            )
            .await
            .unwrap();

        self.metadata_writer.set_my_node_id(self.my_node_id);

        TestCoreEnv {
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
    ) -> Result<impl Stream<Item = Message> + Unpin + Send + 'static, AcceptError>
    where
        S: Stream<Item = Message> + Unpin + Send + 'static,
    {
        self.networking
            .connection_manager()
            .accept_incoming_connection(incoming)
            .await
    }
}

pub fn create_mock_nodes_config(node_id: u32, generation: u32) -> NodesConfiguration {
    let mut nodes_config = NodesConfiguration::new_for_testing();
    let address = AdvertisedAddress::default();
    let node_id = GenerationalNodeId::new(node_id, generation);
    let roles = Role::Admin | Role::Worker;
    let my_node = NodeConfig::builder()
        .name(format!("MyNode-{node_id}"))
        .current_generation(node_id)
        .address(address)
        .roles(roles)
        .binary_version(RestateVersion::current())
        .build();
    nodes_config.upsert_node(my_node);
    nodes_config
}

/// No-op message handler which simply ignores the received messages. Useful if you don't want to
/// react to network messages.
pub struct NoOpMessageHandler<S: Service> {
    phantom_data: PhantomData<S>,
}

impl<S: Service> Default for NoOpMessageHandler<S> {
    fn default() -> Self {
        NoOpMessageHandler {
            phantom_data: PhantomData,
        }
    }
}

impl<S: Service> Handler for NoOpMessageHandler<S> {
    type Service = S;
}
