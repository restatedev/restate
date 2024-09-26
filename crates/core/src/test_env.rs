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

use enumset::EnumSet;
use futures::Stream;

use restate_types::cluster_controller::ReplicationStrategy;
use restate_types::config::NetworkingOptions;
use restate_types::logs::metadata::ProviderKind;
use restate_types::net::codec::{Targeted, WireDecode};
use restate_types::net::AdvertisedAddress;
use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role};
use restate_types::protobuf::node::Message;
use restate_types::retries::RetryPolicy;
use restate_types::{GenerationalNodeId, NodeId, Version};

use crate::metadata_store::MetadataStoreClient;
use crate::network::{
    ConnectionManager, FailingConnector, Incoming, MessageHandler, NetworkError, Networking,
    ProtocolError, TransportConnect,
};
use crate::{CoreBuilder, CoreBuilderFinal, MetadataBuilder, StartedCore};
use crate::{TaskCenter, TaskCenterBuilder};

pub type TestCoreEnvBuilder<T> = CoreBuilderFinal<T>;

impl CoreBuilderFinal<FailingConnector> {
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

        Self::with_networking(tc, networking, metadata_builder)
    }
}
impl<T: TransportConnect> CoreBuilderFinal<T> {
    pub fn with_transport_connector(tc: TaskCenter, connector: Arc<T>) -> Self {
        let metadata_builder = MetadataBuilder::default();
        let net_opts = NetworkingOptions::default();
        let connection_manager =
            ConnectionManager::new(metadata_builder.to_metadata(), connector, net_opts.clone());
        let networking = Networking::with_connection_manager(
            metadata_builder.to_metadata(),
            net_opts,
            connection_manager,
        );

        Self::with_networking(tc, networking, metadata_builder)
    }

    pub fn with_networking(
        tc: TaskCenter,
        networking: Networking<T>,
        metadata_builder: MetadataBuilder,
    ) -> Self {
        // Use memory-loglet as a default if in test-mode
        #[cfg(any(test, feature = "test-util"))]
        let provider_kind = ProviderKind::InMemory;
        #[cfg(not(any(test, feature = "test-util")))]
        let provider_kind = ProviderKind::Local;

        CoreBuilder::with_metadata_store_client(MetadataStoreClient::new_in_memory())
            .with_nodes_config(
                NodesConfiguration::new(Version::MIN, "test-cluster".to_owned()),
                "node-1".to_string(),
                Some(NodeId::new_generational(1, 1)),
                AdvertisedAddress::from_str("http://127.0.0.1:5122/").unwrap(),
                EnumSet::EMPTY,
            )
            .with_networking(networking, metadata_builder)
            .with_tc(tc)
            .set_num_partitions(10)
            .set_replication_strategy(ReplicationStrategy::OnAllNodes)
            .set_provider_kind(provider_kind)
            .set_allow_bootstrap(true)
    }

    pub fn set_my_node_id(self, my_node_id: GenerationalNodeId) -> Self {
        self.set_force_node_id(NodeId::Generational(my_node_id))
    }

    pub fn add_mock_nodes_config(mut self, node_id: GenerationalNodeId) -> Self {
        let node_config = create_mock_node_config(node_id.raw_id(), node_id.raw_generation());
        self.force_node_id = Some(NodeId::Generational(node_id));
        self.nodes_config.upsert_node(node_config.clone());
        self.node_name = node_config.name;
        self.advertise_address = node_config.address;
        self.roles = node_config.roles;
        self
    }

    pub fn add_message_handler<H>(mut self, handler: H) -> Self
    where
        H: MessageHandler + Send + Sync + 'static,
    {
        self.router_builder.add_message_handler(handler);
        self
    }

    pub async fn build(self) -> StartedCore<T> {
        let core = self.build_router();
        core.start(RetryPolicy::None)
            .await
            .expect("Core start to succeed")
    }
}

pub type TestCoreEnv<T> = StartedCore<T>;

impl StartedCore<FailingConnector> {
    pub async fn create_with_single_node(node_id: u32, generation: u32) -> Self {
        CoreBuilderFinal::with_incoming_only_connector()
            .add_mock_nodes_config(GenerationalNodeId::new(node_id, generation))
            .build()
            .await
    }
}

impl<T: TransportConnect> StartedCore<T> {
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

pub fn create_mock_node_config(node_id: u32, generation: u32) -> NodeConfig {
    let address = AdvertisedAddress::from_str("http://127.0.0.1:5122/").unwrap();
    let node_id = GenerationalNodeId::new(node_id, generation);
    let roles = Role::Admin | Role::Worker;
    NodeConfig::new(
        format!("MyNode-{}", node_id),
        node_id,
        address,
        roles,
        LogServerConfig::default(),
    )
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
