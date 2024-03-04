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

use restate_node_protocol::codec::{Targeted, WireSerde};
use restate_node_protocol::metadata::MetadataKind;
use restate_types::nodes_config::{AdvertisedAddress, NodeConfig, NodesConfiguration, Role};
use restate_types::{GenerationalNodeId, NodeId, Version};

use crate::network::{NetworkSendError, NetworkSender};
use crate::spawn_metadata_manager;
use crate::{Metadata, MetadataManager, MetadataWriter};
use crate::{TaskCenter, TaskCenterFactory};

#[derive(Clone)]
pub struct MockNetworkSender;

impl NetworkSender for MockNetworkSender {
    async fn send<M>(&self, _to: NodeId, _message: &M) -> Result<(), NetworkSendError>
    where
        M: WireSerde + Targeted + Send + Sync,
    {
        Ok(())
    }
}

// This might need to be moved to a better place in the future.
pub struct TestCoreEnv {
    pub tc: TaskCenter,
    pub metadata: Metadata,
    pub metadata_writer: MetadataWriter,
}

impl TestCoreEnv {
    fn create_empty() -> Self {
        let tc = TaskCenterFactory::create(tokio::runtime::Handle::current());

        let networking = MockNetworkSender;
        let metadata_manager = MetadataManager::build(networking);
        let metadata = metadata_manager.metadata();
        let metadata_writer = metadata_manager.writer();
        tc.try_set_global_metadata(metadata.clone());
        spawn_metadata_manager(&tc, metadata_manager).expect("metadata manager should start");

        Self {
            tc,
            metadata,
            metadata_writer,
        }
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
