// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::nodes_config::{
    LogServerConfig, NodeConfig, NodesConfiguration, Role, StorageState,
};
use restate_types::{GenerationalNodeId, PlainNodeId, Version};
use tempfile::TempDir; // Use `TempDir` for sharing the same directory

pub fn generate_logserver_node(
    id: impl Into<PlainNodeId>,
    storage_state: StorageState,
    temp_dir: &TempDir, // Accept a reference to the temporary directory
) -> NodeConfig {
    let id: PlainNodeId = id.into();

    // Construct the socket path within the shared temporary directory
    let socket_path = temp_dir.path().join(format!("my_socket-{}", id));

    // Cleanup: Remove the socket file if it already exists
    NodeConfig::new(
        format!("node-{}", id),
        GenerationalNodeId::new(id.into(), 1),
        format!("unix:{}", socket_path.display()).parse().unwrap(),
        Role::LogServer.into(),
        LogServerConfig { storage_state },
    )
}

pub fn generate_logserver_nodes_config(
    num_nodes: u32,
    storage_state: StorageState,
    temp_dir: &TempDir, // Accept a reference to the temporary directory
) -> NodesConfiguration {
    let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());

    // Insert each node into the nodes configuration, sharing the same temp directory
    for i in 1..=num_nodes {
        nodes_config.upsert_node(generate_logserver_node(i, storage_state, temp_dir));
    }
    nodes_config
}
