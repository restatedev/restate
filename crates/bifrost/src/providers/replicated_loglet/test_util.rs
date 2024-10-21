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
use tempfile::tempdir;

pub fn generate_logserver_node(
    id: impl Into<PlainNodeId>,
    storage_state: StorageState,
) -> NodeConfig {
    let id: PlainNodeId = id.into();

    // Create a temporary directory using `tempfile`, but keep control of the socket filename
    let temp_dir = tempdir().expect("Failed to create a temporary directory");

    // Construct the full socket path with the desired format
    let socket_path = temp_dir.path().join(format!("my_socket-{}", id));

    // Generate the NodeConfig with the desired socket path format
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
) -> NodesConfiguration {
    let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
    // all_authoritative
    for i in 1..=num_nodes {
        nodes_config.upsert_node(generate_logserver_node(i, storage_state));
    }
    nodes_config
}
