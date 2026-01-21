// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_types::{GenerationalNodeId, PlainNodeId};

pub fn generate_logserver_node(
    id: impl Into<PlainNodeId>,
    storage_state: StorageState,
) -> NodeConfig {
    let id: PlainNodeId = id.into();
    NodeConfig::builder()
        .name(format!("node-{id}"))
        .current_generation(GenerationalNodeId::new(id.into(), 1))
        .location(format!("region-{id}").parse().unwrap())
        .address(format!("unix:/tmp/my_socket-{id}").parse().unwrap())
        .roles(Role::LogServer.into())
        .log_server_config(LogServerConfig { storage_state })
        .build()
}

pub fn generate_logserver_nodes_config(
    num_nodes: u32,
    storage_state: StorageState,
) -> NodesConfiguration {
    let mut nodes_config = NodesConfiguration::new_for_testing();
    // all_authoritative
    for i in 1..=num_nodes {
        nodes_config.upsert_node(generate_logserver_node(i, storage_state));
    }
    nodes_config
}
