// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enumset::EnumSet;

use restate_types::RestateVersion;
use restate_types::cluster_state::NodeState;
use restate_types::{
    PlainNodeId, Version,
    nodes_config::{NodeConfig, Role},
};

use super::schema::NodeBuilder;

pub(crate) fn append_node_row(
    builder: &mut NodeBuilder,
    ver: Version,
    node_id: PlainNodeId,
    node_config: &NodeConfig,
    node_state: NodeState,
) {
    let mut row = builder.row();

    row.nodes_configuration_version(ver.into());

    row.fmt_plain_node_id(node_id);
    row.fmt_gen_node_id(node_config.current_generation);
    row.fmt_state(node_state);
    row.fmt_name(&node_config.name);
    row.fmt_address(&node_config.address);
    row.fmt_ctrl_address(node_config.ctrl_address());
    row.fmt_location(&node_config.location);
    row.fmt_binary_version(
        node_config
            .binary_version
            .as_ref()
            .unwrap_or(&RestateVersion::unknown()),
    );

    let all: EnumSet<Role> = EnumSet::all();
    for role in all {
        match role {
            Role::Admin => {
                row.has_admin_role(node_config.has_role(role));
            }
            Role::Worker => {
                row.has_worker_role(node_config.has_role(role));
                if node_config.has_role(role) {
                    row.fmt_worker_state(node_config.worker_config.worker_state);
                }
            }
            Role::LogServer => {
                row.has_log_server_role(node_config.has_role(role));
                if node_config.has_role(role) {
                    row.fmt_storage_state(node_config.log_server_config.storage_state);
                }
            }
            Role::MetadataServer => {
                row.has_metadata_server_role(node_config.has_role(role));
                if node_config.has_role(role) {
                    row.fmt_metadata_server_state(
                        node_config.metadata_server_config.metadata_server_state,
                    );
                }
            }
            Role::HttpIngress => {
                row.has_ingress_role(node_config.has_role(role));
            }
        }
    }
}
