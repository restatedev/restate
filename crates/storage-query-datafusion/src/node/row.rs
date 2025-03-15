// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::NodeBuilder;
use crate::table_util::format_using;
use enumset::EnumSet;
use restate_types::{
    PlainNodeId,
    nodes_config::{NodeConfig, Role},
};

#[inline]
pub(crate) fn append_node_row(
    builder: &mut NodeBuilder,
    output: &mut String,
    node_id: PlainNodeId,
    node_config: &NodeConfig,
) {
    let mut row = builder.row();
    row.id(node_id.into());
    row.generation(node_config.current_generation.generation());
    row.name(format_using(output, &node_config.name));
    row.address(format_using(output, &node_config.address));
    row.location(format_using(output, &node_config.location));

    let all: EnumSet<Role> = EnumSet::all();
    for role in all {
        match role {
            Role::Admin => {
                row.admin_role(node_config.has_role(role));
            }
            Role::Worker => {
                row.worker_role(node_config.has_role(role));
            }
            Role::LogServer => {
                row.log_server_role(node_config.has_role(role));
                if node_config.has_role(role) {
                    row.storage_state(format_using(
                        output,
                        &node_config.log_server_config.storage_state,
                    ));
                }
            }
            Role::MetadataServer => {
                row.metadata_server_role(node_config.has_role(role));
                if node_config.has_role(role) {
                    row.metadata_server_state(format_using(
                        output,
                        &node_config.metadata_server_config.metadata_server_state,
                    ));
                }
            }
            Role::HttpIngress => {
                row.ingress_role(node_config.has_role(role));
            }
        }
    }
}
