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
    PlainNodeId, Version,
    nodes_config::{NodeConfig, Role},
};

#[inline]
pub(crate) fn append_node_row(
    builder: &mut NodeBuilder,
    output: &mut String,
    ver: Version,
    node_id: PlainNodeId,
    node_config: &NodeConfig,
) {
    let mut row = builder.row();

    row.metadata_ver(ver.into());

    row.plain_node_id(format_using(output, &node_id));
    row.gen_node_id(format_using(output, &node_config.current_generation));
    row.name(format_using(output, &node_config.name));
    row.address(format_using(output, &node_config.address));
    row.location(format_using(output, &node_config.location));

    let all: EnumSet<Role> = EnumSet::all();
    for role in all {
        match role {
            Role::Admin => {
                row.has_admin_role(node_config.has_role(role));
            }
            Role::Worker => {
                row.has_worker_role(node_config.has_role(role));
            }
            Role::LogServer => {
                row.has_log_server_role(node_config.has_role(role));
                if node_config.has_role(role) {
                    row.storage_state(format_using(
                        output,
                        &node_config.log_server_config.storage_state,
                    ));
                }
            }
            Role::MetadataServer => {
                row.has_metadata_server_role(node_config.has_role(role));
                if node_config.has_role(role) {
                    row.metadata_server_state(format_using(
                        output,
                        &node_config.metadata_server_config.metadata_server_state,
                    ));
                }
            }
            Role::HttpIngress => {
                row.has_ingress_role(node_config.has_role(role));
            }
        }
    }
}
