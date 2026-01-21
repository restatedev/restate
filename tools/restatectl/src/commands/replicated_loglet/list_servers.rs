// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;

use itertools::Itertools;
use restate_cli_util::_comfy_table::{Cell, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_types::Versioned;
use restate_types::logs::metadata::ProviderKind;
use restate_types::nodes_config::Role;
use restate_types::replicated_loglet::ReplicatedLogletParams;

use super::render_storage_state;
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "list_servers")]
#[clap(visible_alias = "servers", visible_alias = "nodes")]
pub struct ListServersOpts;

async fn list_servers(connection: &ConnectionInfo) -> anyhow::Result<()> {
    let nodes_config = connection.get_nodes_configuration().await?;
    let logs = connection.get_logs().await?;

    let mut servers_table = Table::new_styled();
    let header = vec![
        "NODE",
        "GEN",
        "STORAGE-STATE",
        "HISTORICAL-LOGLETS",
        "ACTIVE-LOGLETS",
    ];
    servers_table.set_styled_header(header);
    for (node_id, config) in nodes_config.iter().sorted_by(|a, b| Ord::cmp(&a.0, &b.0)) {
        if !config.has_role(Role::LogServer)
            && config.log_server_config.storage_state.is_provisioning()
        {
            continue;
        }
        let count_loglets = logs
            .iter_replicated_loglets()
            .filter(|(_, loglet)| loglet.params.nodeset.contains(node_id))
            .count();
        let count_active = logs
            .iter_writeable()
            .filter(|(_, segment)| {
                if segment.config.kind != ProviderKind::Replicated {
                    return false;
                }
                let params =
                    ReplicatedLogletParams::deserialize_from(segment.config.params.as_bytes())
                        .expect("loglet config is deserializable");
                params.nodeset.contains(node_id)
            })
            .count();
        servers_table.add_row(vec![
            Cell::new(node_id.to_string()),
            Cell::new(config.current_generation.to_string()),
            render_storage_state(config.log_server_config.storage_state),
            Cell::new(count_loglets),
            Cell::new(count_active),
        ]);
    }
    c_println!("Node configuration {}", nodes_config.version());
    c_println!("Log chain {}", logs.version());
    c_println!("{}", servers_table);

    Ok(())
}
