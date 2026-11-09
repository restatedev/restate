// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use bytesize::ByteSize;
use clap::Parser;
use cling::{Collect, Run};
use futures::future::join_all;
use itertools::Itertools;
use tonic::{IntoRequest, Status};
use tracing::debug;

use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println};
use restate_metadata_server_grpc::grpc::new_metadata_server_client;
use restate_types::nodes_config::Role;
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::{PlainNodeId, Version};

use crate::connection::ConnectionInfo;
use crate::observe::ClusterView;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "list_metadata_servers")]
#[clap(visible_alias = "ls")]
pub struct ListMetadataServers {}

pub async fn list_metadata_servers(
    connection: &ConnectionInfo,
    _list_metadata_servers: &ListMetadataServers,
) -> anyhow::Result<()> {
    debug!("Gathering metadata server status information");

    let nodes_configuration = connection.get_nodes_configuration().await?;
    let mut metadata_nodes_table = Table::new_styled();
    let header = vec![
        "NODE",
        "STATUS",
        "VERSION",
        "LEADER",
        "MEMBERS",
        "APPLIED",
        "COMMITTED",
        "TERM",
        "LOG-LENGTH",
        "SNAP-INDEX",
        "SNAP-SIZE",
    ];
    metadata_nodes_table.set_styled_header(header);

    let mut unreachable_nodes = BTreeMap::default();

    let futures =
        nodes_configuration
            .iter_role(Role::MetadataServer)
            .map(|(node_id, node_config)| {
                let address = connection
                    .ctrl_address_for(&nodes_configuration, node_config)
                    .into_owned();
                async move {
                    let channel = match connection.connect(&address).await {
                        Ok(channel) => channel,
                        Err(conn_error) => {
                            return (node_id, Err(Status::from_error(Box::new(conn_error))));
                        }
                    };

                    debug!("Querying metadata service status on node {address}");
                    let metadata_store_status =
                        new_metadata_server_client(channel, &CliContext::get().network)
                            .status(().into_request())
                            .await;

                    (node_id, metadata_store_status)
                }
            });
    let results = join_all(futures)
        .await
        .into_iter()
        .collect::<BTreeMap<_, _>>();

    for (node_id, metadata_store_status) in results {
        let status = match metadata_store_status {
            Ok(response) => response.into_inner(),
            Err(err) => {
                unreachable_nodes.insert(node_id, err.to_string());
                continue;
            }
        };

        metadata_nodes_table.add_row(vec![
            Cell::new(node_id),
            render_metadata_server_status(status.status()),
            Cell::new(
                status
                    .configuration
                    .as_ref()
                    .and_then(|config| config.version.map(Version::from))
                    .unwrap_or(Version::INVALID),
            ),
            Cell::new(
                status
                    .leader
                    .map(|leader_id| PlainNodeId::new(leader_id).to_string())
                    .unwrap_or_else(|| "-".to_owned()),
            ),
            Cell::new(
                status
                    .configuration
                    .map(|config| {
                        format!(
                            "[{}]",
                            config
                                .members
                                .into_keys()
                                .map(PlainNodeId::from)
                                .sorted()
                                .map(|node_id| node_id.to_string())
                                .join(",")
                        )
                    })
                    .unwrap_or_else(|| "[]".to_owned()),
            ),
            Cell::new(status.raft.map(|raft| raft.applied).unwrap_or_default()),
            Cell::new(status.raft.map(|raft| raft.committed).unwrap_or_default()),
            Cell::new(status.raft.map(|raft| raft.term).unwrap_or_default()),
            // first and last index are inclusive
            Cell::new(
                status
                    .raft
                    .map(|raft| (raft.last_index + 1) - raft.first_index)
                    .unwrap_or_default(),
            ),
            Cell::new(
                status
                    .snapshot
                    .map(|snapshot| snapshot.index)
                    .unwrap_or_default(),
            ),
            Cell::new(
                ByteSize::b(
                    status
                        .snapshot
                        .map(|snapshot| snapshot.size)
                        .unwrap_or_default(),
                )
                .display()
                .iec()
                .to_string(),
            ),
        ]);
    }

    c_println!("Metadata service");
    c_println!("{}", metadata_nodes_table);

    if !unreachable_nodes.is_empty() {
        c_println!();
        c_println!("🔌 Unreachable nodes");
        let mut unreachable_nodes_table = Table::new_styled();
        unreachable_nodes_table.set_styled_header(vec!["NODE", "REASON"]);

        for (node_id, reason) in unreachable_nodes {
            unreachable_nodes_table.add_row(vec![Cell::new(node_id), Cell::new(reason)]);
        }

        c_println!("{}", unreachable_nodes_table);
    }

    Ok(())
}

pub fn render_metadata_server_status(metadata_server_status: MetadataServerStatus) -> Cell {
    match metadata_server_status {
        MetadataServerStatus::Unknown => Cell::new("UNKNOWN").fg(Color::Red),
        MetadataServerStatus::StartingUp => Cell::new("Starting").fg(Color::Yellow),
        MetadataServerStatus::AwaitingProvisioning => Cell::new("Provisioning"),
        MetadataServerStatus::Member => Cell::new("Member").fg(Color::Green),
        MetadataServerStatus::Standby => Cell::new("Standby"),
    }
}

/// The metadata servers' status as captured in `view`.
pub fn metadata_servers_table(view: &ClusterView) -> Table {
    let mut table = Table::new_styled();
    table.set_styled_header(vec![
        "NODE",
        "STATUS",
        "VERSION",
        "LEADER",
        "MEMBERS",
        "APPLIED",
        "COMMITTED",
        "TERM",
        "LOG-LENGTH",
        "SNAP-INDEX",
        "SNAP-SIZE",
    ]);
    for node in &view.nodes {
        let Some(server) = &node.metadata_server else {
            continue;
        };
        let status = match (&server.error, server.status.as_deref()) {
            (Some(err), _) => Cell::new(err).fg(Color::Red),
            (None, Some("member")) => Cell::new("member").fg(Color::Green),
            (None, Some(status @ "standby")) => Cell::new(status),
            (None, Some(status)) => Cell::new(status).fg(Color::Yellow),
            (None, None) => Cell::new(&server.state),
        };
        let raft = server.raft.as_ref();
        table.add_row(vec![
            Cell::new(node.plain()),
            status,
            Cell::new(
                server
                    .config_version
                    .map(|v| format!("v{v}"))
                    .unwrap_or_else(|| "-".to_owned()),
            ),
            Cell::new(
                server
                    .leader
                    .map(|l| l.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
            ),
            Cell::new(format!("[{}]", server.members.iter().join(","))),
            Cell::new(raft.map(|r| r.applied).unwrap_or_default()),
            Cell::new(raft.map(|r| r.committed).unwrap_or_default()),
            Cell::new(raft.map(|r| r.term).unwrap_or_default()),
            Cell::new(raft.map(|r| r.log_length).unwrap_or_default()),
            Cell::new(raft.map(|r| r.snapshot_index).unwrap_or_default()),
            Cell::new(
                ByteSize::b(raft.map(|r| r.snapshot_size).unwrap_or_default())
                    .display()
                    .iec()
                    .to_string(),
            ),
        ]);
    }
    table
}
