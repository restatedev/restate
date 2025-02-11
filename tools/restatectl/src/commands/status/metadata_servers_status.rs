// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use itertools::Itertools;
use tonic::codec::CompressionEncoding;
use tonic::IntoRequest;

use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_metadata_server::grpc::metadata_server_svc_client::MetadataServerSvcClient;
use restate_types::nodes_config::Role;
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::{PlainNodeId, Version};

use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;

pub async fn list_metadata_servers(connection: &ConnectionInfo) -> anyhow::Result<()> {
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

    for (node_id, node_config) in nodes_configuration.iter_role(Role::MetadataServer) {
        let metadata_channel = grpc_channel(node_config.address.clone());
        let mut metadata_client = MetadataServerSvcClient::new(metadata_channel)
            .accept_compressed(CompressionEncoding::Gzip);

        let metadata_store_status = metadata_client.status(().into_request()).await;

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
                    .unwrap_or("-".to_owned()),
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
                    .unwrap_or("[]".to_owned()),
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
            Cell::new(bytesize::to_string(
                status
                    .snapshot
                    .map(|snapshot| snapshot.size)
                    .unwrap_or_default(),
                true,
            )),
        ]);
    }

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

fn render_metadata_server_status(metadata_server_status: MetadataServerStatus) -> Cell {
    match metadata_server_status {
        MetadataServerStatus::Unknown => Cell::new("UNKNOWN").fg(Color::Red),
        MetadataServerStatus::StartingUp => Cell::new("Starting").fg(Color::Yellow),
        MetadataServerStatus::AwaitingProvisioning => Cell::new("Provisioning"),
        MetadataServerStatus::Member => Cell::new("Member").fg(Color::Green),
        MetadataServerStatus::Standby => Cell::new("Standby"),
    }
}
