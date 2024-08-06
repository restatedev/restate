// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use anyhow::Context;
use cling::prelude::*;
use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::ClusterStateRequest;
use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::Tense;

use crate::app::ConnectionInfo;
use crate::commands::display_util;
use crate::util::grpc_connect;
use restate_cli_util::{c_println, c_title};
use restate_types::protobuf::cluster::{node_state, AliveNode, DeadNode, RunMode};
use restate_types::PlainNodeId;
use tonic::codec::CompressionEncoding;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "nodes")]
#[cling(run = "list_nodes")]
pub struct ListNodesOpts {}

async fn list_nodes(connection: &ConnectionInfo, _opts: &ListNodesOpts) -> anyhow::Result<()> {
    let channel = grpc_connect(connection.cluster_controller.clone())
        .await
        .with_context(|| {
            format!(
                "cannot connect to cluster controller at {}",
                connection.cluster_controller
            )
        })?;
    let mut client =
        ClusterCtrlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let req = ClusterStateRequest::default();
    let state = client
        .get_cluster_state(req)
        .await?
        .into_inner()
        .cluster_state
        .ok_or_else(|| anyhow::anyhow!("no cluster state returned"))?;

    let mut nodes: BTreeMap<PlainNodeId, AliveNode> = BTreeMap::new();
    let mut dead_nodes: BTreeMap<PlainNodeId, DeadNode> = BTreeMap::new();
    for (node_id, node_state) in state.nodes {
        match node_state.state.expect("node state is set") {
            node_state::State::Alive(alive_node) => {
                nodes.insert(PlainNodeId::from(node_id), alive_node);
            }
            node_state::State::Dead(dead_node) => {
                dead_nodes.insert(PlainNodeId::from(node_id), dead_node);
            }
        }
    }

    let mut nodes_table = Table::new_styled();
    nodes_table.set_styled_header(vec![
        "NODE",
        "LEADER",
        "FOLLOWER",
        "UNKNOWN",
        "LAST REFRESH",
    ]);
    for (node_id, details) in nodes {
        let (leader_partitions, follower_partitions, unknown) = details.partitions.iter().fold(
            (0, 0, 0),
            |(mut leader, mut follower, mut unknown), (_, status)| {
                match status.effective_mode() {
                    RunMode::Leader => leader += 1,
                    RunMode::Follower => follower += 1,
                    RunMode::Unknown => unknown += 1,
                }
                (leader, follower, unknown)
            },
        );
        let header = vec![
            Cell::new(node_id),
            Cell::new(leader_partitions)
                .fg(Color::Green)
                .add_attribute(Attribute::Bold),
            Cell::new(follower_partitions).fg(Color::DarkBlue),
            match unknown {
                0 => Cell::new(unknown).fg(Color::DarkGrey),
                _ => Cell::new(unknown)
                    .fg(Color::Red)
                    .add_attribute(Attribute::Bold),
            },
            display_util::render_as_duration(details.last_heartbeat_at, Tense::Past),
        ];
        nodes_table.add_row(header);
    }
    c_println!("{}", nodes_table);

    if !dead_nodes.is_empty() {
        c_title!("☠️", "DEAD NODES");
        let mut dead_nodes_table = Table::new_styled();
        dead_nodes_table.set_styled_header(vec!["NODE", "LAST SEEN ALIVE"]);
        for (node_id, dead_node) in dead_nodes {
            dead_nodes_table.add_row(vec![
                Cell::new(node_id),
                display_util::render_as_duration(dead_node.last_seen_alive, Tense::Past),
            ]);
        }
        c_println!("{}", dead_nodes_table);
    }

    Ok(())
}
