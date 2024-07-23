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
use std::time::SystemTime;

use anyhow::Context;
use chrono::{DateTime, Local};
use cling::prelude::*;
use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::ClusterStateRequest;
use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::{timestamp_as_human_duration, Tense};
use restate_types::logs::Lsn;

use restate_cli_util::{c_println, c_title};
use restate_types::protobuf::cluster::{
    node_state, DeadNode, PartitionProcessorStatus, ReplayStatus, RunMode,
};
use restate_types::{GenerationalNodeId, PlainNodeId};
use tonic_0_10::codec::CompressionEncoding;

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "cluster")]
#[cling(run = "dump_cluster_state")]
pub struct ClusterStateOpts {}

struct PartitionDetails {
    host_node: GenerationalNodeId,
    status: PartitionProcessorStatus,
}

async fn dump_cluster_state(
    connection: &ConnectionInfo,
    _opts: &ClusterStateOpts,
) -> anyhow::Result<()> {
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

    let mut processors: BTreeMap<u64, PartitionDetails> = BTreeMap::new();
    let mut dead_nodes: BTreeMap<PlainNodeId, DeadNode> = BTreeMap::new();
    for (node_id, node_state) in state.nodes {
        match node_state.state.expect("node state is set") {
            node_state::State::Dead(dead_node) => {
                dead_nodes.insert(PlainNodeId::from(node_id), dead_node);
            }
            node_state::State::Alive(alive_node) => {
                for (partition_id, status) in alive_node.partitions {
                    let host = alive_node
                        .generational_node_id
                        .as_ref()
                        .expect("alive partition has a node id");
                    let host_node =
                        GenerationalNodeId::new(host.id, host.generation.expect("generation"));
                    let details = PartitionDetails { host_node, status };
                    processors.insert(partition_id, details);
                }
            }
        }
    }
    // Show information organized by partition
    let mut partitions_table = Table::new_styled();
    partitions_table.set_styled_header(vec![
        "P-ID",
        "NODE",
        "RUN MODE",
        "REPLAY",
        "APPLIED LSN",
        "PERSISTED LSN",
        "OBSERVED LEADER",
        "# SKIPS",
        "LAST REFRESH",
    ]);
    for (partition_id, details) in processors {
        partitions_table.add_row(vec![
            Cell::new(partition_id),
            Cell::new(details.host_node),
            render_mode(
                details.status.planned_mode(),
                details.status.effective_mode(),
            ),
            render_replay_status(
                details.status.replay_status(),
                details.status.target_tail_lsn.map(Into::into),
            ),
            Cell::new(
                details
                    .status
                    .last_applied_log_lsn
                    .map(|x| x.to_string())
                    .unwrap_or("??".to_owned()),
            ),
            Cell::new(
                details
                    .status
                    .last_persisted_log_lsn
                    .map(|x| x.to_string())
                    .unwrap_or("??".to_owned()),
            ),
            Cell::new(format!(
                "{} - {}",
                details
                    .status
                    .last_observed_leader_node
                    .map(|x| x.to_string())
                    .unwrap_or("??".to_owned()),
                details
                    .status
                    .last_observed_leader_epoch
                    .map(|x| x.to_string())
                    .unwrap_or("??".to_owned()),
            )),
            Cell::new(details.status.num_skipped_records),
            render_as_duration(details.status.updated_at, Tense::Past),
        ]);
    }
    c_println!("{}", partitions_table);

    if !dead_nodes.is_empty() {
        c_title!("☠️", "DEAD NODES");
        let mut dead_nodes_table = Table::new_styled();
        dead_nodes_table.set_styled_header(vec!["NODE", "LAST SEEN ALIVE"]);
        for (node_id, dead_node) in dead_nodes {
            dead_nodes_table.add_row(vec![
                Cell::new(node_id),
                render_as_duration(dead_node.last_seen_alive, Tense::Past),
            ]);
        }
        c_println!("{}", dead_nodes_table);
    }

    Ok(())
}

fn render_mode(planned: RunMode, effective: RunMode) -> Cell {
    if planned == RunMode::Unknown {
        return Cell::new("UNKNOWN").fg(Color::Red);
    }
    if planned == effective {
        return Cell::new(planned)
            .add_attribute(Attribute::Bold)
            .fg(Color::Green);
    }
    // We are in a transitional state
    Cell::new(format!("{}->{}", effective, planned)).fg(Color::Magenta)
}

fn render_replay_status(status: ReplayStatus, target_lsn: Option<Lsn>) -> Cell {
    match status {
        ReplayStatus::Unknown => Cell::new("UNKNOWN").fg(Color::Red),
        ReplayStatus::Starting => Cell::new("Starting").fg(Color::Yellow),
        ReplayStatus::Active => Cell::new("Active").fg(Color::Green),
        ReplayStatus::CatchingUp => Cell::new(format!(
            "Catching Up ({})",
            target_lsn.map(|x| x.to_string()).unwrap_or("??".to_owned())
        ))
        .fg(Color::Magenta),
    }
}

fn render_as_duration(ts: Option<prost_types_0_12::Timestamp>, tense: Tense) -> Cell {
    let ts: Option<SystemTime> = ts
        .map(TryInto::try_into)
        .transpose()
        .expect("valid timestamp");
    if let Some(ts) = ts {
        let ts: DateTime<Local> = ts.into();
        Cell::new(timestamp_as_human_duration(ts, tense))
    } else {
        Cell::new("??").fg(Color::Red)
    }
}
