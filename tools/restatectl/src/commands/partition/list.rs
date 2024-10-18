// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::PartialOrd;
use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use cling::prelude::*;
use itertools::Itertools;
use tonic::codec::CompressionEncoding;

use crate::app::ConnectionInfo;
use crate::commands::display_util::render_as_duration;
use crate::commands::log::deserialize_replicated_log_params;
use crate::util::grpc_connect;
use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::{ClusterStateRequest, ListLogsRequest};
use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::Tense;
use restate_types::logs::metadata::{Chain, Logs};
use restate_types::logs::{LogId, Lsn};
use restate_types::protobuf::cluster::{
    node_state, DeadNode, PartitionProcessorStatus, ReplayStatus, RunMode,
};
use restate_types::storage::StorageCodec;
use restate_types::{GenerationalNodeId, PlainNodeId, Version};

#[derive(Run, Parser, Collect, Clone, Debug, Default)]
#[cling(run = "list_partitions")]
#[clap(alias = "ls")]
pub struct ListPartitionsOpts {
    /// Sort order
    #[arg(long, short, default_value = "partition")]
    sort: SortMode,
}

#[derive(ValueEnum, Collect, Clone, Debug, Default)]
#[clap(rename_all = "kebab-case")]
enum SortMode {
    /// Order list by partition id
    #[default]
    Partition,
    /// Order list by node id
    Node,
    /// Order list by processor leadership state
    Active,
}

struct PartitionListEntry {
    host_node: GenerationalNodeId,
    status: PartitionProcessorStatus,
}

pub async fn list_partitions(
    connection: &ConnectionInfo,
    opts: &ListPartitionsOpts,
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

    let cluster_state_request = ClusterStateRequest::default();
    let cluster_state = client
        .get_cluster_state(cluster_state_request)
        .await?
        .into_inner()
        .cluster_state
        .ok_or_else(|| anyhow::anyhow!("no cluster state returned"))?;

    // we need the logs to show the current sequencer for each partition's log
    let list_logs_request = ListLogsRequest::default();
    let list_logs_response = client.list_logs(list_logs_request).await?.into_inner();
    let mut buf = list_logs_response.logs;
    let logs = StorageCodec::decode::<Logs, _>(&mut buf)?;
    let logs: HashMap<LogId, &Chain> = logs.iter().map(|(id, chain)| (*id, chain)).collect();

    let mut partitions: Vec<(u32, PartitionListEntry)> = vec![];
    let mut dead_nodes: BTreeMap<PlainNodeId, DeadNode> = BTreeMap::new();
    for (node_id, node_state) in cluster_state.nodes {
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
                    let details = PartitionListEntry { host_node, status };
                    partitions.push((partition_id, details));
                }
            }
        }
    }

    // Show information organized by partition and node
    let mut partitions_table = Table::new_styled();
    partitions_table.set_styled_header(vec![
        "P-ID",
        "NODE",
        "MODE",
        "STATUS",
        "LEADER",
        "EPOCH",
        "SEQUENCER",
        "APPLIED",
        "PERSISTED",
        "SKIPPED",
        "LAST-UPDATE",
    ]);

    partitions
        .into_iter()
        .sorted_by(|a, b| match opts.sort {
            SortMode::Partition => a.0.cmp(&b.0),
            SortMode::Node => {
                a.1.host_node
                    .cmp(&b.1.host_node)
                    .then_with(|| a.0.cmp(&b.0))
            }
            SortMode::Active => {
                a.1.status
                    .effective_mode
                    .cmp(&b.1.status.effective_mode)
                    .then_with(|| a.1.host_node.cmp(&b.1.host_node))
            }
        })
        .for_each(|(partition_id, processor)| {
            let is_leader = processor
                .status
                .last_observed_leader_node
                .map(|n| {
                    n.generation.is_some_and(|g| {
                        PlainNodeId::from(n.id).with_generation(g) == processor.host_node
                    })
                })
                .unwrap_or_default();

            let (in_tail_segment, maybe_sequencer) = logs
                .get(&LogId::from(partition_id))
                .map(|chain| {
                    let tail = chain.tail();
                    let in_tail = processor
                        .status
                        .last_applied_log_lsn
                        .map(|lsn| Lsn::from(lsn))
                        .is_some_and(|applied_lsn| applied_lsn.ge(&tail.base_lsn));
                    (
                        in_tail,
                        deserialize_replicated_log_params(&tail).map(|p| p.sequencer),
                    )
                })
                .unwrap_or((false, None));

            let leader_local_sequencer =
                is_leader && maybe_sequencer.is_some_and(|s| s == processor.host_node);

            let observed_leader_color = if is_leader {
                Color::Green
            } else {
                Color::Reset
            };
            let sequencer_color = if leader_local_sequencer {
                Color::Green
            } else {
                Color::Reset
            };

            partitions_table.add_row(vec![
                Cell::new(partition_id),
                Cell::new(processor.host_node),
                render_mode(
                    processor.status.planned_mode(),
                    processor.status.effective_mode(),
                ),
                render_replay_status(
                    processor.status.effective_mode(),
                    processor.status.replay_status(),
                    processor.status.target_tail_lsn.map(Into::into),
                ),
                Cell::new(
                    processor
                        .status
                        .last_observed_leader_node
                        .map(|n| n.to_string())
                        .unwrap_or("-".to_owned()),
                )
                .fg(observed_leader_color),
                Cell::new(
                    processor
                        .status
                        .last_observed_leader_epoch
                        .map(|x| x.to_string())
                        .unwrap_or("-".to_owned()),
                ),
                Cell::new(match (in_tail_segment, maybe_sequencer) {
                    (true, Some(sequencer)) => sequencer.to_string(),
                    (false, _) => "-".to_owned(), // todo: render stragglers better
                    _ => "".to_owned(),
                })
                .fg(sequencer_color),
                Cell::new(
                    processor
                        .status
                        .last_applied_log_lsn
                        .map(|x| x.to_string())
                        .unwrap_or("-".to_owned()),
                ),
                Cell::new(
                    processor
                        .status
                        .last_persisted_log_lsn
                        .map(|x| x.to_string())
                        .unwrap_or("-".to_owned()),
                ),
                Cell::new(processor.status.num_skipped_records),
                render_as_duration(processor.status.updated_at, Tense::Past),
            ]);
        });

    c_println!(
        "Alive partition processors (nodes config {:#}, partition table {:#})",
        cluster_state
            .nodes_config_version
            .map(Version::from)
            .unwrap_or(Version::INVALID),
        cluster_state
            .partition_table_version
            .map(Version::from)
            .unwrap_or(Version::INVALID)
    );
    c_println!("{}", partitions_table);

    if !dead_nodes.is_empty() {
        c_println!();
        c_println!("☠️ Dead nodes");
        let mut dead_nodes_table = Table::new_styled();
        dead_nodes_table.set_styled_header(vec!["NODE", "LAST-SEEN"]);
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
    match (planned, planned == effective) {
        (RunMode::Unknown, _) => Cell::new("UNKNOWN").fg(Color::Red),
        (RunMode::Leader, true) => Cell::new("Leader")
            .fg(Color::Green)
            .add_attribute(Attribute::Bold),
        (RunMode::Follower, true) => Cell::new("Follower"),
        // We are in a transitional state
        (_, false) => Cell::new(format!("{}->{}", effective, planned)).fg(Color::Magenta),
    }
}

fn render_replay_status(effective: RunMode, status: ReplayStatus, target_lsn: Option<Lsn>) -> Cell {
    match (status, effective) {
        (ReplayStatus::Unknown, _) => Cell::new("UNKNOWN").fg(Color::Red),
        (ReplayStatus::Starting, _) => Cell::new("Starting").fg(Color::Yellow),
        (ReplayStatus::Active, RunMode::Leader) => Cell::new("Active").fg(Color::Green),
        (ReplayStatus::Active, RunMode::Follower) => Cell::new("Active"),
        (ReplayStatus::Active, RunMode::Unknown) => Cell::new("Active?").fg(Color::Red),
        (ReplayStatus::CatchingUp, _) => Cell::new(format!(
            "Catching Up ({})",
            target_lsn.map(|x| x.to_string()).unwrap_or("-".to_owned())
        ))
        .fg(Color::Magenta),
    }
}
