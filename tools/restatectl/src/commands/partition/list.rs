// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};

use cling::prelude::*;
use itertools::Itertools;

use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::ui::Tense;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::cluster_ctrl_svc::{ClusterStateRequest, new_cluster_ctrl_client};
use restate_types::logs::Lsn;
use restate_types::nodes_config::Role;
use restate_types::protobuf::cluster::{
    DeadNode, PartitionProcessorStatus, ReplayStatus, RunMode, node_state,
};
use restate_types::{GenerationalNodeId, PlainNodeId, Version};

use crate::commands::display_util::render_as_duration;
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug, Default)]
#[cling(run = "list_partitions")]
#[clap(alias = "ls")]
#[command(
    after_long_help = "In addition to partition processors, the command displays the current \
    sequencer for the partition's log when the reported applied LSN falls within the tail a \
    replicated segment. If ANSI color is enabled, the leadership epoch and the active sequencer \
    will be highlighted in green they are the most recent and co-located with the leader \
    processor, respectively."
)]
pub struct ListPartitionsOpts {
    /// Sort order
    #[arg(long, default_value = "partition")]
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
    let cluster_state = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .get_cluster_state(ClusterStateRequest::default())
                .await
        })
        .await?
        .into_inner()
        .cluster_state
        .ok_or_else(|| anyhow::anyhow!("no cluster state returned"))?;

    let mut partitions: Vec<(u32, PartitionListEntry)> = vec![];
    let mut dead_nodes: BTreeMap<PlainNodeId, DeadNode> = BTreeMap::new();
    let mut max_epoch_per_partition: HashMap<u32, u64> = HashMap::new();
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
                    let host_node = GenerationalNodeId::from(*host);
                    let details = PartitionListEntry { host_node, status };
                    partitions.push((partition_id, details));

                    let leadership_epoch =
                        status.last_observed_leader_epoch.unwrap_or_default().value;
                    max_epoch_per_partition
                        .entry(partition_id)
                        .and_modify(|existing| {
                            *existing = std::cmp::max(*existing, leadership_epoch);
                        })
                        .or_insert(leadership_epoch);
                }
            }
        }
    }

    // Show information organized by partition and node
    let mut partitions_table = Table::new_styled();
    partitions_table.set_styled_header(vec![
        "ID", "NODE", "MODE", "STATUS", "EPOCH", "APPLIED", "DURABLE", "ARCHIVED", "LSN-LAG",
        "UPDATED",
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
                    .then_with(|| a.0.cmp(&b.0))
            }
        })
        .for_each(|(partition_id, processor)| {
            let pp_sees_itself_as_leader = processor
                .status
                .last_observed_leader_node
                .map(|n| {
                    n.generation.is_some_and(|g| {
                        PlainNodeId::from(n.id).with_generation(g) == processor.host_node
                    })
                })
                .unwrap_or_default();

            let epoch = processor
                .status
                .last_observed_leader_epoch
                .unwrap_or_default()
                .value;
            let outdated_leadership_epoch = epoch
                < max_epoch_per_partition
                    .get(&partition_id)
                    .copied()
                    .unwrap_or_default();

            let observed_leader_color = match (pp_sees_itself_as_leader, outdated_leadership_epoch)
            {
                (true, false) => Color::Green,
                (true, true) => Color::Red,
                (false, true) => Color::Yellow,
                (false, false) => Color::Reset,
            };

            partitions_table.add_row(vec![
                Cell::new(partition_id),
                Cell::new(processor.host_node),
                render_mode(
                    processor.status.planned_mode(),
                    processor.status.effective_mode(),
                    outdated_leadership_epoch,
                ),
                render_replay_status(
                    processor.status.effective_mode(),
                    processor.status.replay_status(),
                    processor.status.target_tail_lsn.map(Into::into),
                ),
                Cell::new(
                    processor
                        .status
                        .last_observed_leader_epoch
                        .map(|x| x.to_string())
                        .unwrap_or("-".to_owned()),
                )
                .fg(observed_leader_color),
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
                        .durable_lsn
                        .map(|x| x.to_string())
                        .unwrap_or("-".to_owned()),
                ),
                Cell::new(
                    processor
                        .status
                        .last_archived_log_lsn
                        .map(|x| x.to_string())
                        .unwrap_or("-".to_owned()),
                ),
                Cell::new(
                    processor
                        .status
                        .target_tail_lsn
                        .zip(processor.status.last_applied_log_lsn)
                        .map(|(tail, applied)| {
                            // (tail - 1) - applied_lsn = tail - (applied_lsn + 1)
                            tail.value.saturating_sub(applied.value + 1).to_string()
                        })
                        .unwrap_or("-".to_owned()),
                ),
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

fn render_mode(planned: RunMode, effective: RunMode, outdated_leadership_epoch: bool) -> Cell {
    match (planned, planned == effective, outdated_leadership_epoch) {
        (RunMode::Leader, true, false) => Cell::new("Leader")
            .fg(Color::Blue)
            .add_attribute(Attribute::Bold),
        (RunMode::Leader, true, true) => Cell::new("Leader").fg(Color::Red),
        (RunMode::Follower, true, _) => Cell::new("Follower"),
        (_, false, false) => Cell::new(format!("{effective}->{planned}")).fg(Color::Magenta),
        (_, false, true) => Cell::new(format!("{effective}->{planned}")).fg(Color::Red),
        (RunMode::Unknown, _, _) => Cell::new("UNKNOWN").fg(Color::Red),
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
