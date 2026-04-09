// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use cling::prelude::*;
use tracing::error;

use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::cluster_ctrl_svc::{ClusterStateRequest, new_cluster_ctrl_client};
use restate_types::PlainNodeId;
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::partitions::leadership_policy::LeaderAffinity;
use restate_types::protobuf::cluster::{RunMode, node_state};
use restate_types::replication::NodeSet;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

use super::read_epoch_metadata;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "show_policy")]
pub struct ShowOpts {
    /// The partition id or range, e.g. "0", "1-4". Defaults to all partitions.
    #[arg()]
    partition_id: Vec<RangeParam<u16>>,
}

async fn show_policy(connection: &ConnectionInfo, opts: &ShowOpts) -> anyhow::Result<()> {
    let partition_table = connection.get_partition_table().await?;

    // Resolve partition IDs: if none specified, use all partitions.
    let partition_ids: Vec<_> = if opts.partition_id.is_empty() {
        partition_table.iter_ids().cloned().collect()
    } else {
        opts.partition_id
            .iter()
            .flatten()
            .map(PartitionId::new_unchecked)
            .collect()
    };

    // Fetch cluster state to determine current leaders per partition.
    let cluster_state = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .get_cluster_state(ClusterStateRequest::default())
                .await
        })
        .await?
        .into_inner()
        .cluster_state;

    let current_leaders = extract_current_leaders(&cluster_state);

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["PARTITION", "LEADER", "FREEZE"]);

    for partition_id in partition_ids {
        if !partition_table.contains(&partition_id) {
            error!("Partition {partition_id} does not exist, skipping.");
            continue;
        }

        let epoch_metadata = match read_epoch_metadata(connection, partition_id).await {
            Ok(em) => em,
            Err(err) => {
                error!("Failed to get epoch metadata for partition {partition_id}: {err}");
                continue;
            }
        };

        let policy = epoch_metadata.leadership_policy();
        let current_leader = current_leaders.get(&partition_id);
        let replica_set = epoch_metadata.current().replica_set();

        let pinned_node = policy.affinity.as_ref().and_then(|a| match a {
            LeaderAffinity::Node(id) => Some(*id),
            _ => None,
        });

        let leader_cell = render_leader(current_leader, pinned_node, replica_set);

        let freeze_cell = match &policy.freeze {
            Some(f) => Cell::new(format!("FROZEN: {}", f.reason))
                .fg(Color::Red)
                .add_attribute(Attribute::Bold),
            None => Cell::new("-"),
        };

        table.add_row(vec![Cell::new(partition_id), leader_cell, freeze_cell]);
    }

    c_println!("{}", table);

    Ok(())
}

fn render_leader(
    current_leader: Option<&PlainNodeId>,
    pinned_node: Option<PlainNodeId>,
    replica_set: &NodeSet,
) -> Cell {
    let leader_str = current_leader
        .map(|l| format!("N{}", u32::from(*l)))
        .unwrap_or_else(|| "none".to_owned());

    match pinned_node {
        None => {
            // No pin set
            if current_leader.is_some() {
                Cell::new(leader_str)
            } else {
                Cell::new(leader_str).fg(Color::Yellow)
            }
        }
        Some(pin) => {
            if current_leader.is_some_and(|l| *l == pin) {
                // Pin satisfied
                Cell::new(leader_str)
                    .fg(Color::Green)
                    .add_attribute(Attribute::Bold)
            } else {
                let in_replica_set = replica_set.contains(pin);
                if in_replica_set {
                    // Pin not yet satisfied but reachable
                    Cell::new(format!("{leader_str} \u{2192} {pin}")).fg(Color::Yellow)
                } else {
                    // Pin target not in replica set
                    Cell::new(format!("{leader_str} \u{2192} {pin} \u{2717}")).fg(Color::Red)
                }
            }
        }
    }
}

/// Extract the current leader node ID for each partition from the cluster state.
fn extract_current_leaders(
    cluster_state: &Option<restate_types::protobuf::cluster::ClusterState>,
) -> HashMap<PartitionId, PlainNodeId> {
    let mut leaders = HashMap::new();

    let Some(state) = cluster_state else {
        return leaders;
    };

    for node_state in state.nodes.values() {
        let Some(ref state) = node_state.state else {
            continue;
        };
        let node_state::State::Alive(alive) = state else {
            continue;
        };
        for (partition_id, status) in &alive.partitions {
            if status.effective_mode() == RunMode::Leader
                && let Some(ref gen_node_id) = alive.generational_node_id
                && let Ok(id) = u16::try_from(*partition_id)
            {
                leaders.insert(
                    PartitionId::new_unchecked(id),
                    PlainNodeId::from(gen_node_id.id),
                );
            }
        }
    }

    leaders
}
