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

use anyhow::{Context, bail};
use bytesize::ByteSize;
use cling::prelude::*;
use tonic::Code;

use restate_cli_util::_comfy_table::Table;
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{CliContext, c_eprintln, c_noop, c_println, c_success, c_warn};
use restate_core::protobuf::cluster_ctrl_svc::{
    ClusterStateRequest, DropPartitionStoreRequest, new_cluster_ctrl_client,
};
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::protobuf::cluster::node_state;
use restate_types::{PlainNodeId, Version};

use super::drop_store::{classify_drop_partition_store_status, format_deleted_bytes};
use super::epoch_metadata::read_epoch_metadata;
use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "drop_stale_stores")]
#[command(
    after_long_help = "Deletes the local copies of partitions that worker nodes keep although \
    they are no longer part of the partition's replica set. Each alive worker node is asked, \
    one partition at a time, to drop its copy of every partition it isn't a replica of. A node \
    without such a copy has nothing to do. A node refuses if it still runs the partition, or \
    if it knows a replica set that includes it, so replicas never lose their data."
)]
pub struct DropStaleStoresOpts {
    /// Partition id or range, e.g. "0", "1-4". Defaults to all partitions.
    #[arg()]
    partition_id: Vec<RangeParam<u16>>,

    /// Only drop stale stores on these nodes. Defaults to all alive worker nodes.
    #[arg(long = "node", short = 'n', value_name = "NODE")]
    nodes: Vec<PlainNodeId>,

    /// Why the stores are being dropped. The admin and the nodes log it for auditing.
    #[arg(long, required = true)]
    reason: String,
}

/// What became of one request to drop a node's copy of a partition.
enum Outcome {
    Dropped {
        deleted_bytes: Option<u64>,
    },
    NoStore,
    /// The node refused, or failed to delete the store. The message says why.
    NotDropped(String),
}

async fn drop_stale_stores(
    connection: &ConnectionInfo,
    opts: &DropStaleStoresOpts,
) -> anyhow::Result<()> {
    let nodes_config = connection.get_nodes_configuration().await?;
    for node in &opts.nodes {
        if !nodes_config.has_worker_role(node) {
            bail!("Node {node} is not a worker node of this cluster.");
        }
    }
    let cluster_state = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .get_cluster_state(ClusterStateRequest::default())
                .await
        })
        .await?
        .into_inner()
        .cluster_state
        .context("The admin did not return the cluster state.")?;

    let (mut nodes, mut dead_nodes) = (Vec::new(), Vec::new());
    for (node_id, _) in nodes_config.iter_role(Role::Worker) {
        if !opts.nodes.is_empty() && !opts.nodes.contains(&node_id) {
            continue;
        }
        let state = cluster_state
            .nodes
            .get(&u32::from(node_id))
            .and_then(|node| node.state.as_ref());
        if matches!(state, Some(node_state::State::Alive(_))) {
            nodes.push(node_id);
        } else {
            dead_nodes.push(node_id);
        }
    }

    let partition_table = connection.get_partition_table().await?;
    let partition_ids: Vec<_> = if opts.partition_id.is_empty() {
        partition_table.iter_ids().copied().collect()
    } else {
        opts.partition_id
            .iter()
            .flatten()
            .map(PartitionId::new_unchecked)
            .collect()
    };

    // Each node's partitions that it isn't a replica of, with the partition configuration that
    // says so. Nodes check their own view against that version before deleting anything.
    let mut plan: BTreeMap<PlainNodeId, Vec<(PartitionId, Version)>> = BTreeMap::new();
    let mut unplaced = Vec::new();
    for partition_id in partition_ids {
        if !partition_table.contains(&partition_id) {
            bail!("Partition {partition_id} does not exist.");
        }
        let epoch_metadata = read_epoch_metadata(connection, partition_id).await?;
        let Some(epoch_metadata) = epoch_metadata.filter(|m| m.current().is_valid()) else {
            unplaced.push(partition_id);
            continue;
        };
        let version = epoch_metadata.latest_configuration_version();
        let is_replica = |node: PlainNodeId| {
            epoch_metadata.current().replica_set().contains(node)
                || epoch_metadata
                    .next()
                    .is_some_and(|next| next.replica_set().contains(node))
        };
        for node in nodes.iter().copied().filter(|node| !is_replica(*node)) {
            plan.entry(node).or_default().push((partition_id, version));
        }
    }

    if !dead_nodes.is_empty() {
        c_warn!(
            "Skipping nodes that are not alive: {}. Their stale stores are kept.",
            join(&dead_nodes)
        );
    }
    if !unplaced.is_empty() {
        c_warn!(
            "Skipping partitions without a replica set yet: {}.",
            join(&unplaced)
        );
    }
    let total: usize = plan.values().map(Vec::len).sum();
    if total == 0 {
        c_noop!("No node keeps partitions it isn't a replica of.");
        return Ok(());
    }

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["NODE", "PARTITIONS IT IS NOT A REPLICA OF"]);
    for (node, partitions) in &plan {
        table.add_row(vec![node.to_string(), partitions.len().to_string()]);
    }
    c_println!("{table}");
    confirm_or_exit(&format!(
        "Delete these nodes' local copies of partitions they are not replicas of ({total} \
        requests, sent one at a time)? This cannot be undone"
    ))?;

    let mut request_number = 0;
    let (mut dropped, mut dropped_bytes, mut unknown_sizes, mut not_dropped) = (0, 0, 0, 0);
    for (node, partitions) in &plan {
        c_eprintln!(
            "[{}/{total}] Checking node {node} ({} partitions)",
            request_number + 1,
            partitions.len()
        );
        for (partition_id, version) in partitions {
            request_number += 1;
            let request = DropPartitionStoreRequest {
                partition_id: u32::from(u16::from(*partition_id)),
                node_id: Some((*node).into()),
                force: false,
                min_partition_configuration_version: Some((*version).into()),
                reason: opts.reason.clone(),
            };
            let outcome = request_drop(connection, request).await.with_context(|| {
                format!(
                    "Stopped after {} of {total} requests: the outcome of dropping node {node}'s \
                    copy of partition {partition_id} is unknown. Run the command again to \
                    continue; stores that are already gone are skipped.",
                    request_number - 1
                )
            })?;
            match outcome {
                Outcome::Dropped { deleted_bytes } => {
                    dropped += 1;
                    match deleted_bytes {
                        Some(bytes) => dropped_bytes += bytes,
                        None => unknown_sizes += 1,
                    }
                    c_success!(
                        "[{request_number}/{total}] Dropped node {node}'s copy of partition \
                        {partition_id} ({})",
                        format_deleted_bytes(deleted_bytes)
                    );
                }
                Outcome::NoStore => {}
                Outcome::NotDropped(message) => {
                    not_dropped += 1;
                    c_warn!(
                        "[{request_number}/{total}] Did not drop node {node}'s copy of partition \
                        {partition_id}: {message}"
                    );
                }
            }
        }
    }

    if dropped == 0 {
        c_noop!("No stale partition stores found.");
    } else {
        let unknown = if unknown_sizes > 0 {
            format!(", plus {unknown_sizes} of unknown size")
        } else {
            String::new()
        };
        c_success!(
            "Dropped {dropped} stale partition stores, which used {} of disk space{unknown}",
            ByteSize::b(dropped_bytes).display().iec()
        );
    }
    if not_dropped > 0 {
        c_warn!("{not_dropped} stores were not dropped, see above.");
    }
    Ok(())
}

/// Sends one drop request. Fails only if the outcome is unknown, e.g. because the connection
/// broke after the request was sent.
async fn request_drop(
    connection: &ConnectionInfo,
    request: DropPartitionStoreRequest,
) -> anyhow::Result<Outcome> {
    let outcome = connection
        .try_each(Some(Role::Admin), |channel| {
            let request = request.clone();
            async move {
                match new_cluster_ctrl_client(channel, &CliContext::get().network)
                    .drop_partition_store(request)
                    .await
                {
                    Ok(response) => {
                        let response = response.into_inner();
                        Ok(if response.dropped {
                            Outcome::Dropped {
                                deleted_bytes: response.deleted_bytes,
                            }
                        } else {
                            Outcome::NoStore
                        })
                    }
                    // the node refused, or deleting failed: nothing is left in doubt
                    Err(status)
                        if matches!(
                            status.code(),
                            Code::FailedPrecondition
                                | Code::Aborted
                                | Code::NotFound
                                | Code::Internal
                        ) =>
                    {
                        Ok(Outcome::NotDropped(status.message().to_owned()))
                    }
                    Err(status) => Err(classify_drop_partition_store_status(status)),
                }
            }
        })
        .await?;
    Ok(outcome)
}

fn join(items: &[impl ToString]) -> String {
    items
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
