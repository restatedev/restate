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
use std::time::Duration;

use anyhow::{Context, bail};
use bytesize::ByteSize;
use cling::prelude::*;
use tonic::Code;

use restate_cli_util::_comfy_table::{Cell, Color, Table};
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

    /// Only drop stale stores on these nodes (comma-separated, or repeat the option). Defaults to
    /// all alive worker nodes.
    #[arg(long = "node", short = 'n', value_name = "NODE", value_delimiter = ',')]
    nodes: Vec<PlainNodeId>,

    /// Why the stores are being dropped. The admin and the nodes log it for auditing.
    #[arg(long, required = true)]
    reason: String,

    /// Only report which stores would be dropped. The nodes make the same checks, but delete
    /// nothing.
    #[arg(long)]
    dry_run: bool,

    /// How long to wait after dropping a store before sending the next request, which spreads
    /// out the deletions. "0s" doesn't wait. Dry runs never wait.
    #[arg(long, default_value = "1s", value_name = "DURATION", value_parser = humantime::parse_duration)]
    delay_between_drops: Duration,
}

/// The stores dropped on one node, or that a dry run would drop.
#[derive(Default)]
struct Dropped {
    stores: usize,
    bytes: u64,
    unknown_sizes: usize,
}

impl Dropped {
    fn add(&mut self, other: &Dropped) {
        self.stores += other.stores;
        self.bytes += other.bytes;
        self.unknown_sizes += other.unknown_sizes;
    }

    fn disk_space(&self) -> String {
        let bytes = ByteSize::b(self.bytes).display().iec();
        match self.unknown_sizes {
            0 => bytes.to_string(),
            unknown => format!("{bytes} + {unknown} of unknown size"),
        }
    }
}

/// What became of one request to drop a node's copy of a partition. `dry_run` is whether the
/// server handled the request as a dry run, which servers without dry runs never do.
enum Outcome {
    Dropped {
        deleted_bytes: Option<u64>,
        dry_run: bool,
    },
    NoStore {
        dry_run: bool,
    },
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

    // The worker nodes to clean up, and whether each one is alive. Only alive nodes are asked.
    let mut workers: Vec<(PlainNodeId, bool)> = nodes_config
        .iter_role(Role::Worker)
        .map(|(node_id, _)| node_id)
        .filter(|node_id| opts.nodes.is_empty() || opts.nodes.contains(node_id))
        .map(|node_id| {
            let state = cluster_state
                .nodes
                .get(&u32::from(node_id))
                .and_then(|node| node.state.as_ref());
            (node_id, matches!(state, Some(node_state::State::Alive(_))))
        })
        .collect();
    workers.sort_unstable();
    let nodes: Vec<_> = workers
        .iter()
        .filter(|(_, alive)| *alive)
        .map(|(node_id, _)| *node_id)
        .collect();

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
    let (mut placed, mut unplaced) = (0, Vec::new());
    for partition_id in partition_ids {
        if !partition_table.contains(&partition_id) {
            bail!("Partition {partition_id} does not exist.");
        }
        let epoch_metadata = read_epoch_metadata(connection, partition_id).await?;
        let Some(epoch_metadata) = epoch_metadata.filter(|m| m.current().is_valid()) else {
            unplaced.push(partition_id);
            continue;
        };
        placed += 1;
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

    if !unplaced.is_empty() {
        c_warn!(
            "Skipping partitions without a replica set yet: {}.",
            join(&unplaced)
        );
    }
    let mut table = Table::new_styled();
    table.set_styled_header(vec!["NODE", "# PARTITIONS TO CHECK"]);
    for (node, alive) in &workers {
        let row = if *alive {
            vec![
                Cell::new(node),
                Cell::new(plan.get(node).map_or(0, Vec::len)),
            ]
        } else {
            // dead nodes aren't asked, so their stale stores stay
            vec![
                Cell::new(node).fg(Color::Red),
                Cell::new("DEAD").fg(Color::Red),
            ]
        };
        table.add_row(row);
    }
    c_println!("{table}");

    let total: usize = plan.values().map(Vec::len).sum();
    if total == 0 {
        if nodes.is_empty() {
            c_noop!("Nothing to check: none of the selected worker nodes is alive.");
        } else if placed == 0 {
            c_noop!("Nothing to check: none of the selected partitions has a replica set yet.");
        } else {
            c_noop!("Nothing to check: every alive node is a replica of all selected partitions.");
        }
        return Ok(());
    }
    confirm_or_exit(&if opts.dry_run {
        format!(
            "[DRY RUN] Check which copies these nodes would drop ({total} requests, sent one at \
            a time)?"
        )
    } else {
        format!(
            "This cannot be undone. Delete these nodes' local copies of partitions they are not \
            replicas of ({total} requests, sent one at a time)?"
        )
    })?;

    let mut request_number = 0;
    let mut not_dropped = 0;
    let mut dropped: BTreeMap<PlainNodeId, Dropped> = plan
        .keys()
        .map(|node| (*node, Dropped::default()))
        .collect();
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
                dry_run: opts.dry_run,
            };
            let outcome = request_drop(connection, request).await.with_context(|| {
                let done = request_number - 1;
                if opts.dry_run {
                    format!(
                        "[DRY RUN] Stopped after {done} of {total} requests: no answer about \
                        node {node}'s copy of partition {partition_id}."
                    )
                } else {
                    format!(
                        "Stopped after {done} of {total} requests: the outcome of dropping node \
                        {node}'s copy of partition {partition_id} is unknown. Run the command \
                        again to continue; stores that are already gone are skipped."
                    )
                }
            })?;
            match outcome {
                // never go on after a server ignored a dry run: the next request would delete too
                Outcome::Dropped {
                    deleted_bytes,
                    dry_run: false,
                } if opts.dry_run => bail!(
                    "Stopped: the server doesn't support dry runs and ignored --dry-run. Node \
                    {node} DROPPED its copy of partition {partition_id} ({}). No further requests \
                    were sent.",
                    format_deleted_bytes(deleted_bytes)
                ),
                Outcome::NoStore { dry_run: false } if opts.dry_run => bail!(
                    "Stopped: the server doesn't support dry runs and ignored --dry-run. Node \
                    {node} had no copy of partition {partition_id}, so nothing was dropped. No \
                    further requests were sent."
                ),
                Outcome::Dropped { deleted_bytes, .. } => {
                    let on_node = dropped.entry(*node).or_default();
                    on_node.stores += 1;
                    match deleted_bytes {
                        Some(bytes) => on_node.bytes += bytes,
                        None => on_node.unknown_sizes += 1,
                    }
                    if opts.dry_run {
                        c_println!(
                            "[DRY RUN] [{request_number}/{total}] Would drop node {node}'s copy \
                            of partition {partition_id} ({})",
                            format_deleted_bytes(deleted_bytes)
                        );
                    } else {
                        c_success!(
                            "[{request_number}/{total}] Dropped node {node}'s copy of partition \
                            {partition_id} ({})",
                            format_deleted_bytes(deleted_bytes)
                        );
                        if request_number < total && !opts.delay_between_drops.is_zero() {
                            tokio::time::sleep(opts.delay_between_drops).await;
                        }
                    }
                }
                Outcome::NoStore { .. } => {}
                Outcome::NotDropped(message) if opts.dry_run => {
                    not_dropped += 1;
                    c_warn!(
                        "[DRY RUN] [{request_number}/{total}] Would not drop node {node}'s copy \
                        of partition {partition_id}: {message}"
                    );
                }
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

    let prefix = if opts.dry_run { "[DRY RUN] " } else { "" };
    let mut all = Dropped::default();
    dropped.values().for_each(|on_node| all.add(on_node));
    if all.stores == 0 {
        c_noop!("{prefix}No stale partition stores found.");
    } else {
        let mut table = Table::new_styled();
        let stores = if opts.dry_run {
            "WOULD DROP"
        } else {
            "DROPPED"
        };
        table.set_styled_header(vec!["NODE", stores, "DISK SPACE"]);
        for (node, on_node) in &dropped {
            table.add_row(vec![
                node.to_string(),
                on_node.stores.to_string(),
                on_node.disk_space(),
            ]);
        }
        table.add_row(vec![
            "TOTAL".to_owned(),
            all.stores.to_string(),
            all.disk_space(),
        ]);
        c_println!("{table}");
        let (stores, disk_space) = (all.stores, all.disk_space());
        if opts.dry_run {
            c_println!(
                "[DRY RUN] Would drop {stores} stale partition stores, which use {disk_space} of \
                disk space"
            );
        } else {
            c_success!(
                "Dropped {stores} stale partition stores, which used {disk_space} of disk space"
            );
        }
    }
    if not_dropped > 0 {
        let would = if opts.dry_run {
            "would not be"
        } else {
            "were not"
        };
        c_warn!("{prefix}{not_dropped} stores {would} dropped, see above.");
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
                                dry_run: response.dry_run,
                            }
                        } else {
                            Outcome::NoStore {
                                dry_run: response.dry_run,
                            }
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
