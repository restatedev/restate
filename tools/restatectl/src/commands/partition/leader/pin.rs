// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, anyhow};
use cling::prelude::*;
use tracing::error;

use restate_cli_util::c_println;
use restate_types::PlainNodeId;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::leadership_policy::LeaderAffinity;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

use super::{signal_sync_epoch_metadata, update_epoch_metadata};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "pin_leader")]
pub struct PinOpts {
    /// The partition id or range, e.g. "0", "1-4"
    #[arg(required = true)]
    partition_id: Vec<RangeParam<u16>>,

    /// Pin leadership to this node (e.g. 2)
    #[arg(long, required = true)]
    node: PlainNodeId,
}

async fn pin_leader(connection: &ConnectionInfo, opts: &PinOpts) -> anyhow::Result<()> {
    let nodes_configuration = connection.get_nodes_configuration().await?;
    nodes_configuration
        .find_node_by_id(opts.node)
        .map_err(|_| anyhow!("Node {} is not part of the cluster.", opts.node))?;

    let partition_table = connection.get_partition_table().await?;
    let mut updated = Vec::new();

    for id in opts.partition_id.iter().flatten() {
        let partition_id = PartitionId::new_unchecked(id);
        if !partition_table.contains(&partition_id) {
            error!("Partition {partition_id} does not exist, skipping.");
            continue;
        }

        update_epoch_metadata(connection, partition_id, |epoch_metadata| {
            let epoch_metadata = epoch_metadata
                .context(format!("partition {partition_id} has not been created yet"))?;
            let mut policy = epoch_metadata.leadership_policy().clone();
            policy.affinity = Some(LeaderAffinity::Node(opts.node));
            Ok(epoch_metadata.set_leadership_policy(policy))
        })
        .await?;
        updated.push(partition_id);

        let node_id = u32::from(opts.node);
        c_println!("Pinned partition {partition_id} leader to node N{node_id}.");
    }

    if !updated.is_empty() {
        signal_sync_epoch_metadata(connection, &updated).await?;
    }

    Ok(())
}
