// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use cling::prelude::*;
use tracing::error;

use restate_cli_util::c_println;
use restate_types::identifiers::PartitionId;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

use super::super::epoch_metadata::{signal_sync_epoch_metadata, update_epoch_metadata};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "unfreeze_placement")]
pub struct UnfreezeOpts {
    /// Partition id or range, e.g. "0", "1-4"
    #[arg(required_unless_present = "all", conflicts_with = "all")]
    partition_id: Vec<RangeParam<u16>>,

    /// Unfreeze automatic placement for all partitions
    #[arg(long)]
    all: bool,
}

async fn unfreeze_placement(
    connection: &ConnectionInfo,
    opts: &UnfreezeOpts,
) -> anyhow::Result<()> {
    let partition_table = connection.get_partition_table().await?;
    let partition_ids: Vec<_> = if opts.all {
        partition_table.iter_ids().copied().collect()
    } else {
        opts.partition_id
            .iter()
            .flatten()
            .map(PartitionId::new_unchecked)
            .collect()
    };
    let mut updated = Vec::new();

    for partition_id in partition_ids {
        if !partition_table.contains(&partition_id) {
            error!("Partition {partition_id} does not exist, skipping.");
            continue;
        }

        update_epoch_metadata(connection, partition_id, |epoch_metadata| {
            let epoch_metadata = epoch_metadata
                .context(format!("partition {partition_id} has not been created yet"))?;
            let mut policy = epoch_metadata.placement_policy().clone();
            policy.freeze = None;
            Ok(epoch_metadata.set_placement_policy(policy))
        })
        .await?;
        updated.push(partition_id);
        c_println!("Unfroze automatic placement for partition {partition_id}.");
    }

    if !updated.is_empty() {
        signal_sync_epoch_metadata(connection, &updated).await?;
    }

    Ok(())
}
