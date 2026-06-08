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

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;
use restate_cli_util::c_println;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::leadership_policy::ElectionFreeze;

use super::{signal_sync_epoch_metadata, update_epoch_metadata};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "freeze_election")]
pub struct FreezeOpts {
    /// The partition id or range, e.g. "0", "1-4"
    #[arg(required = true)]
    partition_id: Vec<RangeParam<u16>>,

    /// Reason for freezing leader election
    #[arg(long, required = true)]
    reason: String,
}

async fn freeze_election(connection: &ConnectionInfo, opts: &FreezeOpts) -> anyhow::Result<()> {
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
            policy.freeze = Some(ElectionFreeze {
                reason: opts.reason.clone(),
            });
            Ok(epoch_metadata.set_leadership_policy(policy))
        })
        .await?;
        updated.push(partition_id);

        c_println!("Froze leader election for partition {partition_id}.");
    }

    if !updated.is_empty() {
        signal_sync_epoch_metadata(connection, &updated).await?;
    }

    Ok(())
}
