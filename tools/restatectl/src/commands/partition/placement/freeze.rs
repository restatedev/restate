// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, bail};
use cling::prelude::*;
use tracing::error;

use restate_cli_util::c_println;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::placement_policy::PlacementFreeze;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

use super::super::epoch_metadata::{signal_sync_epoch_metadata, update_epoch_metadata};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "freeze_placement")]
pub struct FreezeOpts {
    /// Partition id or range, e.g. "0", "1-4"
    #[arg(required = true)]
    partition_id: Vec<RangeParam<u16>>,

    /// Reason for freezing automatic placement
    #[arg(long, required = true)]
    reason: String,
}

async fn freeze_placement(connection: &ConnectionInfo, opts: &FreezeOpts) -> anyhow::Result<()> {
    let partition_table = connection.get_partition_table().await?;
    let mut updated = Vec::new();

    for id in opts.partition_id.iter().flatten() {
        let partition_id = PartitionId::new_unchecked(id);
        if !partition_table.contains(&partition_id) {
            error!("Partition {partition_id} does not exist, skipping.");
            continue;
        }

        update_epoch_metadata(connection, partition_id, |epoch_metadata| {
            apply_freeze(epoch_metadata, opts.reason.clone(), partition_id)
        })
        .await?;
        updated.push(partition_id);
        c_println!("Froze automatic placement for partition {partition_id}.");
    }

    if !updated.is_empty() {
        signal_sync_epoch_metadata(connection, &updated).await?;
    }

    Ok(())
}

fn apply_freeze(
    epoch_metadata: Option<EpochMetadata>,
    reason: String,
    partition_id: PartitionId,
) -> anyhow::Result<EpochMetadata> {
    let epoch_metadata =
        epoch_metadata.context(format!("partition {partition_id} has not been created yet"))?;
    if !epoch_metadata.current().is_valid() {
        bail!("partition {partition_id} has no valid current placement");
    }

    let mut policy = epoch_metadata.placement_policy().clone();
    policy.freeze = Some(PlacementFreeze { reason });
    Ok(epoch_metadata.set_placement_policy(policy))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use restate_types::PlainNodeId;
    use restate_types::partitions::PartitionConfiguration;
    use restate_types::replication::ReplicationProperty;

    use super::*;

    #[test]
    fn freeze_requires_valid_current_placement() {
        let partition_id = PartitionId::MIN;
        assert!(apply_freeze(None, "reason".to_owned(), partition_id).is_err());
        assert!(
            apply_freeze(
                Some(EpochMetadata::new(PartitionConfiguration::default(), None)),
                "reason".to_owned(),
                partition_id,
            )
            .is_err()
        );

        let current = PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(1),
            [PlainNodeId::from(1)].into_iter().collect(),
            HashMap::default(),
        );
        let frozen = apply_freeze(
            Some(EpochMetadata::new(current, None)),
            "reason".to_owned(),
            partition_id,
        )
        .unwrap();
        assert_eq!(
            frozen.placement_policy().freeze.as_ref().unwrap().reason,
            "reason"
        );
    }
}
