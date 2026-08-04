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

use anyhow::{anyhow, bail};
use cling::prelude::*;

use restate_cli_util::c_println;
use restate_types::PlainNodeId;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::PartitionConfiguration;
use restate_types::replication::{NodeSet, ReplicationProperty};

use crate::connection::ConnectionInfo;

use super::super::epoch_metadata::{signal_sync_epoch_metadata, update_epoch_metadata};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "set_placement")]
pub struct SetOpts {
    /// Partition to place
    #[arg()]
    partition_id: PartitionId,

    /// Nodes that should run a partition processor replica (comma-separated)
    #[arg(long, short = 'r', required = true, value_delimiter = ',')]
    replicas: Vec<PlainNodeId>,
}

async fn set_placement(connection: &ConnectionInfo, opts: &SetOpts) -> anyhow::Result<()> {
    let partition_table = connection.get_partition_table().await?;
    if !partition_table.contains(&opts.partition_id) {
        bail!("Partition {} does not exist.", opts.partition_id);
    }

    if opts.replicas.is_empty() {
        bail!("At least one replica is required.");
    }

    let replicas: NodeSet = opts.replicas.iter().copied().collect();
    if replicas.len() != opts.replicas.len() {
        bail!("Replica node IDs must be unique.");
    }

    let nodes_configuration = connection.get_nodes_configuration().await?;
    for replica in replicas.iter().copied() {
        nodes_configuration.find_node_by_id(replica).map_err(|_| {
            anyhow!("Cannot place partition because node {replica} is not part of the cluster.")
        })?;
    }

    let replication = u8::try_from(replicas.len())
        .map_err(|_| anyhow!("Cannot configure more than {} replicas.", u8::MAX))?;
    let placement = PartitionConfiguration::new(
        ReplicationProperty::new_unchecked(replication),
        replicas,
        HashMap::default(),
    );

    update_epoch_metadata(connection, opts.partition_id, |epoch_metadata| {
        apply_placement(epoch_metadata, placement.clone())
    })
    .await?;

    signal_sync_epoch_metadata(connection, &[opts.partition_id]).await?;
    c_println!("Set placement for partition {}.", opts.partition_id);

    Ok(())
}

fn apply_placement(
    epoch_metadata: Option<EpochMetadata>,
    placement: PartitionConfiguration,
) -> anyhow::Result<EpochMetadata> {
    match epoch_metadata {
        None => Ok(EpochMetadata::new(placement, None)),
        Some(epoch_metadata) if !epoch_metadata.current().is_valid() => {
            if epoch_metadata.next().is_some() {
                bail!("Cannot initialize placement while a pending configuration exists.");
            }
            Ok(epoch_metadata.set_initial_current_configuration(placement))
        }
        Some(epoch_metadata) => Ok(epoch_metadata.reconfigure(placement)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use restate_types::Versioned;
    use restate_types::partitions::placement_policy::{PlacementFreeze, PlacementPolicy};
    use restate_types::replication::ReplicationProperty;

    use super::*;

    fn configuration(node_id: u32) -> PartitionConfiguration {
        PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(1),
            [PlainNodeId::from(node_id)].into_iter().collect(),
            HashMap::default(),
        )
    }

    #[test]
    fn placement_initializes_or_reconfigures_as_appropriate() {
        let initial = apply_placement(None, configuration(1)).unwrap();
        assert_eq!(
            initial.current().replica_set(),
            configuration(1).replica_set()
        );
        assert!(initial.next().is_none());

        let policy = PlacementPolicy {
            freeze: Some(PlacementFreeze {
                reason: "maintenance".to_owned(),
            }),
        };
        let unassigned = EpochMetadata::new(PartitionConfiguration::default(), None)
            .set_placement_policy(policy.clone());
        let initialized = apply_placement(Some(unassigned), configuration(2)).unwrap();
        assert_eq!(initialized.current().version(), initialized.version());
        assert_eq!(initialized.placement_policy(), &policy);
        assert!(initialized.next().is_none());

        let reconfigured = apply_placement(Some(initialized), configuration(3)).unwrap();
        assert_eq!(
            reconfigured.next().unwrap().replica_set(),
            configuration(3).replica_set()
        );
        assert_eq!(reconfigured.placement_policy(), &policy);
    }
}
