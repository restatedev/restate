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

use restate_cli_util::ui::console::confirm_or_exit;
use restate_cli_util::{c_println, c_warn};
use restate_types::PlainNodeId;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::PartitionConfiguration;
use restate_types::partitions::placement_policy::PlacementFreeze;
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
    #[arg(
        long,
        short = 'r',
        required = true,
        value_delimiter = ',',
        num_args = 0..
    )]
    replicas: Vec<PlainNodeId>,

    /// Freeze automatic placement after setting the replicas
    #[arg(long, value_name = "REASON")]
    freeze: Option<String>,
}

async fn set_placement(connection: &ConnectionInfo, opts: &SetOpts) -> anyhow::Result<()> {
    let partition_table = connection.get_partition_table().await?;
    if !partition_table.contains(&opts.partition_id) {
        bail!("Partition {} does not exist.", opts.partition_id);
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

    let replication = if replicas.is_empty() {
        c_warn!(
            "An empty replica set stops all partition processors for partition {} once the \
             configuration takes effect. Automatic placement may assign new replicas unless \
             placement is frozen.",
            opts.partition_id
        );
        confirm_or_exit(&format!(
            "Set an empty replica set for partition {}?",
            opts.partition_id
        ))?;

        partition_table.replication_property(&nodes_configuration)
    } else {
        let replication = u8::try_from(replicas.len())
            .map_err(|_| anyhow!("Cannot configure more than {} replicas.", u8::MAX))?;
        ReplicationProperty::new_unchecked(replication)
    };
    let placement = PartitionConfiguration::new(replication, replicas, HashMap::default());

    update_epoch_metadata(connection, opts.partition_id, |epoch_metadata| {
        apply_placement(epoch_metadata, placement.clone(), opts.freeze.as_deref())
    })
    .await?;

    signal_sync_epoch_metadata(connection, &[opts.partition_id]).await?;
    c_println!("Set placement for partition {}.", opts.partition_id);

    Ok(())
}

fn apply_placement(
    epoch_metadata: Option<EpochMetadata>,
    placement: PartitionConfiguration,
    freeze_reason: Option<&str>,
) -> anyhow::Result<EpochMetadata> {
    let epoch_metadata = match epoch_metadata {
        None => EpochMetadata::new(placement, None),
        Some(epoch_metadata) if !epoch_metadata.current().is_valid() => {
            if epoch_metadata.next().is_some() {
                bail!("Cannot initialize placement while a pending configuration exists.");
            }
            epoch_metadata.set_initial_current_configuration(placement)
        }
        Some(epoch_metadata) => epoch_metadata.reconfigure(placement),
    };

    if let Some(reason) = freeze_reason {
        let mut policy = epoch_metadata.placement_policy().clone();
        policy.freeze = Some(PlacementFreeze {
            reason: reason.to_owned(),
        });
        Ok(epoch_metadata.set_placement_policy(policy))
    } else {
        Ok(epoch_metadata)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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
        let initial = apply_placement(None, configuration(1), Some("maintenance")).unwrap();
        assert_eq!(
            initial.current().replica_set(),
            configuration(1).replica_set()
        );
        assert!(initial.next().is_none());
        assert_eq!(
            initial.placement_policy().freeze.as_ref().unwrap().reason,
            "maintenance"
        );

        let unassigned = EpochMetadata::new(PartitionConfiguration::default(), None);
        let initialized =
            apply_placement(Some(unassigned), configuration(2), Some("maintenance")).unwrap();
        assert!(initialized.current().is_valid());
        assert_eq!(
            initialized
                .placement_policy()
                .freeze
                .as_ref()
                .unwrap()
                .reason,
            "maintenance"
        );
        assert!(initialized.next().is_none());

        let reconfigured = apply_placement(Some(initialized), configuration(3), None).unwrap();
        assert_eq!(
            reconfigured.next().unwrap().replica_set(),
            configuration(3).replica_set()
        );
        assert!(reconfigured.placement_policy().is_frozen());
    }
}
