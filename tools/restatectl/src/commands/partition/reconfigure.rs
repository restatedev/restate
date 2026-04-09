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
use clap::Parser;
use cling::{Collect, Run};

use crate::commands::partition::leader::{signal_sync_epoch_metadata, update_epoch_metadata};
use crate::connection::ConnectionInfo;
use restate_cli_util::c_println;
use restate_types::PlainNodeId;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::PartitionConfiguration;
use restate_types::replication::ReplicationProperty;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "reconfigure_partition")]
#[command(after_long_help = "Reconfigure the partition processors for a given partition.")]
pub struct ReconfigureOpts {
    #[arg(long, short = 'i', required = true)]
    id: PartitionId,

    /// The nodes that should run a partition processor replica (comma-separated)
    #[arg(long, short = 'r', required = true, value_delimiter = ',')]
    replicas: Vec<PlainNodeId>,
}

pub async fn reconfigure_partition(
    connection: &ConnectionInfo,
    opts: &ReconfigureOpts,
) -> anyhow::Result<()> {
    // sanity checks
    let partition_table = connection.get_partition_table().await?;

    if !partition_table.contains(&opts.id) {
        bail!("Partition {} does not exist.", opts.id);
    }

    let nodes_configuration = connection.get_nodes_configuration().await?;

    for replica in &opts.replicas {
        nodes_configuration.find_node_by_id(*replica).map_err(|_| {
            anyhow!(
                "Cannot reconfigure partition because node {replica} is not part of the cluster."
            )
        })?;
    }

    let next = PartitionConfiguration::new(
        ReplicationProperty::new_unchecked(
            u8::try_from(opts.replicas.len())
                .expect("cannot configure more than {u8::MAX} replicas"),
        ),
        opts.replicas.clone().into_iter().collect(),
        HashMap::default(),
    );

    update_epoch_metadata(connection, opts.id, |epoch_metadata| {
        Ok(epoch_metadata
            .map(|epoch_metadata| epoch_metadata.reconfigure(next.clone()))
            .unwrap_or(EpochMetadata::new(next.clone(), None)))
    })
    .await?;

    c_println!("Successfully reconfigured partition {}.", opts.id);

    signal_sync_epoch_metadata(connection, &[opts.id]).await?;

    Ok(())
}
