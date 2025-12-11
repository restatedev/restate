// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_cli_util::{CliContext, c_println};
use restate_metadata_store::protobuf::metadata_proxy_svc::{
    GetRequest, PutRequest, client::new_metadata_proxy_client,
};
use restate_metadata_store::serialize_value;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::partitions::PartitionConfiguration;
use restate_types::replication::ReplicationProperty;
use restate_types::storage::StorageCodec;
use restate_types::{PlainNodeId, Versioned};

use crate::connection::ConnectionInfo;

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

    let get_request = GetRequest {
        key: partition_processor_epoch_key(opts.id).to_string(),
    };

    let get_response = connection
        .try_each(None, |channel| async {
            new_metadata_proxy_client(channel, &CliContext::get().network)
                .get(get_request.clone())
                .await
        })
        .await?
        .into_inner();

    let latest_epoch_metadata = if let Some(mut value) = get_response.value {
        Some(StorageCodec::decode::<EpochMetadata, _>(&mut value.bytes)?)
    } else {
        None
    };

    let next = PartitionConfiguration::new(
        ReplicationProperty::new_unchecked(
            u8::try_from(opts.replicas.len())
                .expect("cannot configure more than {u8::MAX} replicas"),
        ),
        opts.replicas.clone().into_iter().collect(),
        HashMap::default(),
    );

    let (precondition, new_epoch_metadata) =
        if let Some(latest_epoch_metadata) = latest_epoch_metadata {
            (
                Precondition::MatchesVersion(latest_epoch_metadata.version()),
                latest_epoch_metadata.reconfigure(next),
            )
        } else {
            (Precondition::DoesNotExist, EpochMetadata::new(next, None))
        };

    let request = PutRequest {
        key: partition_processor_epoch_key(opts.id).to_string(),
        precondition: Some(precondition.into()),
        value: Some(serialize_value(&new_epoch_metadata)?.into()),
    };

    connection
        .try_each(None, |channel| async {
            new_metadata_proxy_client(channel, &CliContext::get().network)
                .put(request.clone())
                .await
        })
        .await?;

    c_println!("Successfully reconfigured partition {}.", opts.id);

    Ok(())
}
