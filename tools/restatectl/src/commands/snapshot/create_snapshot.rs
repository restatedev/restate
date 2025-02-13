// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::CreatePartitionSnapshotRequest;
use restate_cli_util::c_println;
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_core::protobuf::node_ctl_svc::GetMetadataRequest;
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::common::MetadataKind;
use restate_types::storage::StorageCodec;
use tonic::codec::CompressionEncoding;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "create")]
#[cling(run = "create_snapshot")]
pub struct CreateSnapshotOpts {
    /// The partition id or range to snapshot, e.g. "0", "1-4".
    /// All partitions if not provided
    #[arg()]
    partition_id: Vec<RangeParam>,
}

async fn create_snapshot(
    connection: &ConnectionInfo,
    opts: &CreateSnapshotOpts,
) -> anyhow::Result<()> {
    let ids: Vec<_> = if opts.partition_id.is_empty() {
        let mut response = connection
            .try_each(None, |channel| async move {
                let mut client = NodeCtlSvcClient::new(channel.clone())
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip);
                client
                    .get_metadata(GetMetadataRequest {
                        kind: MetadataKind::PartitionTable.into(),
                        sync: false,
                    })
                    .await
            })
            .await?
            .into_inner();

        let partition_table: PartitionTable = StorageCodec::decode(&mut response.encoded)?;

        partition_table.partition_ids().cloned().collect()
    } else {
        opts.partition_id
            .iter()
            .flatten()
            .map(|id| PartitionId::new_unchecked(id as u16))
            .collect()
    };

    for partition_id in ids {
        // make sure partition_id fits in a u16
        if let Err(err) = inner_create_snapshot(connection, partition_id).await {
            error!("Failed to create snapshot for partition {partition_id}: {err}");
        }
    }

    Ok(())
}

async fn inner_create_snapshot(
    connection: &ConnectionInfo,
    partition_id: PartitionId,
) -> anyhow::Result<()> {
    let request = CreatePartitionSnapshotRequest {
        partition_id: partition_id.into(),
    };

    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            let mut client = ClusterCtrlSvcClient::new(channel)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);

            client.create_partition_snapshot(request).await
        })
        .await
        .context("Failed to request snapshot")?
        .into_inner();

    c_println!(
        "Snapshot created for partition {partition_id}: {}",
        response.snapshot_id
    );

    Ok(())
}
