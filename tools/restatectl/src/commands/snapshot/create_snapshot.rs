// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;
use tracing::error;

use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::{
    cluster_ctrl_svc::{CreatePartitionSnapshotRequest, new_cluster_ctrl_client},
    node_ctl_svc::{GetMetadataRequest, new_node_ctl_client},
};
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::common::MetadataKind;
use restate_types::storage::StorageCodec;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "create")]
#[cling(run = "create_snapshot")]
pub struct CreateSnapshotOpts {
    /// The partition id or range to snapshot, e.g. "0", "1-4", defaults to all partitions
    #[arg()]
    partition_id: Vec<RangeParam>,

    /// Create snapshot only if the applied LSN meets the specified minimum
    #[arg(short, long)]
    min_lsn: Option<u64>,

    /// Trim the log to the snapshot-covered LSN following a successful snapshot
    #[arg(short, long, default_value = "false")]
    trim_log: bool,
}

async fn create_snapshot(
    connection: &ConnectionInfo,
    opts: &CreateSnapshotOpts,
) -> anyhow::Result<()> {
    let ids: Vec<_> = if opts.partition_id.is_empty() {
        let mut response = connection
            .try_each(None, |channel| async move {
                let mut client = new_node_ctl_client(channel.clone(), &CliContext::get().network);
                client
                    .get_metadata(GetMetadataRequest {
                        kind: MetadataKind::PartitionTable.into(),
                    })
                    .await
            })
            .await?
            .into_inner();

        let partition_table: PartitionTable = StorageCodec::decode(&mut response.encoded)?;

        partition_table.iter_ids().cloned().collect()
    } else {
        opts.partition_id
            .iter()
            .flatten()
            .map(|id| PartitionId::new_unchecked(id as u16))
            .collect()
    };

    for partition_id in ids {
        // make sure partition_id fits in a u16
        if let Err(err) = inner_create_snapshot(connection, opts, partition_id).await {
            error!("Failed to create snapshot for partition {partition_id}: {err}");
        }
    }

    Ok(())
}

async fn inner_create_snapshot(
    connection: &ConnectionInfo,
    opts: &CreateSnapshotOpts,
    partition_id: PartitionId,
) -> anyhow::Result<()> {
    let request = CreatePartitionSnapshotRequest {
        partition_id: partition_id.into(),
        min_target_lsn: opts.min_lsn,
        trim_log: opts.trim_log,
    };

    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .create_partition_snapshot(request)
                .await
        })
        .await?
        .into_inner();

    c_println!(
        "Snapshot created for partition {partition_id}: {} (log {} @ LSN >= {})",
        response.snapshot_id,
        response.log_id,
        response.min_applied_lsn,
    );

    Ok(())
}
