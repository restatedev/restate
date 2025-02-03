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
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::CreatePartitionSnapshotRequest;
use restate_cli_util::c_println;
use restate_types::nodes_config::Role;

use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "create")]
#[cling(run = "create_snapshot")]
pub struct CreateSnapshotOpts {
    /// The partition to snapshot
    #[arg(short, long)]
    partition_id: u16,
}

async fn create_snapshot(
    connection: &ConnectionInfo,
    opts: &CreateSnapshotOpts,
) -> anyhow::Result<()> {
    let request = CreatePartitionSnapshotRequest {
        partition_id: opts.partition_id as u32,
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

    c_println!("Snapshot created: {}", response.snapshot_id);

    Ok(())
}
