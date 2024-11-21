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

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

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
    let channel = grpc_connect(connection.cluster_controller.clone())
        .await
        .with_context(|| {
            format!(
                "cannot connect to cluster controller at {}",
                connection.cluster_controller
            )
        })?;
    let mut client =
        ClusterCtrlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let request = CreatePartitionSnapshotRequest {
        partition_id: opts.partition_id as u32,
    };

    let response = client
        .create_partition_snapshot(request)
        .await
        .map_err(|e| anyhow::anyhow!("failed to request snapshot: {:?}", e))?
        .into_inner();

    c_println!("Snapshot created: {}", response.snapshot_id);

    Ok(())
}
