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
use restate_admin::cluster_controller::protobuf::TrimLogRequest;
use restate_cli_util::c_println;
use restate_types::nodes_config::Role;

use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "trim_log")]
pub struct TrimLogOpts {
    /// The log to trim
    #[arg(short, long)]
    log_id: u32,

    /// The Log Sequence Number (LSN) to trim the log to, inclusive
    #[arg(short, long)]
    trim_point: u64,
}

async fn trim_log(connection: &ConnectionInfo, opts: &TrimLogOpts) -> anyhow::Result<()> {
    let trim_request = TrimLogRequest {
        log_id: opts.log_id,
        trim_point: opts.trim_point,
    };

    connection
        .try_each(Some(Role::Admin), |channel| async {
            let mut client =
                ClusterCtrlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

            client.trim_log(trim_request).await
        })
        .await
        .with_context(|| "failed to submit trim request")?
        .into_inner();

    c_println!("Submitted");

    Ok(())
}
