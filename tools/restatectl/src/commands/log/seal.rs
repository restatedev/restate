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
use restate_cli_util::c_println;
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::SealAndReconfigureRequest;
use restate_cli_util::ui::console::confirm_or_exit;

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "seal")]
pub struct SealOpts {
    /// LogId to seal
    #[clap(long, short)]
    log_id: u32,
}

async fn seal(connection: &ConnectionInfo, opts: &SealOpts) -> anyhow::Result<()> {
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

    // NOTE:
    // so far leader CC refreshes the configuration every 5 seconds. Which means
    // it still can fail to use the latest configuration if a seal was performed
    // immediately after a `cluster config set`.

    confirm_or_exit(&format!(
        "Force seal and recreation of log id {}?",
        opts.log_id
    ))?;

    client
        .seal_and_reconfigure_chain(SealAndReconfigureRequest {
            log_id: opts.log_id,
        })
        .await?;

    c_println!("✅ Log is scheduled for sealing");
    Ok(())
}
