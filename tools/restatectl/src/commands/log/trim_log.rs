// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::cluster_ctrl_svc::{TrimLogRequest, new_cluster_ctrl_client};
use restate_types::nodes_config::Role;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "trim_log")]
pub struct TrimLogOpts {
    /// The log id or range to trim, e.g. "0", "1-4".
    #[arg(required = true)]
    log_id: Vec<RangeParam>,

    /// The Log Sequence Number (LSN) to trim the log to, inclusive
    #[arg(short, long)]
    trim_point: u64,
}

async fn trim_log(connection: &ConnectionInfo, opts: &TrimLogOpts) -> anyhow::Result<()> {
    for log_id in opts.log_id.iter().flatten() {
        if let Err(err) = trim_log_inner(connection, opts, log_id).await {
            error!("Failed to trim log {log_id}: {err}");
        }
    }

    Ok(())
}

async fn trim_log_inner(
    connection: &ConnectionInfo,
    opts: &TrimLogOpts,
    log_id: u32,
) -> anyhow::Result<()> {
    let trim_request = TrimLogRequest {
        log_id,
        trim_point: opts.trim_point,
    };

    connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .trim_log(trim_request)
                .await
        })
        .await
        .with_context(|| "failed to submit trim request")?
        .into_inner();

    c_println!("Submitted for log {log_id}");

    Ok(())
}
