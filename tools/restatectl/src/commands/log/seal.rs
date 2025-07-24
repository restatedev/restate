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

use restate_cli_util::c_println;
use restate_core::protobuf::cluster_ctrl_svc::{SealChainRequest, new_cluster_ctrl_client};
use restate_types::logs::LogId;
use restate_types::nodes_config::Role;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "seal")]
pub struct SealOpts {
    /// Option segment index to seal. The tail segment is chosen automatically if not provided.
    #[clap(long, short = 'i')]
    segment_index: Option<u32>,
    /// The log id or range to seal and extend, e.g. "0", "1-4".
    #[clap(required = true)]
    log_id: Vec<RangeParam>,
}

async fn seal(connection: &ConnectionInfo, opts: &SealOpts) -> anyhow::Result<()> {
    for log_id in opts.log_id.iter().flatten().map(LogId::from) {
        if let Err(err) = inner_seal(connection, opts, log_id).await {
            error!("Failed to seal log chain for log={log_id}: {err}");
        }
        c_println!("");
    }

    Ok(())
}

async fn inner_seal(
    connection: &ConnectionInfo,
    opts: &SealOpts,
    log_id: LogId,
) -> anyhow::Result<()> {
    let request = SealChainRequest {
        log_id: log_id.into(),
        segment_index: opts.segment_index,
    };

    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel).seal_chain(request).await
        })
        .await?
        .into_inner();

    c_println!("✅ log={log_id} chain has been sealed");
    c_println!(" ├ Tail LSN: {}", response.tail_offset);

    Ok(())
}
