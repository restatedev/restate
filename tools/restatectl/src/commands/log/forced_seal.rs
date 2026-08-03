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

use anyhow::bail;
use cling::prelude::*;

use restate_cli_util::ui::console::confirm_or_exit;
use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::cluster_ctrl_svc::{SealChainRequest, new_cluster_ctrl_client};
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::nodes_config::Role;

use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "seal")]
pub struct SealOpts {
    /// Option segment index to seal. The tail segment is chosen automatically if not provided.
    #[clap(long, short = 'i')]
    segment_index: Option<u32>,
    /// The log id or range to seal and extend, e.g. "0", "1-4".
    #[clap(required = true)]
    log_id: u32,
    #[clap(required = true)]
    tail_lsn: u64,
    /// Reason for sealing the log chain. This will appear as metadata on the chain.
    #[clap(long)]
    reason: Option<String>,
}

async fn seal(connection: &ConnectionInfo, opts: &SealOpts) -> anyhow::Result<()> {
    let log_id = LogId::from(opts.log_id);
    let tail_lsn = Lsn::from(opts.tail_lsn);

    if tail_lsn == Lsn::INVALID {
        bail!("tail LSN must be a valid non-zero value");
    }

    confirm_or_exit(&format!(
        "Force-seal log {log_id} with tail LSN {tail_lsn}? This can cause permanent data loss"
    ))?;

    let mut context = HashMap::default();
    context.insert("source".to_owned(), "restatectl".to_owned());
    if let Some(reason) = &opts.reason {
        context.insert("reason".to_owned(), reason.to_owned());
    }
    let request = SealChainRequest {
        log_id: log_id.into(),
        segment_index: opts.segment_index,
        tail_lsn: Some(tail_lsn.as_u64()),
        context,
    };

    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .seal_chain(request.clone())
                .await
        })
        .await?
        .into_inner();

    c_println!("✅ log={log_id} chain has been sealed");
    c_println!(" ├ Tail LSN: {}", response.tail_offset);

    Ok(())
}
