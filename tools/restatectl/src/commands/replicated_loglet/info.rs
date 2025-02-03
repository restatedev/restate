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

use restate_cli_util::{c_indentln, c_println};
use restate_types::logs::LogletId;
use restate_types::Versioned;

use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "get_info")]
pub struct InfoOpts {
    /// The replicated loglet id
    loglet_id: LogletId,
}

async fn get_info(connection: &ConnectionInfo, opts: &InfoOpts) -> anyhow::Result<()> {
    let logs = connection.get_logs().await?;
    c_println!("Log Configuration ({})", logs.version());

    let Some(loglet_ref) = logs.get_replicated_loglet(&opts.loglet_id) else {
        return Err(anyhow::anyhow!("loglet {} not found", opts.loglet_id));
    };

    c_println!("Loglet Referenced in: ");
    for (log_id, segment) in &loglet_ref.references {
        c_indentln!(2, "- LogId={}, Segment={}", log_id, segment);
    }
    c_println!();
    c_println!("Loglet Parameters:");
    c_indentln!(
        2,
        "Loglet Id: {} ({})",
        loglet_ref.params.loglet_id,
        *loglet_ref.params.loglet_id
    );
    c_indentln!(2, "Sequencer: {}", loglet_ref.params.sequencer);
    c_indentln!(2, "Replication: {}", loglet_ref.params.replication);
    c_indentln!(2, "Node Set: {}", loglet_ref.params.nodeset);

    Ok(())
}
