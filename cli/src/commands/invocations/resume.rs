// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::find_active_invocations_simple;
use crate::clients::{self, AdminClientInterface, collect_and_split_futures};
use crate::ui::invocations::render_simple_invocation_list;

use crate::commands::invocations::create_query_filter;
use anyhow::{Result, anyhow, bail};
use cling::prelude::*;
use comfy_table::{Cell, Color, Table};
use futures::TryFutureExt;
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{c_indent_table, c_println, c_success, c_warn};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_resume")]
pub struct Resume {
    /// Either an invocation id, or a target string exact match or prefix, e.g.:
    /// * `invocationId`
    /// * `serviceName`
    /// * `serviceName/handler`
    /// * `virtualObjectName`
    /// * `virtualObjectName/key`
    /// * `virtualObjectName/key/handler`
    query: String,

    /// When resuming from paused/suspended, provide a deployment id to use to replace the currently pinned deployment id.
    /// If 'latest', use the latest deployment id. If 'keep', keeps the pinned deployment id.
    /// When not provided, the invocation will resume on the pinned deployment id.
    /// When provided and the invocation is either running, or no deployment is pinned, this operation will fail.
    #[clap(long)]
    deployment: Option<String>,
    /// Limit the number of fetched invocations
    #[clap(long, default_value = "500")]
    limit: usize,
}

pub async fn run_resume(State(env): State<CliEnv>, opts: &Resume) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    // Filter only by invoked/suspended/paused, this command has no effect on non-completed invocations
    let filter = format!(
        "{} AND status IN ('paused', 'running', 'backing-off', 'suspended', 'ready') LIMIT {}",
        create_query_filter(&opts.query),
        opts.limit
    );

    let invocations = find_active_invocations_simple(&sql_client, &filter).await?;
    if invocations.is_empty() {
        bail!(
            "No invocations found for query {}! Note that the resume command only works on invocations either 'running', 'backing-off', 'suspended' or 'paused'.",
            opts.query
        );
    };

    render_simple_invocation_list(&invocations);

    // Get the invocation and confirm
    confirm_or_exit("Are you sure you want to resume these invocations?")?;

    // Resume invocations
    let deployment = opts.deployment.as_deref();
    let (resumed, failed_to_resume) =
        collect_and_split_futures(invocations.into_iter().map(|invocation| invocation.id).map(
            |invocation_id| async {
                client
                    .resume_invocation(&invocation_id, deployment)
                    .map_err(anyhow::Error::from)
                    .await
                    .map(|_| invocation_id.clone())
                    .map_err(|e| (invocation_id, e))
            },
        ))
        .await;

    c_println!();
    c_success!("Resumed invocations:");

    // Print new ids
    let mut invocations_table = Table::new_styled();
    invocations_table.set_styled_header(vec!["RESUMED INVOCATIONS"]);
    for id in resumed {
        invocations_table.add_row(vec![Cell::new(&id)]);
    }
    c_indent_table!(0, invocations_table);

    // Print failed ones, if any
    if !failed_to_resume.is_empty() {
        c_warn!("Failed to resume:");
        let mut failed_to_restart_table = Table::new_styled();
        failed_to_restart_table.set_styled_header(vec!["ID", "REASON"]);
        for (id, reason) in failed_to_resume {
            failed_to_restart_table
                .add_row(vec![Cell::new(&id), Cell::new(reason).fg(Color::DarkRed)]);
        }
        c_indent_table!(0, failed_to_restart_table);

        return Err(anyhow!("Failed to resume some invocations"));
    } else {
        c_success!("Request was sent successfully");
    }

    Ok(())
}
