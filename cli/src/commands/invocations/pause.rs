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
use crate::clients::{self, AdminClientInterface, batch_execute};
use crate::ui::invocations::render_simple_invocation_list;

use crate::commands::invocations::{
    DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT, DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
};
use anyhow::{Result, anyhow, bail};
use cling::prelude::*;
use comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{c_indent_table, c_println, c_success, c_warn};
use restate_types::identifiers::InvocationId;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_pause")]
pub struct Pause {
    /// Either an invocation id, or a target string exact match or prefix, e.g.:
    /// * `invocationId`
    /// * `serviceName`
    /// * `serviceName/handler`
    /// * `virtualObjectName`
    /// * `virtualObjectName/key`
    /// * `virtualObjectName/key/handler`
    query: String,
    /// Limit the number of fetched invocations
    #[clap(long, default_value_t = DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT)]
    limit: usize,
}

pub async fn run_pause(State(env): State<CliEnv>, opts: &Pause) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    let q = opts.query.trim();
    let filter = if let Ok(id) = q.parse::<InvocationId>() {
        format!("id = '{id}'")
    } else {
        match q.find('/').unwrap_or_default() {
            0 => format!("target LIKE '{q}/%'"),
            // If there's one slash, let's add the wildcard depending on the service type,
            // so we discriminate correctly with serviceName/handlerName with workflowName/workflowKey
            1 => format!(
                "(target = '{q}' AND target_service_ty = 'service') OR (target LIKE '{q}/%' AND target_service_ty != 'service'))"
            ),
            // Can only be exact match here
            _ => format!("target LIKE '{q}'"),
        }
    };
    // Filter only by invoked/suspended/paused, this command has no effect on non-completed invocations
    let filter = format!("{filter} AND status = 'invoked' LIMIT {}", opts.limit);

    let invocations = find_active_invocations_simple(&sql_client, &filter).await?;
    if invocations.is_empty() {
        bail!(
            "No invocations found for query {}! Note that the pause command only works on invocations either 'running', 'backing-off'.",
            opts.query
        );
    };

    render_simple_invocation_list(
        &invocations,
        DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    );

    // Get the invocation and confirm
    confirm_or_exit("Are you sure you want to pause these invocations?")?;

    // Pause invocations
    let (paused, failed_to_pause) =
        batch_execute(client, invocations, |client, invocation| async move {
            client
                .pause_invocation(&invocation.id)
                .await
                .map_err(anyhow::Error::from)
        })
        .await;
    let succeeded_count = paused.len();
    let failed_count = failed_to_pause.len();

    c_println!();
    c_success!("Paused {} invocations", succeeded_count);

    // Print failed ones, if any
    if !failed_to_pause.is_empty() {
        c_warn!("Failed to pause:");
        let mut failed_to_restart_table = Table::new_styled();
        failed_to_restart_table.set_styled_header(vec!["ID", "REASON"]);
        for (inv, reason) in failed_to_pause {
            failed_to_restart_table.add_row(vec![
                Cell::new(&inv.id),
                Cell::new(reason).fg(Color::DarkRed),
            ]);
        }
        c_indent_table!(0, failed_to_restart_table);

        return Err(anyhow!(
            "Failed to pause {} invocations out of {}",
            failed_count,
            failed_count + succeeded_count
        ));
    }

    Ok(())
}
