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
use crate::clients::{self, AdminClientInterface};
use crate::ui::invocations::render_simple_invocation_list;

use anyhow::{Result, anyhow, bail};
use cling::prelude::*;
use comfy_table::{Cell, Color, Table};
use futures::TryFutureExt;
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{c_indent_table, c_println, c_success, c_warn};
use restate_types::identifiers::InvocationId;

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
}

pub async fn run_resume(State(env): State<CliEnv>, opts: &Resume) -> Result<()> {
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
    let filter = format!(
        "{filter} AND status IN ('paused', 'running', 'backing-off', 'suspended', 'ready')"
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

    // Restart invocations
    let mut resumed = Vec::with_capacity(invocations.len());
    let mut failed_to_resume = vec![];
    for inv in invocations {
        match client
            .resume_invocation(&inv.id)
            .map_err(anyhow::Error::from)
            .await
        {
            Ok(_) => {
                resumed.push(inv.id);
            }
            Err(err) => {
                failed_to_resume.push((inv.id, err));
            }
        }
    }

    c_println!();

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
