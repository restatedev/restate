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
use comfy_table::{Attribute, Cell, Color, Table};
use futures::TryFutureExt;
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{c_indent_table, c_println, c_success, c_warn};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_restart_as_new")]
#[clap(visible_alias = "restart")]
pub struct RestartAsNew {
    /// Either an invocation id, or a target string exact match or prefix, e.g.:
    /// * `invocationId`
    /// * `serviceName`
    /// * `serviceName/handler`
    /// * `virtualObjectName`
    /// * `virtualObjectName/key`
    /// * `virtualObjectName/key/handler`
    query: String,
}

pub async fn run_restart_as_new(State(env): State<CliEnv>, opts: &RestartAsNew) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    let filter = format!(
        "{} AND status = 'completed'",
        create_query_filter(&opts.query)
    );

    let invocations = find_active_invocations_simple(&sql_client, &filter).await?;
    if invocations.is_empty() {
        bail!(
            "No invocations found for query {}! Note that the restart command only works on completed invocations.",
            opts.query
        );
    };

    render_simple_invocation_list(&invocations);

    // Get the invocation and confirm
    confirm_or_exit("Are you sure you want to restart these invocations?")?;

    // Restart invocations
    let (restarted, failed_to_restart) =
        collect_and_split_futures(invocations.into_iter().map(|invocation| invocation.id).map(
            |invocation_id| async {
                client
                    .restart_invocation(&invocation_id)
                    .map_err(anyhow::Error::from)
                    .and_then(|e| async { e.into_body().map_err(anyhow::Error::from).await })
                    .await
                    .map(|response| (invocation_id.clone(), response.new_invocation_id))
                    .map_err(|e| (invocation_id, e))
            },
        ))
        .await;

    c_println!();
    c_success!("Restarted invocations:");

    // Print success
    let mut invocations_table = Table::new_styled();
    invocations_table.set_styled_header(vec!["OLD ID", "NEW ID"]);
    for (old_id, restart_as_new_response) in restarted {
        invocations_table.add_row(vec![
            Cell::new(&old_id),
            Cell::new(restart_as_new_response).add_attribute(Attribute::Bold),
        ]);
    }
    c_indent_table!(0, invocations_table);

    // Print failed ones, if any
    if !failed_to_restart.is_empty() {
        c_println!();
        c_warn!("Failed to restart:");
        let mut failed_to_restart_table = Table::new_styled();
        failed_to_restart_table.set_styled_header(vec!["ID", "REASON"]);
        for (id, reason) in failed_to_restart {
            failed_to_restart_table
                .add_row(vec![Cell::new(&id), Cell::new(reason).fg(Color::DarkRed)]);
        }
        c_indent_table!(0, failed_to_restart_table);

        return Err(anyhow!("Failed to restart some invocations"));
    }

    Ok(())
}
