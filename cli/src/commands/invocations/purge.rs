// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use crate::commands::invocations::{
    DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT, DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    create_query_filter,
};
use crate::ui::invocations::render_simple_invocation_list;

use anyhow::{Result, anyhow, bail};
use cling::prelude::*;
use comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{c_indent_table, c_println, c_success, c_warn};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_purge")]
#[clap(visible_alias = "rm")]
pub struct Purge {
    /// Either an invocation id, or a target string exact match or prefix, e.g.:
    /// * `invocationId`
    /// * `serviceName`
    /// * `serviceName/handler`
    /// * `virtualObjectName`
    /// * `virtualObjectName/key`
    /// * `virtualObjectName/key/handler`
    /// * `workflowName`
    /// * `workflowName/key`
    /// * `workflowName/key/handler`
    query: String,
    /// Limit the number of fetched invocations
    #[clap(long, default_value_t = DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT)]
    limit: usize,
}

pub async fn run_purge(State(env): State<CliEnv>, opts: &Purge) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    let filter = format!(
        "{} AND status = 'completed' LIMIT {}",
        create_query_filter(&opts.query),
        opts.limit
    );

    let invocations = find_active_invocations_simple(&sql_client, &filter).await?;
    if invocations.is_empty() {
        bail!(
            "No invocations found for query {}! Note that the purge command only works on completed invocations. \
            If you need to cancel/kill an invocation, consider using the cancel command instead.",
            opts.query
        );
    };

    render_simple_invocation_list(
        &invocations,
        DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    );

    // Get the invocation and confirm
    confirm_or_exit("Are you sure you want to purge these invocations?")?;

    // Purge invocations
    let (purged, failed_to_purge) =
        batch_execute(client, invocations, |client, invocation| async move {
            client
                .purge_invocation(&invocation.id)
                .await
                .map_err(anyhow::Error::from)
        })
        .await;
    let succeeded_count = purged.len();
    let failed_count = failed_to_purge.len();

    c_println!();
    c_success!("Purged {} invocations", succeeded_count);

    // Print failed ones, if any
    if !failed_to_purge.is_empty() {
        c_println!();
        c_warn!("Failed to purge:");
        let mut failed_to_purge_table = Table::new_styled();
        failed_to_purge_table.set_styled_header(vec!["ID", "REASON"]);
        for (inv, reason) in failed_to_purge {
            failed_to_purge_table.add_row(vec![
                Cell::new(&inv.id),
                Cell::new(reason).fg(Color::DarkRed),
            ]);
        }
        c_indent_table!(0, failed_to_purge_table);

        return Err(anyhow!(
            "Failed to purge {} invocations out of {}",
            failed_count,
            failed_count + succeeded_count
        ));
    }

    Ok(())
}
