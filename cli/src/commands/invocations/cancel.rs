// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Result, anyhow, bail};
use cling::prelude::*;
use comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::{Styled, StyledTable, confirm_or_exit};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{c_indent_table, c_println, c_success, c_warn};

use crate::cli_env::CliEnv;
use crate::clients::batch_execute;
use crate::clients::datafusion_helpers::find_active_invocations_simple;
use crate::clients::{self, AdminClientInterface};
use crate::commands::invocations::{
    DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT, DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    create_query_filter,
};
use crate::ui::invocations::render_simple_invocation_list;
use crate::ui::with_progress;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_cancel")]
pub struct Cancel {
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
    pub(super) query: String,
    /// Ungracefully kill the invocation and its children
    #[clap(long)]
    pub(super) kill: bool,
    /// Limit the number of fetched invocations
    #[clap(long, default_value_t = DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT)]
    pub(super) limit: usize,
}

pub async fn run_cancel(State(env): State<CliEnv>, opts: &Cancel) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    let filter = format!(
        "{} AND status != 'completed' LIMIT {}",
        create_query_filter(&opts.query),
        opts.limit
    );

    let invocations = with_progress(
        "Reading invocations...",
        find_active_invocations_simple(&sql_client, &filter),
    )
    .await?;
    if invocations.is_empty() {
        bail!(
            "No invocations found for query {}! Note that the cancel command only works on non-completed invocations. \
            If you want to remove a completed invocation, consider using the purge command instead.",
            opts.query
        );
    };

    render_simple_invocation_list(
        &invocations,
        DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    );

    // Get the invocation and confirm
    let prompt = format!(
        "Are you sure you want to {} these invocations?",
        if opts.kill {
            Styled(Style::Danger, "kill")
        } else {
            Styled(Style::Warn, "cancel")
        },
    );
    confirm_or_exit(&prompt)?;

    if opts.kill {
        // Kill invocations
        let (killed, failed_to_kill) =
            batch_execute(client, invocations, |client, invocation| async move {
                client
                    .kill_invocation(&invocation.id)
                    .await
                    .map_err(anyhow::Error::from)
            })
            .await;
        let succeeded_count = killed.len();
        let failed_count = failed_to_kill.len();

        c_println!();
        c_success!("Killed {} invocations", succeeded_count);

        // Print failed ones, if any
        if !failed_to_kill.is_empty() {
            c_println!();
            c_warn!("Failed to kill:");
            let mut failed_to_kill_table = Table::new_styled();
            failed_to_kill_table.set_styled_header(vec!["ID", "REASON"]);
            for (inv, reason) in failed_to_kill {
                failed_to_kill_table.add_row(vec![
                    Cell::new(&inv.id),
                    Cell::new(reason).fg(Color::DarkRed),
                ]);
            }
            c_indent_table!(0, failed_to_kill_table);

            return Err(anyhow!(
                "Failed to kill {} invocations out of {}",
                failed_count,
                failed_count + succeeded_count
            ));
        }
    } else {
        // Cancel invocations
        let (cancelled, failed_to_cancel) =
            batch_execute(client, invocations, |client, invocation| async move {
                client
                    .cancel_invocation(&invocation.id)
                    .await
                    .map_err(anyhow::Error::from)
            })
            .await;
        let succeeded_count = cancelled.len();
        let failed_count = failed_to_cancel.len();

        c_println!();
        c_success!("Cancelled {} invocations", succeeded_count);

        // Print failed ones, if any
        if !failed_to_cancel.is_empty() {
            c_println!();
            c_warn!("Failed to cancel:");
            let mut failed_to_cancel_table = Table::new_styled();
            failed_to_cancel_table.set_styled_header(vec!["ID", "REASON"]);
            for (inv, reason) in failed_to_cancel {
                failed_to_cancel_table.add_row(vec![
                    Cell::new(&inv.id),
                    Cell::new(reason).fg(Color::DarkRed),
                ]);
            }
            c_indent_table!(0, failed_to_cancel_table);

            return Err(anyhow!(
                "Failed to cancel {} invocations out of {}",
                failed_count,
                failed_count + succeeded_count
            ));
        }
    }

    Ok(())
}
