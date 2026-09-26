// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use cling::prelude::*;

use restate_cli_util::ui::console::Styled;
use restate_cli_util::ui::stylesheet::Style;

use crate::cli_env::CliEnv;
use crate::clients::batch_execute;
use crate::clients::datafusion_helpers::find_active_invocations_simple;
use crate::clients::{self, AdminClientInterface};
use crate::commands::invocations::{
    DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT, DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    create_query_filter,
};
use crate::ui::fmt::{DryRun, Formatter, OutputFormatter};
use crate::ui::invocations::{
    finish_invocation_results, no_invocations_to_change, print_invocation_changes,
    print_invocation_results,
};
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
    #[clap(verbatim_doc_comment)]
    pub(super) query: String,
    /// Ungracefully kill the invocation and its children
    #[clap(long)]
    pub(super) kill: bool,
    /// Limit the number of fetched invocations
    #[clap(long, default_value_t = DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT)]
    pub(super) limit: usize,
    #[clap(flatten)]
    pub(super) dry_run: DryRun,
}

pub async fn run_cancel(State(env): State<CliEnv>, opts: &Cancel) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    let filter = format!(
        "{} AND status != 'completed' LIMIT {}",
        create_query_filter(&opts.query)?,
        opts.limit
    );

    let invocations = with_progress(
        "Reading invocations...",
        find_active_invocations_simple(&sql_client, &filter),
    )
    .await?;
    if invocations.is_empty() {
        return no_invocations_to_change(format!(
            "No invocations found for query {}! Note that the cancel command only works on non-completed invocations. \
            If you want to remove a completed invocation, consider using the purge command instead.",
            opts.query
        ));
    };

    let (verb, past, style) = if opts.kill {
        ("kill", "Killed", Style::Danger)
    } else {
        ("cancel", "Cancelled", Style::Warn)
    };
    let mut f = Formatter::new();
    print_invocation_changes(
        &mut f,
        &invocations,
        verb,
        DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    )?;
    f.confirm(
        &opts.dry_run,
        &format!(
            "Are you sure you want to {} these invocations?",
            Styled(style, verb)
        ),
    )?;

    let kill = opts.kill;
    let (succeeded, failed) =
        batch_execute(client, invocations, move |client, invocation| async move {
            if kill {
                client.kill_invocation(&invocation.id).await
            } else {
                client.cancel_invocation(&invocation.id).await
            }
            .map_err(anyhow::Error::from)
        })
        .await;

    print_invocation_results(&mut f, past, &succeeded, &failed);
    if let [(inv, _)] = succeeded.as_slice() {
        f.next_step(
            &format!("restate invocations describe {}", inv.id),
            "check the invocation's status",
        );
    }
    finish_invocation_results(f, verb, succeeded.len(), failed)
}
