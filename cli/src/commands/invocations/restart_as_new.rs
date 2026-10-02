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

use restate_cli_util::ui::stylesheet::Style;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::find_active_invocations_simple;
use crate::clients::{self, AdminClientInterface, batch_execute};
use crate::commands::invocations::{
    DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT, DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    create_query_filter,
};
use crate::ui::fmt::{DryRun, Field, Formatter, IfEmpty, IncludeFormatting, OutputFormatter};
use crate::ui::invocations::{
    finish_invocation_results, no_invocations_to_change, print_invocation_changes,
    print_invocation_results,
};

/// Run completed invocations again, as new invocations
///
/// Each new invocation gets a new id and the input and headers of the original one, and runs
/// from the start: nothing of the original execution is kept, and the original invocation is
/// left untouched. Only affects completed invocations (including failed, cancelled and killed
/// ones); workflows are not supported. To continue a paused invocation instead, use `resume`.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_restart_as_new")]
#[clap(visible_alias = "restart")]
#[command(after_help = after_help!(
    examples: [
        "restate invocations restart-as-new inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz --yes",
        "restate invocations restart-as-new Greeter/greet --dry-run",
    ],
    learn_more: "https://docs.restate.dev/services/invocation/managing-invocations#restart-as-new",
))]
pub struct RestartAsNew {
    #[arg(help = super::QUERY_HELP, long_help = super::QUERY_LONG_HELP)]
    query: String,
    /// Act on at most this many of the matching invocations, leaving the others untouched
    #[clap(long, default_value_t = DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT)]
    limit: usize,
    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_restart_as_new(State(env): State<CliEnv>, opts: &RestartAsNew) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    let filter = format!(
        "{} AND status = 'completed' LIMIT {}",
        create_query_filter(&opts.query)?,
        opts.limit
    );

    let invocations = find_active_invocations_simple(&sql_client, &filter).await?;
    if invocations.is_empty() {
        return no_invocations_to_change(format!(
            "No invocations found for query {}! Note that the restart command only works on completed invocations.",
            opts.query
        ));
    };

    let mut f = Formatter::new();
    print_invocation_changes(
        &mut f,
        &invocations,
        "restart",
        DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    )?;
    f.confirm(
        &opts.dry_run,
        "Are you sure you want to restart these invocations?",
    )?;

    let (succeeded, failed) = batch_execute(client, invocations, |client, invocation| async move {
        let envelope = client
            .restart_invocation(&invocation.id)
            .await
            .map_err(anyhow::Error::from)?;
        let response = envelope.into_body().await.map_err(anyhow::Error::from)?;
        Ok(response.new_invocation_id)
    })
    .await;

    print_invocation_results(&mut f, "Restarted", &succeeded, &failed);
    let rows: Vec<Vec<Field>> = succeeded
        .iter()
        .map(|(old, new_id)| {
            vec![
                Field::new(old.id.clone()),
                Field::styled(new_id.to_string(), Style::Info),
            ]
        })
        .collect();
    f.table(
        "restarted",
        &["invocation_id", "new_invocation_id"],
        &rows,
        IfEmpty::Nothing,
    );
    if let [(_, new_id)] = succeeded.as_slice() {
        f.next_step(
            &format!("restate invocations describe {new_id}"),
            "check the new invocation's status",
            IncludeFormatting::Yes,
        );
    }
    finish_invocation_results(f, "restart", succeeded.len(), failed)
}
