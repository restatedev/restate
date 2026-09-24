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
use crate::ui::fmt::{DryRun, Field, Formatter, OutputFormatter};
use crate::ui::invocations::{
    finish_invocation_results, no_invocations_to_change, print_invocation_changes,
    print_invocation_results,
};

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
    #[clap(verbatim_doc_comment)]
    query: String,
    /// Limit the number of fetched invocations
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
    f.table("restarted", &["invocation_id", "new_invocation_id"], &rows);
    if let [(_, new_id)] = succeeded.as_slice() {
        f.next_step(
            &format!("restate invocations describe {new_id}"),
            "check the new invocation's status",
        );
    }
    finish_invocation_results(f, "restart", succeeded.len(), failed)
}
