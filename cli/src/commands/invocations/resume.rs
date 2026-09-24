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
#[cling(run = "run_resume")]
pub struct Resume {
    /// Either an invocation id, or a target string exact match or prefix, e.g.:
    /// * `invocationId`
    /// * `serviceName`
    /// * `serviceName/handler`
    /// * `virtualObjectName`
    /// * `virtualObjectName/key`
    /// * `virtualObjectName/key/handler`
    #[clap(verbatim_doc_comment)]
    query: String,

    /// When resuming from paused/suspended, provide a deployment id to use to replace the currently pinned deployment id.
    /// If 'latest', use the latest deployment id. If 'keep', keeps the pinned deployment id.
    /// When not provided, the invocation will resume on the pinned deployment id.
    /// When provided and the invocation is either running, or no deployment is pinned, this operation will fail.
    #[clap(long)]
    deployment: Option<String>,
    /// Limit the number of fetched invocations
    #[clap(long, default_value_t = DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT)]
    limit: usize,
    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_resume(State(env): State<CliEnv>, opts: &Resume) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    // Filter only by invoked/suspended/paused, this command has no effect on non-completed invocations
    let filter = format!(
        "{} AND status IN ('paused', 'invoked', 'suspended') LIMIT {}",
        create_query_filter(&opts.query)?,
        opts.limit
    );

    let invocations = find_active_invocations_simple(&sql_client, &filter).await?;
    if invocations.is_empty() {
        return no_invocations_to_change(format!(
            "No invocations found for query {}! Note that the resume command only works on invocations either 'running', 'backing-off', 'suspended' or 'paused'.",
            opts.query
        ));
    };

    let mut f = Formatter::new();
    print_invocation_changes(
        &mut f,
        &invocations,
        "resume",
        DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    )?;
    if let Some(deployment) = &opts.deployment {
        f.value("deployment", Field::new(deployment.clone()));
    }
    f.confirm(
        &opts.dry_run,
        "Are you sure you want to resume these invocations?",
    )?;

    let deployment = opts.deployment.clone();
    let (succeeded, failed) = batch_execute(
        client,
        invocations
            .into_iter()
            .map(|i| (i, deployment.clone()))
            .collect(),
        |client, (invocation, deployment)| async move {
            client
                .resume_invocation(&invocation.id, deployment.as_deref())
                .await
                .map_err(anyhow::Error::from)
        },
    )
    .await;
    // Drop the per-invocation deployment carried through the batch.
    let succeeded: Vec<_> = succeeded
        .into_iter()
        .map(|((inv, _), out)| (inv, out))
        .collect();
    let failed: Vec<_> = failed
        .into_iter()
        .map(|((inv, _), err)| (inv, err))
        .collect();

    print_invocation_results(&mut f, "Resumed", &succeeded, &failed);
    if let [(inv, _)] = succeeded.as_slice() {
        f.next_step(
            &format!("restate invocations describe {}", inv.id),
            "check the invocation's status",
        );
    }
    finish_invocation_results(f, "resume", succeeded.len(), failed)
}
