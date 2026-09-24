// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Result, bail};
use cling::prelude::*;

use restate_admin_rest_model::version::AdminApiVersion;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::find_active_invocations_simple;
use crate::clients::{self, AdminClientInterface, batch_execute};
use crate::commands::invocations::{
    DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT, DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    create_query_filter,
};
use crate::ui::fmt::{DryRun, Formatter, OutputFormatter};
use crate::ui::invocations::{
    finish_invocation_results, no_invocations_to_change, print_invocation_changes,
    print_invocation_results,
};

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
    #[clap(verbatim_doc_comment)]
    query: String,
    /// Limit the number of fetched invocations
    #[clap(long, default_value_t = DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT)]
    limit: usize,
    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_pause(State(env): State<CliEnv>, opts: &Pause) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;

    if client.admin_api_version < AdminApiVersion::V3 {
        bail!("Pausing invocations requires admin API version 3 or later (Restate server v1.6+)");
    }

    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    // Pause only applies to in-flight invocations (running, backing-off, or suspended).
    let filter = format!(
        "{} AND status IN ('invoked', 'suspended') LIMIT {}",
        create_query_filter(&opts.query)?,
        opts.limit
    );

    let invocations = find_active_invocations_simple(&sql_client, &filter).await?;
    if invocations.is_empty() {
        return no_invocations_to_change(format!(
            "No invocations found for query {}! Note that the pause command only works on invocations either 'running', 'backing-off' or 'suspended'.",
            opts.query
        ));
    };

    let mut f = Formatter::new();
    print_invocation_changes(
        &mut f,
        &invocations,
        "pause",
        DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT,
    )?;
    f.confirm(
        &opts.dry_run,
        "Are you sure you want to pause these invocations?",
    )?;

    let (succeeded, failed) = batch_execute(client, invocations, |client, invocation| async move {
        client
            .pause_invocation(&invocation.id)
            .await
            .map_err(anyhow::Error::from)
    })
    .await;

    print_invocation_results(&mut f, "Paused", &succeeded, &failed);
    if let [(inv, _)] = succeeded.as_slice() {
        f.next_step(
            &format!("restate invocations describe {}", inv.id),
            "check the invocation's status",
        );
    }
    finish_invocation_results(f, "pause", succeeded.len(), failed)
}
