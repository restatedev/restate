// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT;

use anyhow::Result;
use cling::prelude::*;

use crate::cli_env::CliEnv;
use crate::commands::invocations::cancel::{Cancel, run_cancel};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_kill")]
pub struct Kill {
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

pub async fn run_kill(state: State<CliEnv>, opts: &Kill) -> Result<()> {
    run_cancel(
        state,
        &Cancel {
            query: opts.query.clone(),
            kill: true,
            limit: opts.limit,
        },
    )
    .await
}
