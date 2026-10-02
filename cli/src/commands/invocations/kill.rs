// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use crate::ui::fmt::DryRun;

/// Stop invocations immediately, without running compensations
///
/// Unlike `cancel`, the handler can't react. This can leave virtual object state
/// and other side effects inconsistent, so prefer `cancel` and use `kill` as a last resort.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_kill")]
#[command(after_help = after_help!(
    examples: [
        "restate invocations kill inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz --yes",
        "restate invocations kill Greeter/greet --dry-run",
    ],
    learn_more: "https://docs.restate.dev/services/invocation/managing-invocations#kill",
))]
pub struct Kill {
    #[arg(help = super::QUERY_HELP, long_help = super::QUERY_LONG_HELP)]
    query: String,
    /// Act on at most this many of the matching invocations, leaving the others untouched
    #[clap(long, default_value_t = DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT)]
    limit: usize,
    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_kill(state: State<CliEnv>, opts: &Kill) -> Result<()> {
    run_cancel(
        state,
        &Cancel {
            query: opts.query.clone(),
            kill: true,
            limit: opts.limit,
            dry_run: opts.dry_run.clone(),
        },
    )
    .await
}
