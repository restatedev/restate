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

use super::toggle_disabled;
use crate::cli_env::CliEnv;

/// Enforce a disabled rule again
///
/// Applies without asking for confirmation.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_enable")]
#[command(after_help = after_help!(
    examples: [
        "restate rules enable checkout",
    ],
    learn_more: "https://docs.restate.dev/services/flow-control",
))]
pub struct Enable {
    /// Pattern of the rule to enable, exactly as shown by `restate rules list` (`'*'` is only
    /// the `*` rule)
    pattern: String,
}

pub async fn run_enable(State(env): State<CliEnv>, opts: &Enable) -> Result<()> {
    toggle_disabled(&env, &opts.pattern, false).await
}
