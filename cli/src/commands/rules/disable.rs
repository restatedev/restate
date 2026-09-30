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

/// Stop enforcing a rule, keeping it to enable later
///
/// While disabled, the rule is ignored as if it didn't exist: a less specific matching rule,
/// if any, applies instead. `restate rules enable` enforces it again. Applies without asking
/// for confirmation.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_disable")]
#[command(after_help = after_help!(
    examples: [
        "restate rules disable checkout",
    ],
    learn_more: "https://docs.restate.dev/services/flow-control",
))]
pub struct Disable {
    /// Pattern of the rule to disable, exactly as shown by `restate rules list` (`'*'` is only
    /// the `*` rule)
    pattern: String,
}

pub async fn run_disable(State(env): State<CliEnv>, opts: &Disable) -> Result<()> {
    toggle_disabled(&env, &opts.pattern, true).await
}
