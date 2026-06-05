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

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_disable")]
pub struct Disable {
    /// Pattern of the rule to disable
    pattern: String,
}

pub async fn run_disable(State(env): State<CliEnv>, opts: &Disable) -> Result<()> {
    toggle_disabled(&env, &opts.pattern, true).await
}
