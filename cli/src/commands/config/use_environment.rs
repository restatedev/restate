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

use restate_cli_util::c_success;

use crate::cli_env::CliEnv;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_use_environment")]
#[clap(visible_alias = "use-env")]
pub struct UseEnvironment {
    /// The name of the environment in the CLI config file to switch to
    #[clap(index = 1)]
    environment_name: String,
}

pub async fn run_use_environment(State(env): State<CliEnv>, opts: &UseEnvironment) -> Result<()> {
    env.write_environment(&opts.environment_name)?;
    c_success!(
        "Updated {} to {}",
        env.environment_file.display(),
        &opts.environment_name
    );

    Ok(())
}
