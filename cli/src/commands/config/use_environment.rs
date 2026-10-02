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

/// Set the default environment for later commands; overridden by -e and $RESTATE_ENVIRONMENT
///
/// Writes the name to the `environment` file of the CLI config directory
/// (`$HOME/.config/restate`, or $RESTATE_CLI_CONFIG_HOME). The name isn't checked: an unknown
/// one makes later commands fail with no admin URL configured (see
/// `restate config list-environments`).
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_use_environment")]
#[command(after_help = after_help!(
    examples: [
        "restate config use-environment prod",
        "restate config use-environment local     # back to the server on this machine",
    ],
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
#[clap(visible_alias = "use-env")]
pub struct UseEnvironment {
    /// Name of the environment (a section of the CLI config file) to switch to
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
