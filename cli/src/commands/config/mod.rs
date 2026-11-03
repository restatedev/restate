// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod edit;
mod view;

use anyhow::Result;
use cling::prelude::*;

use crate::cli_env::CliEnv;
use crate::commands::env;

#[derive(Run, Subcommand, Clone)]
pub enum Config {
    // Commands are documented on their own struct.
    View(view::View),
    Edit(edit::Edit),
    /// Deprecated: use `restate env list`
    #[command(hide = true)]
    ListEnvironments(ListEnvironments),
    /// Deprecated: use `restate env use`
    #[command(hide = true)]
    UseEnvironment(UseEnvironment),
}

/// Deprecated: use `restate env list`
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list_environments")]
#[clap(alias = "list-env")]
pub struct ListEnvironments {}

async fn run_list_environments(State(env): State<CliEnv>, _opts: &ListEnvironments) -> Result<()> {
    super::deprecated("restate config list-environments", "restate env list");
    env::list::run_list(State(env), &env::list::List {}).await
}

/// Deprecated: use `restate env use`
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_use_environment")]
#[clap(alias = "use-env")]
pub struct UseEnvironment {
    /// Name of the environment (a section of the CLI config file) to switch to
    #[clap(index = 1)]
    environment_name: String,
}

async fn run_use_environment(State(env): State<CliEnv>, opts: &UseEnvironment) -> Result<()> {
    super::deprecated("restate config use-environment", "restate env use");
    env::use_env::run_use(
        State(env),
        &env::use_env::Use {
            name: Some(opts.environment_name.clone()),
        },
    )
    .await
}
