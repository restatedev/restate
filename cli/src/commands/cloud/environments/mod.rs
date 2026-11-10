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
use crate::error::RestateCliError;

/// Deprecated: use `restate cloud tunnel`
#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "env", alias = "environment")]
pub enum Environments {
    /// Deprecated: run `restate cloud login`, it imports all your Cloud environments as profiles
    #[command(hide = true)]
    Configure(DeprecatedConfigure),
    /// Deprecated: use `restate cloud tunnel`
    #[command(hide = true)]
    Tunnel(DeprecatedTunnel),
}

/// Deprecated: run `restate cloud login`, it imports all your Cloud environments as profiles
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_configure")]
pub struct DeprecatedConfigure {
    /// Accepted so that existing invocations still parse, and ignored
    #[arg(hide = true)]
    _environment: Option<String>,
}

async fn run_configure() -> Result<()> {
    Err(RestateCliError::bad_input(
        "`restate cloud env configure` was removed: `restate cloud login` imports all your Cloud \
         environments as profiles",
    )
    .with_unformatted_next_step("restate cloud login", "import Cloud profiles")
    .with_next_step("restate profile use", "select one of them")
    .into())
}

/// Deprecated: use `restate cloud tunnel`
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_tunnel")]
pub struct DeprecatedTunnel {
    #[clap(flatten)]
    inner: super::tunnel::Tunnel,
}

async fn run_tunnel(State(env): State<CliEnv>, opts: &DeprecatedTunnel) -> Result<()> {
    crate::commands::deprecated("restate cloud env tunnel", "restate cloud tunnel");
    super::tunnel::run_tunnel(State(env), &opts.inner).await
}
