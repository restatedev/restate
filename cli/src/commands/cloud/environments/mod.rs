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

/// Deprecated: use `restate cloud tunnel`
#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "env", alias = "environment")]
pub enum Environments {
    /// Deprecated: use `restate cloud tunnel`
    #[command(hide = true)]
    Tunnel(DeprecatedTunnel),
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
