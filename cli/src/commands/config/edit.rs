// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{cli_env::CliEnv, console};
use anyhow::Result;
use cling::prelude::*;

/// Open the CLI config file in an editor
///
/// Uses $RESTATE_EDITOR, else $VISUAL or $EDITOR, and needs a terminal. Scripts and agents
/// can edit the file directly instead: it's `$HOME/.config/restate/config.toml`, or
/// `config.toml` in $RESTATE_CLI_CONFIG_HOME, or the file named by $RESTATE_CLI_CONFIG. To add
/// an environment, append a TOML section named after it:
///
/// ```toml
/// [prod]
/// admin_base_url = "https://restate.example.com:9070"
/// ingress_base_url = "https://restate.example.com:8080"
/// bearer_token = "..."
/// ```
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_edit")]
#[command(verbatim_doc_comment)]
#[command(after_help = after_help!(
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
pub struct Edit {}

pub async fn run_edit(State(env): State<CliEnv>, _opts: &Edit) -> Result<()> {
    console::c_eprintln!("Editing {}", env.config_file.display());

    env.open_default_editor(
        &env.config_file,
        &format!("edit {} directly instead", env.config_file.display()),
    )?;

    Ok(())
}
