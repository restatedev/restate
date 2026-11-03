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

use crate::cli_env::{CliEnv, LOCAL_PROFILE};
use crate::console::choose;
use crate::error::RestateCliError;
use crate::ui::fmt::{Field, Formatter, IncludeFormatting, Outcome, OutputFormatter};

use super::profile::{is_local, list_profiles};

/// Select the environment for later commands; overridden by -e and $RESTATE_ENVIRONMENT
///
/// The name is one of the CLI config file's environments (see `restate env list`), or
/// `local` (a server on this machine). Without a name, asks to pick one. The choice is saved
/// in the config file.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_use")]
#[command(after_help = after_help!(
    examples: [
        "restate env use prod",
        "restate env use local     # back to the server on this machine",
        "restate env use           # pick one",
    ],
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
pub struct Use {
    /// Name of the environment to switch to; asked for when omitted
    pub(crate) name: Option<String>,
}

pub async fn run_use(State(env): State<CliEnv>, opts: &Use) -> Result<()> {
    let doc = env.load_config_document()?;
    let mut names = vec![LOCAL_PROFILE.as_str().to_string()];
    names.extend(list_profiles(&doc));

    let name = match &opts.name {
        Some(name) if is_local(name) || names.contains(name) => name.clone(),
        Some(name) => {
            return Err(RestateCliError::not_found(format!(
                "No environment \"{name}\" in {}; the environments are: [{}]",
                env.config_file.display(),
                names.join(", ")
            ))
            .with_next_step("restate env list", "see the environments")
            .with_next_step("restate env add", "add one")
            .into());
        }
        None => {
            let current = env.saved_environment()?;
            let labels: Vec<String> = names
                .iter()
                .map(|name| {
                    if current.as_deref() == Some(name.as_str()) {
                        format!("{name} (current)")
                    } else {
                        name.clone()
                    }
                })
                .collect();
            names[choose("Select an environment:", &labels)?].clone()
        }
    };

    let changed = env.saved_environment()?.as_deref() != Some(name.as_str());
    if changed {
        env.write_environment(&name)?;
    }

    let mut f = Formatter::new();
    f.detail(
        "environment",
        [
            ("name", Field::new(name.as_str())),
            (
                "config_file",
                Field::new(env.config_file.display().to_string()),
            ),
            ("changed", Field::json_only(changed)),
        ],
    );
    f.outcome(
        "result",
        Field::with_display(
            "selected",
            if changed {
                format!("Current environment is now \"{name}\"")
            } else {
                format!("\"{name}\" is already the current environment")
            },
        ),
        Outcome::Success,
    );
    f.next_step(
        "restate whoami",
        "check the server it points to",
        IncludeFormatting::Yes,
    );
    f.finish()
}
