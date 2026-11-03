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

use crate::cli_env::{CliEnv, set_saved_environment};
use crate::error::RestateCliError;
use crate::ui::fmt::{
    DryRun, Field, Formatter, IfEmpty, IncludeFormatting, Outcome, OutputFormatter,
};

use super::profile::is_local;

/// Remove an environment from the CLI config file
///
/// If it is the current environment, later commands use `local` (a server on this machine),
/// which can't be removed. Nothing is changed on the server.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_remove")]
#[command(after_help = after_help!(
    examples: [
        "restate env remove staging --yes",
    ],
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
#[clap(visible_alias = "rm")]
pub struct Remove {
    /// Name of the environment (a section of the CLI config file) to remove
    name: String,

    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_remove(State(env): State<CliEnv>, opts: &Remove) -> Result<()> {
    if is_local(&opts.name) {
        return Err(RestateCliError::bad_input(
            "The built-in `local` environment can't be removed",
        )
        .into());
    }

    let mut doc = env.load_config_document()?;
    let mut f = Formatter::new();
    if doc.get(&opts.name).is_none() {
        f.nothing_to_do(format!(
            "No environment \"{}\" in {}",
            opts.name,
            env.config_file.display()
        ));
        f.next_step(
            "restate env list",
            "see the environments",
            IncludeFormatting::Yes,
        );
        return f.finish();
    }

    let is_current = env.saved_environment()?.as_deref() == Some(opts.name.as_str());
    f.table(
        "changes",
        &["environment", "change"],
        [[Field::new(opts.name.as_str()), Field::new("remove")]],
        IfEmpty::Nothing,
    );
    if is_current {
        f.warning(&format!(
            "\"{}\" is the current environment: later commands will use `local`",
            opts.name
        ));
    }
    f.confirm(
        &opts.dry_run,
        &format!("Remove environment \"{}\"?", opts.name),
    )?;

    doc.remove(&opts.name);
    if is_current {
        set_saved_environment(&mut doc, None);
    }
    env.write_config(&doc.to_string())?;

    f.outcome(
        "result",
        Field::with_display(
            "removed",
            format!(
                "Removed environment \"{}\" from {}",
                opts.name,
                env.config_file.display()
            ),
        ),
        Outcome::Success,
    );
    f.next_step(
        "restate env list",
        "see the environments",
        IncludeFormatting::Yes,
    );
    f.finish()
}
