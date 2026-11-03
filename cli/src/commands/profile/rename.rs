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

use crate::cli_env::{CliEnv, set_saved_profile};
use crate::error::RestateCliError;
use crate::ui::fmt::{Field, Formatter, IncludeFormatting, Outcome, OutputFormatter};

use super::sections::{check_name, is_local};

/// Rename a profile of the CLI config file
///
/// The current profile stays selected under its new name; a renamed Cloud profile keeps
/// its name across logins. `local` can't be renamed.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_rename")]
#[command(after_help = after_help!(
    examples: [
        "restate profile rename my-env prod",
    ],
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
#[clap(visible_alias = "mv")]
pub struct Rename {
    /// Name of the profile (a section of the CLI config file) to rename
    name: String,

    /// Its new name
    new_name: String,
}

pub async fn run_rename(State(env): State<CliEnv>, opts: &Rename) -> Result<()> {
    if is_local(&opts.name) {
        return Err(
            RestateCliError::bad_input("The built-in `local` profile can't be renamed").into(),
        );
    }
    check_name(&opts.new_name)?;

    let mut doc = env.load_config_document()?;
    let Some(section) = doc.remove(&opts.name) else {
        return Err(RestateCliError::not_found(format!(
            "No profile \"{}\" in {}",
            opts.name,
            env.config_file.display()
        ))
        .with_next_step("restate profile list", "see the profiles")
        .into());
    };
    if doc.get(&opts.new_name).is_some() {
        return Err(RestateCliError::bad_input(format!(
            "A profile named \"{}\" already exists in {}",
            opts.new_name,
            env.config_file.display()
        ))
        .into());
    }
    doc.insert(&opts.new_name, section);
    let is_current = env.saved_profile()?.as_deref() == Some(opts.name.as_str());
    if is_current {
        set_saved_profile(&mut doc, Some(&opts.new_name));
    }
    env.write_config(&doc.to_string())?;

    let mut f = Formatter::new();
    f.detail(
        "profile",
        [
            ("name", Field::new(opts.new_name.as_str())),
            ("previous_name", Field::new(opts.name.as_str())),
            ("current", Field::json_only(is_current)),
        ],
    );
    f.outcome(
        "result",
        Field::with_display(
            "renamed",
            format!(
                "Renamed profile \"{}\" to \"{}\" in {}",
                opts.name,
                opts.new_name,
                env.config_file.display()
            ),
        ),
        Outcome::Success,
    );
    f.next_step(
        "restate profile list",
        "see the profiles",
        IncludeFormatting::Yes,
    );
    f.finish()
}
