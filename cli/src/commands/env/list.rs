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
use figment::{
    Figment, Profile,
    providers::{Format, Serialized, Toml},
};

use crate::{
    cli_env::{CliConfig, CliEnv, EnvironmentSource, LOCAL_PROFILE},
    ui::fmt::{Field, Formatter, IfEmpty, IncludeFormatting, OutputFormatter},
};

/// List the environments of the CLI config file, marking the current one
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[command(after_help = after_help!(
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
#[clap(visible_alias = "ls")]
pub struct List {}

pub async fn run_list(State(env): State<CliEnv>, _opts: &List) -> Result<()> {
    let defaults = CliConfig::default();
    let local = CliConfig::local();

    let mut figment =
        Figment::from(Serialized::defaults(defaults)).merge(Serialized::from(local, LOCAL_PROFILE));

    // Load configuration file
    if env.config_file.is_file() {
        figment = figment.merge(Toml::file_exact(&env.config_file).nested());
    }

    let profiles: Vec<Profile> = figment
        .profiles()
        .filter(|profile| *profile != Profile::Global && *profile != Profile::Default)
        .cloned()
        .collect();

    let rows: Vec<[Field; 4]> = profiles
        .into_iter()
        .map(|profile| {
            let selected = figment.clone().select(profile.clone());
            let admin_base_url = selected
                .find_value("admin_base_url")
                .ok()
                .and_then(|url| url.as_str().map(str::to_owned));
            let environment_type = selected
                .find_value("environment_type")
                .ok()
                .and_then(|t| t.as_str().map(str::to_owned))
                .filter(|t| t == "cloud")
                .unwrap_or_else(|| "self_hosted".to_owned());
            let current = profile == env.environment
                && !matches!(env.environment_source, EnvironmentSource::Variables);

            [
                Field::with_display(current, if current { "*" } else { "" }),
                Field::new(profile.as_str().as_str()),
                Field::new(environment_type),
                Field::with_display(
                    &admin_base_url,
                    admin_base_url.as_deref().unwrap_or("(NONE)"),
                ),
            ]
        })
        .collect();

    let mut f = Formatter::new();
    f.table(
        "environments",
        &["current", "name", "type", "admin_base_url"],
        rows,
        IfEmpty::Nothing,
    );
    f.next_step(
        "restate env use <name>",
        "switch to another environment",
        IncludeFormatting::No,
    );
    f.finish()
}
