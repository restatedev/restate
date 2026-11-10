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
use figment::Profile;

use crate::{
    cli_env::{CliEnv, EnvironmentType, ProfileSource},
    error::RestateCliError,
    ui::fmt::{Field, Formatter, IncludeFormatting, OutputFormatter},
};

use super::{
    list::cloud_columns,
    sections::{is_local, list_profiles},
};

/// Show a profile: its type, the Cloud user, account and environment, and its URLs
///
/// Defaults to the current profile. The credential is reported as present or absent, never
/// printed.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[command(after_help = after_help!(
    examples: [
        "restate profile describe",
        "restate profile describe staging --json",
    ],
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// Name of the profile (a section of the CLI config file); the current one when omitted
    name: Option<String>,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    let name = match &opts.name {
        Some(name) => name.as_str(),
        None if matches!(env.profile_source, ProfileSource::Variables) => {
            return Err(RestateCliError::bad_input(
                "The current profile comes from the RESTATE_* variables, not from the config file",
            )
            .with_next_step("restate whoami", "see what they resolve to")
            .with_next_step("restate profile describe <name>", "describe a profile")
            .into());
        }
        None => env.profile.as_str().as_str(),
    };
    if !is_local(name)
        && !list_profiles(&env.load_config_document()?)
            .iter()
            .any(|p| p == name)
    {
        return Err(RestateCliError::not_found(format!(
            "No profile \"{name}\" in {}",
            env.config_file.display()
        ))
        .with_next_step("restate profile list", "see the profiles")
        .into());
    }

    let profile = Profile::new(name);
    let config = CliEnv::profile_config(&env.config_file, &profile)?;
    let current = profile == env.profile && !matches!(env.profile_source, ProfileSource::Variables);
    let (user, account, environment) = cloud_columns(&config);
    let url = |url: Option<String>| Field::with_display(&url, url.as_deref().unwrap_or("(NONE)"));
    let credential = match (&config.bearer_token, &config.environment_type) {
        (Some(_), _) => "bearer_token",
        (None, EnvironmentType::Cloud) => "cloud_login",
        (None, EnvironmentType::Default) => "none",
    };

    let mut f = Formatter::new();
    f.detail(
        "profile",
        [
            ("name", Field::new(name)),
            (
                "current",
                Field::with_display(current, if current { "yes" } else { "no" }),
            ),
            (
                "type",
                Field::new(match config.environment_type {
                    EnvironmentType::Default => "self_hosted",
                    EnvironmentType::Cloud => "cloud",
                }),
            ),
            ("user", user),
            ("account", account),
            ("environment", environment),
            (
                "admin_base_url",
                url(config.admin_base_url.as_ref().map(ToString::to_string)),
            ),
            (
                "ingress_base_url",
                url(config.ingress_base_url.as_ref().map(ToString::to_string)),
            ),
            ("credential", Field::new(credential)),
        ],
    );
    if !current {
        f.next_step(
            &format!("restate profile use {name}"),
            "make it the current profile",
            IncludeFormatting::Yes,
        );
    }
    f.next_step(
        "restate whoami",
        "check the connection",
        IncludeFormatting::Yes,
    );
    f.finish()
}
