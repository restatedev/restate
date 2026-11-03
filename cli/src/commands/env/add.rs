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

use restate_types::net::address::{AdminPort, AdvertisedAddress, HttpIngressPort};

use crate::cli_env::CliEnv;
use crate::console::confirm_or_exit;
use crate::error::RestateCliError;
use crate::ui::fmt::{Field, Formatter, IncludeFormatting, Outcome, OutputFormatter};

use super::profile::{ProfileSpec, check_name, list_profiles, select_profile, write_profile};

/// Add an environment to the CLI config file, and select it
///
/// By default a Restate Cloud environment, picked from your accounts (needs `restate cloud
/// login`), or named with --cloud-environment. Its section is named after the environment.
///
/// With --admin-url, a self-hosted server, under the NAME you give: its admin URL, and its
/// ingress URL and bearer token when given. Overwriting an existing environment asks for
/// confirmation, or needs --yes.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_add")]
#[command(after_help = after_help!(
    examples: [
        "restate env add                                    # pick a Cloud account and environment",
        "restate env add --cloud-environment my-acct/prod   # a Cloud environment, by name or id",
        "restate env add staging --admin-url https://restate.example.com:9070 --token \"$TOKEN\"",
        "restate env add ci --admin-url http://restate:9070 --ingress-url http://restate:8080 --no-use",
    ],
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
pub struct Add {
    /// Name of a self-hosted environment (its section in the CLI config file), with
    /// --admin-url. Cloud environments are named after themselves
    name: Option<String>,

    /// The Cloud environment to add, `[ACCOUNT/]ENVIRONMENT` (names or ids), instead of
    /// picking one
    #[cfg(feature = "cloud")]
    #[clap(long, value_name = "[ACCOUNT/]ENVIRONMENT", conflicts_with_all = ["admin_url", "ingress_url", "token"])]
    cloud_environment: Option<crate::commands::cloud::profile::EnvironmentSpecifier>,

    /// Admin API base URL of a self-hosted server, e.g. http://localhost:9070
    #[clap(long, value_name = "URL", help_heading = "Self-hosted server")]
    admin_url: Option<AdvertisedAddress<AdminPort>>,

    /// Ingress base URL of the server, e.g. http://localhost:8080 (needed by commands that
    /// invoke services)
    #[clap(
        long,
        value_name = "URL",
        help_heading = "Self-hosted server",
        requires = "admin_url"
    )]
    ingress_url: Option<AdvertisedAddress<HttpIngressPort>>,

    /// Bearer token the server requires, if any
    #[clap(
        long,
        value_name = "TOKEN",
        help_heading = "Self-hosted server",
        requires = "admin_url"
    )]
    token: Option<String>,

    /// Add the environment without making it the current one
    #[clap(long)]
    no_use: bool,
}

pub async fn run_add(State(env): State<CliEnv>, opts: &Add) -> Result<()> {
    if let Some(admin_url) = &opts.admin_url {
        let Some(name) = &opts.name else {
            return Err(RestateCliError::bad_input(
                "Missing the environment name; pass it before --admin-url, e.g. `restate env add staging --admin-url ...`",
            )
            .into());
        };
        return add_self_hosted(&env, name, admin_url, opts).await;
    }
    if let Some(name) = &opts.name {
        return Err(RestateCliError::bad_input(format!(
            "A Cloud environment's section is named after it, so a name (\"{name}\") can't be given; to add a self-hosted server, pass --admin-url"
        ))
        .into());
    }

    #[cfg(feature = "cloud")]
    {
        add_cloud(&env, opts.cloud_environment.as_ref(), opts.no_use).await
    }
    #[cfg(not(feature = "cloud"))]
    {
        Err(RestateCliError::bad_input(
            "This build of the CLI has no Restate Cloud support; add a self-hosted server with --admin-url",
        )
        .into())
    }
}

#[cfg(feature = "cloud")]
async fn add_cloud(
    env: &CliEnv,
    specifier: Option<&crate::commands::cloud::profile::EnvironmentSpecifier>,
    no_use: bool,
) -> Result<()> {
    use restate_cli_util::CliContext;

    use crate::clients::cloud::CloudClient;
    use crate::commands::cloud::profile::{Choice, choose_environment, nothing_to_add};
    use crate::error::ErrorKind;

    use super::profile::add_cloud_section;

    if env.config.cloud.credentials.is_none() {
        return Err(
            RestateCliError::new(ErrorKind::Auth, "Not logged in to Restate Cloud")
                .with_next_step("restate cloud login", "log in, then add the environment")
                .with_next_step(
                    "restate env add <name> --admin-url <url>",
                    "add a self-hosted server instead",
                )
                .into(),
        );
    }
    if specifier.is_none() && !CliContext::get().can_prompt() {
        return Err(RestateCliError::bad_input(
            "Picking a Cloud environment needs a terminal; in non-interactive mode name it with --cloud-environment [ACCOUNT/]ENVIRONMENT, or add a self-hosted server with `restate env add <name> --admin-url <url>`",
        )
        .into());
    }
    let client = CloudClient::new(env)?;

    let choice = choose_environment(&client, specifier, None).await?;
    if let Some(err) = nothing_to_add(&choice) {
        return Err(err.into());
    }
    let Choice::Environment(chosen) = choice else {
        unreachable!("without a current environment there is nothing to keep");
    };

    let mut doc = env.load_config_document()?;
    let existed = list_profiles(&doc).contains(&chosen.environment.name);
    let name = add_cloud_section(env, &mut doc, &chosen)?;
    env.write_config(&doc.to_string())?;

    let mut f = Formatter::new();
    f.detail(
        "environment",
        [
            ("name", Field::new(name.as_str())),
            ("type", Field::new("cloud")),
            ("account", Field::new(chosen.account_name.as_str())),
            ("account_id", Field::json_only(chosen.account_id.as_str())),
            (
                "environment_id",
                Field::new(chosen.environment.environment_id.as_str()),
            ),
            ("region", Field::new(chosen.environment.region.as_str())),
            (
                "admin_base_url",
                Field::new(chosen.environment.admin_base_url.as_str()),
            ),
            (
                "ingress_base_url",
                Field::new(chosen.environment.ingress_base_url.as_str()),
            ),
        ],
    );
    finish_add(f, env, &name, existed, no_use)
}

async fn add_self_hosted(
    env: &CliEnv,
    name: &str,
    admin_url: &AdvertisedAddress<AdminPort>,
    opts: &Add,
) -> Result<()> {
    check_name(name)?;
    let mut doc = env.load_config_document()?;
    let existed = list_profiles(&doc).iter().any(|p| p == name);
    if existed {
        confirm_or_exit(&format!("Overwrite existing environment \"{name}\"?"))?;
    }
    let token = opts.token.as_deref().filter(|t| !t.is_empty());
    write_profile(
        &mut doc,
        name,
        &ProfileSpec::SelfHosted {
            admin_base_url: admin_url,
            ingress_base_url: opts.ingress_url.as_ref(),
            bearer_token: token,
        },
    );
    env.write_config(&doc.to_string())?;

    let mut f = Formatter::new();
    f.detail(
        "environment",
        [
            ("name", Field::new(name)),
            ("type", Field::new("self_hosted")),
            ("admin_base_url", Field::new(admin_url.to_string())),
            (
                "ingress_base_url",
                Field::with_display(
                    opts.ingress_url.as_ref().map(ToString::to_string),
                    opts.ingress_url
                        .as_ref()
                        .map_or_else(|| "(none)".to_owned(), ToString::to_string),
                ),
            ),
            (
                "bearer_token",
                Field::with_display(
                    token.is_some(),
                    if token.is_some() { "(set)" } else { "(none)" },
                ),
            ),
        ],
    );
    finish_add(f, env, name, existed, opts.no_use)
}

/// Report the written section, select it unless `no_use`, and suggest checking it.
fn finish_add(
    mut f: Formatter,
    env: &CliEnv,
    name: &str,
    existed: bool,
    no_use: bool,
) -> Result<()> {
    let (result, verb) = if existed {
        ("updated", "Updated")
    } else {
        ("added", "Added")
    };
    f.outcome(
        "result",
        Field::with_display(
            result,
            format!(
                "{verb} environment \"{name}\" in {}",
                env.config_file.display()
            ),
        ),
        Outcome::Success,
    );
    if !no_use {
        select_profile(env, name)?;
        f.outcome(
            "current_environment",
            Field::with_display(name, format!("Current environment is now \"{name}\"")),
            Outcome::Success,
        );
    }
    f.next_step(
        "restate whoami",
        "check the connection",
        IncludeFormatting::Yes,
    );
    f.finish()
}
