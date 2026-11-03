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
use crate::ui::fmt::{Field, Formatter, IncludeFormatting, Outcome, OutputFormatter};

use super::sections::{ProfileSpec, check_name, list_profiles, select_profile, write_section};

/// Add a self-hosted server as a profile, and select it
///
/// The profile is written under NAME with the server's admin URL, and its ingress URL and
/// bearer token when given. Overwriting an existing profile asks for confirmation, or
/// needs --yes. Restate Cloud environments aren't added this way: `restate cloud login`
/// imports them.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_add")]
#[command(after_help = after_help!(
    examples: [
        "restate profile add staging --admin-url https://restate.example.com:9070 --token \"$TOKEN\"",
        "restate profile add ci --admin-url http://restate:9070 --ingress-url http://restate:8080 --no-use",
    ],
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
pub struct Add {
    /// Name of the profile (its section in the CLI config file)
    name: String,

    /// Admin API base URL of the server, e.g. http://localhost:9070
    #[clap(long, value_name = "URL")]
    admin_url: AdvertisedAddress<AdminPort>,

    /// Ingress base URL of the server, e.g. http://localhost:8080 (needed by commands that
    /// invoke services)
    #[clap(long, value_name = "URL")]
    ingress_url: Option<AdvertisedAddress<HttpIngressPort>>,

    /// Bearer token the server requires, if any
    #[clap(long, value_name = "TOKEN")]
    token: Option<String>,

    /// Add the profile without making it the current one
    #[clap(long)]
    no_use: bool,
}

pub async fn run_add(State(env): State<CliEnv>, opts: &Add) -> Result<()> {
    let name = opts.name.as_str();
    check_name(name)?;
    let mut doc = env.load_config_document()?;
    let existed = list_profiles(&doc).iter().any(|p| p == name);
    if existed {
        confirm_or_exit(&format!("Overwrite existing profile \"{name}\"?"))?;
    }
    let token = opts.token.as_deref().filter(|t| !t.is_empty());
    write_section(
        &mut doc,
        name,
        &ProfileSpec::SelfHosted {
            admin_base_url: &opts.admin_url,
            ingress_base_url: opts.ingress_url.as_ref(),
            bearer_token: token,
        },
    );
    env.write_config(&doc.to_string())?;

    let mut f = Formatter::new();
    f.detail(
        "profile",
        [
            ("name", Field::new(name)),
            ("type", Field::new("self_hosted")),
            ("admin_base_url", Field::new(opts.admin_url.to_string())),
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
    let (result, verb) = if existed {
        ("updated", "Updated")
    } else {
        ("added", "Added")
    };
    f.outcome(
        "result",
        Field::with_display(
            result,
            format!("{verb} profile \"{name}\" in {}", env.config_file.display()),
        ),
        Outcome::Success,
    );
    if !opts.no_use {
        select_profile(&env, name)?;
        f.outcome(
            "current_profile",
            Field::with_display(name, format!("Current profile is now \"{name}\"")),
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
