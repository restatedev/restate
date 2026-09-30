// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use anyhow::Result;
use cling::prelude::*;
use figment::Profile;
use itertools::Itertools;
use serde_json::{Value, json};
use strum::IntoEnumIterator;

use restate_admin_rest_model::version::AdminApiVersion;
use restate_cli_util::_unicode_width::UnicodeWidthStr;
use restate_cli_util::ui::stylesheet::SUCCESS_ICON;
use restate_cli_util::{CliContext, c_eprintln, c_error, c_println, c_success, exit};
use restate_types::art::render_restate_logo;

use crate::build_info;
use crate::cli_env::{CliEnv, EnvironmentType};
use crate::clients::AdminClientInterface;
use crate::clients::{MAX_ADMIN_API_VERSION, MIN_ADMIN_API_VERSION};
use crate::ui::fmt::{Field, Formatter, OutputFormatter};

/// Show the server the CLI talks to, and check that it's reachable
///
/// Prints the admin and ingress URLs in use, whether a bearer token is set (never the token),
/// the selected environment and where it comes from, the CLI config paths, and the CLI build.
/// Then checks the admin API: exits with code 5 (network error) if it can't be reached, 8
/// (server error) if it's unhealthy.
#[derive(Run, Parser, Clone)]
#[cling(run = "run")]
#[command(after_help = after_help!(
    examples: [
        "restate whoami --json",
        "restate -e prod whoami",
    ],
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
pub struct WhoAmI {}

pub async fn run(State(env): State<CliEnv>) -> Result<()> {
    let json_output = CliContext::get().json_output();

    // Human-only preamble: the logo and the project banner never belong in the
    // structured JSON document.
    if !json_output {
        c_println!(
            "{}",
            render_restate_logo(CliContext::get().colors_enabled())
        );
        c_println!("            Restate");
        c_println!("       https://restate.dev/");
        c_println!();
    }

    let mut f = Formatter::new();
    let or_none = |value: Option<String>| match value {
        Some(v) => Field::new(v),
        None => Field::with_display(Value::Null, "(NONE)"),
    };

    // Connection. Only whether a token is set is reported, never the token itself;
    // humans see the row only when it is.
    let mut connection = vec![
        (
            "ingress_base_url",
            or_none(env.ingress_base_url().map(|u| u.to_string()).ok()),
        ),
        (
            "admin_base_url",
            or_none(env.admin_base_url().map(|u| u.to_string()).ok()),
        ),
    ];
    let token_set = env.config.bearer_token.is_some();
    if token_set || json_output {
        connection.push((
            "authentication_token",
            Field::with_display(token_set, "(set)"),
        ));
    }
    f.title("🔗", "Connection");
    f.detail("connection", &connection);

    // Local environment. Paths are `{path, exists}` in JSON, annotated for humans.
    let path_field = |path: &Path| {
        let exists = path.exists();
        let annotation = if exists { "exists" } else { "does not exist" };
        Field::with_display(
            json!({ "path": path.display().to_string(), "exists": exists }),
            format!("{} ({annotation})", path.display()),
        )
    };
    let (name, source) = (
        env.environment.to_string(),
        env.environment_source.to_string(),
    );
    let display = if env.environment == Profile::Default {
        name.clone()
    } else {
        format!("{name} (source: {source})")
    };
    let environment = Field::with_display(json!({ "name": name, "source": source }), display);
    f.title("🏠", "Local Environment");
    f.detail(
        "environment",
        [
            ("config_dir", path_field(&env.config_home)),
            ("environment_file", path_field(&env.environment_file)),
            ("environment", environment),
            ("config_file", path_field(&env.config_file)),
            (
                "loaded_dotenv",
                or_none(
                    CliContext::get()
                        .loaded_dotenv()
                        .map(|p| p.display().to_string()),
                ),
            ),
        ],
    );

    // Build information.
    let supported_admin_api = if MIN_ADMIN_API_VERSION == MAX_ADMIN_API_VERSION {
        Field::new(MIN_ADMIN_API_VERSION.as_repr())
    } else {
        let reprs: Vec<u16> = AdminApiVersion::iter()
            .skip_while(|value| *value < MIN_ADMIN_API_VERSION)
            .take_while(|value| *value <= MAX_ADMIN_API_VERSION)
            .map(|value| value.as_repr())
            .collect();
        let display = format!("[{}]", reprs.iter().join(","));
        Field::with_display(Value::from(reprs), display)
    };
    let build = vec![
        ("version", Field::new(build_info::RESTATE_CLI_VERSION)),
        ("target", Field::new(build_info::RESTATE_CLI_TARGET_TRIPLE)),
        ("debug_build", Field::new(build_info::is_debug())),
        ("build_time", Field::new(build_info::RESTATE_CLI_BUILD_TIME)),
        (
            "build_features",
            Field::new(build_info::RESTATE_CLI_BUILD_FEATURES),
        ),
        ("supported_admin_api", supported_admin_api),
        ("git_sha", Field::new(build_info::RESTATE_CLI_COMMIT_SHA)),
        (
            "git_commit_date",
            Field::new(build_info::RESTATE_CLI_COMMIT_DATE),
        ),
        ("git_branch", Field::new(build_info::RESTATE_CLI_BRANCH)),
    ];
    f.title("🔧", "Restate CLI Build Information");
    f.detail("build", &build);

    // Cloud.
    match env.config.environment_type {
        EnvironmentType::Default => {}
        #[cfg(feature = "cloud")]
        EnvironmentType::Cloud => {
            let (account_id, environment_id) = match &env.config.cloud.environment_info {
                Some(environment_info) => (
                    Some(environment_info.account_id.as_str().to_owned()),
                    Some(environment_info.environment_id.as_str().to_owned()),
                ),
                None => (None, None),
            };

            let (logged_in, logged_in_status) = match &env.config.cloud.credentials {
                Some(credentials) => match credentials.expiry() {
                    Ok(expiry) => {
                        let delta = expiry.signed_duration_since(chrono::Utc::now());
                        if delta > chrono::TimeDelta::zero() {
                            let left = restate_cli_util::ui::duration_to_human_rough(
                                delta,
                                chrono_humanize::Tense::Present,
                            );
                            (true, format!("expires in {left}"))
                        } else {
                            (false, "token expired".to_string())
                        }
                    }
                    Err(_) => (false, "invalid token".to_string()),
                },
                None => (false, "no token".to_string()),
            };
            f.title("☁️", "Cloud");
            f.detail(
                "cloud",
                [
                    ("account_id", or_none(account_id)),
                    ("environment_id", or_none(environment_id)),
                    (
                        "logged_in",
                        Field::with_display(
                            json!({ "value": logged_in, "status": logged_in_status }),
                            format!("{logged_in} ({logged_in_status})"),
                        ),
                    ),
                ],
            );
        }
    }

    // Admin service health. A failed probe is reported in the output, then signalled
    // through the exit code so automation gets a liveness signal. Humans get styled
    // status messages (errors on stderr), which the formatter has no block for.
    let mut health = Vec::new();
    let mut health_exit_code = None;
    let (headline, details) = match crate::clients::AdminClient::new(&env).await {
        Ok(client) => {
            let base_url = client.base_url.to_string();
            health.push(("base_url", Field::new(base_url.as_str())));
            match client.health().await {
                Ok(envelope) if envelope.status_code().is_success() => {
                    let server_version = client.restate_server_version.to_string();
                    let headline = format!(
                        "Admin Service '{base_url}' is healthy! (server version {server_version})"
                    );
                    health.push(("server_version", Field::new(server_version)));
                    let mut details = Vec::new();
                    if let Some(address) = client.advertised_ingress_address {
                        details.push(format!("Advertised ingress address: {address}"));
                        health.push(("advertised_ingress_address", Field::new(address)));
                    }
                    (headline, details)
                }
                Ok(envelope) => {
                    health_exit_code = Some(exit::SERVER);
                    let status_code = envelope.status_code().to_string();
                    let url = envelope.url().to_string();
                    let body = envelope.into_text().await.unwrap_or_default();
                    let details = vec![format!("[{status_code}] from '{url}'"), body.clone()];
                    health.extend([
                        ("status_code", Field::new(status_code)),
                        ("url", Field::new(url)),
                        ("error", Field::new(body)),
                    ]);
                    (format!("Admin Service '{base_url}' is unhealthy:"), details)
                }
                Err(e) => {
                    health_exit_code = Some(exit::NETWORK);
                    health.push(("error", Field::new(e.to_string())));
                    (
                        format!("Admin Service '{base_url}' is unhealthy:"),
                        vec![e.to_string()],
                    )
                }
            }
        }
        Err(e) => {
            health_exit_code = Some(exit::NETWORK);
            health.push(("error", Field::new(e.to_string())));
            (
                "Could not connect to Admin Service:".to_owned(),
                vec![e.to_string()],
            )
        }
    };
    health.insert(0, ("healthy", Field::new(health_exit_code.is_none())));

    f.title("🩺", "Admin Service Health");
    if json_output {
        f.detail("admin_health", &health);
    } else if health_exit_code.is_none() {
        c_success!("{headline}");
        // Align with the text after the (color-dependent) success icon.
        let indent = SUCCESS_ICON.to_string().width() + 1;
        for line in details {
            c_println!("{:indent$}{line}", "");
        }
    } else {
        c_error!("{headline}");
        for line in details {
            c_eprintln!("   >> {line}");
        }
    }

    f.finish()?;

    // Output is already written; signal admin-probe failure via the exit code without
    // letting the error reporter print a second (duplicate) message.
    match health_exit_code {
        Some(code) => Err(exit::AlreadyReported { code }.into()),
        None => Ok(()),
    }
}
