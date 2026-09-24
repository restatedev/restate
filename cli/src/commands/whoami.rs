// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use anyhow::Result;
use cling::prelude::*;
use figment::Profile;
use itertools::Itertools;
use serde_json::Value;
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

#[derive(Run, Parser, Clone)]
#[cling(run = "run")]
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
    }

    let mut f = Formatter::new();

    // Connection.
    let url_field = |url: Option<String>| match url {
        Some(u) => Field::new(u),
        None => Field::with_display(Value::Null, "(NONE)"),
    };
    let token_set = env.config.bearer_token.is_some();
    let mut connection = vec![
        (
            "ingress_base_url",
            url_field(env.ingress_base_url().map(|u| u.to_string()).ok()),
        ),
        (
            "admin_base_url",
            url_field(env.admin_base_url().map(|u| u.to_string()).ok()),
        ),
    ];
    if json_output {
        connection.push(("authentication_token_set", Field::new(token_set)));
    } else if token_set {
        connection.push(("authentication_token", Field::with_display(true, "(set)")));
    }
    f.title("🔗", "Connection");
    f.detail("connection", &connection);

    // Local environment.
    let annotate_path = |path: &std::path::Path| -> Field {
        let annotation = if path.exists() {
            "(exists)"
        } else {
            "(does not exist)"
        };
        Field::with_display(
            path.display().to_string(),
            format!("{} {annotation}", path.display()),
        )
    };

    let mut environment: Vec<(&str, Field)> = Vec::new();
    environment.push(("config_dir", annotate_path(&env.config_home)));
    if json_output {
        environment.push(("config_dir_exists", Field::new(env.config_home.exists())));
    }
    environment.push(("environment_file", annotate_path(&env.environment_file)));
    if json_output {
        environment.push((
            "environment_file_exists",
            Field::new(env.environment_file.exists()),
        ));
    }
    let environment_field = if env.environment == Profile::Default {
        Field::with_display(env.environment.to_string(), "default")
    } else {
        Field::with_display(
            env.environment.to_string(),
            format!("{} (source: {})", env.environment, env.environment_source),
        )
    };
    environment.push(("environment", environment_field));
    if json_output {
        environment.push((
            "environment_source",
            Field::new(env.environment_source.to_string()),
        ));
    }
    environment.push(("config_file", annotate_path(&env.config_file)));
    if json_output {
        environment.push(("config_file_exists", Field::new(env.config_file.exists())));
    }
    let loaded_dotenv = CliContext::get()
        .loaded_dotenv()
        .map(|p| p.display().to_string());
    environment.push((
        "loaded_dotenv",
        match loaded_dotenv {
            Some(p) => Field::new(p),
            None => Field::with_display(Value::Null, "(NONE)"),
        },
    ));
    f.title("🏠", "Local Environment");
    f.detail("environment", &environment);

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
            let logged_in_display = format!("{logged_in} ({logged_in_status})");

            let mut cloud = vec![
                (
                    "account_id",
                    match account_id {
                        Some(id) => Field::new(id),
                        None => Field::with_display(Value::Null, "(NONE)"),
                    },
                ),
                (
                    "environment_id",
                    match environment_id {
                        Some(id) => Field::new(id),
                        None => Field::with_display(Value::Null, "(NONE)"),
                    },
                ),
                (
                    "logged_in",
                    Field::with_display(logged_in, logged_in_display),
                ),
            ];
            if json_output {
                cloud.push(("logged_in_status", Field::new(logged_in_status)));
            }
            f.title("☁️", "Cloud");
            f.detail("cloud", &cloud);
        }
    }

    // Admin service health. Never fails the command: a failed probe is reported as
    // unhealthy in the output. Human mode keeps the styled success/error messages;
    // JSON mode carries a structured `admin_health` result.
    f.title("🩺", "Admin Service Health");
    // Non-zero exit when the admin probe fails, so automation gets a liveness signal.
    let mut health_exit_code: Option<u8> = None;
    match crate::clients::AdminClient::new(&env).await {
        Ok(client) => match client.health().await {
            Ok(envelope) if envelope.status_code().is_success() => {
                let server_version = client.restate_server_version.to_string();
                if json_output {
                    let mut health = vec![
                        ("healthy", Field::new(true)),
                        ("base_url", Field::new(client.base_url.to_string())),
                        ("server_version", Field::new(server_version)),
                    ];
                    if let Some(advertised_ingress_address) = &client.advertised_ingress_address {
                        health.push((
                            "advertised_ingress_address",
                            Field::new(advertised_ingress_address.clone()),
                        ));
                    }
                    f.detail("admin_health", &health);
                } else {
                    c_success!(
                        "Admin Service '{}' is healthy! (server version {})",
                        client.base_url,
                        server_version
                    );
                    if let Some(advertised_ingress_address) = client.advertised_ingress_address {
                        // Align with the text after the (color-dependent) success icon.
                        let indent = SUCCESS_ICON.to_string().width() + 1;
                        c_println!(
                            "{:indent$}Advertised ingress address: {advertised_ingress_address}",
                            ""
                        );
                    }
                }
            }
            Ok(envelope) => {
                health_exit_code = Some(exit::SERVER);
                let url = envelope.url().clone();
                let status_code = envelope.status_code();
                let body = envelope.into_text().await;
                if json_output {
                    f.detail(
                        "admin_health",
                        &[
                            ("healthy", Field::new(false)),
                            ("base_url", Field::new(client.base_url.to_string())),
                            ("status_code", Field::new(status_code.to_string())),
                            ("url", Field::new(url.to_string())),
                            ("error", Field::new(body.unwrap_or_default())),
                        ],
                    );
                } else {
                    c_error!("Admin Service '{}' is unhealthy:", client.base_url);
                    c_eprintln!("   >> [{}] from '{}'", status_code.to_string(), url);
                    c_eprintln!("   >> {}", body.unwrap_or_default());
                }
            }
            Err(e) => {
                health_exit_code = Some(exit::NETWORK);
                if json_output {
                    f.detail(
                        "admin_health",
                        &[
                            ("healthy", Field::new(false)),
                            ("base_url", Field::new(client.base_url.to_string())),
                            ("error", Field::new(e.to_string())),
                        ],
                    );
                } else {
                    c_error!("Admin Service '{}' is unhealthy:", client.base_url);
                    c_eprintln!("   >> {}", e);
                }
            }
        },
        Err(e) => {
            health_exit_code = Some(exit::NETWORK);
            if json_output {
                f.detail(
                    "admin_health",
                    &[
                        ("healthy", Field::new(false)),
                        ("error", Field::new(e.to_string())),
                    ],
                );
            } else {
                c_error!("Could not connect to Admin Service:");
                c_eprintln!("   >> {}", e);
            }
        }
    }

    if !json_output {
        c_println!();
    }

    f.finish()?;

    // Output is already written; signal admin-probe failure via the exit code without
    // letting the error reporter print a second (duplicate) message.
    if let Some(code) = health_exit_code {
        let _ = std::io::stdout().flush();
        let _ = std::io::stderr().flush();
        std::process::exit(i32::from(code));
    }

    Ok(())
}
