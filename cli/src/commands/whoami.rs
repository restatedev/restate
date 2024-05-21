// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;
use comfy_table::Table;
use figment::Profile;
use restate_types::art::render_restate_logo;

use crate::build_info;
use crate::cli_env::{CliEnv, EnvironmentType};
use crate::clients::AdminClientInterface;
use crate::{c_eprintln, c_error, c_println, c_success};

#[derive(Run, Parser, Clone)]
#[cling(run = "run")]
pub struct WhoAmI {}

pub async fn run(State(env): State<CliEnv>) {
    c_println!("{}", render_restate_logo(crate::console::colors_enabled()));
    c_println!("            Restate");
    c_println!("       https://restate.dev/");
    c_println!();
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::NOTHING);
    table.add_row(vec![
        "Ingress base URL",
        env.ingress_base_url()
            .map(|u| u.as_ref())
            .unwrap_or("(NONE)"),
    ]);

    table.add_row(vec![
        "Admin base URL",
        env.admin_base_url().map(|u| u.as_ref()).unwrap_or("(NONE)"),
    ]);

    if env.config.bearer_token.is_some() {
        table.add_row(vec!["Authentication Token", "(set)"]);
    }

    c_println!("{}", table);

    c_println!();
    c_println!("Local Environment");
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::NOTHING);
    table.add_row(vec![
        "Config Dir",
        &format!(
            "{} {}",
            env.config_home.display(),
            if env.config_home.exists() {
                "(exists)"
            } else {
                "(does not exist)"
            }
        ),
    ]);

    table.add_row(vec![
        "Environment File",
        &format!(
            "{} {}",
            env.environment_file.display(),
            if env.environment_file.exists() {
                "(exists)"
            } else {
                "(does not exist)"
            }
        ),
    ]);

    if env.environment == Profile::Default {
        table.add_row(vec!["Environment", "default"]);
    } else {
        table.add_row(vec![
            "Environment",
            &format!("{} (source: {})", env.environment, env.environment_source),
        ]);
    }

    table.add_row(vec![
        "Config File",
        &format!(
            "{} {}",
            env.config_file.display(),
            if env.config_file.exists() {
                "(exists)"
            } else {
                "(does not exist)"
            }
        ),
    ]);

    table.add_row(vec![
        "Loaded .env file",
        &env.loaded_env_file
            .as_ref()
            .map(|x| x.display().to_string())
            .unwrap_or("(NONE)".to_string()),
    ]);
    c_println!("{}", table);

    c_println!();
    c_println!("Build Information");
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::NOTHING);
    table.add_row(vec!["Version", build_info::RESTATE_CLI_VERSION]);
    table.add_row(vec!["Target", build_info::RESTATE_CLI_TARGET_TRIPLE]);
    table.add_row(vec!["Debug Build?", build_info::RESTATE_CLI_DEBUG]);
    table.add_row(vec!["Build Time", build_info::RESTATE_CLI_BUILD_TIME]);
    table.add_row(vec![
        "Build Features",
        build_info::RESTATE_CLI_BUILD_FEATURES,
    ]);
    table.add_row(vec!["Git SHA", build_info::RESTATE_CLI_COMMIT_SHA]);
    table.add_row(vec!["Git Commit Date", build_info::RESTATE_CLI_COMMIT_DATE]);
    table.add_row(vec!["Git Commit Branch", build_info::RESTATE_CLI_BRANCH]);
    c_println!("{}", table);

    match env.config.environment_type {
        EnvironmentType::Default => {}
        #[cfg(feature = "cloud")]
        EnvironmentType::Cloud => {
            c_println!();
            c_println!("Cloud");
            let mut table = Table::new();

            let (account_id, environment_id) = match &env.config.cloud.environment_info {
                Some(environment_info) => (
                    environment_info.account_id.as_str(),
                    environment_info.environment_id.as_str(),
                ),
                None => ("(NONE)", "(NONE)"),
            };

            table.load_preset(comfy_table::presets::NOTHING);
            table.add_row(vec!["Account ID", account_id]);
            table.add_row(vec!["Environment ID", environment_id]);

            if let Some(credentials) = &env.config.cloud.credentials {
                match credentials.expiry() {
                    Ok(expiry) => {
                        let delta = expiry.signed_duration_since(chrono::Utc::now());
                        if delta > chrono::TimeDelta::zero() {
                            let left = crate::ui::duration_to_human_rough(
                                delta,
                                chrono_humanize::Tense::Present,
                            );
                            table.add_row(vec![
                                "Logged in?",
                                &format!("true (expires in {})", left),
                            ]);
                        } else {
                            table.add_row(vec!["Logged in?", "false (token expired)"]);
                        }
                    }
                    Err(_) => {
                        table.add_row(vec!["Logged in?", "false (invalid token)"]);
                    }
                }
            } else {
                table.add_row(vec!["Logged in?", "false (no token)"]);
            }

            c_println!("{}", table);
        }
    }

    c_println!();
    // Get admin client, don't fail completely if we can't get one!
    if let Ok(client) = crate::clients::AdminClient::new(&env).await {
        match client.health().await {
            Ok(envelope) if envelope.status_code().is_success() => {
                c_success!("Admin Service '{}' is healthy!", client.base_url);
            }
            Ok(envelope) => {
                c_error!("Admin Service '{}' is unhealthy:", client.base_url);
                let url = envelope.url().clone();
                let status_code = envelope.status_code();
                let body = envelope.into_text().await;
                c_eprintln!("   >> [{}] from '{}'", status_code.to_string(), url);
                c_eprintln!("   >> {}", body.unwrap_or_default());
            }
            Err(e) => {
                c_error!("Admin Service '{}' is unhealthy:", client.base_url);
                c_eprintln!("   >> {}", e);
            }
        }
    }

    c_println!();
}
