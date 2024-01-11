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

use crate::build_info;
use crate::cli_env::CliEnv;
use crate::clients::MetaClientInterface;
use crate::{c_eprintln, c_error, c_println, c_success};

#[derive(Run, Parser, Clone)]
#[cling(run = "run")]
pub struct WhoAmI {}

pub async fn run(State(env): State<CliEnv>) {
    if crate::console::colors_enabled() {
        c_println!("{}", crate::art::render_logo());
        c_println!("            Restate");
        c_println!("       https://restate.dev/");
        c_println!();
    }
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::NOTHING);
    table.add_row(vec!["Ingress base URL", env.ingress_base_url.as_ref()]);

    table.add_row(vec!["Admin base URL", env.admin_base_url.as_ref()]);

    if env.bearer_token.is_some() {
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

    c_println!();
    // Get admin client, don't fail completely if we can't get one!
    if let Ok(client) = crate::clients::MetasClient::new(&env) {
        match client.health().await {
            Ok(envelope) if envelope.status_code().is_success() => {
                c_success!("Admin Service '{}' is healthy!", env.admin_base_url);
            }
            Ok(envelope) => {
                c_error!("Admin Service '{}' is unhealthy:", env.admin_base_url);
                let url = envelope.url().clone();
                let status_code = envelope.status_code();
                let body = envelope.into_text().await;
                c_eprintln!("   >> [{}] from '{}'", status_code.to_string(), url);
                c_eprintln!("   >> {}", body.unwrap_or_default());
            }
            Err(e) => {
                c_error!("Admin Service '{}' is unhealthy:", env.admin_base_url);
                c_eprintln!("   >> {}", e);
            }
        }
    }

    c_println!();
}
