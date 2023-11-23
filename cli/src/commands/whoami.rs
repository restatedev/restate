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

#[derive(Run, Parser, Clone)]
#[cling(run = "run")]
pub struct WhoAmI {}

pub async fn run(State(env): State<CliEnv>) {
    if env.colorful() {
        println!("{}", crate::art::render_logo());
        println!("            Restate");
        println!("       https://restate.dev/");
        println!();
    }
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::NOTHING);
    table.add_row(vec![
        "Ingress base URL",
        &env.ingress_base_url().to_string(),
    ]);

    table.add_row(vec!["Meta URL", &env.meta_base_url().to_string()]);
    println!("{}", table);

    println!();
    println!("Local Environment");
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::NOTHING);
    table.add_row(vec![
        "Config Dir",
        &format!(
            "{} {}",
            env.config_home().display(),
            if env.config_home().exists() {
                "(exists)"
            } else {
                "(does not exist)"
            }
        ),
    ]);
    let config_file = env.config_file_path();

    table.add_row(vec![
        "Config File",
        &format!(
            "{} {}",
            config_file.display(),
            if config_file.exists() {
                "(exists)"
            } else {
                "(does not exist)"
            }
        ),
    ]);

    table.add_row(vec![
        "Loaded .env file",
        &env.env_file_path()
            .map(|x| x.display().to_string())
            .unwrap_or("(NONE)".to_string()),
    ]);
    println!("{}", table);

    println!();
    println!("Build Information");
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
    println!("{}", table);
}
