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
use comfy_table::{Cell, Table};
use figment::{
    Figment, Profile,
    providers::{Format, Serialized, Toml},
};

use restate_cli_util::c_println;

use crate::{
    cli_env::{CliConfig, CliEnv, LOCAL_PROFILE},
    console::StyledTable,
};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list_environments")]
#[clap(visible_alias = "list-env")]
pub struct ListEnvironments {}

pub async fn run_list_environments(
    State(env): State<CliEnv>,
    _opts: &ListEnvironments,
) -> Result<()> {
    let defaults = CliConfig::default();
    let local = CliConfig::local();

    let mut figment =
        Figment::from(Serialized::defaults(defaults)).merge(Serialized::from(local, LOCAL_PROFILE));

    // Load configuration file
    if env.config_file.is_file() {
        figment = figment.merge(Toml::file_exact(env.config_file).nested());
    }

    let mut table = Table::new_styled();
    let header = vec!["CURRENT", "NAME", "ADMIN_BASE_URL"];
    table.set_styled_header(header);

    for profile in figment.profiles() {
        if profile == Profile::Global || profile == Profile::Default {
            continue;
        }

        let figment = figment.clone().select(profile.clone());

        let admin_base_url = figment.find_value("admin_base_url").ok();

        let current = if profile == env.environment { "*" } else { "" };

        let row = vec![
            Cell::new(current),
            Cell::new(profile.as_str()),
            Cell::new(
                admin_base_url
                    .as_ref()
                    .and_then(|u| u.as_str())
                    .unwrap_or("(NONE)"),
            ),
        ];

        table.add_row(row);
    }

    c_println!("{}", table);

    Ok(())
}
