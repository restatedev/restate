// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::c_title;
use crate::cli_env::CliEnv;
use crate::console::c_println;
use crate::ui::console::StyledTable;
use crate::ui::watcher::Watch;

use crate::commands::state::util::{as_json, get_current_state, pretty_print_json_object};
use anyhow::Result;
use cling::prelude::*;
use comfy_table::{Cell, Table};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_get")]
pub struct Get {
    /// Don't try to convert the values to a UTF8 string
    #[clap(long, alias = "bin")]
    binary: bool,

    /// Don't try to convert the values to a UTF8 string
    #[clap(long, alias = "s")]
    silent: bool,

    /// Target service
    service: String,

    /// Target service key
    key: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_get(State(env): State<CliEnv>, opts: &Get) -> Result<()> {
    opts.watch.run(|| get(&env, opts)).await
}

async fn get(env: &CliEnv, opts: &Get) -> Result<()> {
    let current_state = get_current_state(env, &opts.service, &opts.key).await?;
    let current_state_json = as_json(current_state, opts.binary)?;

    if opts.silent {
        c_println!("{current_state_json}");
        return Ok(());
    }

    c_title!("🤖", "State");

    let mut table = Table::new_styled(&env.ui_config);
    table.set_styled_header(vec!["", ""]);
    table.add_row(vec![Cell::new("service"), Cell::new(&opts.service)]);
    table.add_row(vec![Cell::new("key"), Cell::new(&opts.key)]);

    c_println!("{table}");
    c_println!();

    let pretty_json = pretty_print_json_object(&current_state_json)?;
    let mut table = Table::new_styled(&env.ui_config);
    table.set_styled_header(vec!["KEY", "VAL"]);
    for (k, v) in pretty_json {
        table.add_row(vec![Cell::new(k), Cell::new(v)]);
    }

    c_println!("{table}");
    c_println!();

    Ok(())
}
