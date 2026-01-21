// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, Result};
use cling::prelude::*;
use comfy_table::{Cell, Table};

use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{c_println, c_title};

use crate::cli_env::CliEnv;
use crate::commands::state::util::{
    as_json, compute_version, from_json, get_current_state, pretty_print_json, update_state,
};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "patch")]
pub struct Patch {
    /// Force means, ignore the current version
    #[clap(long, short)]
    force: bool,

    /// Service name
    service: String,

    /// Service key
    key: String,

    /// JSON patch
    #[arg(short, long)]
    patch: String,
}

pub async fn patch(State(env): State<CliEnv>, opts: &Patch) -> Result<()> {
    let patch = serde_json::from_str::<json_patch::Patch>(&opts.patch)
        .map_err(|e| anyhow::anyhow!("Parsing JSON patch: {}", e))?;

    let current_state = get_current_state(&env, &opts.service, &opts.key, false).await?;
    let current_version = compute_version(&current_state);

    let mut state = as_json(current_state, false)?;

    json_patch::patch(&mut state, &patch).context("Patch failed")?;

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["", ""]);
    table.add_row(vec![Cell::new("Service"), Cell::new(&opts.service)]);
    table.add_row(vec![Cell::new("Key"), Cell::new(&opts.key)]);
    table.add_row(vec![Cell::new("Force?"), Cell::new(opts.force)]);

    c_title!("ℹ️ ", "Patch State");
    c_println!("{table}");
    c_println!();

    c_title!("ℹ️ ", "New State");
    c_println!("{}", pretty_print_json(&state)?);
    c_println!();

    c_println!("About to submit the new state mutation to the system for processing.");
    c_println!(
        "If there are ongoing invocations for this key this mutation will be enqueued to be processed after them."
    );
    c_println!();
    confirm_or_exit("Are you sure?")?;

    c_println!();

    let modified_state = from_json(state, false)?;
    let version = if opts.force {
        None
    } else {
        Some(current_version)
    };
    update_state(&env, version, &opts.service, &opts.key, modified_state).await?;

    c_println!();
    c_println!("Successfully submitted state update.");

    Ok(())
}
