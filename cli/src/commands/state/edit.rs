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
use tempfile::tempdir;

use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{c_println, c_title};

use crate::cli_env::CliEnv;
use crate::commands::state::util::{
    as_json, compute_version, from_json, get_current_state, pretty_print_json, read_json_file,
    update_state, write_json_file,
};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_edit")]
pub struct Edit {
    /// Don't try to convert the values to a UTF-8 string
    #[clap(long, alias = "bin")]
    binary: bool,

    /// Force means, ignore the current version
    #[clap(long, short)]
    force: bool,

    /// service name
    service: String,

    /// service key
    key: String,
}

pub async fn run_edit(State(env): State<CliEnv>, opts: &Edit) -> Result<()> {
    edit(&env, opts).await
}

async fn edit(env: &CliEnv, opts: &Edit) -> Result<()> {
    let current_state = get_current_state(env, &opts.service, &opts.key, false).await?;
    let current_version = compute_version(&current_state);

    let tempdir = tempdir().context("unable to create a temporary directory")?;
    let edit_file = tempdir.path().join(".restate_edit");
    let current_state_json = as_json(current_state, opts.binary)?;
    write_json_file(&edit_file, current_state_json)?;
    env.open_default_editor(&edit_file)?;
    let modified_state_json = read_json_file(&edit_file)?;

    //
    // confirm change
    //

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["", ""]);
    table.add_row(vec![Cell::new("Service"), Cell::new(&opts.service)]);
    table.add_row(vec![Cell::new("Key"), Cell::new(&opts.key)]);
    table.add_row(vec![Cell::new("Force?"), Cell::new(opts.force)]);
    table.add_row(vec![Cell::new("Binary?"), Cell::new(opts.binary)]);

    c_title!("ℹ️ ", "State Update");
    c_println!("{table}");
    c_println!();

    c_title!("ℹ️ ", "New State");
    c_println!("{}", pretty_print_json(&modified_state_json)?);
    c_println!();

    c_println!("About to submit the new state mutation to the system for processing.");
    c_println!(
        "If there are ongoing invocations for this key this mutation will be enqueued to be processed after them."
    );
    c_println!();
    confirm_or_exit("Are you sure?")?;

    c_println!();

    //
    // back to binary
    //
    let modified_state = from_json(modified_state_json, opts.binary)?;
    //
    // attach the current version
    //
    let version = if opts.force {
        None
    } else {
        Some(current_version)
    };
    update_state(env, version, &opts.service, &opts.key, modified_state).await?;

    //
    // done
    //

    c_println!();
    c_println!("Successfully submitted state update.");

    Ok(())
}
