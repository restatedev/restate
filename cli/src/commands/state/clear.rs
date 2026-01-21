// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use anyhow::{Result, bail};
use cling::prelude::*;
use comfy_table::{Cell, Table};
use crossterm::style::Stylize;
use itertools::Itertools;
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{c_indent_table, c_println};

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::get_state_keys;
use crate::commands::state::util::{compute_version, update_state};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_clear")]
pub struct Clear {
    /// A string with either service name and key, or only the service name, e.g.:
    /// * `virtualObjectName`
    /// * `virtualObjectName/key`
    /// * `workflowName`
    /// * `workflowName/key`
    query: String,

    /// Force means, ignore the current version
    #[clap(long, short)]
    force: bool,
}

pub async fn run_clear(State(env): State<CliEnv>, opts: &Clear) -> Result<()> {
    clear(&env, opts).await
}

async fn clear(env: &CliEnv, opts: &Clear) -> Result<()> {
    let sql_client = crate::clients::DataFusionHttpClient::new(env).await?;

    let (svc, key) = match opts.query.split_once('/') {
        None => (opts.query.as_str(), None),
        Some((svc, key)) => (svc, Some(key)),
    };

    #[allow(clippy::mutable_key_type)]
    let services_state = get_state_keys(&sql_client, svc, key).await?;
    if services_state.is_empty() {
        bail!("No state found!");
    }

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["SERVICE/KEY", "STATE"]);
    for (svc_id, svc_state) in &services_state {
        table.add_row(vec![
            Cell::new(format!("{}/{}", svc_id.service_name, svc_id.key)),
            Cell::new(format!("[{}]", svc_state.keys().join(", "))),
        ]);
    }
    c_indent_table!(0, table);

    c_println!();

    c_println!(
        "Going to {} all the aforementioned state entries.",
        "remove".bold().red()
    );
    c_println!("About to submit the new state mutation to the system for processing.");
    c_println!(
        "If there are currently active invocations, then this mutation will be enqueued to be processed after them."
    );
    c_println!();
    confirm_or_exit("Are you sure?")?;

    c_println!();

    for (svc_id, svc_state) in services_state {
        let version = if opts.force {
            None
        } else {
            Some(compute_version(&svc_state))
        };
        update_state(
            env,
            version,
            &svc_id.service_name,
            &svc_id.key,
            HashMap::default(),
        )
        .await?;
    }

    c_println!();
    c_println!("Enqueued successfully for processing");

    Ok(())
}
