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
use comfy_table::Table;

use restate_cli_util::c_println;
use restate_cli_util::c_success;
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_delete")]
#[clap(visible_alias = "rm", alias = "remove")]
pub struct Delete {
    /// Subscription ID
    id: String,
}

pub async fn run_delete(State(env): State<CliEnv>, opts: &Delete) -> Result<()> {
    let client = AdminClient::new(&env).await?;
    let sub = client.get_subscription(&opts.id).await?.into_body().await?;

    let mut table = Table::new_styled();
    table.add_kv_row("ID:", sub.id.to_string());
    table.add_kv_row("Source:", &sub.source);
    table.add_kv_row("Sink:", &sub.sink);
    c_println!("{table}");

    confirm_or_exit(&format!("Delete subscription {}?", opts.id))?;

    client
        .delete_subscription(&opts.id)
        .await?
        .success_or_error()?;

    c_success!("Subscription {} deleted", &opts.id);
    Ok(())
}
