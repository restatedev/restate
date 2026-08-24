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

use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::watcher::Watch;

use super::{VQUEUE_COLUMNS, VQueueRow};
use crate::cli_env::CliEnv;
use crate::clients::DataFusionHttpClient;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[clap(visible_alias = "ls")]
pub struct List {
    /// Limit the number of results
    #[clap(long, default_value = "100")]
    limit: usize,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, opts: &List) -> Result<()> {
    let client = DataFusionHttpClient::new(env).await?;
    let rows: Vec<VQueueRow> = client
        .run_json_query(format!(
            "SELECT {VQUEUE_COLUMNS} FROM sys_vqueue_meta LIMIT {}",
            opts.limit
        ))
        .await?;

    if rows.is_empty() {
        c_println!("No virtual queues found.");
        return Ok(());
    }

    let mut table = Table::new_styled();
    table.set_styled_header(vec![
        "ID",
        "SERVICE",
        "SCOPE",
        "LIMIT-KEY",
        "LOCK",
        "QUEUE-PAUSED",
        "INBOX",
        "RUNNING",
        "SUSPENDED",
        "PAUSED ENTRIES",
        "FINISHED",
    ]);

    for row in rows {
        let service_name = row.service_name.as_deref().unwrap_or("-");
        let scope = row.scope.as_deref().unwrap_or("-");
        table.add_row(vec![
            Cell::new(row.id),
            Cell::new(service_name),
            Cell::new(scope),
            Cell::new(row.limit_key.as_deref().unwrap_or("-")),
            Cell::new(row.lock_name.as_deref().unwrap_or("-")),
            Cell::new(row.queue_is_paused),
            Cell::new(row.num_inbox),
            Cell::new(row.num_running),
            Cell::new(row.num_suspended),
            Cell::new(row.num_paused),
            Cell::new(row.num_finished),
        ]);
    }

    c_println!("{table}");
    Ok(())
}
