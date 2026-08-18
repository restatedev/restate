// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Result, anyhow};
use chrono::{DateTime, Local};
use cling::prelude::*;
use comfy_table::{Cell, Table};
use serde::Deserialize;

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_eprintln, c_println, c_title};
use restate_types::vqueues::VQueueId;

use crate::cli_env::CliEnv;
use crate::clients::DataFusionHttpClient;
use crate::ui::datetime::DateTimeExt;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// Virtual queue ID
    vqueue_id: String,

    /// Limit the number of displayed entries
    #[clap(long, default_value = "100")]
    limit: usize,

    #[clap(flatten)]
    watch: Watch,
}

#[derive(Debug, Deserialize)]
struct VQueueEntryRow {
    entry_id: String,
    entry_kind: String,
    stage: String,
    status: String,
    has_lock: bool,
    created_at: DateTime<Local>,
    num_attempts: u32,
    deployment: Option<String>,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let vqueue_id = opts
        .vqueue_id
        .parse::<VQueueId>()
        .map_err(|err| anyhow!("Invalid virtual queue ID '{}': {err}", opts.vqueue_id))?
        .to_string();

    let client = DataFusionHttpClient::new(env).await?;
    let queue = super::get_vqueue(&client, &opts.vqueue_id).await?;

    let entries: Vec<VQueueEntryRow> = client
        .run_json_query(format!(
            "SELECT entry_id, entry_kind, stage, status, has_lock, created_at, num_attempts, \
             deployment FROM sys_vqueues WHERE id = '{vqueue_id}' \
             ORDER BY sequence_number DESC LIMIT {}",
            opts.limit
        ))
        .await?;

    let mut info = Table::new_styled();
    info.add_kv_row("ID:", &queue.id);
    info.add_kv_row("Service:", queue.service_name.as_deref().unwrap_or("-"));
    info.add_kv_row("Scope:", queue.scope.as_deref().unwrap_or("-"));
    info.add_kv_row("Limit key:", queue.limit_key.as_deref().unwrap_or("-"));
    info.add_kv_row("Lock:", queue.lock_name.as_deref().unwrap_or("-"));
    info.add_kv_row("Active:", queue.is_active);
    info.add_kv_row("Paused:", queue.queue_is_paused);
    info.add_kv_row("Created at:", queue.created_at.display());
    info.add_kv_row(
        "Last enqueued at:",
        display_optional(queue.last_enqueued_at),
    );
    info.add_kv_row("Last started at:", display_optional(queue.last_start_at));
    info.add_kv_row(
        "Last attempted at:",
        display_optional(queue.last_attempt_at),
    );
    info.add_kv_row("Last finished at:", display_optional(queue.last_finish_at));

    c_title!("📜", "Virtual Queue Information");
    c_println!("{info}");
    c_println!();

    let mut counts = Table::new_styled();
    counts.set_styled_header(vec!["INBOX", "RUNNING", "SUSPENDED", "PAUSED", "FINISHED"]);
    counts.add_row(vec![
        Cell::new(queue.num_inbox),
        Cell::new(queue.num_running),
        Cell::new(queue.num_suspended),
        Cell::new(queue.num_paused),
        Cell::new(queue.num_finished),
    ]);
    c_title!("📊", "Entry Counts");
    c_println!("{counts}");
    c_println!();

    c_title!("📥", "Entries");
    if entries.is_empty() {
        c_println!("No entries found.");
    } else {
        let mut entries_table = Table::new_styled();
        entries_table.set_styled_header(vec![
            "ENTRY ID",
            "KIND",
            "STAGE",
            "STATUS",
            "HAS LOCK",
            "ATTEMPTS",
            "CREATED-AT",
            "DEPLOYMENT",
        ]);
        for entry in &entries {
            entries_table.add_row(vec![
                Cell::new(&entry.entry_id),
                Cell::new(&entry.entry_kind),
                Cell::new(&entry.stage),
                Cell::new(&entry.status),
                Cell::new(entry.has_lock),
                Cell::new(entry.num_attempts),
                Cell::new(entry.created_at.display()),
                Cell::new(entry.deployment.as_deref().unwrap_or("-")),
            ]);
        }
        c_println!("{entries_table}");
    }

    let total_entries = queue.num_inbox
        + queue.num_running
        + queue.num_suspended
        + queue.num_paused
        + queue.num_finished;
    c_eprintln!("Showing {}/{} entries.", entries.len(), total_entries);
    Ok(())
}

fn display_optional(value: Option<DateTime<Local>>) -> String {
    value
        .map(|value| value.display())
        .unwrap_or_else(|| "-".to_owned())
}
