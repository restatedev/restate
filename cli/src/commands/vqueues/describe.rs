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
use chrono::{DateTime, Local};
use cling::prelude::*;
use serde::Deserialize;

use restate_cli_util::c_eprintln;
use restate_cli_util::ui::watcher::Watch;
use restate_types::vqueues::VQueueId;

use super::{optional_str, optional_time, time};
use crate::cli_env::CliEnv;
use crate::clients::DataFusionHttpClient;
use crate::ui::fmt::{Field, Formatter, IfEmpty, IncludeFormatting, OutputFormatter};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// Virtual queue ID
    vqueue_id: VQueueId,

    /// Limit the number of displayed entries
    #[clap(long, default_value = "100")]
    limit: usize,

    #[clap(long, default_value = "false")]
    newest_first: bool,

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
    let client = DataFusionHttpClient::new(env).await?;
    let queue = super::get_vqueue(&client, &opts.vqueue_id).await?;

    let order_clause = if opts.newest_first {
        "ORDER BY sequence_number DESC"
    } else {
        ""
    };

    let entries: Vec<VQueueEntryRow> = client
        .run_json_query(format!(
            "SELECT entry_id, entry_kind, stage, status, has_lock, created_at, num_attempts, \
             deployment FROM sys_vqueues WHERE id = '{}' \
            {order_clause} LIMIT {}",
            opts.vqueue_id, opts.limit
        ))
        .await?;

    let mut f = Formatter::new();
    f.title("📜", "Virtual Queue Information");
    f.detail(
        "vqueue",
        [
            ("id", Field::new(queue.id.as_str())),
            ("service", optional_str(queue.service_name.as_deref())),
            ("scope", optional_str(queue.scope.as_deref())),
            ("limit_key", optional_str(queue.limit_key.as_deref())),
            ("lock", optional_str(queue.lock_name.as_deref())),
            ("active", Field::new(queue.is_active)),
            ("paused", Field::new(queue.queue_is_paused)),
            ("created_at", time(queue.created_at)),
            ("last_enqueued_at", optional_time(queue.last_enqueued_at)),
            ("last_started_at", optional_time(queue.last_start_at)),
            ("last_attempted_at", optional_time(queue.last_attempt_at)),
            ("last_finished_at", optional_time(queue.last_finish_at)),
        ],
    );

    f.title("📊", "Entry Counts");
    f.table(
        "entry_counts",
        &["inbox", "running", "suspended", "paused", "finished"],
        [[
            Field::new(queue.num_inbox),
            Field::new(queue.num_running),
            Field::new(queue.num_suspended),
            Field::new(queue.num_paused),
            Field::new(queue.num_finished),
        ]],
        IfEmpty::Nothing,
    );

    f.title("📥", "Entries");
    let rows = entries.iter().map(|entry| {
        [
            Field::new(entry.entry_id.as_str()),
            Field::new(entry.entry_kind.as_str()),
            Field::new(entry.stage.as_str()),
            Field::new(entry.status.as_str()),
            Field::new(entry.has_lock),
            Field::new(entry.num_attempts),
            time(entry.created_at),
            optional_str(entry.deployment.as_deref()),
        ]
    });
    f.table(
        "entries",
        &[
            "entry_id",
            "kind",
            "stage",
            "status",
            "has_lock",
            "attempts",
            "created_at",
            "deployment",
        ],
        rows,
        IfEmpty::Say("No entries found."),
    );
    if let Some(entry) = entries.iter().find(|e| e.entry_kind == "invocation") {
        f.next_step(
            &format!("restate invocations describe {}", entry.entry_id),
            "inspect the invocation's status, progress, and journal",
            IncludeFormatting::Yes,
        );
    }

    let total_entries = queue.num_inbox
        + queue.num_running
        + queue.num_suspended
        + queue.num_paused
        + queue.num_finished;
    c_eprintln!("Showing {}/{} entries.", entries.len(), total_entries);
    f.finish()
}
