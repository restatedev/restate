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
use serde_json::Value;

use restate_cli_util::CliContext;
use restate_cli_util::c_eprintln;
use restate_cli_util::ui::watcher::Watch;
use restate_types::vqueues::VQueueId;

use crate::cli_env::CliEnv;
use crate::clients::DataFusionHttpClient;
use crate::ui::datetime::DateTimeExt;
use crate::ui::fmt::{Field, Formatter, OutputFormatter};

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

    let json = CliContext::get().json_output();
    let total_entries = queue.num_inbox
        + queue.num_running
        + queue.num_suspended
        + queue.num_paused
        + queue.num_finished;

    let mut f = Formatter::new();

    f.title("📜", "Virtual Queue Information");
    f.detail(
        "vqueue",
        &[
            ("id", Field::new(queue.id)),
            ("service_name", optional(queue.service_name)),
            ("scope", optional(queue.scope)),
            ("limit_key", optional(queue.limit_key)),
            ("lock_name", optional(queue.lock_name)),
            ("active", Field::new(queue.is_active)),
            ("queue_paused", Field::new(queue.queue_is_paused)),
            ("created_at", datetime(queue.created_at)),
            (
                "last_enqueued_at",
                optional_datetime(queue.last_enqueued_at),
            ),
            ("last_start_at", optional_datetime(queue.last_start_at)),
            ("last_attempt_at", optional_datetime(queue.last_attempt_at)),
            ("last_finish_at", optional_datetime(queue.last_finish_at)),
        ],
    );

    f.title("📊", "Entry Counts");
    f.detail(
        "entry_counts",
        &[
            ("inbox", Field::new(queue.num_inbox)),
            ("running", Field::new(queue.num_running)),
            ("suspended", Field::new(queue.num_suspended)),
            ("paused_entries", Field::new(queue.num_paused)),
            ("finished", Field::new(queue.num_finished)),
        ],
    );

    let entry_headers = [
        "entry_id",
        "kind",
        "stage",
        "status",
        "has_lock",
        "attempts",
        "created_at",
        "deployment",
    ];
    let shown = entries.len();
    let entry_rows: Vec<Vec<Field>> = entries
        .into_iter()
        .map(|entry| {
            vec![
                Field::new(entry.entry_id),
                Field::new(entry.entry_kind),
                Field::new(entry.stage),
                Field::new(entry.status),
                Field::new(entry.has_lock),
                Field::new(entry.num_attempts),
                datetime(entry.created_at),
                optional(entry.deployment),
            ]
        })
        .collect();

    f.title("📥", "Entries");
    if entry_rows.is_empty() && !json {
        c_eprintln!("No entries found.");
    } else {
        f.table("entries", &entry_headers, &entry_rows);
    }

    f.finish()?;

    if !json {
        c_eprintln!("Showing {}/{} entries.", shown, total_entries);
    }

    Ok(())
}

/// An optional string: the real value in JSON (`null` when absent), a `-` placeholder
/// for humans.
fn optional(value: Option<String>) -> Field {
    match value {
        Some(v) => Field::new(v),
        None => Field::with_display(Value::Null, "-"),
    }
}

/// A timestamp: machine-readable RFC 3339 in JSON, the friendly local rendering for
/// humans.
fn datetime(value: DateTime<Local>) -> Field {
    Field::with_display(value.to_rfc3339(), value.display())
}

/// An optional timestamp, rendering a `-` placeholder (and `null` in JSON) when absent.
fn optional_datetime(value: Option<DateTime<Local>>) -> Field {
    match value {
        Some(dt) => datetime(dt),
        None => Field::with_display(Value::Null, "-"),
    }
}
