// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Result, bail};
use cling::prelude::*;
use comfy_table::Table;
use dialoguer::console::style;

use restate_cli_util::CliContext;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::duration_to_human_rough;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_println, c_title};

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{
    Invocation, InvocationCompletion, InvocationState, JournalEventRow, JournalFetch,
    get_invocation, get_journal, get_journal_events,
};
use crate::clients::{self};
use crate::ui::fmt::{Field, Formatter, JournalScope, OutputFormatter};
use crate::ui::invocations::{add_invocation_to_kv_table, journal_event_lines};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// The ID of the invocation
    invocation_id: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    super::parse_invocation_id(&opts.invocation_id)?;
    let sql_client = clients::DataFusionHttpClient::new(env).await?;

    let Some(inv) = get_invocation(&sql_client, &opts.invocation_id).await? else {
        bail!("Invocation {} not found!", opts.invocation_id);
    };

    // The latest timeline event, when the server exposes them.
    let event = get_journal_events(&sql_client, &opts.invocation_id, 1)
        .await?
        .pop();

    let mut f = Formatter::new();
    f.next_step(
        &format!("restate invocations journal {}", opts.invocation_id),
        "see the full journal, a range (e.g. 20..30), or entry payloads (--payload)",
    );
    // Next steps are meant to be read-only, but resuming is the one obvious way forward
    // for a paused invocation, so it is suggested explicitly.
    if inv.status == InvocationState::Paused {
        f.next_step(
            &format!("restate invocations resume {}", opts.invocation_id),
            "resume the paused invocation once its cause is fixed",
        );
    }

    if CliContext::get().json_output() {
        // The journal call-graph is a human-oriented rendering; its structured form
        // is available via `restate invocations journal <id> --json`.
        let mut invocation = serde_json::to_value(&inv)?;
        // `completion` is `#[serde(skip)]` on the model (it's computed, not a column),
        // so inject it explicitly — an agent must be able to tell success from failure.
        if let Some(object) = invocation.as_object_mut() {
            let completion = match &inv.completion {
                Some(InvocationCompletion::Success) => serde_json::json!({ "result": "success" }),
                Some(InvocationCompletion::Failure(message)) => {
                    serde_json::json!({ "result": "failure", "message": message })
                }
                None => serde_json::Value::Null,
            };
            object.insert("completion".to_owned(), completion);
        }
        f.value("invocation", Field::json(invocation));
        if let Some(event) = &event {
            f.value("event", Field::json(event_json(event)));
        }
        return f.finish();
    }

    let mut table = Table::new_styled();
    add_invocation_to_kv_table(&mut table, &inv);
    if inv.invoked_by_id.is_none() {
        table.add_kv_row(
            "Invoked by:",
            format!("[{}]", style("Ingress").dim().italic()),
        );
    }
    c_title!("📜", "Invocation Information");
    c_println!("{}", table);

    // `c_title!` starts with a blank line of its own.
    c_title!("🕒", "Lifecycle");
    c_println!("{}", lifecycle_table(&inv));

    super::journal::print_journal_header();

    // Journal preview (metadata only). The dedicated `journal` command offers ranges,
    // full listing, and payloads.
    let head: u32 = 3;
    let tail: u32 = 10;
    let entries = get_journal(
        &sql_client,
        &opts.invocation_id,
        JournalFetch::Preview { head, tail },
        false,
    )
    .await?;

    if !entries.is_empty() {
        let rows = super::journal::journal_rows(&entries, false);
        f.journal("journal", &rows, JournalScope::Preview);
    }
    super::journal::print_journal_footer(Some(&inv));

    // The latest timeline event, if the server exposes any.
    if let Some(event) = &event {
        let completions = super::journal::completion_commands(&entries);
        let lines =
            journal_event_lines(event, inv.num_retries, &|id| completions.get(&id).cloned());
        c_title!("📅", "Last Event");
        let mut lines = lines.into_iter();
        if let Some(headline) = lines.next() {
            let when = event
                .appended_at
                .map(|at| format!(", {}", chrono_humanize::HumanTime::from(at)))
                .unwrap_or_default();
            c_println!("  {}{}", style(headline).bold(), style(when).dim());
        }
        for line in lines {
            c_println!("    {line}");
        }
    }

    f.finish()
}

/// The journal event for `describe --json`.
fn event_json(event: &JournalEventRow) -> serde_json::Value {
    serde_json::json!({
        "after_journal_entry_index": event.after_journal_entry_index,
        "appended_at": event.appended_at.map(|t| t.to_rfc3339()),
        "event_type": event.event_type,
        "event": event.event,
    })
}

/// The invocation's timestamps, in lifecycle order, skipping stages it never went
/// through. The creation time also says how long ago it was.
fn lifecycle_table(inv: &Invocation) -> Table {
    let mut table = Table::new_styled();
    table.add_kv_row(
        "Created at:",
        format!(
            "{} ({})",
            inv.created_at,
            duration_to_human_rough(
                chrono::Local::now().signed_duration_since(inv.created_at),
                chrono_humanize::Tense::Past
            )
        ),
    );
    for (label, at) in [
        ("Scheduled at:", inv.scheduled_at),
        ("Scheduled to start at:", inv.scheduled_start_at),
        ("Inboxed at:", inv.inboxed_at),
        ("First run at:", inv.running_at),
        ("Modified at:", inv.state_modified_at),
        ("Completed at:", inv.completed_at),
    ] {
        if let Some(at) = at {
            table.add_kv_row(label, at.to_string());
        }
    }
    table
}
