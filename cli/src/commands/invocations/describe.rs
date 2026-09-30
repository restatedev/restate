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
use dialoguer::console::style;

use restate_cli_util::ui::duration_to_human_rough;
use restate_cli_util::ui::watcher::Watch;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{
    Invocation, InvocationState, JournalFetch, get_invocation, get_journal, get_journal_events,
};
use crate::clients::{self};
use crate::error::RestateCliError;
use crate::ui::fmt::{
    Field, Formatter, IncludeFormatting, JournalScope, OutputFormatter, compact_duration,
};
use crate::ui::invocations::{invocation_detail, journal_event_lines, journal_status};

/// Show an invocation: status, target, deployment, retries and last failure
///
/// Shows who called it, its idempotency key, when it was created and last changed, its
/// deployment, and for invocations that are retrying or paused the retry count and the last
/// failure (with the journal entry that caused it).
/// Also shows a preview of the journal: see `restate invocations journal` for all of it.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
#[command(after_help = after_help!(
    examples: [
        "restate invocations describe inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz --json",
    ],
    learn_more: "https://docs.restate.dev/services/invocation/managing-invocations#lifecycle",
))]
pub struct Describe {
    /// Invocation id (`inv_...`)
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
        return Err(RestateCliError::not_found(format!(
            "Invocation {} not found",
            opts.invocation_id
        ))
        .into());
    };

    // The latest timeline event, when the server exposes them.
    let event = get_journal_events(&sql_client, &opts.invocation_id, 1)
        .await?
        .pop();

    let mut f = Formatter::new();
    f.next_step(
        &format!("restate invocations journal {}", opts.invocation_id),
        "see the full journal, a range (e.g. 20..30), or entry payloads (--payload)",
        IncludeFormatting::Yes,
    );
    // Next steps are meant to be read-only, but resuming is the one obvious way forward
    // for a paused invocation, so it is suggested explicitly.
    if inv.status == InvocationState::Paused {
        f.next_step(
            &format!("restate invocations resume {}", opts.invocation_id),
            "resume the paused invocation once its cause is fixed",
            IncludeFormatting::Yes,
        );
    }

    f.title("📜", "Invocation Information");
    f.detail("invocation", invocation_detail(&inv));
    f.title("🕒", "Lifecycle");
    f.detail("lifecycle", lifecycle_detail(&inv));
    f.title("🚂", "Journal");

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

    let rows = super::journal::journal_rows(&entries, false);
    f.journal(
        "journal",
        &rows,
        JournalScope::Preview,
        Some(journal_status(&inv)),
    );

    // The latest timeline event, if the server exposes any.
    if let Some(event) = &event {
        let completions = super::journal::completion_commands(&entries);
        let mut lines =
            journal_event_lines(event, inv.num_retries, &|id| completions.get(&id).cloned())
                .into_iter();
        // `value` indents each line by one space.
        let mut display = String::new();
        if let Some(headline) = lines.next() {
            let when = event
                .appended_at
                .map(|at| format!(", {}", chrono_humanize::HumanTime::from(at)))
                .unwrap_or_default();
            display = format!(" {}{}", style(headline).bold(), style(when).dim());
        }
        for line in lines {
            display.push_str(&format!("\n   {line}"));
        }
        f.title("📅", "Last Event");
        f.value("event", Field::with_display(event, display));
    }

    f.finish()
}

/// The invocation's timestamps, in lifecycle order, skipping stages it never went
/// through. The creation time also says how long ago it was, the others how long after
/// it they happened.
fn lifecycle_detail(inv: &Invocation) -> Vec<(&'static str, Field)> {
    let created_at = Field::with_display(
        inv.created_at,
        format!(
            "{} ({})",
            inv.created_at,
            duration_to_human_rough(
                chrono::Local::now().signed_duration_since(inv.created_at),
                chrono_humanize::Tense::Past
            )
        ),
    );
    let mut rows = vec![("created_at", created_at)];
    for (key, at) in [
        ("scheduled_at", inv.scheduled_at),
        ("scheduled_to_start_at", inv.scheduled_start_at),
        ("inboxed_at", inv.inboxed_at),
        ("first_run_at", inv.running_at),
        ("modified_at", inv.state_modified_at),
        ("completed_at", inv.completed_at),
    ] {
        let field = match at {
            Some(at) => Field::with_display(
                at,
                format!(
                    "{at} (+{})",
                    compact_duration(at.signed_duration_since(inv.created_at))
                ),
            ),
            None => Field::new(()),
        };
        rows.push((key, field));
    }
    rows
}
