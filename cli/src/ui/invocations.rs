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
use chrono::{DateTime, Local};
use chrono_humanize::Tense;
use comfy_table::{Cell, Color, Table};
use dialoguer::console::Style as DStyle;
use dialoguer::console::StyledObject;
use dialoguer::console::{Style, style};
use serde::Serialize;
use serde_json::Value;

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::duration_to_human_precise;
use restate_cli_util::{
    CliContext, c_eprintln, c_indent_table, c_println, c_success, c_warn, exit,
};

use restate_types::journal_events::{Event, TransientErrorEvent};
use restate_types::journal_v2::{BuiltInSignal, NotificationId, UnresolvedFuture};

use crate::clients::datafusion_helpers::{
    Invocation, InvocationCompletion, InvocationState, JournalEventRow, SimpleInvocation,
};
use crate::ui::fmt::{Field, Formatter, ListItem, OutputFormatter};

pub fn invocation_status_note(invocation: &Invocation) -> String {
    let mut msg = String::new();

    match invocation.status {
        InvocationState::Running => {
            // active attempt duration
            if let Some(attempt_duration) = invocation.current_attempt_duration {
                let dur = duration_to_human_precise(attempt_duration, Tense::Present);
                let dur_style = if attempt_duration.num_seconds() > 5 {
                    // too long...
                    DStyle::new().red()
                } else {
                    DStyle::new()
                };
                msg.push_str(&format!(" ({})", dur_style.apply_to(dur)));
            }
        }
        InvocationState::Suspended => {
            if let Some(modified_at) = invocation.state_modified_at {
                let suspend_duration = chrono::Local::now().signed_duration_since(modified_at);
                // its keyed and in suspension
                if suspend_duration.num_seconds() > 5 && invocation.target_service_ty.is_keyed() {
                    let dur = duration_to_human_precise(suspend_duration, Tense::Present);
                    let dur_style = if suspend_duration.num_seconds() > 5 {
                        // too long...
                        DStyle::new().red()
                    } else {
                        DStyle::new()
                    };
                    msg.push_str(&format!(
                        " ({}. The key will not be released until this invocation is complete)",
                        dur_style.apply_to(dur)
                    ));
                } else {
                    let dur = duration_to_human_precise(suspend_duration, Tense::Past);
                    msg.push_str(&format!(" ({})", style(dur).dim()));
                }
            }
        }
        InvocationState::BackingOff => {
            let num_retries = invocation.num_retries.unwrap_or(0);
            let num_retries = if num_retries > 10 {
                style(num_retries).red()
            } else {
                style(num_retries).yellow()
            };

            msg.push_str(" (");
            if let Some(modified_at) = invocation.state_modified_at {
                let invoking_since = chrono::Local::now().signed_duration_since(modified_at);
                let dur = duration_to_human_precise(invoking_since, Tense::Present);
                msg.push_str(&format!("{dur}."));
            };
            msg.push_str(&format!(" Retried {num_retries} time(s).",));

            if let Some(next_retry) = invocation.next_retry_at {
                let next_retry = next_retry.signed_duration_since(chrono::Local::now());
                let next_retry = duration_to_human_precise(next_retry, Tense::Future);
                msg.push_str(&format!(" Next retry {next_retry}."));
            }

            msg.push(')');
        }
        _ => {}
    }

    msg
}

pub fn invocation_status_style(status: InvocationState) -> Style {
    match status {
        InvocationState::Unknown => DStyle::new().red(),
        InvocationState::Pending => DStyle::new().yellow(),
        InvocationState::Scheduled => DStyle::new().blue(),
        InvocationState::Ready => DStyle::new().blue(),
        InvocationState::Running => DStyle::new().green(),
        InvocationState::Suspended => DStyle::new().dim(),
        InvocationState::BackingOff => DStyle::new().red(),
        InvocationState::Completed => DStyle::new().blue(),
        InvocationState::Paused => DStyle::new().yellow(),
    }
}

pub fn invocation_status(status: InvocationState) -> StyledObject<InvocationState> {
    invocation_status_style(status).apply_to(status)
}

pub fn rich_invocation_status(
    status: InvocationState,
    completion: Option<&InvocationCompletion>,
) -> StyledObject<String> {
    match completion {
        None => invocation_status_style(status).apply_to(status.to_string()),
        Some(InvocationCompletion::Success) => DStyle::new()
            .green()
            .bold()
            .apply_to("completed with success".to_string()),
        Some(InvocationCompletion::Failure(_)) => DStyle::new()
            .red()
            .bold()
            .apply_to("completed with failure".to_string()),
    }
}

pub fn add_invocation_to_kv_table(table: &mut Table, invocation: &Invocation) {
    table.add_kv_row("Target:", &invocation.target);

    // Status: backing-off (Retried 1198 time(s). Next retry in 5 seconds and 78 ms) (if not pending....)
    let status_msg = invocation_status_note(invocation);
    let status = format!(
        "{} {}",
        rich_invocation_status(invocation.status, invocation.completion.as_ref()),
        status_msg
    );
    table.add_kv_row("Status:", status);

    if let Some(idempotency_key) = &invocation.idempotency_key {
        table.add_kv_row("Idempotency key:", idempotency_key);
    }

    // Invoked by: TicketDb p4DGRWa7OTJwAYxelm96fFWSV9woYc0MLQ
    if let Some(invoked_by_id) = &invocation.invoked_by_id {
        let invoked_by_msg = format!(
            "{} {}",
            invocation
                .invoked_by_target
                .as_ref()
                .map(|x| style(x.to_owned()).italic().blue())
                .unwrap_or_else(|| style("<UNKNOWN>".to_owned()).red()),
            style(invoked_by_id).italic(),
        );
        table.add_kv_row("Invoked by:", invoked_by_msg);
    }

    // Deployment: "bG9jYWxob3N0OjkwODAv" [pinned]
    let deployment_id = invocation
        .pinned_deployment_id
        .as_deref()
        .or(invocation.last_attempt_deployment_id.as_deref());

    if let Some(deployment_id) = deployment_id {
        let deployment_msg = format!(
            "{} {}{}",
            deployment_id,
            if invocation.pinned_deployment_id.is_some() {
                if invocation.pinned_deployment_exists {
                    format!("[{}]", style("pinned").bold())
                } else {
                    // deployment is missing!
                    format!("[{}]", style("ZOMBIE").red().bold())
                }
            } else {
                "".to_string()
            },
            if let Some(server) = &invocation.last_attempt_server {
                format!(" using {server}")
            } else {
                "".to_string()
            }
        );
        table.add_kv_row("Deployment:", deployment_msg);
    }

    // Trace Id: "12343345345"
    if let Some(trace_id) = &invocation.trace_id {
        table.add_kv_row("Trace ID:", trace_id);
    }

    // Error: [Internal] other client error: error trying to connect: tcp connect error: Connection refused (os error 61)
    if invocation.status == InvocationState::BackingOff
        && let Some(error) = &invocation.last_failure_message
    {
        let when = format!(
            "[{}]",
            invocation
                .last_attempt_started_at
                .map(|d| d.to_string())
                .unwrap_or_else(|| "UNKNOWN".to_owned())
        );

        table.add_kv_row(
            "Error:",
            format!("{}\n{}", style(when).dim(), style(error).red()),
        );

        table.add_kv_row(
            "Caused by:",
            format!(
                "{}{}",
                invocation
                    .last_failure_entry_ty
                    .as_deref()
                    .unwrap_or("UNKNOWN"),
                invocation
                    .last_failure_entry_name
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .map(|n| format!(" [{n}]"))
                    .unwrap_or_default()
            ),
        );
    }

    if let Some(InvocationCompletion::Failure(error)) = invocation.completion.clone() {
        table.add_kv_row("Error:", format!("{}", style(error).red()));
    }
}

/// Columns of the `changes` plan table of the batch invocation commands.
/// One invocation a batch command (cancel, pause, purge, …) will act on: listed like
/// `invocations list` (without detail lines); JSON keeps the flat plan row.
#[derive(Serialize)]
struct InvocationChange<'a> {
    invocation_id: &'a str,
    target: &'a str,
    status: &'a str,
    action: &'a str,
}

impl ListItem for InvocationChange<'_> {
    const HEADERS: &'static [&'static str] = &["invocation", "status"];

    fn columns(&self) -> Vec<Field> {
        let status = match self.status.parse::<InvocationState>() {
            Ok(InvocationState::Unknown) | Err(_) => self.status.to_owned(),
            Ok(state) => invocation_status(state).to_string(),
        };
        vec![
            Field::new(format!("[{}] {}", self.invocation_id, self.target)),
            Field::new(status),
        ]
    }
}

/// Write the invocations a batch command will apply `action` to as the `changes` plan.
/// Human output shows at most `limit` of them; JSON lists them all.
pub fn print_invocation_changes(
    f: &mut Formatter,
    invocations: &[SimpleInvocation],
    action: &str,
    limit: usize,
) -> Result<()> {
    let json = CliContext::get().json_output();
    let shown = if json { invocations.len() } else { limit };
    let changes: Vec<InvocationChange> = invocations
        .iter()
        .take(shown)
        .map(|inv| InvocationChange {
            invocation_id: &inv.id,
            target: &inv.target,
            status: &inv.status,
            action,
        })
        .collect();
    f.list("changes", &changes)?;
    if !json {
        if invocations.len() > shown {
            c_println!("And other {} invocations...", invocations.len() - shown)
        }
        c_println!();
    }
    Ok(())
}

/// No invocation matched: `--json` gets an empty `changes` plan, humans get `message`
/// as an error.
pub fn no_invocations_to_change(message: String) -> Result<()> {
    if !CliContext::get().json_output() {
        bail!(message);
    }
    let mut f = Formatter::new();
    f.list::<InvocationChange>("changes", &[])?;
    f.finish()
}

/// Write the outcome of a batch invocation command: the success count for humans, and a
/// `results` table (one row per invocation, `outcome` being `past` or `failed`) for JSON.
pub fn print_invocation_results<T>(
    f: &mut Formatter,
    past: &str,
    succeeded: &[(SimpleInvocation, T)],
    failed: &[(SimpleInvocation, anyhow::Error)],
) {
    if !CliContext::get().json_output() {
        c_println!();
        c_success!("{past} {} invocations", succeeded.len());
        return;
    }
    let outcome = past.to_lowercase();
    let rows: Vec<Vec<Field>> = succeeded
        .iter()
        .map(|(inv, _)| (inv, outcome.as_str(), Value::Null))
        .chain(
            failed
                .iter()
                .map(|(inv, err)| (inv, "failed", Value::from(err.to_string()))),
        )
        .map(|(inv, outcome, error)| {
            vec![
                Field::new(inv.id.clone()),
                Field::new(inv.target.clone()),
                Field::new(outcome),
                Field::json(error),
            ]
        })
        .collect();
    f.table(
        "results",
        &["invocation_id", "target", "outcome", "error"],
        &rows,
    );
}

/// Finish a batch invocation command, failing when any invocation could not be
/// changed. The failures are shown as a table for humans; under `--json` they are
/// carried by the error document (stdout holds a single document).
pub fn finish_invocation_results(
    f: Formatter,
    verb: &str,
    succeeded_count: usize,
    failed: Vec<(SimpleInvocation, anyhow::Error)>,
) -> Result<()> {
    if failed.is_empty() {
        return f.finish();
    }
    let summary = format!(
        "Failed to {verb} {} invocations out of {}",
        failed.len(),
        failed.len() + succeeded_count
    );
    if CliContext::get().json_output() {
        // The `results` section already carries each failure; keep stdout a single
        // document and signal the partial failure through the exit code only.
        f.finish()?;
        c_eprintln!("{summary}");
        return Err(exit::AlreadyReported {
            code: exit::GENERIC_ERROR,
        }
        .into());
    }

    c_println!();
    c_warn!("Failed to {verb}:");
    let mut table = Table::new_styled();
    table.set_styled_header(vec!["ID", "REASON"]);
    for (inv, reason) in failed {
        table.add_row(vec![
            Cell::new(&inv.id),
            Cell::new(reason).fg(Color::DarkRed),
        ]);
    }
    c_indent_table!(0, table);
    bail!(summary)
}

// [2023-12-14 15:38:52.500 +00:00] rIEqK14GCdkAYxo-wzTfrK2e6tJssIrtQ CheckoutProcess::checkout
//    Status:      backing-off  (Retried 67 time(s). Next retry in in 9 seconds and 616 ms))
//    Deployment:  bG9jYWxob3N0OjkwODEv
//    Error:       [Internal] other client error: error trying to connect: tcp connect error: Connection refused (os error 61)
/// Status cell of the invocations list: `succeeded` / `failed` for completed ones,
/// with the retry count when backing off.
fn invocation_list_status(invocation: &Invocation) -> String {
    match (&invocation.completion, invocation.status) {
        (Some(InvocationCompletion::Success), _) => style("succeeded").green().to_string(),
        (Some(InvocationCompletion::Failure(_)), _) => style("failed").red().to_string(),
        (None, InvocationState::BackingOff) => {
            let status = invocation_status(InvocationState::BackingOff);
            match invocation.num_retries {
                Some(retries) if retries > 0 => format!("{status} (retry {retries})"),
                _ => status.to_string(),
            }
        }
        (None, status) => invocation_status(status).to_string(),
    }
}

/// The dimmed lines under an invocation: its timestamps, then its idempotency key.
fn invocation_list_details(invocation: &Invocation) -> Vec<String> {
    let mut times = vec![format!("created {}", short_ago(invocation.created_at))];
    if let Some(modified) = invocation.state_modified_at {
        times.push(format!("modified {}", short_ago(modified)));
    }
    if invocation.status == InvocationState::Scheduled
        && let Some(start) = invocation.scheduled_start_at
    {
        times.push(format!("starts {}", short_in(start)));
    }
    let mut lines = vec![times.join(" · ")];
    if let Some(key) = &invocation.idempotency_key {
        lines.push(format!("idempotency key {key}"));
    }
    lines
}

/// A compact relative time for table cells, e.g. `45s ago`, `12m ago`, `3h ago`, `2d ago`.
pub fn short_ago(at: DateTime<Local>) -> String {
    let secs = Local::now().signed_duration_since(at).num_seconds().max(0);
    format!("{} ago", short_duration(secs))
}

/// A compact time until `at`, e.g. `in 58m` (or `now` when it's due).
fn short_in(at: DateTime<Local>) -> String {
    let secs = at.signed_duration_since(Local::now()).num_seconds();
    if secs <= 0 {
        "now".to_owned()
    } else {
        format!("in {}", short_duration(secs))
    }
}

fn short_duration(secs: i64) -> String {
    match secs {
        ..60 => format!("{secs}s"),
        60..3600 => format!("{}m", secs / 60),
        3600..86400 => format!("{}h", secs / 3600),
        _ => format!("{}d", secs / 86400),
    }
}

/// `invocations list` item: `[id] target` and status, then timestamps and idempotency
/// key; JSON is the full invocation.
impl ListItem for Invocation {
    const HEADERS: &'static [&'static str] = &["invocation", "status"];

    fn columns(&self) -> Vec<Field> {
        vec![
            Field::new(format!("[{}] {}", self.id, self.target)),
            Field::new(invocation_list_status(self)),
        ]
    }

    fn details(&self) -> Vec<String> {
        invocation_list_details(self)
    }
}

/// Stacktrace lines shown for an event's error.
const STACKTRACE_LINES: usize = 10;
/// Signal indexes below this are reserved for built-in signals (like the web UI's
/// `futureEntries.ts`); awakeables use the ones from here on.
const FIRST_AWAKEABLE_SIGNAL_INDEX: u32 = 17;

/// Human rendering of a journal event: a headline (e.g. `Transient error (retried 3
/// times) after entry #5`), then detail lines. `retries` is the invocation's retry count;
/// `completion` describes the command owning an awaited completion id, when known.
pub fn journal_event_lines(
    event: &JournalEventRow,
    retries: Option<u64>,
    completion: &dyn Fn(u32) -> Option<String>,
) -> Vec<String> {
    let anchor = format!("after entry #{}", event.after_journal_entry_index);
    let Some(decoded) = event.decoded() else {
        return vec![format!("{} {anchor}", event.event_type)];
    };
    match decoded {
        Event::TransientError(failure) => {
            let retried = match retries {
                Some(n) if n > 0 => format!(" (retried {n} times)"),
                _ => String::new(),
            };
            let mut lines = vec![format!("Transient error{retried} {anchor}")];
            lines.extend(failure_lines(&failure));
            lines
        }
        Event::Paused(paused) => match paused.last_failure {
            Some(failure) => {
                let mut lines = vec![format!("Paused {anchor}")];
                lines.extend(failure_lines(&failure));
                lines
            }
            None => vec![format!(
                "Paused {anchor} (no failure recorded, e.g. paused manually)"
            )],
        },
        Event::Suspended(suspended) => vec![
            format!("Suspended {anchor}"),
            format!(
                "waiting on: {}",
                describe_future(&suspended.awaiting_on, completion, false)
            ),
        ],
        Event::Unknown => vec![format!("{} {anchor}", event.event_type)],
    }
}

fn failure_lines(failure: &TransientErrorEvent) -> Vec<String> {
    let mut lines = vec![format!("error: {}", failure.error_message)];
    let code = u16::from(failure.error_code);
    lines.push(match &failure.restate_doc_error_code {
        Some(doc_code) => format!("code: {code} ({doc_code})"),
        None => format!("code: {code}"),
    });
    // The index counts commands only, unlike the journal entry index of the anchor.
    if let Some(index) = failure.related_command_index {
        let ty = failure
            .related_command_type
            .map(|ty| format!("{ty} "))
            .unwrap_or_default();
        let name = failure
            .related_command_name
            .as_deref()
            .filter(|name| !name.is_empty())
            .map(|name| format!("[{name}] "))
            .unwrap_or_default();
        lines.push(format!("failed at: {ty}{name}(command #{index})"));
    }
    if let Some(trace) = &failure.error_stacktrace {
        let trace = stacktrace_lines(trace, &failure.error_message, STACKTRACE_LINES);
        if !trace.is_empty() {
            lines.push("stacktrace:".to_owned());
            lines.extend(trace.into_iter().map(|line| format!("  {line}")));
        }
    }
    lines
}

/// The first `max` non-empty lines of `trace`, trimmed, with a `(N more lines)` marker.
/// A leading line repeating the error message is dropped.
fn stacktrace_lines(trace: &str, message: &str, max: usize) -> Vec<String> {
    let mut lines: Vec<&str> = trace
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if lines.first().is_some_and(|first| first.ends_with(message)) {
        lines.remove(0);
    }
    let mut shown: Vec<String> = lines.iter().take(max).map(|l| (*l).to_owned()).collect();
    if lines.len() > max {
        shown.push(format!("... ({} more lines)", lines.len() - max));
    }
    shown
}

/// `awakeable (signal 17) or cancel`: `FirstCompleted`-like combinators read as `or`,
/// `All*` as `and`; nested groups are parenthesized.
fn describe_future(
    future: &UnresolvedFuture,
    completion: &dyn Fn(u32) -> Option<String>,
    nested: bool,
) -> String {
    let (children, separator) = match future {
        UnresolvedFuture::Single(id) => return describe_notification(id, completion),
        UnresolvedFuture::FirstCompleted(children)
        | UnresolvedFuture::FirstSucceededOrAllFailed(children)
        | UnresolvedFuture::Unknown(children) => (children, " or "),
        UnresolvedFuture::AllCompleted(children)
        | UnresolvedFuture::AllSucceededOrFirstFailed(children) => (children, " and "),
    };
    let joined = children
        .iter()
        .map(|child| describe_future(child, completion, true))
        .collect::<Vec<_>>()
        .join(separator);
    if nested && children.len() > 1 {
        format!("({joined})")
    } else {
        joined
    }
}

fn describe_notification(
    id: &NotificationId,
    completion: &dyn Fn(u32) -> Option<String>,
) -> String {
    match id {
        NotificationId::CompletionId(id) => {
            completion(*id).unwrap_or_else(|| format!("completion {id}"))
        }
        NotificationId::SignalIndex(index) if *index == BuiltInSignal::Cancel as u32 => {
            "cancel".to_owned()
        }
        NotificationId::SignalIndex(index) if *index >= FIRST_AWAKEABLE_SIGNAL_INDEX => {
            format!("awakeable (signal {index})")
        }
        NotificationId::SignalIndex(index) => format!("signal {index}"),
        NotificationId::SignalName(name) => format!("signal '{name}'"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn event(after: u32, event: Value) -> JournalEventRow {
        JournalEventRow {
            after_journal_entry_index: after,
            appended_at: None,
            event_type: event["ty"].as_str().unwrap().to_owned(),
            event: Some(event),
        }
    }

    #[test]
    fn transient_error_event_shows_failure_and_truncated_stacktrace() {
        let trace = (1..=12).map(|i| format!("    at f{i}")).collect::<Vec<_>>();
        let transient = event(
            3,
            json!({
                "ty": "TransientError",
                "error_code": 500,
                "error_message": "boom",
                "error_stacktrace": format!("Error: boom\n{}", trace.join("\n")),
                "restate_doc_error_code": "RT0007",
                "related_command_index": 2,
                "related_command_name": "call downstream",
                "related_command_type": "Run"
            }),
        );
        let expected_trace = (1..=STACKTRACE_LINES).map(|i| format!("  at f{i}"));
        let expected: Vec<String> = [
            "Transient error (retried 20 times) after entry #3",
            "error: boom",
            "code: 500 (RT0007)",
            "failed at: Run [call downstream] (command #2)",
            "stacktrace:",
        ]
        .into_iter()
        .map(str::to_owned)
        .chain(expected_trace)
        .chain(["  ... (2 more lines)".to_owned()])
        .collect();
        assert_eq!(
            journal_event_lines(&transient, Some(20), &|_| None),
            expected
        );
    }

    #[test]
    fn suspended_event_shows_what_it_awaits() {
        let suspended = event(
            265,
            json!({"ty": "Suspended", "awaiting_on": {"FirstCompleted": [
                {"Single": {"SignalIndex": 17}},
                {"AllCompleted": [{"Single": {"CompletionId": 2}}, {"Single": {"SignalName": "go"}}]},
                {"Single": {"SignalIndex": 1}}
            ]}}),
        );
        let completion = |id: u32| (id == 2).then(|| "Sleep [nap] (entry #3)".to_owned());
        assert_eq!(
            journal_event_lines(&suspended, None, &completion),
            [
                "Suspended after entry #265",
                "waiting on: awakeable (signal 17) or (Sleep [nap] (entry #3) and signal 'go') or cancel"
            ]
        );
    }
}
