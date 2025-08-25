// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::clients::datafusion_helpers::{
    Invocation, InvocationState, JournalEntry, JournalEntryV2,
};
use crate::clients::datafusion_helpers::{InvocationCompletion, JournalEntryTypeV1};
use crate::clients::datafusion_helpers::{JournalEntryV1, SimpleInvocation};
use chrono_humanize::Tense;
use comfy_table::{Attribute, Cell, Table};
use dialoguer::console::Style as DStyle;
use dialoguer::console::StyledObject;
use dialoguer::console::{Style, style};
use restate_cli_util::c_indent_table;
use restate_cli_util::c_indentln;
use restate_cli_util::c_println;
use restate_cli_util::ui::console::Icon;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::duration_to_human_precise;
use restate_types::invocation::InvocationQuery;
use restate_types::journal_v2::{
    AttachInvocationCommand, CallCommand, ClearStateCommand, Command, CompleteAwakeableCommand,
    CompletePromiseCommand, Entry, GetEagerStateCommand, GetInvocationOutputCommand,
    GetLazyStateCommand, GetPromiseCommand, OneWayCallCommand, PeekPromiseCommand,
    SendSignalCommand, SetStateCommand, SleepCommand,
};
use std::time::SystemTime;

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
                msg.push_str(&format!(" Next retry in {next_retry})"));
            }

            msg.push(')');
        }
        _ => {}
    }

    msg
}

// ❯ [2023-12-14 15:38:52.500 +00:00] rIEqK14GCdkAYxo-wzTfrK2e6tJssIrtQ CheckoutProcess::checkout
fn invocation_header(invocation: &Invocation) -> String {
    // Unkeyed -> [2023-12-14 15:38:52.500 +00:00] rIEqK14GCdkAYxo-wzTfrK2e6tJssIrtQ CheckoutProcess::checkout
    // Keyed   -> [2023-12-13 12:04:54.902 +00:00] g4Ej8NLd-aYAYxjEM31dhOLXpCi5azJNA [TicketDb @ my-user-200]::trySomethingNew
    let date_style = DStyle::new().dim();
    let created_at = date_style.apply_to(format!("[{}]", invocation.created_at));

    format!("❯ {} {}", created_at, style(&invocation.id).bold())
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

    // Deployment: "bG9jYWxob3N0OjkwODAv" [required]
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
                    format!("[{}]", style("required").bold())
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
                .unwrap_or("UNKNOWN".to_owned())
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
                    .and_then(|s| if s.is_empty() { None } else { Some(s) })
                    .map(|n| format!(" [{n}]"))
                    .or_else(|| invocation
                        .last_failure_entry_index
                        .map(|idx| format!(" [{idx}]")))
                    .unwrap_or("".to_string())
            ),
        );
    }

    if let Some(InvocationCompletion::Failure(error)) = invocation.completion.clone() {
        table.add_kv_row("Error:", format!("{}", style(error).red()));
    }
}

pub fn render_simple_invocation_list(invocations: &[SimpleInvocation]) {
    let mut invocations_table = Table::new_styled();
    invocations_table.set_styled_header(vec!["ID", "TARGET", "STATUS"]);

    for inv in invocations {
        invocations_table.add_row(vec![
            Cell::new(&inv.id).add_attribute(Attribute::Bold),
            Cell::new(&inv.target),
            Cell::new(invocation_status(inv.status)),
        ]);
    }
    c_indent_table!(0, invocations_table);
    c_println!();
}

// [2023-12-14 15:38:52.500 +00:00] rIEqK14GCdkAYxo-wzTfrK2e6tJssIrtQ CheckoutProcess::checkout
//    Status:      backing-off  (Retried 67 time(s). Next retry in in 9 seconds and 616 ms))
//    Deployment:  bG9jYWxob3N0OjkwODEv
//    Error:       [Internal] other client error: error trying to connect: tcp connect error: Connection refused (os error 61)
pub fn render_invocation_compact(invocation: &Invocation) {
    c_indentln!(1, "{}", invocation_header(invocation));
    let mut table = Table::new_styled();
    add_invocation_to_kv_table(&mut table, invocation);
    c_indent_table!(2, table);
    c_println!();
}

pub fn format_journal_entry(entry: &JournalEntry) -> String {
    match entry {
        JournalEntry::V1(v1) => format_journal_entry_v1(v1),
        JournalEntry::V2(v2) => format_journal_entry_v2(v2),
    }
}

pub fn format_journal_entry_v1(entry: &JournalEntryV1) -> String {
    let state_icon = if entry.is_completed() {
        Icon("☑️ ", "[DONE]")
    } else if matches!(entry.entry_type, JournalEntryTypeV1::Sleep { .. }) {
        Icon("⏰", "[PENDING]")
    } else {
        Icon("⏸️ ", "[PENDING]")
    };

    let type_style = if entry.is_completed() {
        DStyle::new().dim().italic()
    } else {
        DStyle::new().green().bold()
    };

    let seq = format!("#{}", entry.seq);
    let entry_ty_and_name = if let Some(name) = &entry.name {
        format!("{} [{}]", entry.entry_type, name)
    } else {
        entry.entry_type.to_string()
    };
    format!(
        " {} {} {} {}",
        state_icon,
        type_style.apply_to(seq),
        type_style.apply_to(entry_ty_and_name),
        format_entry_type_v1_details(&entry.entry_type)
    )
}

fn format_entry_type_v1_details(entry_type: &JournalEntryTypeV1) -> String {
    match entry_type {
        JournalEntryTypeV1::Sleep {
            wakeup_at: Some(wakeup_at),
        } => {
            let left = wakeup_at.signed_duration_since(chrono::Local::now());
            if left.num_milliseconds() >= 0 {
                let left = duration_to_human_precise(left, Tense::Present);
                format!("until {} ({} left)", wakeup_at, style(left).cyan())
            } else {
                format!("until {}", style(wakeup_at).dim())
            }
        }
        JournalEntryTypeV1::Call(inv) | JournalEntryTypeV1::OneWayCall(inv) => {
            format!(
                "{} {}",
                inv.invoked_target.as_ref().unwrap(),
                inv.invocation_id.as_deref().unwrap_or(""),
            )
        }
        JournalEntryTypeV1::Awakeable(awakeable_id) => {
            format!("{}", style(awakeable_id.to_string()).cyan())
        }
        JournalEntryTypeV1::GetPromise(Some(promise_name)) => {
            format!("{}", style(promise_name).cyan())
        }
        _ => String::new(),
    }
}

pub fn format_journal_entry_v2(entry: &JournalEntryV2) -> String {
    let seq_and_time = if let Some(timestamp) = &entry.appended_at {
        let date_style = DStyle::new().dim();
        format!("#{} [{}]", entry.seq, date_style.apply_to(timestamp))
    } else {
        format!("#{}", entry.seq)
    };
    let entry_ty_and_name = if let Some(name) = &entry.name {
        if !name.is_empty() {
            format!("{} [{}]", entry.entry_type, name)
        } else {
            entry.entry_type.clone()
        }
    } else {
        entry.entry_type.clone()
    };

    format!(
        " {} {} {}",
        seq_and_time,
        entry_ty_and_name,
        format_entry_type_v2_details(&entry.entry)
    )
}

fn format_entry_type_v2_details(entry: &Option<Entry>) -> String {
    if entry.is_none() {
        return "".to_owned();
    }
    match entry.as_ref().unwrap() {
        Entry::Command(Command::Sleep(SleepCommand { wake_up_time, .. })) => {
            let wakeup_at: chrono::DateTime<chrono::Local> =
                chrono::DateTime::from(SystemTime::from(*wake_up_time));
            let left = wakeup_at.signed_duration_since(chrono::Local::now());
            if left.num_milliseconds() >= 0 {
                let left = duration_to_human_precise(left, Tense::Present);
                format!("until {} ({} left)", wakeup_at, style(left).cyan())
            } else {
                format!("until {}", style(wakeup_at).dim())
            }
        }
        Entry::Command(Command::Call(CallCommand { request, .. }))
        | Entry::Command(Command::OneWayCall(OneWayCallCommand { request, .. })) => {
            format!("{} {}", request.invocation_target, request.invocation_id)
        }
        Entry::Command(Command::CompleteAwakeable(CompleteAwakeableCommand { id, .. })) => {
            format!("id {id}")
        }
        Entry::Command(Command::SendSignal(SendSignalCommand {
            target_invocation_id,
            signal_id,
            ..
        })) => {
            format!("signal {signal_id} to {target_invocation_id}")
        }
        Entry::Command(Command::GetLazyState(GetLazyStateCommand { key, .. }))
        | Entry::Command(Command::GetEagerState(GetEagerStateCommand { key, .. }))
        | Entry::Command(Command::SetState(SetStateCommand { key, .. }))
        | Entry::Command(Command::ClearState(ClearStateCommand { key, .. }))
        | Entry::Command(Command::GetPromise(GetPromiseCommand { key, .. }))
        | Entry::Command(Command::PeekPromise(PeekPromiseCommand { key, .. }))
        | Entry::Command(Command::CompletePromise(CompletePromiseCommand { key, .. })) => {
            format!("key {}", style(key).cyan())
        }
        Entry::Command(Command::AttachInvocation(AttachInvocationCommand { target, .. }))
        | Entry::Command(Command::GetInvocationOutput(GetInvocationOutputCommand {
            target, ..
        })) => {
            format!(
                "{}",
                InvocationQuery::from(target.clone()).to_invocation_id()
            )
        }
        _ => String::new(),
    }
}
