// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono_humanize::Tense;
use comfy_table::Table;
use dialoguer::console::style;
use dialoguer::console::Style as DStyle;
use dialoguer::console::StyledObject;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::JournalEntry;
use crate::clients::datafusion_helpers::JournalEntryType;
use crate::clients::datafusion_helpers::{Invocation, InvocationState};
use crate::ui::console::Icon;
use crate::ui::console::StyledTable;
use crate::{c_indent_table, c_indentln, c_println};

use super::duration_to_human_precise;

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
                msg.push_str(&format!("{}.", dur));
            };
            msg.push_str(&format!(" Retried {} time(s).", num_retries,));

            if let Some(next_retry) = invocation.next_retry_at {
                let next_retry = next_retry.signed_duration_since(chrono::Local::now());
                let next_retry = duration_to_human_precise(next_retry, Tense::Future);
                msg.push_str(&format!(" Next retry in {})", next_retry));
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

pub fn invocation_status(status: InvocationState) -> StyledObject<InvocationState> {
    let status_style = match status {
        InvocationState::Unknown => DStyle::new().red(),
        InvocationState::Pending => DStyle::new().yellow(),
        InvocationState::Ready => DStyle::new().blue(),
        InvocationState::Running => DStyle::new().green(),
        InvocationState::Suspended => DStyle::new().dim(),
        InvocationState::BackingOff => DStyle::new().red(),
        InvocationState::Completed => DStyle::new().blue(),
    };
    status_style.apply_to(status)
}

pub fn add_invocation_to_kv_table(table: &mut Table, invocation: &Invocation) {
    table.add_kv_row("Target:", &invocation.target);

    // Status: backing-off (Retried 1198 time(s). Next retry in 5 seconds and 78 ms) (if not pending....)
    let status_msg = invocation_status_note(invocation);
    let status = format!("{} {}", invocation_status(invocation.status), status_msg);
    table.add_kv_row("Status:", status);

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
            "{} {}",
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
        );
        table.add_kv_row("Deployment:", deployment_msg);
    }

    // Trace Id: "12343345345"
    if let Some(trace_id) = &invocation.trace_id {
        table.add_kv_row("Trace ID:", trace_id);
    }

    // Error: [Internal] other client error: error trying to connect: tcp connect error: Connection refused (os error 61)
    if invocation.status == InvocationState::BackingOff {
        if let Some(error) = &invocation.last_failure_message {
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
                        .map(|n| format!(" [{}]", n))
                        .or_else(|| invocation
                            .last_failure_entry_index
                            .map(|idx| format!(" [{}]", idx)))
                        .unwrap_or("".to_string())
                ),
            );
        }
    }
}

// [2023-12-14 15:38:52.500 +00:00] rIEqK14GCdkAYxo-wzTfrK2e6tJssIrtQ CheckoutProcess::checkout
//    Status:      backing-off  (Retried 67 time(s). Next retry in in 9 seconds and 616 ms))
//    Deployment:  bG9jYWxob3N0OjkwODEv
//    Error:       [Internal] other client error: error trying to connect: tcp connect error: Connection refused (os error 61)
pub fn render_invocation_compact(env: &CliEnv, invocation: &Invocation) {
    c_indentln!(1, "{}", invocation_header(invocation));
    let mut table = Table::new_styled(&env.ui_config);
    add_invocation_to_kv_table(&mut table, invocation);
    c_indent_table!(2, table);
    c_println!();
}

pub fn format_journal_entry(entry: &JournalEntry) -> String {
    let state_icon = if entry.is_completed() {
        Icon("☑️ ", "[DONE]")
    } else if matches!(entry.entry_type, JournalEntryType::Sleep { .. }) {
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
        format_entry_type_details(&entry.entry_type)
    )
}

pub fn format_entry_type_details(entry_type: &JournalEntryType) -> String {
    match entry_type {
        JournalEntryType::Sleep {
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
        JournalEntryType::Call(inv) | JournalEntryType::OneWayCall(inv) => {
            format!(
                "{} {}",
                inv.invoked_target.as_ref().unwrap(),
                inv.invocation_id.as_deref().unwrap_or(""),
            )
        }
        JournalEntryType::Awakeable(awakeable_id) => {
            format!("{}", style(awakeable_id.to_string()).cyan())
        }
        _ => String::new(),
    }
}
