// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod agg_status;
mod detailed_status;

use anyhow::Result;
use chrono_humanize::Tense;
use cling::prelude::*;
use comfy_table::{Cell, Table};

use restate_cli_util::c_println;
use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::ui::{duration_to_human_precise, duration_to_human_rough};
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::AdminClient;
use crate::clients::datafusion_helpers::{
    InvocationState, ServiceHandlerLockedKeysMap, ServiceStatus, ServiceStatusMap,
};
use crate::ui::invocations::invocation_status;
use crate::ui::service_handlers::icon_for_service_type;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_status")]
pub struct Status {
    #[clap(long, default_value = "5")]
    locked_keys_limit: usize,

    #[clap(long, default_value = "5")]
    locked_key_held_threshold_second: i64,

    #[clap(long, default_value = "5")]
    sample_invocations_limit: usize,

    /// Service name, prints all services if omitted
    service: Option<String>,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_status(State(env): State<CliEnv>, opts: &Status) -> Result<()> {
    opts.watch.run(|| status(&env, opts)).await
}

async fn status(env: &CliEnv, opts: &Status) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let sql_client = crate::clients::DataFusionHttpClient::from(client.clone());

    if let Some(svc) = &opts.service {
        detailed_status::run_detailed_status(svc, opts, client, sql_client).await
    } else {
        agg_status::run_aggregated_status(opts, client, sql_client).await
    }
}

async fn render_services_status(
    services: Vec<ServiceMetadata>,
    status_map: ServiceStatusMap,
) -> Result<()> {
    let empty = ServiceStatus::default();
    let mut table = Table::new_styled();
    table.set_styled_header(vec![
        "",
        "PENDING",
        "SCHEDULED",
        "READY",
        "RUNNING",
        "BACKING-OFF",
        "SUSPENDED",
        "OLDEST-NON-SUSPENDED-INVOCATION",
    ]);
    for svc in services {
        let svc_status = status_map.get_service_status(&svc.name).unwrap_or(&empty);
        // Service title
        let flavor = icon_for_service_type(&svc.ty);
        let svc_title = format!("{} {}", svc.name, flavor);
        table.add_row(vec![
            Cell::new(svc_title).add_attribute(comfy_table::Attribute::Bold),
        ]);

        render_handlers_status(&mut table, svc, svc_status).await?;
        table.add_row(vec![""]);
    }
    c_println!("{}", table);
    Ok(())
}

fn render_handler_state_stats(
    svc_status: &ServiceStatus,
    method: &str,
    state: InvocationState,
) -> Cell {
    use comfy_table::Color;
    // Pending
    if let Some(state_stats) = svc_status.get_handler_stats(state, method) {
        let cell = Cell::new(state_stats.num_invocations);
        let color = match state {
            InvocationState::Unknown => Color::Magenta,
            InvocationState::Scheduled => Color::Blue,
            InvocationState::Pending if state_stats.num_invocations > 10 => Color::Yellow,
            InvocationState::Running if state_stats.num_invocations > 0 => Color::Green,
            InvocationState::BackingOff if state_stats.num_invocations > 5 => Color::Red,
            InvocationState::BackingOff if state_stats.num_invocations > 0 => Color::Yellow,
            _ => comfy_table::Color::Reset,
        };
        cell.fg(color)
    } else {
        Cell::new("-")
    }
}

async fn render_handlers_status(
    table: &mut Table,
    svc: ServiceMetadata,
    svc_status: &ServiceStatus,
) -> Result<()> {
    for (_, handler) in svc.handlers {
        let mut row = vec![];
        row.push(Cell::new(format!("  {}", &handler.name)));
        // Pending
        row.push(render_handler_state_stats(
            svc_status,
            &handler.name,
            InvocationState::Pending,
        ));

        // Scheduled
        row.push(render_handler_state_stats(
            svc_status,
            &handler.name,
            InvocationState::Scheduled,
        ));

        // Ready
        row.push(render_handler_state_stats(
            svc_status,
            &handler.name,
            InvocationState::Ready,
        ));

        // Running
        row.push(render_handler_state_stats(
            svc_status,
            &handler.name,
            InvocationState::Running,
        ));

        // Backing-off
        row.push(render_handler_state_stats(
            svc_status,
            &handler.name,
            InvocationState::BackingOff,
        ));

        row.push(render_handler_state_stats(
            svc_status,
            &handler.name,
            InvocationState::Suspended,
        ));

        let oldest_cell = if let Some(current_handler) = svc_status.get_handler(&handler.name) {
            if let Some((oldest_state, oldest_stats)) =
                current_handler.oldest_non_suspended_invocation_state()
            {
                let dur = chrono::Local::now().signed_duration_since(oldest_stats.oldest_at);
                let style = if dur.num_seconds() < 60 {
                    Style::Info
                } else if dur.num_seconds() < 120 {
                    Style::Warn
                } else {
                    Style::Danger
                };

                let oldest_at_human = duration_to_human_rough(dur, Tense::Past);
                Cell::new(format!(
                    "{} {} (invoked {})",
                    oldest_stats.oldest_invocation,
                    invocation_status(oldest_state),
                    Styled(style, oldest_at_human)
                ))
            } else {
                Cell::new("-")
            }
        } else {
            Cell::new("-")
        };

        row.push(oldest_cell);

        table.add_row(row);
    }

    Ok(())
}
async fn render_locked_keys(
    locked_keys: ServiceHandlerLockedKeysMap,
    limit_per_service: usize,
    held_threshold_second: i64,
) -> Result<()> {
    let locked_keys = locked_keys.into_inner();
    if locked_keys.is_empty() {
        return Ok(());
    }

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["", "QUEUE", "LOCKED-BY", "HANDLER", "NOTES"]);
    for (svc_name, locked_keys) in locked_keys {
        let mut keys: Vec<_> = locked_keys.into_iter().collect();
        keys.sort_by(|(_, a), (_, b)| b.num_pending.cmp(&a.num_pending));

        let svc_title = format!("{} ({} active keys)", svc_name, keys.len());
        table.add_row(vec![
            Cell::new(svc_title).add_attribute(comfy_table::Attribute::Bold),
        ]);

        // Truncate to fit the limit
        keys.truncate(limit_per_service);

        for (key, key_info) in keys {
            let mut row = vec![];
            // Key
            row.push(Cell::new(format!("  {}", &key)));

            // Queue
            let queue_color = if key_info.num_pending > 10 {
                comfy_table::Color::Red
            } else if key_info.num_pending > 0 {
                comfy_table::Color::Yellow
            } else {
                comfy_table::Color::Reset
            };

            row.push(Cell::new(key_info.num_pending).fg(queue_color));

            // Holding invocation
            if let Some(invocation) = &key_info.invocation_holding_lock {
                row.push(Cell::new(format!(
                    "{} ({})",
                    invocation,
                    invocation_status(
                        key_info
                            .invocation_status
                            .unwrap_or(InvocationState::Unknown)
                    )
                )));
            } else {
                row.push(Cell::new("-"));
            }

            // Holding method
            if let Some(method) = &key_info.invocation_method_holding_lock {
                row.push(Cell::new(method));
            } else {
                row.push(Cell::new("-"));
            }

            let mut notes = Cell::new("");
            // Notes
            if let Some(invocation_status) = key_info.invocation_status {
                match invocation_status {
                    // Heuristic for issues, it's not accurate since we don't have the full picture
                    // in the CLI. Ideally, we should get metrics like "total flight duration" and
                    // "total suspension duration", "time_of_first_attempt", etc.
                    InvocationState::Running => {
                        // Check for duration...,
                        if let Some(run_duration) = key_info.invocation_attempt_duration {
                            let lock_held_period_msg = if let Some(state_duration) =
                                key_info.invocation_state_duration
                            {
                                format!(
                                    "It's been holding the lock for {}",
                                    Styled(
                                        Style::Danger,
                                        duration_to_human_precise(state_duration, Tense::Present)
                                    )
                                )
                            } else {
                                String::new()
                            };
                            if run_duration.num_seconds() > held_threshold_second {
                                // too long...
                                notes = Cell::new(format!(
                                    "Current attempt has been in-flight for {}. {}",
                                    Styled(
                                        Style::Danger,
                                        duration_to_human_precise(run_duration, Tense::Present)
                                    ),
                                    lock_held_period_msg,
                                ));
                            }
                        }
                    }
                    InvocationState::Suspended => {
                        if let Some(suspend_duration) = key_info.invocation_state_duration
                            && suspend_duration.num_seconds() > held_threshold_second
                        {
                            // too long...
                            notes = Cell::new(format!(
                                "Suspended for {}. The lock will not be \
                                    released until this invocation is complete",
                                Styled(
                                    Style::Danger,
                                    duration_to_human_precise(suspend_duration, Tense::Present)
                                )
                            ));
                        }
                    }
                    InvocationState::BackingOff => {
                        // Important to note,
                        let next_retry = key_info.next_retry_at.expect("No scheduled retry!");
                        let next_retry = next_retry.signed_duration_since(chrono::Local::now());
                        let next_retry = duration_to_human_precise(next_retry, Tense::Future);

                        let num_retries = key_info.num_retries.expect("No retries");
                        let num_retries = if num_retries > 10 {
                            Styled(Style::Danger, num_retries)
                        } else {
                            Styled(Style::Notice, num_retries)
                        };
                        notes = Cell::new(format!(
                            "Retried {num_retries} time(s). Next retry {next_retry}.",
                        ));
                    }
                    _ => {}
                }
            }

            row.push(notes);

            table.add_row(row);
        }
        table.add_row(vec![""]);
    }
    c_println!("{}", table);
    Ok(())
}
