// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{
    get_locked_keys_status, get_services_status, EnrichedInvocationState,
    ServiceMethodLockedKeysMap, ServiceStatus, ServiceStatusMap,
};
use crate::clients::{MetaClientInterface, MetasClient};
use crate::ui::console::{Styled, StyledTable};
use crate::ui::service_methods::icon_for_service_flavor;
use crate::ui::stylesheet::Style;
use crate::{c_error, c_println};

use anyhow::Result;
use chrono::Duration;
use chrono_humanize::{Accuracy, Tense};
use cling::prelude::*;
use comfy_table::{Cell, Table};
use indicatif::ProgressBar;
use restate_meta_rest_model::services::{InstanceType, ServiceMetadata};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_status")]
pub struct Status {
    //// Show additional columns
    #[clap(long)]
    extra: bool,

    #[clap(long, default_value = "5")]
    locked_keys_limit: usize,

    /// Service name, prints all services if omitted
    service: Option<String>,
}

pub async fn run_status(State(env): State<CliEnv>, opts: &Status) -> Result<()> {
    // First, let's get the service metadata
    let client = MetasClient::new(&env)?;
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message("Fetching services status");
    let services = if let Some(ref svc_name) = opts.service {
        vec![client.get_service(svc_name).await?.into_body().await?]
    } else {
        client.get_services().await?.into_body().await?.services
    };
    if services.is_empty() {
        progress.finish_and_clear();
        c_error!(
            "No services were found! Services are added by registering deployments with 'restate dep register'"
        );
        return Ok(());
    }

    let all_service_names: Vec<_> = services.iter().map(|x| x.name.clone()).collect();

    let keyed: Vec<_> = services
        .iter()
        .filter(|svc| svc.instance_type == InstanceType::Keyed)
        .cloned()
        .collect();

    let sql_client = crate::clients::DataFusionHttpClient::new(&env)?;
    let status_map = get_services_status(&sql_client, all_service_names).await?;

    let locked_keys = get_locked_keys_status(&sql_client, keyed.iter().map(|x| &x.name)).await?;
    // Render UI
    progress.finish_and_clear();
    // Render Status Table
    render_services_status(&env, services, status_map).await?;
    // Render Locked Keys
    render_locked_keys(&env, locked_keys, opts.locked_keys_limit).await?;
    Ok(())
}

async fn render_locked_keys(
    env: &CliEnv,
    locked_keys: ServiceMethodLockedKeysMap,
    limit_per_service: usize,
) -> Result<()> {
    let locked_keys = locked_keys.into_inner();
    if locked_keys.is_empty() {
        return Ok(());
    }

    c_println!();
    c_println!();
    let mut table = Table::new_styled(&env.ui_config);
    table.set_styled_header(vec!["", "QUEUE", "LOCKED BY", "METHOD", "NOTES"]);
    for (svc_name, locked_keys) in locked_keys {
        let mut keys: Vec<_> = locked_keys.into_iter().collect();
        keys.sort_by(|(_, a), (_, b)| b.num_pending.cmp(&a.num_pending));

        let svc_title = format!("{} ({} active keys)", svc_name, keys.len());
        table.add_row(vec![
            Cell::new(svc_title).add_attribute(comfy_table::Attribute::Bold)
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
                    key_info
                        .invocation_status
                        .unwrap_or(EnrichedInvocationState::Unknown)
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
                    EnrichedInvocationState::Running => {
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
                            // TODO: Make this a configurable threshold
                            if run_duration.num_seconds() > 5 {
                                // too long...
                                notes = Cell::new(format!(
                                    "Current invocation attempt has been in-flight for {}. {}",
                                    Styled(
                                        Style::Danger,
                                        duration_to_human_precise(run_duration, Tense::Present)
                                    ),
                                    lock_held_period_msg,
                                ));
                            }
                        }
                        // last run attempt has been going for....
                    }
                    EnrichedInvocationState::Suspended => {
                        if let Some(suspend_duration) = key_info.invocation_state_duration {
                            if suspend_duration.num_seconds() > 5 {
                                // too long...
                                notes = Cell::new(format!(
                                    "Invocation has been suspended for {}. The lock will not be \
                                    released until this invocation is complete",
                                    Styled(
                                        Style::Danger,
                                        duration_to_human_precise(suspend_duration, Tense::Present)
                                    )
                                ));
                            }
                        }
                    }
                    EnrichedInvocationState::BackingOff => {
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
                            "Invocation is fail-looping, retried {} time(s). Next retry {}",
                            num_retries, next_retry,
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

fn duration_to_human_precise(duration: Duration, tense: Tense) -> String {
    let duration =
        chrono_humanize::HumanTime::from(Duration::milliseconds(duration.num_milliseconds()));
    duration.to_text_en(Accuracy::Precise, tense)
}

fn duration_to_human_rough(duration: Duration, tense: Tense) -> String {
    let duration = chrono_humanize::HumanTime::from(duration);
    duration.to_text_en(Accuracy::Rough, tense)
}

async fn render_services_status(
    env: &CliEnv,
    services: Vec<ServiceMetadata>,
    status_map: ServiceStatusMap,
) -> Result<()> {
    let empty = ServiceStatus::default();
    let mut table = Table::new_styled(&env.ui_config);
    table.set_styled_header(vec![
        "",
        "PENDING",
        "READY",
        "RUNNING",
        "BACKING-OFF",
        "SUSPENDED",
        "OLDEST NON-SUSPENDED INVOCATION",
    ]);
    for svc in services {
        let svc_status = status_map.get_service_status(&svc.name).unwrap_or(&empty);
        // Service title
        let flavor = icon_for_service_flavor(&svc.instance_type);
        let svc_title = format!("{} {}", svc.name, flavor);
        table.add_row(vec![
            Cell::new(svc_title).add_attribute(comfy_table::Attribute::Bold),
            Cell::new(""),
            Cell::new(""),
            Cell::new(""),
            Cell::new(""),
            Cell::new(""),
            Cell::new(""),
        ]);

        render_methods_status(&mut table, svc, svc_status).await?;
        table.add_row(vec!["", "", "", "", "", "", ""]);
    }
    c_println!("{}", table);
    Ok(())
}

fn render_method_state_stats(
    svc_status: &ServiceStatus,
    method: &str,
    state: EnrichedInvocationState,
) -> Cell {
    use comfy_table::Color;
    // Pending
    if let Some(state_stats) = svc_status.get_method_stats(state, method) {
        let cell = Cell::new(state_stats.num_invocations);
        let color = match state {
            EnrichedInvocationState::Unknown => Color::Magenta,
            EnrichedInvocationState::Pending if state_stats.num_invocations > 10 => Color::Yellow,
            EnrichedInvocationState::Running if state_stats.num_invocations > 0 => Color::Green,
            EnrichedInvocationState::BackingOff if state_stats.num_invocations > 5 => Color::Red,
            EnrichedInvocationState::BackingOff if state_stats.num_invocations > 0 => Color::Yellow,
            _ => comfy_table::Color::Reset,
        };
        cell.fg(color)
    } else {
        Cell::new("0")
    }
}

async fn render_methods_status(
    table: &mut Table,
    svc: ServiceMetadata,
    svc_status: &ServiceStatus,
) -> Result<()> {
    for method in svc.methods {
        let mut row = vec![];
        row.push(Cell::new(format!("  {}", &method.name)));
        // Pending
        row.push(render_method_state_stats(
            svc_status,
            &method.name,
            EnrichedInvocationState::Pending,
        ));

        // Ready
        row.push(render_method_state_stats(
            svc_status,
            &method.name,
            EnrichedInvocationState::Ready,
        ));

        // Running
        row.push(render_method_state_stats(
            svc_status,
            &method.name,
            EnrichedInvocationState::Running,
        ));

        // Backing-off
        row.push(render_method_state_stats(
            svc_status,
            &method.name,
            EnrichedInvocationState::BackingOff,
        ));

        row.push(render_method_state_stats(
            svc_status,
            &method.name,
            EnrichedInvocationState::Suspended,
        ));

        let oldest_cell = if let Some(current_method) = svc_status.get_method(&method.name) {
            if let Some((oldest_state, oldest_stats)) =
                current_method.oldest_non_suspended_invocation_state()
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
                    Styled(style, oldest_state),
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
