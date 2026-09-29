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
use chrono::{DateTime, Local};
use chrono_humanize::Tense;
use cling::prelude::*;
use comfy_table::{Cell, Table};
use itertools::Itertools;
use serde_json::{Value, json};

use restate_cli_util::c_println;
use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::ui::{duration_to_human_precise, duration_to_human_rough};
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::AdminClient;
use crate::clients::datafusion_helpers::{
    InvocationState, LockedKey, LockedKeysMap, ServiceStatus, ServiceStatusMap,
};
use crate::ui::invocations::invocation_status;
use crate::ui::service_handlers::{service_type_label, service_type_machine};

/// Invocation states reported per handler in `services status`, with their JSON keys.
const REPORTED_STATES: &[(InvocationState, &str)] = &[
    (InvocationState::Pending, "pending"),
    (InvocationState::Scheduled, "scheduled"),
    (InvocationState::Ready, "ready"),
    (InvocationState::Running, "running"),
    (InvocationState::BackingOff, "backing_off"),
    (InvocationState::Suspended, "suspended"),
    (InvocationState::Paused, "paused"),
];

/// JSON for the per-handler invocation-state counts of the given services.
pub(super) fn services_status_json(
    services: &[ServiceMetadata],
    status_map: &ServiceStatusMap,
) -> Value {
    let empty = ServiceStatus::default();
    let services: Vec<Value> = services
        .iter()
        .map(|svc| {
            let svc_status = status_map.get_service_status(&svc.name).unwrap_or(&empty);

            let mut handlers: Vec<_> = svc.handlers.values().collect();
            handlers.sort_by(|a, b| a.name.cmp(&b.name));
            let handlers: Vec<Value> = handlers
                .into_iter()
                .map(|handler| {
                    let states: serde_json::Map<String, Value> = REPORTED_STATES
                        .iter()
                        .map(|(state, key)| {
                            let count = svc_status
                                .get_handler_stats(*state, &handler.name)
                                .map(|stats| stats.num_invocations)
                                .unwrap_or(0);
                            ((*key).to_owned(), json!(count))
                        })
                        .collect();
                    let oldest = svc_status
                        .get_handler(&handler.name)
                        .and_then(|info| info.oldest_non_suspended_invocation_state())
                        .map(|(state, stats)| {
                            json!({
                                "state": state.to_string(),
                                "at": stats.oldest_at.to_rfc3339(),
                                "invocation": stats.oldest_invocation,
                            })
                        });
                    json!({
                        "handler": handler.name,
                        "states": Value::Object(states),
                        "oldest_non_suspended": oldest,
                    })
                })
                .collect();

            json!({
                "name": svc.name,
                "service_type": service_type_machine(&svc.ty),
                "handlers": handlers,
            })
        })
        .collect();
    Value::Array(services)
}

/// JSON for active (locked) keys of keyed services.
pub(super) fn locked_keys_json(locked_keys: &LockedKeysMap) -> Value {
    let services: Vec<Value> = locked_keys
        .iter()
        .map(|(service, keys)| {
            let keys: Vec<Value> = keys
                .iter()
                .map(|k| {
                    json!({
                        "key": k.key,
                        "scope": k.scope,
                        "pending": k.num_queued,
                        "invocation_holding_lock": k.acquired_by,
                        "invocation_method_holding_lock": k.handler,
                        "invocation_status": k.status.map(|s| s.to_string()),
                        "lock_acquired_at": k.acquired_at.map(|at| at.to_rfc3339()),
                    })
                })
                .collect();
            json!({ "service": service, "keys": keys })
        })
        .collect();
    Value::Array(services)
}

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
        "PAUSED",
        "OLDEST-NON-SUSPENDED-INVOCATION",
    ]);
    for svc in services {
        let svc_status = status_map.get_service_status(&svc.name).unwrap_or(&empty);
        // Service title
        let flavor = service_type_label(&svc.ty);
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
            InvocationState::Paused if state_stats.num_invocations > 0 => Color::Red,
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
    for handler in svc
        .handlers
        .values()
        .sorted_unstable_by(|a, b| a.name.cmp(&b.name))
    {
        let mut row = vec![Cell::new(format!("  {}", handler.name))];
        row.extend(
            REPORTED_STATES
                .iter()
                .map(|(state, _)| render_handler_state_stats(svc_status, &handler.name, *state)),
        );

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
/// Renders the locked keys, at most `limit_per_service` per service.
fn render_locked_keys(
    locked_keys: &LockedKeysMap,
    limit_per_service: usize,
    held_threshold_second: i64,
) {
    let now = Local::now();
    let mut table = Table::new_styled();
    table.set_styled_header(vec!["", "QUEUE", "LOCKED-BY", "HANDLER", "NOTES"]);
    for (svc_name, keys) in locked_keys {
        let svc_title = if keys.len() > limit_per_service {
            format!(
                "{svc_name} ({} active keys, showing {limit_per_service}, see --locked-keys-limit)",
                keys.len()
            )
        } else {
            format!("{svc_name} ({} active keys)", keys.len())
        };
        table.add_row(vec![
            Cell::new(svc_title).add_attribute(comfy_table::Attribute::Bold),
        ]);

        for key in keys.iter().take(limit_per_service) {
            let label = match &key.scope {
                Some(scope) => format!("  {} [scope {scope}]", key.key),
                None => format!("  {}", key.key),
            };
            let queue_color = match key.num_queued {
                0 => comfy_table::Color::Reset,
                1..=10 => comfy_table::Color::Yellow,
                _ => comfy_table::Color::Red,
            };
            let holder = match (&key.acquired_by, key.status) {
                (Some(holder), Some(status)) => format!("{holder} ({})", invocation_status(status)),
                (Some(holder), None) => holder.clone(),
                (None, _) => "-".to_owned(),
            };
            table.add_row(vec![
                Cell::new(label),
                Cell::new(key.num_queued).fg(queue_color),
                Cell::new(holder),
                Cell::new(key.handler.as_deref().unwrap_or("-")),
                Cell::new(lock_note(key, now, held_threshold_second).unwrap_or_default()),
            ]);
        }
        table.add_row(vec![""]);
    }
    c_println!("{}", table);
}

/// Heuristic hint on why a key has been locked for long.
fn lock_note(key: &LockedKey, now: DateTime<Local>, held_threshold_second: i64) -> Option<String> {
    let since = |at: Option<DateTime<Local>>| at.map(|at| now.signed_duration_since(at));
    let danger = |d| Styled(Style::Danger, duration_to_human_precise(d, Tense::Present));
    let over_threshold = |d: &chrono::Duration| d.num_seconds() > held_threshold_second;
    let held = since(key.acquired_at)
        .map(|d| format!(" Holding the lock for {}.", danger(d)))
        .unwrap_or_default();

    Some(match key.status? {
        InvocationState::Running => {
            let attempt = since(key.last_start_at).filter(over_threshold)?;
            format!(
                "Current attempt has been in-flight for {}.{held}",
                danger(attempt)
            )
        }
        InvocationState::Suspended => {
            let suspended = since(key.modified_at).filter(over_threshold)?;
            format!(
                "Suspended for {}. The lock will not be released until this invocation is complete.",
                danger(suspended)
            )
        }
        InvocationState::Paused => format!(
            "Paused. The lock will not be released until this invocation is resumed or killed.{held}"
        ),
        InvocationState::BackingOff => {
            let retries = key.retry_count.unwrap_or_default();
            let retries = if retries > 10 {
                Styled(Style::Danger, retries)
            } else {
                Styled(Style::Notice, retries)
            };
            let next_retry = key
                .next_retry_at
                .map(|at| {
                    format!(
                        " Next retry {}.",
                        duration_to_human_precise(at.signed_duration_since(now), Tense::Future)
                    )
                })
                .unwrap_or_default();
            format!("Retried {retries} time(s).{next_retry}")
        }
        _ => return None,
    })
}
