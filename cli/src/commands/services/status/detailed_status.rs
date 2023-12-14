// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{duration_to_human_precise, render_locked_keys, render_services_status, Status};
use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{
    get_locked_keys_status, get_service_invocations, get_services_status, EnrichedInvocationState,
    Invocation,
};
use crate::clients::{DataFusionHttpClient, MetaClientInterface, MetasClient};
use crate::ui::console::StyledTable;
use crate::{c_indent_table, c_indentln, c_println, c_title};

use anyhow::Result;
use chrono_humanize::Tense;
use comfy_table::Table;
use dialoguer::console::Style as DStyle;
use indicatif::ProgressBar;
use restate_meta_rest_model::services::InstanceType;

pub async fn run_detailed_status(
    env: CliEnv,
    service_name: &str,
    opts: &Status,
    metas_client: MetasClient,
    sql_client: DataFusionHttpClient,
) -> Result<()> {
    // First, let's get the service metadata
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message("Fetching service status");
    let service = metas_client
        .get_service(service_name)
        .await?
        .into_body()
        .await?;

    let is_keyed = service.instance_type == InstanceType::Keyed;

    // Print summary table first.
    let status_map = get_services_status(&sql_client, vec![service_name]).await?;
    let (inbox, active) = get_service_invocations(
        &sql_client,
        service_name,
        is_keyed,
        opts.sample_invocations_limit,
        opts.sample_invocations_limit,
    )
    .await?;
    progress.finish_and_clear();

    // Render Summary
    c_title!("📷", "Summary");
    render_services_status(&env, vec![service], status_map).await?;

    if is_keyed {
        let locked_keys = get_locked_keys_status(&sql_client, vec![service_name]).await?;
        if !locked_keys.is_empty() {
            c_title!("📨", "Active Keys");
            render_locked_keys(&env, locked_keys, opts.locked_keys_limit).await?;
        }
    }

    // Sample of active invocations
    if !active.is_empty() {
        c_title!("🚂", "Recent Active Invocations");
        for inv in active {
            render_invocation(&env, inv, false);
        }
    }
    // Sample of inbox...
    if !inbox.is_empty() {
        c_title!("🧘", "Recent Pending Invocations");
        for inv in inbox {
            render_invocation(&env, inv, true);
        }
    }

    Ok(())
}

// [2023-12-14 15:38:52.500 +00:00] rIEqK14GCdkAYxo-wzTfrK2e6tJssIrtQ CheckoutProcess::checkout
//    Status:      backing-off  (Retried 67 time(s). Next retry in in 9 seconds and 616 ms))
//    Deployment:  bG9jYWxob3N0OjkwODEv
//    Error:       [Internal] other client error: error trying to connect: tcp connect error: Connection refused (os error 61)
fn render_invocation(env: &CliEnv, invocation: Invocation, status_implied: bool) {
    // Explicit styling
    let yellow = DStyle::new().yellow();
    let red = DStyle::new().red();
    let blue = DStyle::new().blue();
    let green = DStyle::new().green();
    let normal = DStyle::new();
    let bold = DStyle::new().bold();
    let italic = DStyle::new().italic();
    let dim = DStyle::new().dim();

    // Unkeyed -> [2023-12-14 15:38:52.500 +00:00] rIEqK14GCdkAYxo-wzTfrK2e6tJssIrtQ CheckoutProcess::checkout
    // Keyed   -> [2023-12-13 12:04:54.902 +00:00] g4Ej8NLd-aYAYxjEM31dhOLXpCi5azJNA [TicketDb @ my-user-200]::trySomethingNew
    let date_style = DStyle::new().dim();
    let created_at = date_style.apply_to(format!("[{}]", invocation.created_at));

    let svc = if let Some(key) = &invocation.key {
        format!(
            "[{} {} {}]",
            invocation.service,
            dim.apply_to("@"),
            dim.apply_to(key)
        )
    } else {
        invocation.service.to_string()
    };
    c_indentln!(
        1,
        "{} {} {}{}{}",
        created_at,
        DStyle::new().bold().apply_to(&invocation.id),
        svc,
        dim.apply_to("::"),
        invocation.method
    );

    let mut table = Table::new_styled(&env.ui_config);
    if !status_implied {
        // Status: backing-off (Invocation is fail-looping, retried 1198 time(s). Next retry in 5 seconds and 78 ms) (if not pending....)
        let status_style = match invocation.status {
            EnrichedInvocationState::Unknown => &red,
            EnrichedInvocationState::Pending => &yellow,
            EnrichedInvocationState::Ready => &blue,
            EnrichedInvocationState::Running => &green,
            EnrichedInvocationState::Suspended => &dim,
            EnrichedInvocationState::BackingOff => &red,
        };
        let status_msg = get_status_note(&invocation);
        let status = format!(
            "{} {}",
            status_style.apply_to(invocation.status),
            status_msg
        );
        table.add_kv_row("Status:", status);
    }

    // Invoked by: TicketDb p4DGRWa7OTJwAYxelm96fFWSV9woYc0MLQ
    if let Some(invoked_by_id) = &invocation.invoked_by_id {
        let invoked_by_msg = format!(
            "{} {}",
            invocation
                .invoked_by_service
                .map(|x| italic.apply_to(x))
                .unwrap_or_else(|| red.apply_to("<UNKNOWN>".to_owned())),
            italic.apply_to(invoked_by_id),
        );
        table.add_kv_row("Invoked By:", invoked_by_msg);
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
                format!("[{}]", bold.apply_to("required"))
            // TODO: check if deployment is missing
            } else {
                normal.apply_to("").to_string()
            },
        );
        table.add_kv_row("Deployment:", deployment_msg);
    }

    // Error: [Internal] other client error: error trying to connect: tcp connect error: Connection refused (os error 61)
    if invocation.status == EnrichedInvocationState::BackingOff {
        if let Some(error) = &invocation.last_failure_message {
            table.add_kv_row("Error:", red.apply_to(error));
        }
    }

    c_indent_table!(2, table);
    c_println!();
}

fn get_status_note(invocation: &Invocation) -> String {
    let yellow = DStyle::new().yellow();
    let red = DStyle::new().red();
    let normal = DStyle::new();
    let mut msg = String::new();

    match invocation.status {
        EnrichedInvocationState::Running => {
            // active attempt duration
            if let Some(attempt_duration) = invocation.current_attempt_duration {
                let dur = duration_to_human_precise(attempt_duration, Tense::Present);
                let dur = if attempt_duration.num_seconds() > 5 {
                    // too long...
                    red.apply_to(dur)
                } else {
                    normal.apply_to(dur)
                };
                msg.push_str(&format!(" ({})", dur));
            }
        }
        EnrichedInvocationState::Suspended => {
            if let Some(suspend_duration) = invocation.duration_in_state {
                // its keyed and in suspension
                if suspend_duration.num_seconds() > 5 && invocation.key.is_some() {
                    let dur = duration_to_human_precise(suspend_duration, Tense::Present);
                    let dur = if suspend_duration.num_seconds() > 5 {
                        // too long...
                        red.apply_to(dur)
                    } else {
                        normal.apply_to(dur)
                    };
                    msg.push_str(&format!(
                        " ({}. The key will not be released until this invocation is complete)",
                        dur
                    ));
                }
            }
        }
        EnrichedInvocationState::BackingOff => {
            let num_retries = invocation.num_retries.unwrap_or(0);
            let num_retries = if num_retries > 10 {
                red.apply_to(num_retries)
            } else {
                yellow.apply_to(num_retries)
            };

            msg.push_str(&format!(" (Retried {} time(s).", num_retries,));

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
