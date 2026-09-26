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
use indicatif::ProgressBar;
use serde_json::Value;

use restate_cli_util::CliContext;
use restate_cli_util::c_title;

use super::{
    Status, locked_keys_json, render_locked_keys, render_services_status, services_status_json,
};
use crate::clients::datafusion_helpers::{
    get_locked_keys, get_service_invocations, get_service_status,
};
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient};
use crate::ui::fmt::{Field, Formatter, OutputFormatter};

pub async fn run_detailed_status(
    service_name: &str,
    opts: &Status,
    metas_client: AdminClient,
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

    let is_stateful = service.ty.has_state();

    // Print summary table first.
    let status_map = get_service_status(&sql_client, vec![service_name]).await?;
    let active =
        get_service_invocations(&sql_client, service_name, opts.sample_invocations_limit).await?;
    let locked_keys = if is_stateful {
        get_locked_keys(&sql_client, [service_name])
            .await?
            .filter(|keys| !keys.is_empty())
    } else {
        None
    };
    progress.finish_and_clear();

    if CliContext::get().json_output() {
        let mut f = Formatter::new();
        f.value(
            "services",
            Field::json(services_status_json(
                std::slice::from_ref(&service),
                &status_map,
            )),
        );
        if let Some(locked_keys) = &locked_keys {
            f.value("locked_keys", Field::json(locked_keys_json(locked_keys)));
        }
        if !active.is_empty() {
            let recent = active
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<Value>, _>>()?;
            f.value("recent_invocations", Field::json(Value::Array(recent)));
        }
        return f.finish();
    }

    // Render Summary
    c_title!("📷", "Summary");
    render_services_status(vec![service], status_map).await?;

    if let Some(locked_keys) = &locked_keys {
        c_title!("📨", "Active Keys");
        render_locked_keys(
            locked_keys,
            opts.locked_keys_limit,
            opts.locked_key_held_threshold_second,
        );
    }

    // Sample of active invocations
    if !active.is_empty() {
        c_title!("🚂", "Recent Invocations");
        let mut f = Formatter::new();
        f.list("invocations", &active)?;
        f.finish()?;
    }

    Ok(())
}
