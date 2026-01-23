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

use restate_cli_util::c_title;

use super::{Status, render_locked_keys, render_services_status};
use crate::clients::datafusion_helpers::{
    get_locked_keys_status, get_service_invocations, get_service_status,
};
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient};
use crate::ui::invocations::render_invocation_compact;

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
    progress.finish_and_clear();

    // Render Summary
    c_title!("📷", "Summary");
    render_services_status(vec![service], status_map).await?;

    if is_stateful {
        let locked_keys = get_locked_keys_status(&sql_client, vec![service_name]).await?;
        if !locked_keys.is_empty() {
            c_title!("📨", "Active Keys");
            render_locked_keys(
                locked_keys,
                opts.locked_keys_limit,
                opts.locked_key_held_threshold_second,
            )
            .await?;
        }
    }

    // Sample of active invocations
    if !active.is_empty() {
        c_title!("🚂", "Recent Invocations");
        for inv in active {
            render_invocation_compact(&inv);
        }
    }

    Ok(())
}
