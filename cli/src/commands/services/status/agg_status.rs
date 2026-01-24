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

use restate_cli_util::{c_error, c_title};

use super::{Status, render_locked_keys, render_services_status};
use crate::clients::datafusion_helpers::{get_locked_keys_status, get_service_status};
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient};

pub async fn run_aggregated_status(
    opts: &Status,
    metas_client: AdminClient,
    sql_client: DataFusionHttpClient,
) -> Result<()> {
    // First, let's get the service metadata
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message("Fetching services status");
    let services = metas_client
        .get_services()
        .await?
        .into_body()
        .await?
        .services;
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
        .filter(|svc| svc.ty.is_keyed())
        .cloned()
        .collect();

    let status_map = get_service_status(&sql_client, all_service_names).await?;

    let locked_keys = get_locked_keys_status(&sql_client, keyed.iter().map(|x| &x.name)).await?;
    // Render UI
    progress.finish_and_clear();
    // Render Status Table
    c_title!("📷", "Summary");
    render_services_status(services, status_map).await?;
    // Render Locked Keys
    if !locked_keys.is_empty() {
        c_title!("📨", "Active Keys");
        render_locked_keys(
            locked_keys,
            opts.locked_keys_limit,
            opts.locked_key_held_threshold_second,
        )
        .await?;
    }
    Ok(())
}
