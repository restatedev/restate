// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{render_locked_keys, render_services_status, Status};
use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{get_locked_keys_status, get_services_status};
use crate::clients::{DataFusionHttpClient, MetaClientInterface, MetasClient};
use crate::{c_error, c_title};

use restate_meta_rest_model::services::InstanceType;

use anyhow::Result;
use indicatif::ProgressBar;

pub async fn run_aggregated_status(
    env: CliEnv,
    opts: &Status,
    metas_client: MetasClient,
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
        .filter(|svc| svc.instance_type == InstanceType::Keyed)
        .cloned()
        .collect();

    let status_map = get_services_status(&sql_client, all_service_names).await?;

    let locked_keys = get_locked_keys_status(&sql_client, keyed.iter().map(|x| &x.name)).await?;
    // Render UI
    progress.finish_and_clear();
    // Render Status Table
    c_title!("📷", "Summary");
    render_services_status(&env, services, status_map).await?;
    // Render Locked Keys
    if !locked_keys.is_empty() {
        c_title!("📨", "Active Keys");
        render_locked_keys(&env, locked_keys, opts.locked_keys_limit).await?;
    }
    Ok(())
}
