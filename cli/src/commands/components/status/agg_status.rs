// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{render_components_status, render_locked_keys, Status};
use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{get_components_status, get_locked_keys_status};
use crate::clients::{DataFusionHttpClient, MetaClientInterface, MetasClient};
use crate::{c_error, c_title};

use anyhow::Result;
use indicatif::ProgressBar;
use restate_meta_rest_model::components::ComponentType;

pub async fn run_aggregated_status(
    env: &CliEnv,
    opts: &Status,
    metas_client: MetasClient,
    sql_client: DataFusionHttpClient,
) -> Result<()> {
    // First, let's get the service metadata
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message("Fetching components status");
    let components = metas_client
        .get_components()
        .await?
        .into_body()
        .await?
        .components;
    if components.is_empty() {
        progress.finish_and_clear();
        c_error!(
            "No components were found! Components are added by registering deployments with 'restate dep register'"
        );
        return Ok(());
    }

    let all_component_names: Vec<_> = components.iter().map(|x| x.name.clone()).collect();

    let keyed: Vec<_> = components
        .iter()
        .filter(|svc| svc.ty == ComponentType::VirtualObject)
        .cloned()
        .collect();

    let status_map = get_components_status(&sql_client, all_component_names).await?;

    let locked_keys = get_locked_keys_status(&sql_client, keyed.iter().map(|x| &x.name)).await?;
    // Render UI
    progress.finish_and_clear();
    // Render Status Table
    c_title!("📷", "Summary");
    render_components_status(env, components, status_map).await?;
    // Render Locked Keys
    if !locked_keys.is_empty() {
        c_title!("📨", "Active Keys");
        render_locked_keys(env, locked_keys, opts.locked_keys_limit).await?;
    }
    Ok(())
}
