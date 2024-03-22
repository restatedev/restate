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
use crate::c_title;
use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{
    get_component_invocations, get_components_status, get_locked_keys_status,
};
use crate::clients::{DataFusionHttpClient, MetaClientInterface, MetasClient};
use crate::ui::invocations::render_invocation_compact;

use anyhow::Result;
use indicatif::ProgressBar;
use restate_meta_rest_model::components::ComponentType;

pub async fn run_detailed_status(
    env: &CliEnv,
    component_name: &str,
    opts: &Status,
    metas_client: MetasClient,
    sql_client: DataFusionHttpClient,
) -> Result<()> {
    // First, let's get the component metadata
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message("Fetching component status");
    let component = metas_client
        .get_component(component_name)
        .await?
        .into_body()
        .await?;

    let is_object = component.ty == ComponentType::VirtualObject;

    // Print summary table first.
    let status_map = get_components_status(&sql_client, vec![component_name]).await?;
    let (inbox, active) = get_component_invocations(
        &sql_client,
        component_name,
        opts.sample_invocations_limit,
        opts.sample_invocations_limit,
    )
    .await?;
    progress.finish_and_clear();

    // Render Summary
    c_title!("📷", "Summary");
    render_components_status(env, vec![component], status_map).await?;

    if is_object {
        let locked_keys = get_locked_keys_status(&sql_client, vec![component_name]).await?;
        if !locked_keys.is_empty() {
            c_title!("📨", "Active Keys");
            render_locked_keys(env, locked_keys, opts.locked_keys_limit).await?;
        }
    }

    // Sample of active invocations
    if !active.is_empty() {
        c_title!("🚂", "Recent Active Invocations");
        for inv in active {
            render_invocation_compact(env, &inv);
        }
    }
    // Sample of inbox...
    if !inbox.is_empty() {
        c_title!("🧘", "Recent Pending Invocations");
        for inv in inbox {
            render_invocation_compact(env, &inv);
        }
    }

    Ok(())
}
