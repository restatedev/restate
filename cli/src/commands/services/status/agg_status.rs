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

use serde_json::json;

use restate_cli_util::c_error;

use super::{Status, render_locked_keys, render_services_status};
use crate::clients::datafusion_helpers::{get_locked_keys, get_service_status};
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient};
use crate::ui::fmt::{Field, OutputFormatter};

pub async fn run_aggregated_status(
    f: &mut impl OutputFormatter,
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
        f.value("services", Field::with_display(json!([]), ""));
        return Ok(());
    }

    let all_service_names: Vec<_> = services.iter().map(|x| x.name.clone()).collect();

    let keyed: Vec<_> = services
        .iter()
        .filter(|svc| svc.ty.is_keyed())
        .cloned()
        .collect();

    let status_map = get_service_status(&sql_client, all_service_names).await?;

    let locked_keys = get_locked_keys(&sql_client, keyed.iter().map(|x| &x.name))
        .await?
        .filter(|keys| !keys.is_empty());
    // Render UI
    progress.finish_and_clear();

    render_services_status(f, &services, &status_map);
    if let Some(keys) = &locked_keys {
        render_locked_keys(f, keys, opts);
    }
    Ok(())
}
