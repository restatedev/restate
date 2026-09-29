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

use super::{Status, render_locked_keys, render_services_status};
use crate::clients::datafusion_helpers::{
    get_locked_keys, get_service_invocations, get_service_status,
};
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient};
use crate::ui::fmt::{IfEmpty, OutputFormatter};

pub async fn run_detailed_status(
    f: &mut impl OutputFormatter,
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

    render_services_status(f, std::slice::from_ref(&service), &status_map);
    if let Some(keys) = &locked_keys {
        render_locked_keys(f, keys, opts);
    }
    // Sample of active invocations
    if !active.is_empty() {
        f.title("🚂", "Recent Invocations");
        f.list("recent_invocations", &active, IfEmpty::Nothing)?;
    }
    Ok(())
}
