// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::debug;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct DiscoveredAuthConfig {
    pub provider: String,
    pub client_id: String,
    pub login_base_url: Option<Url>,
}

pub async fn fetch_auth_config(discovery_url: &Url) -> Result<DiscoveredAuthConfig> {
    debug!(%discovery_url, "Fetching authentication config");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("Failed to build HTTP client for config discovery")?;

    let response = client
        .get(discovery_url.clone())
        .send()
        .await
        .with_context(|| {
            format!("Failed to discover authentication configuration at {discovery_url}")
        })?;

    if !response.status().is_success() {
        anyhow::bail!(
            "Auth config discovery returned non-success status {} from {discovery_url}",
            response.status()
        );
    }

    response
        .json::<DiscoveredAuthConfig>()
        .await
        .with_context(|| format!("Failed to parse auth config from {discovery_url}"))
}
