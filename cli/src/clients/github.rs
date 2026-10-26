// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Minimal client for the public GitHub releases API.

use serde::Deserialize;

use crate::build_info;

#[derive(Debug, Clone, Deserialize)]
pub struct Release {
    pub tag_name: String,
    pub html_url: String,
    pub published_at: Option<String>,
    #[serde(default)]
    pub assets: Vec<Asset>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Asset {
    pub name: String,
    pub browser_download_url: String,
}

#[derive(Clone)]
pub struct GithubClient {
    inner: reqwest::Client,
}

impl GithubClient {
    pub fn new() -> reqwest::Result<Self> {
        let inner = reqwest::Client::builder()
            .user_agent(format!(
                "{}/{} {}-{}",
                env!("CARGO_PKG_NAME"),
                build_info::RESTATE_CLI_VERSION,
                std::env::consts::OS,
                std::env::consts::ARCH,
            ))
            .https_only(true)
            .build()?;
        Ok(Self { inner })
    }

    pub async fn latest_release(&self, owner: &str, repo: &str) -> reqwest::Result<Release> {
        self.inner
            .get(format!(
                "https://api.github.com/repos/{owner}/{repo}/releases/latest"
            ))
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    /// Downloads the given release asset. Use [`reqwest::Response::bytes_stream`] to consume it.
    pub async fn download_asset(&self, asset: &Asset) -> reqwest::Result<reqwest::Response> {
        self.inner
            .get(&asset.browser_download_url)
            .send()
            .await?
            .error_for_status()
    }
}
