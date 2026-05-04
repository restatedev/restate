// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod config_discovery;
mod environments;
mod login;

use base64::Engine;
use cling::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::debug;
use url::Url;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct CloudConfig {
    #[serde(flatten)]
    pub environment_info: Option<EnvironmentInfo>, // Set on a profile on login
    pub api_base_url: Option<Url>,
    pub client_id: Option<String>,
    #[serde(flatten)]
    pub credentials: Option<Credentials>, // Set globally on login
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    pub account_id: String,
    pub environment_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Credentials {
    access_token: String,
}

pub const DEFAULT_API_BASE_URL: &str = "https://api.us.restate.cloud";
pub const DEFAULT_WORKOS_CLIENT_ID: &str = "client_01K8ZW3DSQK9D7PW0DT4DBRVSC";

impl CloudConfig {
    pub fn api_base_url(&self) -> Url {
        self.api_base_url
            .clone()
            .unwrap_or_else(|| Url::parse(DEFAULT_API_BASE_URL).expect("default API URL is valid"))
    }

    pub async fn resolve_workos_client_id(&self) -> anyhow::Result<String> {
        if let Some(client_id) = &self.client_id {
            debug!("Using configured WorkOS client_id={}", client_id);
            return Ok(client_id.clone());
        }

        let api_base_url = self.api_base_url();
        let default_api_base_url =
            Url::parse(DEFAULT_API_BASE_URL).expect("default API URL is valid");
        if api_base_url == default_api_base_url {
            debug!("Using built-in prod WorkOS client_id");
            return Ok(DEFAULT_WORKOS_CLIENT_ID.to_string());
        }

        let discovery_url = api_base_url
            .join(".auth-config")
            .expect("valid auth-config path");

        let config = config_discovery::fetch_auth_config(&discovery_url).await?;
        debug!("Discovered WorkOS client_id={}", config.client_id);
        Ok(config.client_id)
    }
}

impl Credentials {
    pub fn expiry(&self) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
        let claims = match self.access_token.split('.').nth(1).and_then(
            |claims: &str| -> Option<TokenClaims> {
                base64::prelude::BASE64_URL_SAFE_NO_PAD
                    .decode(claims)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            },
        ) {
            Some(claims) => claims,
            None => {
                return Err(anyhow::anyhow!(
                    "Restate Cloud credentials are invalid; first run `restate cloud login`"
                ));
            }
        };

        chrono::DateTime::from_timestamp(claims.exp, 0).ok_or(anyhow::anyhow!(
            "Restate Cloud credentials are invalid; first run `restate cloud login`"
        ))
    }

    pub fn access_token(&self) -> anyhow::Result<&str> {
        if self.expiry()? > chrono::Local::now() {
            Ok(&self.access_token)
        } else {
            Err(anyhow::anyhow!(
                "Restate Cloud credentials have expired; first run `restate cloud login`"
            ))
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TokenClaims {
    exp: i64,
}

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "c")]
pub enum Cloud {
    /// Authenticate to Restate Cloud
    Login(login::Login),
    /// Manage Restate Cloud Environments
    #[clap(subcommand)]
    Environments(environments::Environments),
}
