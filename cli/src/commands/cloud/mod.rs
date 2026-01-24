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
use tracing::{debug, warn};
use url::Url;

#[derive(Clone, Debug)]
pub enum IdentityProvider {
    Cognito {
        client_id: String,
        login_base_url: Url,
    },
    WorkOS {
        client_id: String,
    },
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct CloudConfig {
    #[serde(flatten)]
    pub environment_info: Option<EnvironmentInfo>, // Set on a profile on login
    pub api_base_url: Option<Url>,
    // Optional manual overrides
    pub provider: Option<String>,
    pub client_id: Option<String>,
    pub login_base_url: Option<Url>,
    pub redirect_ports: Option<Vec<u16>>,
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

pub const DEFAULT_REDIRECT_PORTS: &[u16] = &[33912, 44643, 47576, 54788, 61844];
pub const DEFAULT_API_BASE_URL: &str = "https://api.us.restate.cloud";

// Legacy fallback defaults (used when discovery fails)
const LEGACY_CLIENT_ID: &str = "5q3dsdnrr5r400jvibd8d3k66l";
const LEGACY_LOGIN_BASE_URL: &str = "https://auth.restate.cloud";

impl CloudConfig {
    pub fn redirect_ports(&self) -> &[u16] {
        self.redirect_ports
            .as_deref()
            .unwrap_or(DEFAULT_REDIRECT_PORTS)
    }

    pub fn api_base_url(&self) -> Url {
        self.api_base_url
            .clone()
            .unwrap_or_else(|| Url::parse(DEFAULT_API_BASE_URL).expect("default API URL is valid"))
    }

    pub async fn resolve_identity_provider(&self) -> anyhow::Result<IdentityProvider> {
        if let Some(provider) = &self.provider {
            return self.resolve_explicit_provider(provider);
        }

        let discovery_url = self
            .api_base_url()
            .join(".auth-config")
            .expect("valid auth-config path");

        match config_discovery::fetch_auth_config(&discovery_url).await {
            Ok(config) => {
                debug!(
                    "Discovered auth config: provider={}, client_id={}",
                    config.provider, config.client_id
                );
                self.resolve_discovered_provider(&config)
            }
            Err(e) => {
                warn!("Auth config discovery failed, using legacy defaults: {}", e);
                Ok(self.resolve_legacy_provider())
            }
        }
    }

    fn resolve_explicit_provider(&self, provider: &str) -> anyhow::Result<IdentityProvider> {
        let client_id = self.client_id.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Explicit provider config requires client_id to be set")
        })?;

        match provider {
            "cognito" => {
                let login_base_url = self.login_base_url.clone().ok_or_else(|| {
                    anyhow::anyhow!("Cognito provider requires login_base_url to be set")
                })?;
                debug!(
                    "Using explicit Cognito provider: client_id={}, login_base_url={}",
                    client_id, login_base_url
                );
                Ok(IdentityProvider::Cognito {
                    client_id: client_id.to_string(),
                    login_base_url,
                })
            }
            "workos" => {
                debug!("Using explicit WorkOS provider: client_id={}", client_id);
                Ok(IdentityProvider::WorkOS {
                    client_id: client_id.to_string(),
                })
            }
            other => {
                anyhow::bail!(
                    "Unknown identity provider: {other}. Expected 'cognito' or 'workos'."
                );
            }
        }
    }

    fn resolve_discovered_provider(
        &self,
        config: &config_discovery::DiscoveredAuthConfig,
    ) -> anyhow::Result<IdentityProvider> {
        match config.provider.as_str() {
            "cognito" => {
                let login_base_url = config.login_base_url.clone().ok_or_else(|| {
                    anyhow::anyhow!("Discovered Cognito provider missing login_base_url")
                })?;
                debug!(
                    "Using discovered Cognito provider: client_id={}, login_base_url={}",
                    config.client_id, login_base_url
                );
                Ok(IdentityProvider::Cognito {
                    client_id: config.client_id.clone(),
                    login_base_url,
                })
            }
            "workos" => {
                debug!(
                    "Using discovered WorkOS provider: client_id={}",
                    config.client_id
                );
                Ok(IdentityProvider::WorkOS {
                    client_id: config.client_id.clone(),
                })
            }
            other => {
                anyhow::bail!(
                    "Unknown identity provider from discovery: {other}. Expected 'cognito' or 'workos'."
                );
            }
        }
    }

    fn resolve_legacy_provider(&self) -> IdentityProvider {
        debug!(
            "Using legacy Cognito provider: client_id={}, login_base_url={}",
            LEGACY_CLIENT_ID, LEGACY_LOGIN_BASE_URL
        );
        IdentityProvider::Cognito {
            client_id: LEGACY_CLIENT_ID.to_string(),
            login_base_url: Url::parse(LEGACY_LOGIN_BASE_URL).expect("valid legacy login URL"),
        }
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
