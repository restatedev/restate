// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod environments;
mod login;

use base64::Engine;
use cling::prelude::*;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Clone, Serialize, Deserialize)]
pub struct CloudConfig {
    #[serde(flatten)]
    pub environment_info: Option<EnvironmentInfo>, // Set on a profile on login
    pub api_base_url: Url,
    pub login_base_url: Url,
    pub client_id: String,
    pub redirect_ports: Vec<u16>,
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

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            environment_info: None,
            api_base_url: Url::parse("https://api.us.restate.cloud").unwrap(),
            login_base_url: Url::parse("https://auth.restate.cloud").unwrap(),
            client_id: "5q3dsdnrr5r400jvibd8d3k66l".into(),
            redirect_ports: vec![33912, 44643, 47576, 54788, 61844],
            credentials: None,
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
