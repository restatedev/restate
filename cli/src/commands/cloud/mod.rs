// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
    pub access_token: String,
    pub access_token_expiry: u64, // unix seconds
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            environment_info: None,
            // TODO; move to production URLs
            api_base_url: Url::parse("https://api.dev.restate.cloud").unwrap(),
            login_base_url: Url::parse(
                "https://restate-cloud-signup-test.auth.eu-central-1.amazoncognito.com",
            )
            .unwrap(),
            client_id: "1qlr34bcko4s77sub02g6t4baj".into(),
            credentials: None,
        }
    }
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
