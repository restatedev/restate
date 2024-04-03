// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::net::SocketAddr;
use std::path::PathBuf;

use super::{data_dir, QueryEngineOptions};

/// # Admin server options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "AdminOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct AdminOptions {
    /// # Endpoint address
    ///
    /// Address to bind for the Admin APIs.
    pub bind_address: SocketAddr,

    /// # Concurrency limit
    ///
    /// Concurrency limit for the Admin APIs.
    pub concurrent_api_requests_limit: usize,
    pub query_engine: QueryEngineOptions,
}

impl AdminOptions {
    pub fn data_dir(&self) -> PathBuf {
        data_dir("registry")
    }
}

impl Default for AdminOptions {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9070".parse().unwrap(),
            concurrent_api_requests_limit: i64::MAX as usize,
            query_engine: Default::default(),
        }
    }
}
