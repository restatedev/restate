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
use tokio::sync::Semaphore;

use super::QueryEngineOptions;

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
    /// Concurrency limit for the Admin APIs. Max allowed value is 2305843009213693950
    pub concurrent_api_requests_limit: usize,
    pub query_engine: QueryEngineOptions,

    #[cfg(any(test, feature = "test-util"))]
    #[serde(skip, default = "super::default_arc_tmp")]
    data_dir: std::sync::Arc<tempfile::TempDir>,
}

impl AdminOptions {
    #[cfg(not(any(test, feature = "test-util")))]
    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("registry")
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.path().join("registry")
    }
}

impl Default for AdminOptions {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9070".parse().unwrap(),
            // max is limited by Tower's LoadShedLayer.
            concurrent_api_requests_limit: Semaphore::MAX_PERMITS - 1,
            query_engine: Default::default(),
            #[cfg(any(test, feature = "test-util"))]
            data_dir: super::default_arc_tmp(),
        }
    }
}
