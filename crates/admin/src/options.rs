// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

/// # Admin server options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "AdminOptions", default))]
#[builder(default)]
pub struct Options {
    #[serde(flatten)]
    pub meta: restate_meta::Options,
    /// # Endpoint address
    ///
    /// Address to bind for the Admin APIs.
    pub bind_address: SocketAddr,

    /// # Concurrency limit
    ///
    /// Concurrency limit for the Admin APIs.
    pub concurrent_api_requests_limit: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            meta: Default::default(),
            bind_address: "0.0.0.0:9070".parse().unwrap(),
            concurrent_api_requests_limit: 1000,
        }
    }
}
