// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::service::HTTPQueryService;
use restate_storage_query_datafusion::context::QueryContext;
use std::net::SocketAddr;

/// # Storage query http options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "StorageQueryHttpOptions")
)]
#[cfg_attr(feature = "options_schema", schemars(default))]
pub struct Options {
    /// # Rest endpoint address
    ///
    /// Address to bind for the Storage HTTP APIs.
    http_address: SocketAddr,

    /// # Rest concurrency limit
    ///
    /// Concurrency limit for the Storage HTTP APIs.
    http_concurrency_limit: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            http_address: "0.0.0.0:9072".parse().unwrap(),
            http_concurrency_limit: 1000,
        }
    }
}

impl Options {
    pub fn build(self, query_context: QueryContext) -> HTTPQueryService {
        let Options {
            http_address,
            http_concurrency_limit,
        } = self;

        HTTPQueryService::new(http_address, http_concurrency_limit, query_context)
    }
}
