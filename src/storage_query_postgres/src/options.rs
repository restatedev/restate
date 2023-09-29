// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::service::PostgresQueryService;
use restate_storage_query_datafusion::context::QueryContext;
use std::net::SocketAddr;

/// # Storage query postgres options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "StorageQueryPostgresOptions")
)]
pub struct Options {
    /// # Bind address
    ///
    /// The address to bind for the psql service.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_bind_address")
    )]
    pub bind_address: SocketAddr,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: Options::default_bind_address(),
        }
    }
}

impl Options {
    fn default_bind_address() -> SocketAddr {
        "0.0.0.0:9071".parse().unwrap()
    }

    pub fn build(self, query_context: QueryContext) -> PostgresQueryService {
        let Options { bind_address } = self;

        PostgresQueryService {
            bind_address,
            query_context,
        }
    }
}
