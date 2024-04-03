// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// # Storage query engine options
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "QueryEngineOptions"))]
#[cfg_attr(feature = "schemars", schemars(default))]
#[serde(rename_all = "kebab-case")]
pub struct QueryEngineOptions {
    /// # Memory limit
    ///
    /// The total memory in bytes that can be used to preform sql queries
    pub memory_limit: Option<usize>,

    /// # Temp folder to use for spill
    ///
    /// The path to spill to
    pub tmp_dir: Option<String>,

    /// # Default query parallelism
    ///
    /// The number of parallel partitions to use for a query execution
    pub query_parallelism: Option<usize>,

    /// # Pgsql Bind address
    ///
    /// The address to bind for the psql service.
    pub pgsql_bind_address: SocketAddr,
}

impl Default for QueryEngineOptions {
    fn default() -> Self {
        Self {
            memory_limit: None,
            tmp_dir: None,
            query_parallelism: None,
            pgsql_bind_address: "0.0.0.0:9071".parse().unwrap(),
        }
    }
}
