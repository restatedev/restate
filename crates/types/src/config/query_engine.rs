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
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_serde_util::NonZeroByteCount;

/// # Storage query engine options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "QueryEngineOptions"))]
#[cfg_attr(feature = "schemars", schemars(default))]
#[serde(rename_all = "kebab-case")]
pub struct QueryEngineOptions {
    /// # Memory size limit
    ///
    /// The total memory in bytes that can be used to preform sql queries
    #[cfg_attr(feature = "schemars", schemars(with = "NonZeroByteCount"))]
    #[serde_as(as = "NonZeroByteCount")]
    pub memory_size: NonZeroUsize,

    /// # Temp folder to use for spill
    ///
    /// The path to spill to
    pub tmp_dir: Option<String>,

    /// # Default query parallelism
    ///
    /// The number of parallel partitions to use for a query execution
    query_parallelism: Option<NonZeroUsize>,

    /// # Pgsql Bind address
    ///
    /// The address to bind for the psql service.
    pub pgsql_bind_address: SocketAddr,
}

impl QueryEngineOptions {
    pub fn query_parallelism(&self) -> Option<usize> {
        self.query_parallelism.map(Into::into)
    }
}
impl Default for QueryEngineOptions {
    fn default() -> Self {
        Self {
            memory_size: NonZeroUsize::new(4_000_000_000).unwrap(), // 4GB
            tmp_dir: None,
            query_parallelism: None,
            pgsql_bind_address: "0.0.0.0:9071".parse().unwrap(),
        }
    }
}
