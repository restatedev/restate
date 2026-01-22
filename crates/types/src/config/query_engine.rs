// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
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
    /// The degree of parallelism to use for query execution (Defaults to the number of available cores).
    query_parallelism: Option<NonZeroUsize>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub datafusion_options: HashMap<String, String>,
}

impl QueryEngineOptions {
    pub fn query_parallelism(&self) -> Option<usize> {
        self.query_parallelism.map(Into::into)
    }
}

impl Default for QueryEngineOptions {
    fn default() -> Self {
        #[allow(deprecated)]
        Self {
            memory_size: NonZeroUsize::new(1024 * 1024 * 1024).unwrap(), // 1GiB
            tmp_dir: None,
            query_parallelism: None,
            datafusion_options: HashMap::new(),
        }
    }
}
