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

use restate_util_bytecount::NonZeroByteCount;
use restate_util_time::NonZeroFriendlyDuration;

use crate::config::throttling::ThrottlingOptions;

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

    /// # Query rate limiting
    ///
    /// Per-node rate limiting options for datafusion query execution. When exceeded,
    /// the query will fail with TOO_MANY_REQUESTS. If unset, no rate limiting is applied.
    ///
    /// Since v1.7.1
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limiting: Option<ThrottlingOptions>,

    /// # Batch compression
    ///
    /// Compression of the record batches that the experimental query engine V2 sends from one
    /// node to another: `none`, `lz4` or `zstd`. A node compresses the batches it sends and
    /// reads batches compressed either way, so nodes with different settings can talk to each
    /// other. Lz4 uses little CPU; Zstd sends fewer bytes at a higher CPU cost.
    ///
    /// Batches a node sends to itself are never compressed. Nor are any batches while
    /// `networking.disable-compression` is `false`, since the connection compresses them
    /// already.
    ///
    /// Default: `none`
    ///
    /// Applies to the query tasks installed after a change.
    ///
    /// Since v1.8.2
    #[serde(default, skip_serializing_if = "is_default_batch_compression")]
    pub batch_compression: QueryBatchCompression,

    /// # Index catalog refresh interval
    ///
    /// How often an admin node checks which indexes queries can read, from
    /// `restate.cluster.registry`. The `/version` endpoint reports the result: for
    /// example, `query_engine.stats_tables` turns false when a partition's leader,
    /// which statistics reads go to, answers and lacks a complete status gauge,
    /// and true again once no such leader is left. Partitions without a known
    /// leader, and leaders that are down, don't answer or have no open store, don't
    /// count.
    /// A check that fails as a whole, or takes longer than this interval, keeps
    /// the last result and is retried sooner: after 5 seconds, then twice as
    /// long each time, up to this interval. Where
    /// queries don't read indexes, nothing is checked and the result is `false`.
    ///
    /// Default: 1 minute
    ///
    /// Since v1.8.2
    pub index_catalog_refresh_interval: NonZeroFriendlyDuration,
}

fn is_default_batch_compression(compression: &QueryBatchCompression) -> bool {
    *compression == QueryBatchCompression::default()
}

/// # Query batch compression
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum QueryBatchCompression {
    /// No compression.
    #[default]
    None,
    /// LZ4: fast, with a moderate compression ratio.
    Lz4,
    /// Zstandard: slower, with a higher compression ratio.
    Zstd,
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
            rate_limiting: None,
            batch_compression: QueryBatchCompression::default(),
            index_catalog_refresh_interval: NonZeroFriendlyDuration::from_secs_unchecked(60),
        }
    }
}
