// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_serde_util::{ByteCount, NonZeroByteCount};
use tracing::warn;

use crate::logs::metadata::ProviderKind;
use crate::retries::RetryPolicy;

use super::{CommonOptions, RocksDbOptions, RocksDbOptionsBuilder};

/// # Bifrost options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "BifrostOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct BifrostOptions {
    /// # The default kind of loglet to be used
    pub default_provider: ProviderKind,
    /// An opaque string that gets passed to the loglet provider to seed the creation of new
    /// loglets.
    pub default_provider_config: Option<String>,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    /// Configuration of local loglet provider
    pub local: LocalLogletOptions,
    #[cfg(feature = "replicated-loglet")]
    /// [IN DEVELOPMENT]
    /// Configuration of replicated loglet provider
    pub replicated_loglet: ReplicatedLogletOptions,

    /// # Read retry policy
    ///
    /// Retry policy to use when bifrost waits for reconfiguration to complete during
    /// read operations
    pub read_retry_policy: RetryPolicy,

    /// # Seal retry interval
    ///
    /// Interval to wait between retries of loglet seal failures
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub seal_retry_interval: humantime::Duration,

    /// # Append retry minimum interval
    ///
    /// Minimum retry duration used by the exponential backoff mechanism for bifrost appends.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    append_retry_min_interval: humantime::Duration,
    /// # Append retry maximum interval
    ///
    /// Maximum retry duration used by the exponential backoff mechanism for bifrost appends.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    append_retry_max_interval: humantime::Duration,

    /// # In-memory RecordCache memory limit
    ///
    /// Optional size of record cache in bytes.
    /// If set to 0, record cache will be disabled.
    /// Defaults: 20M
    #[cfg_attr(feature = "schemars", schemars(with = "ByteCount"))]
    pub record_cache_memory_size: ByteCount,
}

impl BifrostOptions {
    pub fn default_provider_config(&self) -> Option<&str> {
        self.default_provider_config.as_deref()
    }

    pub fn append_retry_policy(&self) -> RetryPolicy {
        // Appends are retried with exponential backoff, forever.
        RetryPolicy::exponential(
            self.append_retry_min_interval.into(),
            2.0,
            None,
            Some(self.append_retry_max_interval.into()),
        )
    }
}

impl Default for BifrostOptions {
    fn default() -> Self {
        Self {
            default_provider: ProviderKind::Local,
            default_provider_config: None,
            #[cfg(feature = "replicated-loglet")]
            replicated_loglet: ReplicatedLogletOptions::default(),
            local: LocalLogletOptions::default(),
            read_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(50),
                2.0,
                Some(50),
                Some(Duration::from_secs(1)),
            ),
            append_retry_min_interval: Duration::from_millis(10).into(),
            append_retry_max_interval: Duration::from_secs(1).into(),
            seal_retry_interval: Duration::from_secs(2).into(),
            record_cache_memory_size: 20_000_000u64.into(), // 20MB
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "LocalLoglet", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct LocalLogletOptions {
    #[serde(flatten)]
    pub rocksdb: RocksDbOptions,

    /// The memory budget for rocksdb memtables in bytes
    ///
    /// If this value is set, it overrides the ratio defined in `rocksdb-memory-ratio`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_memory_budget: Option<NonZeroUsize>,

    /// The memory budget for rocksdb memtables as ratio
    ///
    /// This defines the total memory for rocksdb as a ratio of all memory available to the loglet
    /// (See `rocksdb-total-memtables-ratio` in common).
    rocksdb_memory_ratio: f32,

    /// Disable fsync of WAL on every batch
    rocksdb_disable_wal_fsync: bool,

    /// Whether to perform commits in background IO thread pools eagerly or not
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub always_commit_in_background: bool,

    /// Trigger a commit when the batch size exceeds this threshold.
    ///
    /// Set to 0 or 1 to commit the write batch on every command.
    pub writer_batch_commit_count: usize,

    /// Trigger a commit when the time since the last commit exceeds this threshold.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub writer_batch_commit_duration: humantime::Duration,
}

impl LocalLogletOptions {
    pub fn apply_common(&mut self, common: &CommonOptions) {
        self.rocksdb.apply_common(&common.rocksdb);
        if self.rocksdb_memory_budget.is_none() {
            self.rocksdb_memory_budget = Some(
                // 1MB minimum
                NonZeroUsize::new(
                    (common.rocksdb_safe_total_memtables_size() as f64
                        * self.rocksdb_memory_ratio as f64)
                        .floor()
                        .max(1024.0 * 1024.0) as usize,
                )
                .unwrap(),
            );
        }
    }

    pub fn rocksdb_disable_wal_fsync(&self) -> bool {
        self.rocksdb_disable_wal_fsync
    }

    pub fn rocksdb_memory_budget(&self) -> usize {
        self.rocksdb_memory_budget
            .unwrap_or_else(|| {
                warn!("LocalLoglet rocksdb_memory_budget is not set, defaulting to 1MB");
                // 1MB minimum
                NonZeroUsize::new(1024 * 1024).unwrap()
            })
            .get()
    }

    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("local-loglet")
    }
}

impl Default for LocalLogletOptions {
    fn default() -> Self {
        let rocksdb = RocksDbOptionsBuilder::default()
            .rocksdb_disable_wal(Some(false))
            .build()
            .unwrap();
        Self {
            rocksdb,
            // set by apply_common in runtime
            rocksdb_memory_budget: None,
            rocksdb_memory_ratio: 0.5,
            writer_batch_commit_count: 5000,
            writer_batch_commit_duration: Duration::ZERO.into(),
            rocksdb_disable_wal_fsync: false,
            always_commit_in_background: false,
        }
    }
}

#[cfg(feature = "replicated-loglet")]
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "ReplicatedLoglet", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct ReplicatedLogletOptions {
    /// Maximum number of inflight records sequencer can accept
    ///
    /// Once this maximum is hit, sequencer will induce back pressure
    /// on clients. This controls the total number of records regardless of how many batches.
    ///
    /// Note that this will be increased to fit the biggest batch of records being enqueued.
    pub maximum_inflight_records: NonZeroUsize,

    /// Sequencer backoff strategy
    ///
    /// Backoff introduced when sequencer fail to find a suitable spread
    /// of log servers
    pub sequencer_backoff_strategy: RetryPolicy,

    /// Log Server RPC timeout
    ///
    /// Timeout waiting on log server response
    pub log_server_rpc_timeout: Duration,

    /// log_server RPC retry policy
    ///
    /// Retry policy for log server RPCs
    pub log_server_retry_policy: RetryPolicy,
}

#[cfg(feature = "replicated-loglet")]
impl Default for ReplicatedLogletOptions {
    fn default() -> Self {
        Self {
            maximum_inflight_records: NonZeroUsize::new(1000).unwrap(),

            sequencer_backoff_strategy: RetryPolicy::exponential(
                Duration::from_millis(100),
                2.0,
                None,
                Some(Duration::from_millis(2000)),
            ),
            log_server_rpc_timeout: Duration::from_millis(2000),
            log_server_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                Some(10),
                Some(Duration::from_millis(2000)),
            ),
        }
    }
}
