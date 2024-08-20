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
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;
use tracing::warn;

use restate_serde_util::NonZeroByteCount;

use super::{CommonOptions, RocksDbOptions, RocksDbOptionsBuilder};
use crate::retries::RetryPolicy;

/// # Worker options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "WorkerOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct WorkerOptions {
    /// # Internal queue for partition processor communication
    internal_queue_length: NonZeroUsize,

    /// # Num timers in memory limit
    ///
    /// The number of timers in memory limit is used to bound the amount of timers loaded in memory. If this limit is set, when exceeding it, the timers farther in the future will be spilled to disk.
    num_timers_in_memory_limit: Option<NonZeroUsize>,

    /// # Cleanup interval
    ///
    /// In order to clean up completed invocations, that is invocations invoked with an idempotency id, or workflows,
    /// Restate periodically scans among the completed invocations to check whether they need to be removed or not.
    /// This interval sets the scan interval of the cleanup procedure. Default: 1 hour.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    cleanup_interval: humantime::Duration,

    #[cfg_attr(feature = "schemars", schemars(skip))]
    experimental_feature_new_invocation_status_table: bool,

    pub storage: StorageOptions,

    pub invoker: InvokerOptions,

    /// # Maximum command batch size for partition processors
    ///
    /// The maximum number of commands a partition processor will apply in a batch. The larger this
    /// value is, the higher the throughput and latency are.
    max_command_batch_size: NonZeroUsize,
}

impl WorkerOptions {
    pub fn internal_queue_length(&self) -> usize {
        self.internal_queue_length.into()
    }

    pub fn max_command_batch_size(&self) -> usize {
        self.max_command_batch_size.into()
    }

    pub fn num_timers_in_memory_limit(&self) -> Option<usize> {
        self.num_timers_in_memory_limit.map(Into::into)
    }

    pub fn cleanup_interval(&self) -> Duration {
        self.cleanup_interval.into()
    }

    pub fn experimental_feature_new_invocation_status_table(&self) -> bool {
        self.experimental_feature_new_invocation_status_table
    }
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            internal_queue_length: NonZeroUsize::new(1000).expect("Non zero number"),
            num_timers_in_memory_limit: None,
            cleanup_interval: Duration::from_secs(60 * 60).into(),
            experimental_feature_new_invocation_status_table: false,
            storage: StorageOptions::default(),
            invoker: Default::default(),
            max_command_batch_size: NonZeroUsize::new(4).expect("Non zero number"),
        }
    }
}

/// # Invoker options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "InvokerOptions", default))]
#[builder(default)]
#[serde(rename_all = "kebab-case")]
pub struct InvokerOptions {
    /// # Retry policy
    ///
    /// Retry policy to use for all the invocations handled by this invoker.
    pub retry_policy: RetryPolicy,

    /// # Inactivity timeout
    ///
    /// This timer guards against stalled service/handler invocations. Once it expires,
    /// Restate triggers a graceful termination by asking the service invocation to
    /// suspend (which preserves intermediate progress).
    ///
    /// The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    /// the request to suspend.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub inactivity_timeout: humantime::Duration,

    /// # Abort timeout
    ///
    /// This timer guards against stalled service/handler invocations that are supposed to
    /// terminate. The abort timeout is started after the 'inactivity timeout' has expired
    /// and the service/handler invocation has been asked to gracefully terminate. Once the
    /// timer expires, it will abort the service/handler invocation.
    ///
    /// This timer potentially **interrupts** user code. If the user code needs longer to
    /// gracefully terminate, then this value needs to be set accordingly.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub abort_timeout: humantime::Duration,

    /// # Message size warning
    ///
    /// Threshold to log a warning in case protocol messages coming from a service are larger than the specified amount.
    #[serde_as(as = "NonZeroByteCount")]
    #[cfg_attr(feature = "schemars", schemars(with = "NonZeroByteCount"))]
    pub message_size_warning: NonZeroUsize,

    /// # Message size limit
    ///
    /// Threshold to fail the invocation in case protocol messages coming from a service are larger than the specified amount.
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    message_size_limit: Option<NonZeroUsize>,

    /// # Temporary directory
    ///
    /// Temporary directory to use for the invoker temporary files.
    /// If empty, the system temporary directory will be used instead.
    tmp_dir: Option<PathBuf>,

    /// # Spill invocations to disk
    ///
    /// Defines the threshold after which queues invocations will spill to disk at
    /// the path defined in `tmp-dir`. In other words, this is the number of invocations
    /// that can be kept in memory before spilling to disk.
    in_memory_queue_length_limit: NonZeroUsize,

    /// # Limit number of concurrent invocations from this node
    ///
    /// Number of concurrent invocations that can be processed by the invoker.
    concurrent_invocations_limit: Option<NonZeroUsize>,

    // -- Private config options (not exposed in the schema)
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub disable_eager_state: bool,
}

impl InvokerOptions {
    pub fn gen_tmp_dir(&self) -> PathBuf {
        self.tmp_dir.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!("{}-{}", "invoker", ulid::Ulid::new()))
        })
    }

    pub fn concurrent_invocations_limit(&self) -> Option<usize> {
        self.concurrent_invocations_limit.map(Into::into)
    }

    pub fn in_memory_queue_length_limit(&self) -> usize {
        self.in_memory_queue_length_limit.into()
    }

    pub fn message_size_limit(&self) -> Option<usize> {
        self.message_size_limit.map(Into::into)
    }
}

impl Default for InvokerOptions {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::exponential(
                Duration::from_millis(50),
                2.0,
                // see https://github.com/toml-rs/toml/issues/705
                None,
                Some(Duration::from_secs(10)),
            ),
            in_memory_queue_length_limit: NonZeroUsize::new(1_056_784).unwrap(),
            inactivity_timeout: Duration::from_secs(60).into(),
            abort_timeout: Duration::from_secs(60).into(),
            message_size_warning: NonZeroUsize::new(10_000_000).unwrap(), // 10MB
            message_size_limit: None,
            tmp_dir: None,
            concurrent_invocations_limit: Some(NonZeroUsize::new(100).unwrap()),
            disable_eager_state: false,
        }
    }
}

/// # Storage options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "StorageOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct StorageOptions {
    #[serde(flatten)]
    pub rocksdb: RocksDbOptions,

    /// How many partitions to divide memory across?
    ///
    /// By default this uses the value defined in `bootstrap-num-partitions` in the common section of
    /// the config.
    #[serde(skip_serializing_if = "Option::is_none")]
    num_partitions_to_share_memory_budget: Option<NonZeroU64>,

    /// The memory budget for rocksdb memtables in bytes
    ///
    /// The total is divided evenly across partitions. The divisor is defined in
    /// `num-partitions-to-share-memory-budget`. If this value is set, it overrides the ratio
    /// defined in `rocksdb-memory-ratio`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_memory_budget: Option<NonZeroUsize>,

    /// The memory budget for rocksdb memtables as ratio
    ///
    /// This defines the total memory for rocksdb as a ratio of all memory available to memtables
    /// (See `rocksdb-total-memtables-ratio` in common). The budget is then divided evenly across
    /// partitions. The divisor is defined in `num-partitions-to-share-memory-budget`
    rocksdb_memory_ratio: f32,

    /// # Persist lsn interval
    ///
    /// Controls the interval at which worker tries to persist the last applied lsn. Lsn persisting
    /// can be disabled by setting it to "".
    #[serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub persist_lsn_interval: Option<humantime::Duration>,

    /// # Persist lsn threshold
    ///
    /// Minimum number of applied log entries before persisting the lsn. The worker will only
    /// persist a lsn if the partition processor has applied at least #threshold log entries since
    /// the last persisting. This prevents the worker from flushing the RocksDB memtables too often.
    pub persist_lsn_threshold: u64,

    /// Whether to perform commits in background IO thread pools eagerly or not
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub always_commit_in_background: bool,
}

impl StorageOptions {
    pub fn apply_common(&mut self, common: &CommonOptions) {
        self.rocksdb.apply_common(&common.rocksdb);
        if self.num_partitions_to_share_memory_budget.is_none() {
            self.num_partitions_to_share_memory_budget = Some(common.bootstrap_num_partitions);
        }

        // todo: move to a shared struct and deduplicate
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

    pub fn rocksdb_memory_budget(&self) -> usize {
        self.rocksdb_memory_budget
            .unwrap_or_else(|| {
                warn!("PartitionStore rocksdb_memory_budget is not set, defaulting to 1MB");
                // 1MB minimum
                NonZeroUsize::new(1024 * 1024).unwrap()
            })
            .get()
    }

    pub fn num_partitions_to_share_memory_budget(&self) -> u64 {
        self.num_partitions_to_share_memory_budget
            .unwrap_or_else(|| {
                warn!("num-partitions-to-share-memory-budget is not set, defaulting to 10");
                NonZeroU64::new(10).unwrap()
            })
            .get()
    }

    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("db")
    }
}

impl Default for StorageOptions {
    fn default() -> Self {
        let rocksdb = RocksDbOptionsBuilder::default()
            .rocksdb_disable_wal(Some(true))
            .build()
            .expect("valid RocksDbOptions");

        StorageOptions {
            rocksdb,
            num_partitions_to_share_memory_budget: None,
            // set by apply_common in runtime
            rocksdb_memory_budget: None,
            rocksdb_memory_ratio: 0.49,
            // persist the lsn every hour
            persist_lsn_interval: Some(Duration::from_secs(60 * 60).into()),
            persist_lsn_threshold: 1000,
            always_commit_in_background: false,
        }
    }
}
