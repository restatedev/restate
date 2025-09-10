// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU16, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::warn;

use restate_serde_util::{ByteCount, NonZeroByteCount};
use restate_time_util::{FriendlyDuration, NonZeroFriendlyDuration};

use crate::logs::metadata::{NodeSetSize, ProviderKind};
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
    ///
    /// Default: Replicated
    pub default_provider: ProviderKind,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    /// Configuration of local loglet provider
    pub local: LocalLogletOptions,
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
    pub seal_retry_interval: NonZeroFriendlyDuration,

    /// # Auto recovery threshold
    ///
    /// Time interval after which bifrost's auto-recovery mechanism will kick in. This
    /// is triggered in scenarios where the control plane took too long to complete loglet
    /// reconfigurations.
    pub auto_recovery_interval: NonZeroFriendlyDuration,

    /// # Append retry minimum interval
    ///
    /// Minimum retry duration used by the exponential backoff mechanism for bifrost appends.
    append_retry_min_interval: NonZeroFriendlyDuration,
    /// # Append retry maximum interval
    ///
    /// Maximum retry duration used by the exponential backoff mechanism for bifrost appends.
    append_retry_max_interval: NonZeroFriendlyDuration,

    /// # In-memory RecordCache memory limit
    ///
    /// Optional size of record cache in bytes.
    /// If set to 0, record cache will be disabled.
    /// Defaults: 250MB
    #[cfg_attr(feature = "schemars", schemars(with = "ByteCount"))]
    pub record_cache_memory_size: ByteCount,

    /// # Disable Automatic Improvement
    ///
    /// When enabled, automatic improvement periodically checks with the loglet provider
    /// if the loglet configuration can be improved by performing a reconfiguration.
    ///
    /// This allows the log to pick up replication property changes, apply better placement
    /// of replicas, or for other reasons.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub disable_auto_improvement: bool,

    // Should be enabled by default in v1.5 or v1.6 depending on whether we'll
    // allow rolling back to a release prior to <v1.4.3 or not.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub experimental_chain_sealing: bool,
}

impl BifrostOptions {
    pub fn append_retry_policy(&self) -> RetryPolicy {
        // Appends are retried with exponential backoff, forever.
        RetryPolicy::exponential(
            self.append_retry_min_interval.into(),
            2.0,
            None,
            Some(self.append_retry_max_interval.into()),
        )
    }

    pub fn apply_common(&mut self, common: &CommonOptions) {
        self.local.apply_common(common);
    }
}

impl Default for BifrostOptions {
    fn default() -> Self {
        Self {
            default_provider: ProviderKind::Replicated,
            replicated_loglet: ReplicatedLogletOptions::default(),
            local: LocalLogletOptions::default(),
            read_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(50),
                2.0,
                Some(50),
                Some(Duration::from_secs(1)),
            ),
            append_retry_min_interval: NonZeroFriendlyDuration::from_millis_unchecked(10),
            append_retry_max_interval: NonZeroFriendlyDuration::from_secs_unchecked(1),
            auto_recovery_interval: NonZeroFriendlyDuration::from_secs_unchecked(15),
            seal_retry_interval: NonZeroFriendlyDuration::from_secs_unchecked(2),
            record_cache_memory_size: ByteCount::from(250u64 * 1024 * 1024), // 250 MiB
            disable_auto_improvement: false,
            experimental_chain_sealing: false,
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
    /// Batching is disabled if this is set to zero.
    pub writer_batch_commit_duration: FriendlyDuration,
}

impl LocalLogletOptions {
    fn apply_common(&mut self, common: &CommonOptions) {
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
            writer_batch_commit_duration: FriendlyDuration::ZERO,
            rocksdb_disable_wal_fsync: false,
            always_commit_in_background: false,
        }
    }
}

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

    /// Sequencer retry policy
    ///
    /// Backoff introduced when sequencer fail to find a suitable spread of log servers
    pub sequencer_retry_policy: RetryPolicy,

    /// Sequencer inactivity timeout
    ///
    /// The sequencer is allowed to consider itself quiescent if it did not commit records for this period of time.
    /// It may use this to sends pre-emptive release/seal check requests to log-servers.
    ///
    /// The sequencer is also allowed to use this value as interval to send seal/release checks even if it's not quiescent.
    pub sequencer_inactivity_timeout: NonZeroFriendlyDuration,

    /// Log Server RPC timeout
    ///
    /// Timeout waiting on log server response
    pub log_server_rpc_timeout: NonZeroFriendlyDuration,

    /// Log Server RPC retry policy
    ///
    /// Retry policy for log server RPCs
    pub log_server_retry_policy: RetryPolicy,

    /// Maximum number of records to prefetch from log servers
    ///
    /// The number of records bifrost will attempt to prefetch from replicated loglet's log-servers
    /// for every loglet reader (e.g. partition processor). Note that this mainly impacts readers
    /// that are not co-located with the loglet sequencer (i.e. partition processor followers).
    pub readahead_records: NonZeroU16,

    /// # Limits memory per remote batch read
    ///
    /// When reading from a log-server, the server stops reading if the next record will tip over the
    /// total number of bytes allowed in this configuration option.
    ///
    /// Note the limit is not strict and the server will always allow at least a single record to be
    /// read even if that record exceeds the stated budget.
    pub read_batch_size: NonZeroByteCount,

    /// Trigger to prefetch more records
    ///
    /// When read-ahead is used (readahead-records), this value (percentage in float) will determine when
    /// readers should trigger a prefetch for another batch to fill up the buffer. For instance, if
    /// this value is 0.3, then bifrost will trigger a prefetch when 30% or more of the read-ahead
    /// slots become available (e.g. partition processor consumed records and freed up enough slots).
    ///
    /// The higher the value is, the longer bifrost will wait before it triggers the next fetch, potentially
    /// fetching more records as a result.
    ///
    /// To illustrate, if readahead-records is set to 100 and readahead-trigger-ratio is 1.0. Then
    /// bifrost will prefetch up to 100 records from log-servers and will not trigger the next
    /// prefetch unless the consumer consumes 100% of this buffer. This means that bifrost will
    /// read in batches but will not do while the consumer is still reading the previous batch.
    ///
    /// Value must be between 0 and 1. It will be clamped at `1.0`.
    pub readahead_trigger_ratio: f32,

    /// # Default nodeset size
    ///
    /// Configures the target nodeset size used by the replicated loglet when generating new
    /// nodesets for logs. Setting this to 0 will let the system choose a reasonable value based on
    /// the effective replication_property at the time of logs reconfiguration.
    ///
    /// Note that this value only impacts the cluster initial provisioning and will not be respected after
    /// the cluster has been provisioned.
    ///
    /// To update existing clusters use the `restatectl` utility.
    // hide the configuration option from serialization if it is the default
    #[serde(default, skip_serializing_if = "nodeset_size_is_zero")]
    // hide the configuration option by excluding it from the Json schema
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub default_nodeset_size: NodeSetSize,
}

fn nodeset_size_is_zero(i: &NodeSetSize) -> bool {
    *i == NodeSetSize::ZERO
}

impl Default for ReplicatedLogletOptions {
    fn default() -> Self {
        #[allow(deprecated)]
        Self {
            maximum_inflight_records: NonZeroUsize::new(1000).unwrap(),

            sequencer_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                None,
                Some(Duration::from_millis(5000)),
            ),
            sequencer_inactivity_timeout: NonZeroFriendlyDuration::from_secs_unchecked(15),
            read_batch_size: NonZeroByteCount::new(
                NonZeroUsize::new(32 * 1024).expect("Non zero number"),
            ),
            log_server_rpc_timeout: NonZeroFriendlyDuration::from_millis_unchecked(2000),

            log_server_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                Some(3),
                Some(Duration::from_millis(2000)),
            ),
            readahead_records: NonZeroU16::new(20).unwrap(),
            readahead_trigger_ratio: 0.5,
            default_nodeset_size: NodeSetSize::default(),
        }
    }
}
