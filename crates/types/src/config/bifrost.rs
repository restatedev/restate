// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use adaptive_timeout::BackoffInterval;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::warn;

use restate_serde_util::{ByteCount, NonZeroByteCount};
use restate_time_util::{FriendlyDuration, NonZeroFriendlyDuration};

use crate::logs::metadata::{NodeSetSize, ProviderKind};
use crate::net::connect_opts::MESSAGE_SIZE_OVERHEAD;
use crate::retries::RetryPolicy;

use super::networking::DEFAULT_MESSAGE_SIZE_LIMIT;
use super::{CommonOptions, NetworkingOptions, RocksDbOptions, RocksDbOptionsBuilder};

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

    /// # Record size limit
    ///
    /// Maximum size of a single record that can be appended to Bifrost.
    /// If a record exceeds this limit, the append operation will fail immediately.
    ///
    /// If unset, defaults to `networking.message-size-limit`. If set, it will be clamped at
    /// the value of `networking.message-size-limit` since larger records cannot be transmitted
    /// over the cluster internal network.
    #[serde(skip_serializing_if = "Option::is_none")]
    // Hide the configuration until record size estimation is implemented, currently this option is
    // not effective due to the fixed size estimation of typed records.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    record_size_limit: Option<NonZeroByteCount>,
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

    /// Returns the record size limit, defaulting to `networking.message-size-limit` if not set.
    pub fn record_size_limit(&self) -> NonZeroUsize {
        let limit = self
            .record_size_limit
            .map(|v| v.as_non_zero_usize())
            .unwrap_or(DEFAULT_MESSAGE_SIZE_LIMIT);

        if cfg!(any(test, feature = "test-util")) {
            // In tests, we don't want to leave overhead
            limit
        } else {
            // Add a bit of overhead to account for the overhead of the record envelope
            limit.saturating_add(MESSAGE_SIZE_OVERHEAD.div_ceil(2))
        }
    }

    pub fn apply_common(&mut self, common: &CommonOptions) {
        self.local.apply_common(common);
    }

    /// Clamps the record size limit to the networking message size limit.
    pub(crate) fn set_derived_values(&mut self, networking: &NetworkingOptions) {
        self.record_size_limit = Some(
            self.record_size_limit
                .map(|limit| limit.min(networking.message_size_limit))
                .unwrap_or(networking.message_size_limit),
        );
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
            record_size_limit: None,
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
    #[deprecated(since = "1.6.3", note = "Use `rpc_timeout` instead")]
    #[serde(skip_serializing_if = "Option::is_none")]
    sequencer_retry_policy: Option<RetryPolicy>,

    /// Adaptive timeout for LogServer RPC
    ///
    /// This configures the adaptive timeout range for RPC operations from this node to log servers.
    /// The timeout range is also used to determine the appropriate retry delay between retry attempts.
    pub rpc_timeout: BackoffInterval,

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
    #[deprecated(since = "1.6.3", note = "Use `rpc_timeout` instead")]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_server_rpc_timeout: Option<NonZeroFriendlyDuration>,

    /// Log Server RPC retry policy
    ///
    /// Retry policy for log server RPCs
    #[deprecated(since = "1.6.3", note = "Use `rpc_timeout` instead")]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_server_retry_policy: Option<RetryPolicy>,

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

            sequencer_retry_policy: None,
            rpc_timeout: BackoffInterval {
                min_ms: NonZeroU32::new(250).unwrap(),
                max_ms: NonZeroU32::new(60_000).unwrap(),
            },
            sequencer_inactivity_timeout: NonZeroFriendlyDuration::from_secs_unchecked(15),
            read_batch_size: NonZeroByteCount::new(
                NonZeroUsize::new(32 * 1024).expect("Non zero number"),
            ),
            log_server_rpc_timeout: None,
            log_server_retry_policy: None,
            readahead_records: NonZeroU16::new(20).unwrap(),
            readahead_trigger_ratio: 0.5,
            default_nodeset_size: NodeSetSize::default(),
        }
    }
}
