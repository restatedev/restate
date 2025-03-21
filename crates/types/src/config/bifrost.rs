// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU8, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_serde_util::{ByteCount, NonZeroByteCount};
use tracing::warn;

use crate::logs::metadata::{NodeSetSize, ProviderKind};
use crate::replication::ReplicationProperty;
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
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub seal_retry_interval: humantime::Duration,

    /// # Auto recovery threshold
    ///
    /// Time interval after which bifrost's auto-recovery mechanism will kick in. This
    /// is triggered in scenarios where the control plane took too long to complete loglet
    /// reconfigurations.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub auto_recovery_interval: humantime::Duration,

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
            default_provider: ProviderKind::Replicated,
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
            auto_recovery_interval: Duration::from_secs(3).into(),
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
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub sequencer_inactivity_timeout: humantime::Duration,

    /// Log Server RPC timeout
    ///
    /// Timeout waiting on log server response
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub log_server_rpc_timeout: humantime::Duration,

    /// Log Server RPC retry policy
    ///
    /// Retry policy for log server RPCs
    pub log_server_retry_policy: RetryPolicy,

    /// Maximum number of records to prefetch from log servers
    ///
    /// The number of records bifrost will attempt to prefetch from replicated loglet's log-servers
    /// for every loglet reader (e.g. partition processor). Note that this mainly impacts readers
    /// that are not co-located with the loglet sequencer (i.e. partition processor followers).
    pub readahead_records: NonZeroUsize,

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

    /// # Default log replication factor
    ///
    /// Configures the default replication factor to be used by the replicated loglets.
    ///
    /// Note that this value only impacts the cluster initial provisioning and will not be respected after
    /// the cluster has been provisioned.
    ///
    /// To update existing clusters use the `restatectl` utility.
    // Also allow to specify the replication property as non-zero u8 value to make it simpler to
    // pass it in via an env variable.
    #[serde_as(
        as = "serde_with::PickFirst<(serde_with::DisplayFromStr, crate::replication::ReplicationPropertyFromNonZeroU8)>"
    )]
    #[serde(default = "default_log_replication")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub default_log_replication: ReplicationProperty,

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

fn default_log_replication() -> ReplicationProperty {
    ReplicationProperty::new(NonZeroU8::new(1).expect("to be non-zero"))
}

impl Default for ReplicatedLogletOptions {
    fn default() -> Self {
        Self {
            maximum_inflight_records: NonZeroUsize::new(1000).unwrap(),

            sequencer_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                None,
                Some(Duration::from_millis(5000)),
            ),
            sequencer_inactivity_timeout: Duration::from_secs(15).into(),
            log_server_rpc_timeout: Duration::from_millis(2000).into(),
            log_server_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                Some(3),
                Some(Duration::from_millis(2000)),
            ),
            readahead_records: NonZeroUsize::new(100).unwrap(),
            readahead_trigger_ratio: 0.5,
            default_nodeset_size: NodeSetSize::default(),
            default_log_replication: default_log_replication(),
        }
    }
}
