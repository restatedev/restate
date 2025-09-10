// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::warn;

use super::{
    CommonOptions, ObjectStoreOptions, RocksDbOptions, RocksDbOptionsBuilder,
    print_warning_deprecated_config_option,
};
use crate::identifiers::PartitionId;
use crate::rate::Rate;
use crate::retries::RetryPolicy;
use restate_serde_util::NonZeroByteCount;

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

    pub storage: StorageOptions,

    pub invoker: InvokerOptions,

    /// # Maximum command batch size for partition processors
    ///
    /// The maximum number of commands a partition processor will apply in a batch. The larger this
    /// value is, the higher the throughput and latency are.
    max_command_batch_size: NonZeroUsize,

    /// # Snapshots
    ///
    /// Snapshots provide a mechanism for safely trimming the log and efficient bootstrapping of new
    /// worker nodes.
    #[serde(default)]
    pub snapshots: SnapshotsOptions,

    /// # Durability mode
    ///
    /// Every partition store is backed up by a durable log that is used to recover the state of
    /// the partition on restart or failover. The durability mode defines the criteria used
    /// to determine whether a partition is considered fully durable or not at a given point in the
    /// log history. Once a partition is fully durable, its backing log is allowed to be trimmed to
    /// the durability point.
    ///
    /// This helps keeping the log's disk usage under control but it forces nodes that need to restore
    /// the state of the partition to fetch a snapshot of that partition that covers the changes up to
    /// and including the "durability point".
    ///
    /// Since v1.4.2 (not compatible with earlier versions)
    // todo: auto enable in v1.6
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durability_mode: Option<DurabilityMode>,
    /// # Delayed log trimming
    ///
    /// Log trimming normally happens immediately after the partition becomes fully durable. A
    /// partition is considered fully durable when one of the following conditions is met:
    ///
    /// 1. The partition has been fully replicated and flushed to all nodes in its replica-set.
    /// 2. The partition has been snapshotted into the snapshot repository.
    ///
    /// The delay interval is the time that Restate will wait before trimming the log _after_ the
    /// durability condition is met. It's useful to set this to a non-zero duration if you want to
    /// cover the time needed for the snapshot repository (i.e. S3) to replicate the snapshot
    /// across regions (typically a few seconds, but can be longer. Check S3's guidelines and
    /// cross-region replication SLA for more information).
    ///
    /// This setting is only effective if `worker.experimental-partition-driven-log-trimming` is
    /// set to `true`.
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "Option::is_none")]
    trim_delay_interval: Option<humantime::Duration>,

    // todo: remove and auto-enable in v1.6
    //
    // Underlying dependency `PartitionDurability` wal-protocol message is
    // supported since v1.4.2
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub experimental_partition_driven_log_trimming: bool,
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

    pub fn trim_delay_interval(&self) -> Duration {
        self.trim_delay_interval.unwrap_or_default().into()
    }
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            internal_queue_length: NonZeroUsize::new(1000).expect("Non zero number"),
            num_timers_in_memory_limit: None,
            cleanup_interval: Duration::from_secs(60 * 60).into(),
            storage: StorageOptions::default(),
            invoker: Default::default(),
            max_command_batch_size: NonZeroUsize::new(32).expect("Non zero number"),
            snapshots: SnapshotsOptions::default(),
            trim_delay_interval: None,
            durability_mode: None,
            experimental_partition_driven_log_trimming: false,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum DurabilityMode {
    /// This disables durability tracking and trimming completely.
    ///
    /// Trims and snapshots are still possible if performed manually or by an external
    /// component.
    None,

    /// In this mode, a partition is considered durable when its state can be restored from
    /// any of members of the replica-set as well as the latest snapshot.
    ///
    /// In other words, do not trim unless **all** replicas cover this Lsn, **and** the snapshot.
    ///
    /// [requires snapshot repository]
    /// DurabilityPoint = Min(Min(ReplicaSetDurablePoints), SnapshotDurablePoint)
    SnapshotAndReplicaSet,

    // A partition is fully durable if the _entire_ replica-set is durable at this Lsn
    // **or** if a snapshot is available.
    //
    // Do not trim unless the Lsn is covered by a snapshot, **or** by _all_ the replicas
    // in the replica-set.
    //
    // DurabilityPoint = Max(Min(ReplicaSetDurablePoints), SnapshotDurablePoint)
    //
    // [Requires node-to-node sharing of ad-hoc snapshots] so it's commented until support is
    // added
    // SnapshotOrReplicaSet,
    // -----
    //
    /// In this mode, a partition is considered durable when its state can be restored from
    /// the snapshot and at least a single replica.
    ///
    /// Do not trim unless the Lsn is covered (durably) by _any_ of the replicas **and** by
    /// the snapshot. Gives weight to snapshots over the durability of the replica-set but
    /// without ignoring the replica-set completely.
    ///
    /// In practice, this means that after a snapshot has been created on the leader, the
    /// system will wait for the nearest memtable flush that cover this Lsn before considering
    /// this Lsn for trimming. If the leader crashes before the memtable flush, we are confident
    /// that the leader will be able to replay the log without any trim-gaps. This is under the
    /// condition that the leader didn't move to another node. In the latter case, the system will
    /// fetch the snapshot as usual.
    ///
    /// [requires snapshot repository]
    /// [default] if snapshot repository configured
    /// DurabilityPoint = Min(Max(ReplicaSetDurablePoints), SnapshotDurablePoint)
    Balanced,

    /// A partition is considered durable once all nodes in the replica-set are durable, regardless
    /// of the state of snapshots.
    ///
    /// Do not trim unless all replicas durably include this Lsn.
    ///
    /// default in standalone-mode with no snapshot repository configured
    ///
    /// [default] if snapshot repository is not configured
    /// DurabilityPoint = Min(ReplicaSetDurablePoints)
    // [Requires node-to-on-node sharing of ad-hoc snapshots] if used in cluster mode.
    ReplicaSetOnly,

    /// A partition is durable ONLY after a snapshot has been created.
    /// [requires snapshot repository]
    ///
    /// Do not trim unless the Lsn is covered by the snapshot with no regard to the
    /// state of durability of the replica-set members.
    ///
    /// DurabilityPoint = SnapshotDurablePoint
    SnapshotOnly,
}

pub const DEFAULT_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);
pub const DEFAULT_ABORT_TIMEOUT: Duration = Duration::from_secs(60);

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
    /// This is **deprecated** and will be removed in the next Restate releases.
    ///
    /// Please refer to [`InvocationOptions.retry_policy`] for the new configuration options.
    #[deprecated]
    #[serde(default)]
    pub retry_policy: Option<RetryPolicy>,

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
    /// that can be kept in memory before spilling to disk. This is a per-partition limit.
    in_memory_queue_length_limit: NonZeroUsize,

    /// # Limit number of concurrent invocations from this node
    ///
    /// Number of concurrent invocations that can be processed by the invoker.
    concurrent_invocations_limit: Option<NonZeroUsize>,

    // -- Private config options (not exposed in the schema)
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub disable_eager_state: bool,

    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    experimental_features_propose_events: bool,

    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    experimental_features_allow_protocol_v6: bool,

    /// # Action throttling
    ///
    /// Configures rate limiting for service actions at the node level.
    /// This throttling mechanism uses a token bucket algorithm to control the rate
    /// at which actions can be processed, helping to prevent resource exhaustion
    /// and maintain system stability under high load.
    ///
    /// The throttling limit is shared across all partitions running on this node,
    /// providing a global rate limit for the entire node rather than per-partition limits.
    /// When `unset`, no throttling is applied and actions are processed
    /// without rate limiting.
    pub action_throttling: Option<ThrottlingOptions>,
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

    pub fn experimental_features_propose_events(&self) -> bool {
        self.experimental_features_propose_events
    }

    pub fn experimental_features_allow_protocol_v6(&self) -> bool {
        self.experimental_features_allow_protocol_v6
    }

    #[allow(deprecated)]
    pub fn print_deprecation_warnings(&self) {
        if self.retry_policy.is_some() {
            print_warning_deprecated_config_option(
                "invoker.retry-policy",
                Some("default-retry-policy"),
            );
        }
    }
}

impl Default for InvokerOptions {
    fn default() -> Self {
        Self {
            #[allow(deprecated)]
            retry_policy: Some(RetryPolicy::exponential(
                Duration::from_millis(50),
                2.0,
                // see https://github.com/toml-rs/toml/issues/705
                None,
                Some(Duration::from_secs(10)),
            )),
            in_memory_queue_length_limit: NonZeroUsize::new(66_049).unwrap(),
            inactivity_timeout: DEFAULT_INACTIVITY_TIMEOUT.into(),
            abort_timeout: DEFAULT_ABORT_TIMEOUT.into(),
            message_size_warning: NonZeroUsize::new(10 * 1024 * 1024).unwrap(), // 10MiB
            message_size_limit: None,
            tmp_dir: None,
            concurrent_invocations_limit: Some(NonZeroUsize::new(1000).expect("is non zero")),
            disable_eager_state: false,
            experimental_features_propose_events: false,
            experimental_features_allow_protocol_v6: false,
            action_throttling: None,
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
    /// By default this uses the value defined in `default-num-partitions` in the common section of
    /// the config.
    #[serde(skip_serializing_if = "Option::is_none")]
    num_partitions_to_share_memory_budget: Option<NonZeroU16>,

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

    /// # Persist LSN interval (deprecated)
    ///
    /// This configuration option is deprecated and ignored in Restate >= 1.3.3.
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    #[deprecated(since = "1.4.0", note = "no longer used, will be removed with >1.4.0")]
    #[serde(skip_serializing)]
    persist_lsn_interval: Option<humantime::Duration>,

    /// # Persist LSN threshold (deprecated)
    ///
    /// This configuration option is deprecated and ignored in Restate >= 1.3.3.
    #[deprecated(since = "1.4.0", note = "no longer used, will be removed with >1.4.0")]
    #[serde(skip_serializing)]
    pub persist_lsn_threshold: Option<u64>,

    /// Whether to perform commits in background IO thread pools eagerly or not
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub always_commit_in_background: bool,
}

impl StorageOptions {
    pub fn apply_common(&mut self, common: &CommonOptions) {
        self.rocksdb.apply_common(&common.rocksdb);
        if self.num_partitions_to_share_memory_budget.is_none() {
            self.num_partitions_to_share_memory_budget = Some(
                NonZeroU16::new(common.default_num_partitions)
                    // todo add support for configuring the memory budget after the system has
                    //  been started.
                    // In the absence of a configured number of default partitions, we default to
                    // its default value of 24 partitions. This is not great :-(
                    .unwrap_or(NonZeroU16::new(24).expect("to be non zero")),
            )
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

    #[allow(deprecated)]
    pub fn print_deprecation_warnings(&self) {
        if self.persist_lsn_interval.is_some() {
            print_warning_deprecated_config_option("storage.persist-lsn-interval", None);
        }
        if self.persist_lsn_threshold.is_some() {
            print_warning_deprecated_config_option("storage.persist-lsn-threshold", None);
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

    pub fn num_partitions_to_share_memory_budget(&self) -> u16 {
        self.num_partitions_to_share_memory_budget
            .unwrap_or_else(|| {
                warn!("num-partitions-to-share-memory-budget is not set, defaulting to 10");
                NonZeroU16::new(10).unwrap()
            })
            .get()
    }

    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("db")
    }

    pub fn snapshots_staging_dir(&self) -> PathBuf {
        super::data_dir("pp-snapshots")
    }
}

impl Default for StorageOptions {
    fn default() -> Self {
        let rocksdb = RocksDbOptionsBuilder::default()
            .rocksdb_disable_wal(Some(true))
            .build()
            .expect("valid RocksDbOptions");

        #[allow(deprecated)]
        StorageOptions {
            rocksdb,
            num_partitions_to_share_memory_budget: None,
            // set by apply_common in runtime
            rocksdb_memory_budget: None,
            rocksdb_memory_ratio: 0.49,
            always_commit_in_background: false,

            // todo: remove deprecated persist-lsn-* attributes in 1.4+
            persist_lsn_interval: None,
            persist_lsn_threshold: None,
        }
    }
}

/// # Snapshot options.
///
/// Partition store snapshotting settings. At a minimum, set `destination` and
/// `snapshot-interval-num-records` to enable snapshotting. For a complete example, see
/// [Snapshots](https://docs.restate.dev/operate/snapshots).
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "SnapshotsOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct SnapshotsOptions {
    /// # Snapshot destination URL
    ///
    /// Base URL for cluster snapshots. Supports `s3://` and `file://` protocol scheme.
    /// S3-compatible object stores must support ETag-based conditional writes.
    ///
    /// Default: `None`
    pub destination: Option<String>,

    /// # Automatic snapshot creation frequency
    ///
    /// Number of log records that trigger a snapshot to be created.
    ///
    /// As snapshots are created asynchronously, the actual number of new records that will trigger
    /// a snapshot will vary. The counter for the subsequent snapshot begins from the LSN at which
    /// the previous snapshot export was initiated. Only leader Partition Processors will take
    /// snapshots for a given partition.
    ///
    /// This setting does not influence explicitly requested snapshots triggered using `restatectl`.
    ///
    /// Default: `None` - automatic snapshots are disabled
    pub snapshot_interval_num_records: Option<NonZeroU64>,

    #[serde(flatten)]
    pub object_store: ObjectStoreOptions,

    /// # Error retry policy
    ///
    /// A retry policy for dealing with retryable object store errors.
    pub object_store_retry_policy: RetryPolicy,
}

impl Default for SnapshotsOptions {
    fn default() -> Self {
        Self {
            destination: None,
            snapshot_interval_num_records: None,
            object_store: Default::default(),
            object_store_retry_policy: Self::default_retry_policy(),
        }
    }
}

impl SnapshotsOptions {
    fn default_retry_policy() -> RetryPolicy {
        RetryPolicy::exponential(
            Duration::from_millis(100),
            2.,
            Some(10),
            Some(Duration::from_secs(10)),
        )
    }

    pub fn snapshots_base_dir(&self) -> PathBuf {
        super::data_dir("db-snapshots")
    }

    pub fn snapshots_dir(&self, partition_id: PartitionId) -> PathBuf {
        super::data_dir("db-snapshots").join(partition_id.to_string())
    }
}

/// # Throttling options
///
/// Throttling options per invoker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ThrottlingOptions {
    /// # Refill rate
    ///
    /// The rate at which the tokens are replenished.
    ///
    /// Syntax: `<rate>/<unit>` where `<unit>` is `s|sec|second`, `m|min|minute`, or `h|hr|hour`.
    /// unit defaults to per second if not specified.
    pub rate: Rate,

    /// # Burst capacity
    ///
    /// The maximum number of tokens the bucket can hold.
    /// Default to the rate value if not specified.
    pub capacity: Option<NonZeroU32>,
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for ThrottlingOptions {
    fn schema_name() -> std::string::String {
        "ThrottlingOptions".to_owned()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ThrottlingOptions").into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::schema::Schema {
        use schemars::schema::*;

        let schema_object = SchemaObject {
            metadata: Some(Box::new(Metadata {
                title: Some("ThrottlingOptions".to_string()),
                description: Some("Throttling options per invoker".to_string()),
                ..Default::default()
            })),
            instance_type: Some(InstanceType::Object.into()),
            object: Some(Box::new(ObjectValidation {
                properties: {
                    let mut properties = schemars::Map::new();

                    // Rate field - represented as a string with pattern
                    let mut rate_schema: SchemaObject =
                        generator.subschema_for::<String>().into_object();
                    let rate_validation = rate_schema.string();
                    rate_validation.pattern =
                        Some(r"^\d+(\/(sec|min|hr|s|m|h|second|minute|hour))?$".to_string());
                    let rate_metadata = rate_schema.metadata();
                    rate_metadata.description = Some("The rate at which the tokens are replenished. Syntax: `<rate>/<unit>` where `<unit>` is `s|sec|second`, `m|min|minute`, or `h|hr|hour`. Unit defaults to per second if not specified.".to_string());
                    properties.insert("rate".to_string(), Schema::Object(rate_schema));

                    // Capacity field - optional non-zero u32
                    let mut capacity_schema: SchemaObject =
                        generator.subschema_for::<u32>().into_object();
                    let capacity_validation = capacity_schema.number();
                    capacity_validation.minimum = Some(1.0);
                    let capacity_metadata = capacity_schema.metadata();
                    capacity_metadata.description = Some("The maximum number of tokens the bucket can hold. Defaults to the rate value if not specified.".to_string());
                    properties.insert("capacity".to_string(), Schema::Object(capacity_schema));

                    properties
                },
                required: {
                    let mut required = schemars::Set::new();
                    required.insert("rate".to_string());
                    required
                },
                additional_properties: Some(Box::new(Schema::Bool(false))),
                ..Default::default()
            })),
            ..Default::default()
        };

        Schema::Object(schema_object)
    }
}

impl From<ThrottlingOptions> for gardal::Limit {
    fn from(options: ThrottlingOptions) -> Self {
        use gardal::Limit;

        let mut limit = match options.rate {
            Rate::PerSecond(rate) => Limit::per_second(rate),
            Rate::PerMinute(rate) => Limit::per_minute(rate),
            Rate::PerHour(rate) => Limit::per_hour(rate),
        };

        if let Some(capacity) = options.capacity {
            limit = limit.with_burst(capacity);
        }

        limit
    }
}
