// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_serde_util::NonZeroByteCount;

use super::{RocksDbOptions, RocksDbOptionsBuilder};
use crate::retries::RetryPolicy;

/// # Worker options
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

    pub storage: StorageOptions,

    pub invoker: InvokerOptions,

    /// # Partitions
    ///
    /// Number of partitions that will be provisioned during cluster bootstrap,
    /// partitions used to process messages.
    ///
    /// NOTE: This config entry only impacts the initial number of partitions, the
    /// value of this entry is ignored for bootstrapped nodes/clusters.
    ///
    /// Cannot be higher than `4611686018427387903` (You should almost never need as many partitions anyway)
    bootstrap_num_partitions: NonZeroU64,
}

impl WorkerOptions {
    pub fn internal_queue_length(&self) -> usize {
        self.internal_queue_length.into()
    }

    pub fn bootstrap_num_partitions(&self) -> u64 {
        self.bootstrap_num_partitions.into()
    }

    pub fn num_timers_in_memory_limit(&self) -> Option<usize> {
        self.num_timers_in_memory_limit.map(Into::into)
    }
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            internal_queue_length: NonZeroUsize::new(64).unwrap(),
            num_timers_in_memory_limit: None,
            storage: StorageOptions::default(),
            invoker: Default::default(),
            bootstrap_num_partitions: NonZeroU64::new(64).unwrap(),
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
            inactivity_timeout: Duration::from_secs(60).into(),
            abort_timeout: Duration::from_secs(60).into(),
            message_size_warning: NonZeroUsize::new(10_000_000).unwrap(), // 10MB
            message_size_limit: None,
            tmp_dir: None,
            concurrent_invocations_limit: None,
            disable_eager_state: false,
        }
    }
}

/// # Storage options
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "StorageOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct StorageOptions {
    #[serde(flatten)]
    pub rocksdb: RocksDbOptions,

    #[cfg(any(test, feature = "test-util"))]
    #[serde(skip, default = "super::default_arc_tmp")]
    data_dir: std::sync::Arc<tempfile::TempDir>,

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
}

impl StorageOptions {
    #[cfg(not(any(test, feature = "test-util")))]
    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("db")
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.path().join("db")
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
            #[cfg(any(test, feature = "test-util"))]
            data_dir: super::default_arc_tmp(),
            // persist the lsn every hour
            persist_lsn_interval: Some(Duration::from_secs(60 * 60).into()),
            persist_lsn_threshold: 1000,
        }
    }
}
