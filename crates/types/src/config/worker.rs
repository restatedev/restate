// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::retries::RetryPolicy;

use super::{RocksDbOptions, RocksDbOptionsBuilder};

/// # Worker options
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "WorkerOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct WorkerOptions {
    /// # Internal queue for partition processor communication
    pub internal_queue_length: usize,

    /// # Num timers in memory limit
    ///
    /// The number of timers in memory limit is used to bound the amount of timers loaded in memory. If this limit is set, when exceeding it, the timers farther in the future will be spilled to disk.
    pub num_timers_in_memory_limit: Option<usize>,

    #[serde(flatten)]
    pub rocksdb: RocksDbOptions,

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
    pub bootstrap_num_partitions: u64,

    #[cfg(any(test, feature = "test-util"))]
    #[serde(skip, default = "super::default_arc_tmp")]
    data_dir: std::sync::Arc<tempfile::TempDir>,
}

impl WorkerOptions {
    #[cfg(not(any(test, feature = "test-util")))]
    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("db")
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.path().join("db")
    }
}

impl Default for WorkerOptions {
    fn default() -> Self {
        let rocksdb = RocksDbOptionsBuilder::default()
            .rocksdb_disable_wal(Some(true))
            .build()
            .unwrap();

        Self {
            internal_queue_length: 64,
            num_timers_in_memory_limit: None,
            rocksdb,
            invoker: Default::default(),
            bootstrap_num_partitions: 64,
            #[cfg(any(test, feature = "test-util"))]
            data_dir: super::default_arc_tmp(),
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
    pub message_size_warning: usize,

    /// # Message size limit
    ///
    /// Threshold to fail the invocation in case protocol messages coming from a service are larger than the specified amount.
    pub message_size_limit: Option<usize>,

    /// # Temporary directory
    ///
    /// Temporary directory to use for the invoker temporary files.
    /// If empty, the system temporary directory will be used instead.
    tmp_dir: Option<PathBuf>,

    /// # Limit number of concurrent invocations from this node
    ///
    /// Number of concurrent invocations that can be processed by the invoker.
    pub concurrent_invocations_limit: Option<usize>,

    // -- Private config options (not exposed in the schema)
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub disable_eager_state: bool,
}

impl InvokerOptions {
    pub fn gen_tmp_dir(&self) -> PathBuf {
        self.tmp_dir.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!("{}-{}", "invoker", ulid::Ulid::new()))
        })
    }
}

impl Default for InvokerOptions {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::exponential(
                Duration::from_millis(50),
                2.0,
                // see https://github.com/toml-rs/toml/issues/705
                i64::MAX as usize,
                Some(Duration::from_secs(10)),
            ),
            inactivity_timeout: Duration::from_secs(60).into(),
            abort_timeout: Duration::from_secs(60).into(),
            message_size_warning: 1024 * 1024 * 10, // 10mb
            message_size_limit: None,
            tmp_dir: None,
            concurrent_invocations_limit: None,
            disable_eager_state: false,
        }
    }
}
