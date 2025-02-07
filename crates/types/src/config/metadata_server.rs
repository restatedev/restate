// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use restate_serde_util::NonZeroByteCount;
use tracing::warn;

use super::{data_dir, CommonOptions, RocksDbOptions, RocksDbOptionsBuilder};

/// # Metadata store options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "MetadataServerOptions", default)
)]
#[serde(rename_all = "kebab-case", default)]
#[builder(default)]
pub struct MetadataServerOptions {
    /// Limit number of in-flight requests
    ///
    /// Number of in-flight metadata store requests.
    request_queue_length: NonZeroUsize,

    /// The memory budget for rocksdb memtables in bytes
    ///
    /// If this value is set, it overrides the ratio defined in `rocksdb-memory-ratio`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_memory_budget: Option<NonZeroUsize>,

    /// The memory budget for rocksdb memtables as ratio
    ///
    /// This defines the total memory for rocksdb as a ratio of all memory available to memtables
    /// (See `rocksdb-total-memtables-ratio` in common).
    rocksdb_memory_ratio: f32,

    /// RocksDB options for metadata store's RocksDB instance
    ///
    /// The RocksDB options which will be used to configure the metadata store's RocksDB instance.
    #[serde(flatten)]
    pub rocksdb: RocksDbOptions,

    /// Type of metadata server to start
    ///
    /// The type of metadata server to start when running the metadata store role.
    // defined as Option<_> for backward compatibility with version < v1.2
    #[serde(flatten)]
    kind: Option<MetadataServerKind>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum MetadataServerKind {
    #[default]
    Local,
    // make the Raft based metadata server primarily known as the replicated metadata server
    #[serde(rename = "replicated")]
    Raft(RaftOptions),
}

impl MetadataServerOptions {
    pub fn kind(&self) -> MetadataServerKind {
        self.kind.clone().unwrap_or_default()
    }

    pub fn set_kind(&mut self, kind: MetadataServerKind) {
        self.kind = Some(kind);
    }

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

    pub fn rocksdb_memory_budget(&self) -> usize {
        self.rocksdb_memory_budget
            .unwrap_or_else(|| {
                warn!("MetadataStore rocksdb_memory_budget is not set, defaulting to 1MB");
                // 1MB minimum
                NonZeroUsize::new(1024 * 1024).unwrap()
            })
            .get()
    }

    pub fn data_dir(&self) -> PathBuf {
        data_dir("local-metadata-store")
    }

    pub fn request_queue_length(&self) -> usize {
        self.request_queue_length.get()
    }
}

impl Default for MetadataServerOptions {
    fn default() -> Self {
        let rocksdb = RocksDbOptionsBuilder::default()
            .rocksdb_disable_wal(Some(false))
            .build()
            .expect("valid RocksDbOptions");
        Self {
            request_queue_length: NonZeroUsize::new(32).unwrap(),
            // set by apply_common in runtime
            rocksdb_memory_budget: None,
            rocksdb_memory_ratio: 0.01,
            rocksdb,
            kind: Some(MetadataServerKind::default()),
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case", default)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RaftOptions {
    /// The number of ticks before triggering an election
    ///
    /// The number of ticks before triggering an election. The value must be larger than
    /// `raft_heartbeat_tick`. It's recommended to set `raft_election_tick = 10 * raft_heartbeat_tick`.
    /// Decrease this value if you want to react faster to failed leaders. Note, decreasing this
    /// value too much can lead to cluster instabilities due to falsely detecting dead leaders.
    pub raft_election_tick: NonZeroUsize,
    /// The number of ticks before sending a heartbeat
    ///
    /// A leader sends heartbeat messages to maintain its leadership every heartbeat ticks.
    /// Decrease this value to send heartbeats more often.
    pub raft_heartbeat_tick: NonZeroUsize,
    /// The raft tick interval
    ///
    /// The interval at which the raft node will tick. Decrease this value in order to let the Raft
    /// node react more quickly to changes. Note, that every tick comes with an overhead. Moreover,
    /// the tick interval directly affects the election timeout. If the election timeout becomes too
    /// small, then this can cause cluster instabilities due to frequent leader changes.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub raft_tick_interval: humantime::Duration,
    /// The status update interval
    ///
    /// The interval at which the raft node will update its status. Decrease this value in order to
    /// see more recent status updates.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub status_update_interval: humantime::Duration,
}

impl Default for RaftOptions {
    fn default() -> Self {
        RaftOptions {
            raft_election_tick: NonZeroUsize::new(20).expect("20 to be non zero"),
            raft_heartbeat_tick: NonZeroUsize::new(2).expect("2 to be non zero"),
            raft_tick_interval: Duration::from_millis(100).into(),
            status_update_interval: Duration::from_secs(5).into(),
        }
    }
}
