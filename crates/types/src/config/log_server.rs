// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_serde_util::{ByteCount, NonZeroByteCount};
use tracing::warn;

use super::{CommonOptions, RocksDbOptions, RocksDbOptionsBuilder};

/// # Log server options
///
/// Configuration is only used on nodes running with `log-server` role.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "LogServer", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct LogServerOptions {
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
    /// This defines the total memory for rocksdb as a ratio of all memory available to the
    /// log-server.
    ///
    /// (See `rocksdb-total-memtables-ratio` in common).
    rocksdb_memory_ratio: f32,

    /// Disable fsync of WAL on every batch
    rocksdb_disable_wal_fsync: bool,

    /// The maximum number of subcompactions to run in parallel.
    ///
    /// Setting this to 1 means no sub-compactions are allowed (i.e. only 1 thread will do the compaction).
    ///
    /// Default is 0 which maps to floor(number of CPU cores / 2)
    rocksdb_max_sub_compactions: u32,

    /// The size limit of all WAL files
    ///
    /// Use this to limit the size of WAL files. If the size of all WAL files exceeds this limit,
    /// the oldest WAL file will be deleted and if needed, memtable flush will be triggered.
    ///
    /// Note: RocksDB internally counts the uncompressed bytes to determine the WAL size, and since the WAL
    /// is compressed, the actual size on disk will be significantly smaller than this value (~1/4
    /// depending on the compression ratio). For instance, if this is set to "1 MiB", then rocksdb
    /// might decide to flush if the total WAL (on disk) reached ~260 KiB (compressed).
    ///
    /// Default is `0` which translates into 6 times the memory allocated for membtables for this
    /// database.
    #[serde(skip_serializing_if = "is_zero")]
    #[serde_as(as = "ByteCount")]
    #[serde(default)]
    #[cfg_attr(feature = "schemars", schemars(with = "ByteCount"))]
    rocksdb_max_wal_size: usize,

    /// Whether to perform commits in background IO thread pools eagerly or not
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub always_commit_in_background: bool,

    /// Trigger a commit when the batch size exceeds this threshold.
    ///
    /// Set to 0 or 1 to commit the write batch on every command.
    pub writer_batch_commit_count: usize,

    /// The number of messages that can queue up on input network stream while request processor is busy.
    pub incoming_network_queue_length: NonZeroUsize,
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

impl LogServerOptions {
    pub fn apply_common(&mut self, common: &CommonOptions) {
        self.rocksdb.apply_common(&common.rocksdb);
        if self.rocksdb_memory_budget.is_none() {
            self.rocksdb_memory_budget = Some(
                // 1MiB minimum
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

    pub fn rocksdb_max_wal_size(&self) -> usize {
        if self.rocksdb_max_wal_size == 0 {
            6 * self.rocksdb_memory_budget()
        } else {
            self.rocksdb_max_wal_size
        }
    }

    pub fn rocksdb_max_sub_compactions(&self) -> u32 {
        if self.rocksdb_max_sub_compactions == 0 {
            let parallelism: NonZeroU32 = std::thread::available_parallelism()
                .unwrap_or(NonZeroUsize::new(2).unwrap())
                .try_into()
                .expect("number of cpu cores fits in u32");

            // floor(number of CPU cores / 2)
            parallelism.get() / 2
        } else {
            self.rocksdb_max_sub_compactions
        }
    }

    pub fn rocksdb_memory_budget(&self) -> usize {
        self.rocksdb_memory_budget
            .unwrap_or_else(|| {
                warn!("LogServer's rocksdb_memory_budget is not set, defaulting to 1MB");
                // 1MiB minimum
                NonZeroUsize::new(1024 * 1024).unwrap()
            })
            .get()
    }

    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("log-store")
    }
}

impl Default for LogServerOptions {
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
            rocksdb_max_sub_compactions: 0,
            rocksdb_max_wal_size: 0,
            writer_batch_commit_count: 5000,
            rocksdb_disable_wal_fsync: false,
            always_commit_in_background: false,
            incoming_network_queue_length: NonZeroUsize::new(1000).unwrap(),
        }
    }
}
