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

use super::{BackgroundWorkBudget, CommonOptions, RocksDbOptions, RocksDbOptionsBuilder};

const MIN_ROCKSDB_MEMORY: NonZeroByteCount =
    NonZeroByteCount::new(NonZeroUsize::new(32 * 1024 * 1024).unwrap());

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
    rocksdb_memory_budget: Option<NonZeroByteCount>,

    /// The memory budget for rocksdb memtables as ratio
    ///
    /// This defines the total memory for rocksdb as a ratio of the node's total budget
    /// for rocksdb memtables.
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
    /// Note: RocksDB internally counts the uncompressed bytes to determine the WAL size, and since the
    /// WAL may be compressed, the actual size on disk might be significantly smaller than this value (~1/4
    /// depending on the compression ratio). For instance, if this is set to "1 MiB", then rocksdb
    /// might decide to flush if the total WAL (on disk) reached ~260 KiB (compressed).
    ///
    /// Default is `0` which translates into 6 times the memory allocated for memtables for this
    /// database.
    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    rocksdb_max_wal_size: ByteCount<true>,

    /// Whether to perform commits in background IO thread pools eagerly or not
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub always_commit_in_background: bool,

    /// The number of messages that can queue up on input network stream while request processor is busy.
    #[deprecated = "Memory-based flow control is now managed through memory pools"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incoming_network_queue_length: Option<NonZeroUsize>,

    /// Enable storing large blobs in separate files
    ///
    /// [Experimental]
    /// Enables the use of BlobDB to store payloads larger than 512 KiB to separate blob
    /// files. This may improve performance and write amplification at the cost of some
    /// space amplification.
    ///
    /// This is safe to enable/disable at any time.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub rocksdb_enable_blob_separation: bool,

    /// # Max background flushes
    ///
    /// Maximum number of concurrent flush operations for this database. Flushes are
    /// latency-critical (they unblock writes) and are allocated equally across databases.
    ///
    /// If unset, defaults are computed based on CPU count and active node roles.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    rocksdb_max_background_flushes: Option<NonZeroU32>,

    /// # Max background compactions
    ///
    /// Maximum number of concurrent compaction operations for this database.
    ///
    /// If unset, defaults are computed based on CPU count and active node roles.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    rocksdb_max_background_compactions: Option<NonZeroU32>,
}

fn is_zero(value: &ByteCount<true>) -> bool {
    *value == ByteCount::ZERO
}

impl LogServerOptions {
    pub fn apply_common(&mut self, common: &CommonOptions) {
        self.rocksdb.apply_common(&common.rocksdb);
        if self.rocksdb_memory_budget.is_none() {
            self.rocksdb_memory_budget = Some(
                NonZeroByteCount::try_from(
                    ((common.rocksdb_total_memtables_size().as_u64() as f64
                        * self.rocksdb_memory_ratio as f64) as u64)
                        .max(MIN_ROCKSDB_MEMORY.as_u64()),
                )
                .unwrap(),
            );
        }
    }

    pub fn apply_background_work_budget(&mut self, budget: &BackgroundWorkBudget) {
        if self.rocksdb_max_background_flushes.is_none() {
            self.rocksdb_max_background_flushes = Some(budget.max_background_flushes);
        }
        if self.rocksdb_max_background_compactions.is_none() {
            self.rocksdb_max_background_compactions = Some(budget.max_background_compactions);
        }
    }

    pub fn rocksdb_max_background_flushes(&self) -> NonZeroU32 {
        self.rocksdb_max_background_flushes
            .unwrap_or(NonZeroU32::new(2).unwrap())
    }

    pub fn rocksdb_max_background_compactions(&self) -> NonZeroU32 {
        self.rocksdb_max_background_compactions
            .unwrap_or(NonZeroU32::new(2).unwrap())
    }

    pub fn rocksdb_disable_wal_fsync(&self) -> bool {
        self.rocksdb_disable_wal_fsync
    }

    pub fn rocksdb_max_wal_size(&self) -> NonZeroByteCount {
        NonZeroByteCount::try_from(if self.rocksdb_max_wal_size == ByteCount::ZERO {
            6 * self.rocksdb_memory_budget().as_u64()
        } else {
            self.rocksdb_max_wal_size.as_u64()
        })
        .unwrap()
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

    pub fn rocksdb_memory_budget(&self) -> NonZeroByteCount {
        self.rocksdb_memory_budget.unwrap_or_else(|| {
            warn!(
                "LogServer's rocksdb_memory_budget is not set, defaulting to {}",
                MIN_ROCKSDB_MEMORY
            );
            MIN_ROCKSDB_MEMORY
        })
    }

    pub fn rocksdb_data_memtables_budget(&self) -> NonZeroByteCount {
        // The entire memory budget goes to the data CF. The metadata CF's write_buffer_size
        // matches the data CF to avoid independently triggering atomic flushes, but its
        // actual memory consumption is negligible (a few KB of real data per flush).
        self.rocksdb_memory_budget()
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
            rocksdb_max_wal_size: ByteCount::ZERO,
            rocksdb_disable_wal_fsync: false,
            always_commit_in_background: false,
            #[allow(deprecated)]
            incoming_network_queue_length: None,
            rocksdb_enable_blob_separation: false,
            rocksdb_max_background_flushes: None,
            rocksdb_max_background_compactions: None,
        }
    }
}
