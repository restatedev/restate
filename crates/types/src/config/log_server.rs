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

// We'd like to leave as much space as possible for data memtables. The strategy
// is to avoid triggering data cf because metadata is full, instead, we'd like to
// have the data cf to be the trigger and metadata cf to follow (due to atomic flush).
//
// This means that we'd want to give enough space to the metadata cf such that it almost
// never causes flush. Given that the metadata updates are tiny, we assume that an 8MiB
// is sufficiently large to achieve this goal.
const METADATA_MEMORY_BUDGET: NonZeroByteCount =
    NonZeroByteCount::new(NonZeroUsize::new(8 * 1024 * 1024).unwrap());

const MIN_DATA_MEMTABLES_MEMORY: NonZeroByteCount =
    NonZeroByteCount::new(NonZeroUsize::new(32 * 1024 * 1024).unwrap());

const MIN_ROCKSDB_MEMORY: NonZeroByteCount =
    MIN_DATA_MEMTABLES_MEMORY.saturating_add(METADATA_MEMORY_BUDGET);

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
        // memory budget is in bytes. We divide the budget between the data cf and metadata cf.
        let budget = self
            .rocksdb_memory_budget()
            .as_usize()
            .saturating_sub(METADATA_MEMORY_BUDGET.as_usize());
        // sanitize minimum to 32MiB
        NonZeroByteCount::new(NonZeroUsize::new(std::cmp::max(budget, 32 * 1024 * 1024)).unwrap())
    }

    pub fn rocksdb_metadata_memtables_budget(&self) -> NonZeroByteCount {
        METADATA_MEMORY_BUDGET
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
        }
    }
}
