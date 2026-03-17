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

    /// Enable storing large values in separate blob files
    ///
    /// [Experimental]
    /// Enables BlobDB key-value separation for the data column family. Values at or
    /// above `rocksdb-blob-min-size` are written to dedicated blob files, leaving only
    /// small pointers in the LSM tree. This dramatically reduces write amplification
    /// during compaction at the cost of some space amplification from unreferenced blobs
    /// awaiting garbage collection.
    ///
    /// This is safe to enable/disable at any time.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub rocksdb_enable_blob_separation: bool,

    /// Minimum value size for blob file separation
    ///
    /// Values at or above this threshold are stored in separate blob files when
    /// blob separation is enabled. Smaller values remain inline in SST files.
    /// Lower thresholds reduce write amplification (compactions only rewrite small
    /// BlobIndex pointers) at the cost of an extra read indirection on cache misses.
    ///
    /// Only takes effect when `rocksdb-enable-blob-separation` is true.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    rocksdb_blob_min_size: Option<NonZeroByteCount>,

    /// Disable compression for blob files
    ///
    /// When false (default), blob files are compressed with Zstd. Set to true to
    /// store blobs uncompressed, trading higher disk usage for reduced CPU overhead
    /// on writes and blob-GC reads.
    ///
    /// Only takes effect when `rocksdb-enable-blob-separation` is true.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub rocksdb_disable_blob_compression: bool,

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

    /// # Write Batch (in bytes)
    ///
    /// If unset, a reasonable value is automatically derived from the memory budget
    /// available for memtables of the log-server.
    ///
    /// If `write-batch-size` is set, the write batch will be limited to whichever
    /// of the two is reached first.
    #[serde(skip_serializing_if = "Option::is_none")]
    write_batch_bytes: Option<NonZeroByteCount>,

    /// # Write Batch (count)
    ///
    /// The number of records to batch in a single write batch. This is a (soft) upper bound
    /// and the actual number of records may be lower in low-throughput scenarios or if
    /// the write batch size (in bytes) is reached first.
    ///
    /// If unset, the write batch will only be limited by its size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_batch_count: Option<NonZeroUsize>,
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

    pub fn write_batch_bytes(&self) -> NonZeroByteCount {
        // The assumption here is that most of the bytes will go into
        // the data column family.
        //
        // todo(asoli): replace 4 with a shared const (or config). We divide by 4
        // to cap the batch size to a single memtable's worth of data at most
        // (as we allow total of 4 memtables for the data CF).
        self.write_batch_bytes.unwrap_or_else(|| {
            // we divide by 4 to get the size of a single memtable, then by 4
            // to put an upper bound over the ballooning per individual memtables to 25%
            // over its budget.
            self.rocksdb_data_memtables_budget().div_ceil(8)
        })
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

    /// Minimum value size for blob separation. Defaults to 4 KiB.
    pub fn rocksdb_blob_min_size(&self) -> NonZeroByteCount {
        self.rocksdb_blob_min_size
            .unwrap_or(NonZeroByteCount::new(NonZeroUsize::new(4 * 1024).unwrap()))
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
            rocksdb_blob_min_size: None,
            rocksdb_disable_blob_compression: false,
            rocksdb_max_background_flushes: None,
            rocksdb_max_background_compactions: None,
            write_batch_bytes: None,
            write_batch_count: None,
        }
    }
}
