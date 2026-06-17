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
use tracing::warn;

use restate_util_bytecount::NonZeroByteCount;

use super::{BackgroundWorkBudget, CommonOptions, RocksDbOptions};

const MIN_ROCKSDB_MEMORY: NonZeroByteCount =
    NonZeroByteCount::new(NonZeroUsize::new(32 * 1024 * 1024).unwrap());
// We'll use 50% extra memory in the worst case, but will reduce write stalls.
const MAX_WRITE_BUFFERS: u32 = 12;
const NOMINAL_WRITE_BUFFERS: u32 = 8;
const WRITE_BUFFERS_TO_MERGE: u32 = 1;
const LEVEL_ZERO_FILE_NUM_COMPACTION_TRIGGER: u32 = 8;
/// Matches rocksdb default (target_file_size_base * 25)
const COMPACTION_BYTES_MULTIPLIER: u32 = 25;
/// Try to keep the table files above this size if partition write buffers are too small
const MIN_FILE_SIZE: usize = 16 * 1024 * 1024;
/// The absolute minimum write buffer size
const MIN_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;

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
    /// Setting this to 1 means no sub-compactions are allowed.
    ///
    /// Default is 1
    rocksdb_max_sub_compactions: u32,

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
    /// If `write-batch-commit-count` is set, the write batch will be limited to whichever
    /// of the two is reached first. The value is clamped to the size of a single memtable
    /// which is derived from rocksdb-memory-budget.
    #[serde(skip_serializing_if = "Option::is_none")]
    write_batch_commit_bytes: Option<NonZeroByteCount>,

    /// # Write Batch (count)
    ///
    /// The number of records to batch in a single write batch. This is a (soft) upper bound
    /// and the actual number of records may be lower in low-throughput scenarios or if
    /// the write batch size (in bytes) is reached first.
    ///
    /// If unset, the write batch will only be limited by its size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_batch_commit_count: Option<NonZeroUsize>,

    /// Custom Path for WAL files
    ///
    /// If unset, the default data directory is used for WAL files. If set, the path
    /// must point to a directory that exists and is writable. The directory will be
    /// used to store WAL files.
    ///
    /// The recommendation is to use low-latency NVMe SSD for the WAL directory with
    /// sufficient IOPS capacity and bandwidth to your sustained workload. The `fdatasync`
    /// latency of the storage device is a significant factor in the WAL performance if
    /// `rocksdb-disable-wal-fsync` is set to `false`.
    ///
    /// Since v1.7.0
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rocksdb_wal_dir: Option<PathBuf>,

    /// Disable WAL for the log-server
    ///
    /// [DANGEROUS] Not recommended for production use.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    rocksdb_disable_wal: bool,

    /// # Clamps target size for sst files
    ///
    /// The target size for sst files. Restate uses this value to internally determine
    /// the number of memtables to keep in memory and how much to merge before flushing.
    ///
    /// The value is automatically sanitized to 16 MiB if set to a smaller value.
    ///
    /// [default] is 128 MiB
    ///
    /// Since v1.7.0
    pub rocksdb_max_file_size: NonZeroByteCount,

    /// Disable RocksDB paranoid checks
    ///
    /// Since v1.7.0
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rocksdb_disable_paranoid_checks: bool,

    /// Starts in read-only mode
    ///
    /// This is useful for testing, debugging, and development.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,

    /// [DANGEROUS] Starts in in-memory
    /// This is useful for testing, and development.
    ///
    /// Since v1.7.0
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub in_memory: bool,
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
        // for log-server, we only need the budget for compactions, not flushes
        // since flushes are internally capped. This is due to the nature of
        // this database (number of column families).
        if self.rocksdb_max_background_compactions.is_none() {
            self.rocksdb_max_background_compactions = Some(budget.max_background_compactions);
        }
    }

    pub fn rocksdb_max_background_compactions(&self) -> NonZeroU32 {
        self.rocksdb_max_background_compactions
            .unwrap_or(NonZeroU32::new(2).unwrap())
    }

    pub fn rocksdb_disable_wal(&self) -> bool {
        self.rocksdb_disable_wal
    }

    pub fn rocksdb_disable_wal_fsync(&self) -> bool {
        self.rocksdb_disable_wal_fsync
    }

    pub fn rocksdb_max_sub_compactions(&self) -> u32 {
        if self.rocksdb_max_sub_compactions == 0 {
            1
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

    /// Minimum value size for blob separation. Defaults to 512 KiB.
    pub fn rocksdb_blob_min_size(&self) -> NonZeroByteCount {
        self.rocksdb_blob_min_size.unwrap_or(NonZeroByteCount::new(
            NonZeroUsize::new(512 * 1024).unwrap(),
        ))
    }

    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("log-store")
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.rocksdb_wal_dir
            .clone()
            .unwrap_or_else(|| self.data_dir())
    }

    pub fn write_batch_commit_bytes(&self) -> usize {
        // let's decide on the absolute maximum batch size we'd like to accept
        let upper_bound = self.write_buffer_size();

        if let Some(user_limits) = self.write_batch_commit_bytes {
            // sanitize the user-provided limit to the write buffer size
            user_limits.as_usize().min(upper_bound)
        } else {
            // we come up with a reasonable default. That's 10% of the write buffer size
            // to put an upper bound over the ballooning per individual memtables to 10%
            // over its budget.
            self.write_buffer_size().div_ceil(10)
        }
    }

    /// Memory budget for the data service admission-control pool.
    ///
    /// Derived from memory available for write buffers, but clamped to at least `min_size` (the maximum
    /// accepted record/message size). Otherwise a low-memory configuration could size the pool
    /// below the largest valid append and reject it with `CapacityExceeded` before the writer
    /// ever sees it (see `BackPressureMode::Lossy`).
    pub fn data_service_memory_size(&self, min_size: NonZeroByteCount) -> NonZeroByteCount {
        self.rocksdb_memory_budget().max(min_size)
    }

    pub fn write_buffer_size(&self) -> usize {
        MIN_WRITE_BUFFER_SIZE
            .max(self.rocksdb_memory_budget().as_usize() / NOMINAL_WRITE_BUFFERS as usize)
    }

    pub const fn min_write_buffer_number_to_merge(&self) -> u32 {
        WRITE_BUFFERS_TO_MERGE
    }

    pub const fn max_write_buffer_number(&self) -> u32 {
        MAX_WRITE_BUFFERS
    }

    pub const fn level_zero_file_num_compaction_trigger(&self) -> u32 {
        LEVEL_ZERO_FILE_NUM_COMPACTION_TRIGGER
    }

    pub fn max_bytes_for_level_base(&self) -> usize {
        self.write_buffer_size()
            * self.min_write_buffer_number_to_merge() as usize
            * self.level_zero_file_num_compaction_trigger() as usize
    }

    pub fn target_file_size_base(&self) -> usize {
        // Set the target file within the range of acceptable values
        self.write_buffer_size()
            .clamp(MIN_FILE_SIZE, self.max_file_size())
    }

    pub fn max_compaction_bytes(&self) -> usize {
        self.target_file_size_base() * COMPACTION_BYTES_MULTIPLIER as usize
    }

    pub fn max_wal_total_size(&self) -> usize {
        const SAFETY_MULTIPLIER: usize = 8;
        self.write_buffer_size() * self.max_write_buffer_number() as usize * SAFETY_MULTIPLIER
    }

    pub fn max_file_size(&self) -> usize {
        MIN_FILE_SIZE.max(self.rocksdb_max_file_size.as_usize())
    }
}

impl Default for LogServerOptions {
    fn default() -> Self {
        Self {
            read_only: false,
            rocksdb: RocksDbOptions::default(),
            in_memory: false,
            // set by apply_common in runtime
            rocksdb_memory_budget: None,
            rocksdb_memory_ratio: 0.5,
            rocksdb_max_sub_compactions: 1,
            rocksdb_wal_dir: None,
            rocksdb_disable_wal: false,
            rocksdb_disable_wal_fsync: false,
            rocksdb_disable_paranoid_checks: false,
            always_commit_in_background: false,
            #[allow(deprecated)]
            incoming_network_queue_length: None,
            rocksdb_enable_blob_separation: false,
            rocksdb_blob_min_size: None,
            rocksdb_disable_blob_compression: false,
            rocksdb_max_background_compactions: None,
            write_batch_commit_bytes: None,
            write_batch_commit_count: None,
            rocksdb_max_file_size: NonZeroByteCount::new(
                NonZeroUsize::new(128 * 1024 * 1024).unwrap(),
            ),
        }
    }
}
