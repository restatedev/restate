// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU32, NonZeroUsize};

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_serde_util::NonZeroByteCount;

#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize, derive_builder::Builder, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "RocksDbOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
// NOTE: Prefix with rocksdb_
pub struct RocksDbOptions {
    /// # Disable Direct IO for reads
    ///
    /// Files will be opened in "direct I/O" mode
    /// which means that data r/w from the disk will not be cached or
    /// buffered. The hardware buffer of the devices may however still
    /// be used. Memory mapped files are not impacted by these parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_direct_io_for_reads: Option<bool>,

    /// # Disable Direct IO for flush and compactions
    ///
    /// Use O_DIRECT for writes in background flush and compactions.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_direct_io_for_flush_and_compactions: Option<bool>,

    /// # Disable WAL
    ///
    /// The default depends on the different rocksdb use-cases at Restate.
    ///
    /// Supports hot-reloading (Partial / Bifrost only)
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_wal: Option<bool>,

    /// Disable rocksdb statistics collection
    ///
    /// Default: False (statistics enabled)
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_statistics: Option<bool>,

    /// # RocksDB max background jobs (flushes and compactions)
    ///
    /// Default: the number of CPU cores on this node.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_max_background_jobs: Option<NonZeroU32>,

    /// # RocksDB compaction readahead size in bytes
    ///
    /// If non-zero, we perform bigger reads when doing compaction. If you're
    /// running RocksDB on spinning disks, you should set this to at least 2MB.
    /// That way RocksDB's compaction is doing sequential instead of random reads.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_compaction_readahead_size: Option<NonZeroUsize>,

    /// # RocksDB disable auto compactions
    ///
    /// Disables rocksdb automatic compactions. This can be useful in performance tuning
    /// tests but should not be used in production to avoid write stall.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    rocksdb_disable_auto_compactions: Option<bool>,

    /// # RocksDB statistics level
    ///
    /// StatsLevel can be used to reduce statistics overhead by skipping certain
    /// types of stats in the stats collection process.
    ///
    /// Default: "except-detailed-timers"
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_statistics_level: Option<StatisticsLevel>,

    /// # RocksDB log level
    ///
    /// Verbosity of the LOG.
    ///
    /// Default: "error"
    rocksdb_log_level: Option<RocksDbLogLevel>,

    /// # RocksDB log keep file num
    ///
    /// Number of info LOG files to keep
    ///
    /// Default: 1
    rocksdb_log_keep_file_num: Option<usize>,

    /// # RocksDB log max file size
    ///
    /// Max size of info LOG file
    ///
    /// Default: 64MB
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_log_max_file_size: Option<NonZeroByteCount>,

    /// # RocksDB block size
    ///
    /// Uncompressed block size
    ///
    /// Default: 64KiB
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_block_size: Option<NonZeroUsize>,
}

/// Verbosity of the LOG.
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[derive(Debug, Clone, Copy, Hash, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
#[repr(i32)]
pub enum RocksDbLogLevel {
    Debug = 0,
    Info,
    Warn,
    #[default]
    Error,
    Fatal,
    Header,
}

impl RocksDbOptions {
    pub fn apply_common(&mut self, common: &RocksDbOptions) {
        if self.rocksdb_disable_direct_io_for_reads.is_none() {
            self.rocksdb_disable_direct_io_for_reads =
                Some(common.rocksdb_disable_direct_io_for_reads());
        }
        if self
            .rocksdb_disable_direct_io_for_flush_and_compactions
            .is_none()
        {
            self.rocksdb_disable_direct_io_for_flush_and_compactions =
                Some(common.rocksdb_disable_direct_io_for_flush_and_compaction());
        }
        if self.rocksdb_disable_wal.is_none() {
            self.rocksdb_disable_wal = Some(common.rocksdb_disable_wal());
        }
        if self.rocksdb_disable_statistics.is_none() {
            self.rocksdb_disable_statistics = Some(common.rocksdb_disable_statistics());
        }
        if self.rocksdb_max_background_jobs.is_none() {
            self.rocksdb_max_background_jobs = Some(common.rocksdb_max_background_jobs());
        }
        if self.rocksdb_compaction_readahead_size.is_none() {
            self.rocksdb_compaction_readahead_size =
                Some(common.rocksdb_compaction_readahead_size());
        }
        if self.rocksdb_disable_auto_compactions.is_none() {
            self.rocksdb_disable_auto_compactions = Some(common.rocksdb_disable_auto_compactions());
        }
        if self.rocksdb_statistics_level.is_none() {
            self.rocksdb_statistics_level = Some(common.rocksdb_statistics_level());
        }
        if self.rocksdb_log_level.is_none() {
            self.rocksdb_log_level = Some(common.rocksdb_log_level());
        }
        if self.rocksdb_log_keep_file_num.is_none() {
            self.rocksdb_log_keep_file_num = Some(common.rocksdb_log_keep_file_num());
        }
        if self.rocksdb_log_max_file_size.is_none() {
            self.rocksdb_log_max_file_size = Some(common.rocksdb_log_max_file_size());
        }
        if self.rocksdb_block_size.is_none() {
            self.rocksdb_block_size = Some(common.rocksdb_block_size());
        }
    }

    pub fn rocksdb_disable_wal(&self) -> bool {
        self.rocksdb_disable_wal.unwrap_or(false)
    }

    pub fn rocksdb_disable_direct_io_for_reads(&self) -> bool {
        self.rocksdb_disable_direct_io_for_reads.unwrap_or(true)
    }

    pub fn rocksdb_disable_direct_io_for_flush_and_compaction(&self) -> bool {
        self.rocksdb_disable_direct_io_for_flush_and_compactions
            .unwrap_or(true)
    }

    pub fn rocksdb_disable_statistics(&self) -> bool {
        self.rocksdb_disable_statistics.unwrap_or(false)
    }

    pub fn rocksdb_max_background_jobs(&self) -> NonZeroU32 {
        self.rocksdb_max_background_jobs.unwrap_or(
            std::thread::available_parallelism()
                .unwrap_or(NonZeroUsize::new(2).unwrap())
                .try_into()
                .expect("number of cpu cores fits in u32"),
        )
    }

    pub fn rocksdb_compaction_readahead_size(&self) -> NonZeroUsize {
        self.rocksdb_compaction_readahead_size
            .unwrap_or(NonZeroUsize::new(2_000_000).unwrap())
    }

    pub fn rocksdb_disable_auto_compactions(&self) -> bool {
        self.rocksdb_disable_auto_compactions.unwrap_or(false)
    }

    pub fn rocksdb_statistics_level(&self) -> StatisticsLevel {
        self.rocksdb_statistics_level
            .unwrap_or(StatisticsLevel::ExceptTimers)
    }

    pub fn rocksdb_log_level(&self) -> RocksDbLogLevel {
        self.rocksdb_log_level.unwrap_or_default()
    }

    pub fn rocksdb_log_keep_file_num(&self) -> usize {
        self.rocksdb_log_keep_file_num.unwrap_or(1)
    }

    pub fn rocksdb_log_max_file_size(&self) -> NonZeroByteCount {
        self.rocksdb_log_max_file_size
            .unwrap_or(NonZeroByteCount::new(
                NonZeroUsize::new(64_000_000).expect("is valid size"),
            ))
    }

    pub fn rocksdb_block_size(&self) -> NonZeroUsize {
        self.rocksdb_block_size
            .unwrap_or(NonZeroUsize::new(64 * 1024).unwrap())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "RocksbStatistics"))]
#[serde(rename_all = "kebab-case")]
pub enum StatisticsLevel {
    /// Disable all metrics
    DisableAll,
    /// Disable timer stats, and skip histogram stats
    ExceptHistogramOrTimers,
    /// Skip timer stats
    ExceptTimers,
    /// Collect all stats except time inside mutex lock AND time spent on
    /// compression.
    ExceptDetailedTimers,
    /// Collect all stats except the counters requiring to get time inside the
    /// mutex lock.
    ExceptTimeForMutex,
    /// Collect all stats, including measuring duration of mutex operations.
    /// If getting time is expensive on the platform to run, it can
    /// reduce scalability to more threads, especially for writes.
    All,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "RocksbPerfStatisticsLevel"))]
#[serde(rename_all = "kebab-case")]
pub enum PerfStatsLevel {
    /// Disable perf stats
    Disable,
    /// Enables only count stats
    EnableCount,
    /// Count stats and enable time stats except for mutexes
    EnableTimeExceptForMutex,
    /// Other than time, also measure CPU time counters. Still don't measure
    /// time (neither wall time nor CPU time) for mutexes
    EnableTimeAndCPUTimeExceptForMutex,
    /// Enables count and time stats
    EnableTime,
}
