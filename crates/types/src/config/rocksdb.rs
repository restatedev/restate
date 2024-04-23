// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use restate_serde_util::ByteCount;

#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "RocksDbOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
// NOTE: Prefix with rocksdb_
pub struct RocksDbOptions {
    /// # Write Buffer size
    ///
    /// The size of a single memtable. Once memtable exceeds this size, it is marked
    /// immutable and a new one is created. Default is 256MB per memtable.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<ByteCount<false>>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<ByteCount<false>>"))]
    rocksdb_write_buffer_size: Option<usize>,

    /// # Maximum total WAL size
    ///
    /// Max WAL size, that after this Rocksdb start flushing mem tables to disk.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<ByteCount<false>>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<ByteCount<false>>"))]
    rocksdb_max_total_wal_size: Option<u64>,

    /// # Disable WAL
    ///
    /// The default depends on the different rocksdb use-cases at Restate.
    ///
    /// Supports hot-reloading (Partial / Bifrost only)
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_wal: Option<bool>,

    /// # Flush WAL in batches
    ///
    /// when WAL is enabled, this allows Restate server to control WAL flushes in batches.
    /// This trades off latency for IO throughput.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_batch_wal_flushes: Option<bool>,

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
    #[serde_as(as = "Option<ByteCount<true>>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<ByteCount<true>>"))]
    rocksdb_compaction_readahead_size: Option<usize>,

    /// # RocksDB statistics level
    ///
    /// StatsLevel can be used to reduce statistics overhead by skipping certain
    /// types of stats in the stats collection process.
    ///
    /// Default: "except-detailed-timers"
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_statistics_level: Option<StatisticsLevel>,
}

impl RocksDbOptions {
    pub fn apply_common(&mut self, common: &RocksDbOptions) {
        // apply memory limits?
        if self.rocksdb_write_buffer_size.is_none() {
            self.rocksdb_write_buffer_size = Some(common.rocksdb_write_buffer_size());
        }
        if self.rocksdb_max_total_wal_size.is_none() {
            self.rocksdb_max_total_wal_size = Some(common.rocksdb_max_total_wal_size());
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
        if self.rocksdb_statistics_level.is_none() {
            self.rocksdb_statistics_level = Some(common.rocksdb_statistics_level());
        }
    }

    pub fn rocksdb_write_buffer_size(&self) -> usize {
        self.rocksdb_write_buffer_size.unwrap_or(256_000_000) // 256MB
    }

    pub fn rocksdb_max_total_wal_size(&self) -> u64 {
        self.rocksdb_max_total_wal_size.unwrap_or(2_000_000_000)
    }

    pub fn rocksdb_disable_wal(&self) -> bool {
        self.rocksdb_disable_wal.unwrap_or(true)
    }

    pub fn rocksdb_disable_statistics(&self) -> bool {
        self.rocksdb_disable_statistics.unwrap_or(false)
    }

    pub fn rocksdb_batch_wal_flushes(&self) -> bool {
        self.rocksdb_batch_wal_flushes.unwrap_or(true)
    }

    pub fn rocksdb_max_background_jobs(&self) -> NonZeroU32 {
        self.rocksdb_max_background_jobs.unwrap_or(
            std::thread::available_parallelism()
                .unwrap_or(NonZeroUsize::new(2).unwrap())
                .try_into()
                .expect("number of cpu cores fits in u32"),
        )
    }

    pub fn rocksdb_compaction_readahead_size(&self) -> usize {
        self.rocksdb_compaction_readahead_size.unwrap_or(2_000_000)
    }

    pub fn rocksdb_statistics_level(&self) -> StatisticsLevel {
        self.rocksdb_statistics_level
            .unwrap_or(StatisticsLevel::ExceptDetailedTimers)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
