// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "WorkerRocksDbOptions", default)
)]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
// NOTE: Prefix with rocksdb_
pub struct RocksDbOptions {
    /// # Threads
    ///
    /// The number of threads to reserve to Rocksdb background tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_num_threads: Option<usize>,

    /// # Write Buffer size
    ///
    /// The size of a single memtable. Once memtable exceeds this size, it is marked immutable and a new one is created.
    /// The default is set such that 3 column families per table will use a total of 50% of the global memory limit
    /// (`MEMORY_LIMIT`), which defaults to 3GiB, leading to a value of 64MiB with 8 tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_write_buffer_size: Option<usize>,

    /// # Maximum total WAL size
    ///
    /// Max WAL size, that after this Rocksdb start flushing mem tables to disk.
    /// Default is 2GB.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_max_total_wal_size: Option<u64>,

    /// # Disable WAL
    ///
    /// The default depends on the different rocksdb use-cases at Restate.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_wal: Option<bool>,

    /// # Maximum cache size
    ///
    /// The memory size used for rocksdb caches. Default is 1GB.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_cache_size: Option<usize>,

    /// Disable rocksdb statistics collection
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_statistics: Option<bool>,
}

impl RocksDbOptions {
    pub fn apply_common(&mut self, common: &RocksDbOptions) {
        // apply memory limits?
        if self.rocksdb_num_threads.is_none() {
            self.rocksdb_num_threads = Some(common.rocksdb_num_threads());
        }
        if self.rocksdb_write_buffer_size.is_none() {
            self.rocksdb_write_buffer_size = Some(common.rocksdb_write_buffer_size());
        }
        if self.rocksdb_max_total_wal_size.is_none() {
            self.rocksdb_max_total_wal_size = Some(common.rocksdb_max_total_wal_size());
        }
        if self.rocksdb_disable_wal.is_none() {
            self.rocksdb_disable_wal = Some(common.rocksdb_disable_wal());
        }
        if self.rocksdb_cache_size.is_none() {
            self.rocksdb_cache_size = Some(common.rocksdb_cache_size());
        }
        if self.rocksdb_disable_statistics.is_none() {
            self.rocksdb_disable_statistics = Some(common.rocksdb_disable_statistics());
        }
    }

    pub fn rocksdb_num_threads(&self) -> usize {
        self.rocksdb_num_threads.unwrap_or(10)
    }

    pub fn rocksdb_write_buffer_size(&self) -> usize {
        self.rocksdb_write_buffer_size.unwrap_or(0)
    }

    pub fn rocksdb_max_total_wal_size(&self) -> u64 {
        self.rocksdb_max_total_wal_size.unwrap_or(2 * (1 << 30))
    }

    pub fn rocksdb_disable_wal(&self) -> bool {
        self.rocksdb_disable_wal.unwrap_or(true)
    }

    pub fn rocksdb_cache_size(&self) -> usize {
        self.rocksdb_cache_size.unwrap_or(1 << 30)
    }

    pub fn rocksdb_disable_statistics(&self) -> bool {
        self.rocksdb_disable_statistics.unwrap_or(false)
    }
}
