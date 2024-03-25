// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Options {
    pub rocksdb_threads: usize,
    pub rocksdb_disable_statistics: bool,
    pub rocksdb_disable_wal: bool,
    pub rocksdb_cache_size: usize,
    pub rocksdb_max_total_wal_size: u64,
    pub rocksdb_write_buffer_size: usize,
    /// Trigger a commit when the batch size exceeds this threshold. Set to 0 or 1 to commit the
    /// write batch on every command.
    pub writer_commit_batch_size_threshold: usize,
    /// Trigger a commit when the time since the last commit exceeds this threshold.
    pub writer_commit_time_interval: Duration,
    /// The maximum number of write commands that can be queued.
    pub writer_queue_len: usize,
    /// If true, rocksdb flushes follow writing record batches, otherwise, we
    /// fallback to rocksdb automatic WAL flushes.
    pub flush_wal_on_commit: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            rocksdb_threads: 10,
            // todo: enable when we have a way to expose the statistics through node-ctrl
            rocksdb_disable_statistics: true,
            rocksdb_disable_wal: false,
            rocksdb_cache_size: 0,
            rocksdb_max_total_wal_size: 2 * (1 << 30), // 2 GiB
            rocksdb_write_buffer_size: 0,
            writer_commit_batch_size_threshold: 200,
            writer_commit_time_interval: Duration::from_millis(13),
            writer_queue_len: 200,
            flush_wal_on_commit: true,
        }
    }
}
