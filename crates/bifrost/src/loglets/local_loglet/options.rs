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

#[derive(Debug, Serialize, Deserialize)]
pub struct Options {
    pub rocksdb_threads: usize,
    pub rocksdb_disable_statistics: bool,
    pub rocksdb_disable_wal: bool,
    pub rocksdb_cache_size: usize,
    pub rocksdb_max_total_wal_size: u64,
    pub rocksdb_write_buffer_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            rocksdb_threads: 10,
            rocksdb_disable_statistics: false,
            rocksdb_disable_wal: false,
            rocksdb_cache_size: 0,
            rocksdb_max_total_wal_size: 2 * (1 << 30), // 2 GiB
            rocksdb_write_buffer_size: 0,
        }
    }
}
