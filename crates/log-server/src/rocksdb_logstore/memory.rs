// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_memory::NonZeroByteCount;
use restate_types::config::LogServerOptions;

// We'll use 25% extra memory in the worst case, but will reduce write stalls.
pub const MAX_WRITE_BUFFERS: u32 = 12;
pub const NOMINAL_WRITE_BUFFERS: u32 = 8;
// merge 2 memtables when flushing to L0
pub const WRITE_BUFFERS_TO_MERGE: u32 = 1;
// start promoting L0->L1 as soon as possible. each file on level0 is
// (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
// memtable_memory_budget.
pub const LEVEL_ZERO_FILE_NUM_COMPACTION_TRIGGER: u32 = 8;
/// Matches rocksdb default (target_file_size_base * 25)
pub const COMPACTION_BYTES_MULTIPLIER: u32 = 25;
/// Try to keep the table files above this size if partition write buffers are too small
pub const MIN_FILE_SIZE: usize = 16 * 1024 * 1024;
/// The absolute minimum write buffer size
pub const MIN_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;

pub struct LogStoreMemoryConfig {
    memory_budget: NonZeroByteCount,
    max_file_size: usize,
}

impl LogStoreMemoryConfig {
    pub fn calculate(opts: &LogServerOptions) -> Self {
        let memory_budget = opts.rocksdb_data_memtables_budget();
        Self {
            memory_budget,
            max_file_size: MIN_FILE_SIZE.max(opts.rocksdb_max_file_size.as_usize()),
        }
    }

    pub fn memory_budget(&self) -> NonZeroByteCount {
        self.memory_budget
    }

    pub fn write_buffer_size(&self) -> usize {
        MIN_WRITE_BUFFER_SIZE.max(self.memory_budget.as_usize() / NOMINAL_WRITE_BUFFERS as usize)
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
            .clamp(MIN_FILE_SIZE, self.max_file_size)
    }

    pub fn max_compaction_bytes(&self) -> usize {
        self.target_file_size_base() * COMPACTION_BYTES_MULTIPLIER as usize
    }

    pub fn max_wal_total_size(&self) -> usize {
        const SAFETY_MULTIPLIER: usize = 8;
        self.write_buffer_size() * self.max_write_buffer_number() as usize * SAFETY_MULTIPLIER
    }
}
