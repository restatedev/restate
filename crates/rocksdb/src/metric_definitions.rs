// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub const STORAGE_BG_TASK_IN_FLIGHT: &str = "restate.rocksdb_manager.bg_task_in_flight.total";
pub const STORAGE_IO_OP: &str = "restate.rocksdb_manager.io_operation.total";
pub const STORAGE_BG_TASK_WAIT_DURATION: &str =
    "restate.rocksdb_manager.bg_task_wait_duration.seconds";

pub const STORAGE_BG_TASK_RUN_DURATION: &str =
    "restate.rocksdb_manager.bg_task_run_duration.seconds";

// Perf guard metrics
pub const BLOCK_READ_BYTES: &str = "restate.rocksdb.perf.block_read_bytes.total";
pub const BLOCK_READ_DURATION: &str = "restate.rocksdb.perf.block_read_duration.seconds";
pub const BLOCK_DECOMPRESS_DURATION: &str =
    "restate.rocksdb.perf.block_decompress_duration.seconds";
pub const GET_FROM_MEMTABLE_DURATION: &str =
    "restate.rocksdb.perf.get_from_memtable_duration.seconds";
pub const WRITE_WAL_DURATION: &str = "restate.rocksdb.perf.write_wal_duration.seconds";
pub const WRITE_MEMTABLE_DURATION: &str = "restate.rocksdb.perf.write_memtable_duration.seconds";
pub const TOTAL_DURATION: &str = "restate.rocksdb.perf.total_duration.seconds";
pub const SEEK_ON_MEMTABLE: &str = "restate.rocksdb.perf.seek_on_memtable.seconds";
pub const NEXT_ON_MEMTABLE: &str = "restate.rocksdb.perf.next_on_memtable.total";
pub const FIND_NEXT_USER_ENTRY: &str = "restate.rocksdb.perf.find_next_user_entry.seconds";
pub const WRITE_PRE_AND_POST_DURATION: &str =
    "restate.rocksdb.perf.write_pre_and_post_duration.seconds";
pub const WRITE_ARTIFICIAL_DELAY_DURATION: &str =
    "restate.rocksdb.perf.write_artificial_delay_duration.seconds";

pub const OP_TYPE: &str = "operation";
pub const OP_NAME: &str = "name";
pub const PRIORITY: &str = "priority";

pub const DISPOSITION: &str = "disposition";

pub const DISPOSITION_MAYBE_BLOCKING: &str = "maybe-blocking";
pub const DISPOSITION_NON_BLOCKING: &str = "non-blocking";
pub const DISPOSITION_BACKGROUND: &str = "background";
pub const DISPOSITION_MOVED_TO_BG: &str = "moved-to-bg";
pub const DISPOSITION_FAILED: &str = "failed";

pub fn describe_metrics() {
    describe_gauge!(
        STORAGE_BG_TASK_IN_FLIGHT,
        Unit::Count,
        "Number of background storage tasks in-flight"
    );

    describe_counter!(
        STORAGE_IO_OP,
        Unit::Count,
        "Number of foreground rocksdb operations, label 'disposition' defines how IO was actually handled. Options are 'maybe-blocking', 'non-blocking', 'moved-to-bg'"
    );

    describe_counter!(
        BLOCK_READ_BYTES,
        Unit::Bytes,
        "Total number of bytes read from disk during this operation"
    );

    describe_counter!(
        NEXT_ON_MEMTABLE,
        Unit::Count,
        "Number of next() issued on memtables"
    );

    describe_histogram!(
        STORAGE_BG_TASK_WAIT_DURATION,
        Unit::Seconds,
        "Queueing time of storage task queues, with 'priority' label"
    );

    describe_histogram!(
        STORAGE_BG_TASK_RUN_DURATION,
        Unit::Seconds,
        "Run time of storage tasks, with 'priority' label"
    );

    describe_histogram!(
        WRITE_WAL_DURATION,
        Unit::Seconds,
        "Time spent writing to WAL"
    );

    describe_histogram!(
        WRITE_MEMTABLE_DURATION,
        Unit::Seconds,
        "Time spent writing to memtable"
    );

    describe_histogram!(
        WRITE_PRE_AND_POST_DURATION,
        Unit::Seconds,
        "Time spent in pre/post write operations by rocksdb"
    );

    describe_histogram!(
        WRITE_ARTIFICIAL_DELAY_DURATION,
        Unit::Seconds,
        "Extra write delay introduced by rocksdb to meet target write rates"
    );

    describe_histogram!(
        SEEK_ON_MEMTABLE,
        Unit::Seconds,
        "Total time spent seeking on memtable"
    );
    describe_histogram!(
        BLOCK_READ_DURATION,
        Unit::Seconds,
        "Total time spent reading blocks"
    );

    describe_histogram!(
        BLOCK_DECOMPRESS_DURATION,
        Unit::Seconds,
        "Total time spent block decompression"
    );

    describe_histogram!(
        GET_FROM_MEMTABLE_DURATION,
        Unit::Seconds,
        "Total time spent on querying memtables"
    );

    describe_histogram!(
        FIND_NEXT_USER_ENTRY,
        Unit::Seconds,
        "Total time spent on iterating internal entries to find the next user entry"
    );
}
