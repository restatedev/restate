// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{describe_counter, describe_histogram, Unit};

pub const STORAGE_BG_TASK_SPAWNED: &str = "restate.rocksdb_manager.bg_task_spawned.total";
pub const STORAGE_IO_OP: &str = "restate.rocksdb_manager.io_operation.total";
pub const STORAGE_BG_TASK_WAIT_DURATION: &str =
    "restate.rocksdb_manager.bg_task_wait_duration.seconds";

pub const STORAGE_BG_TASK_RUN_DURATION: &str =
    "restate.rocksdb_manager.bg_task_run_duration.seconds";

pub const STORAGE_BG_TASK_TOTAL_DURATION: &str =
    "restate.rocksdb_manager.bg_task_total_duration.seconds";

pub const OP_TYPE: &str = "operation";
pub const OP_WRITE: &str = "write-batch";

pub const DISPOSITION: &str = "disposition";

pub const DISPOSITION_MAYBE_BLOCKING: &str = "maybe-blocking";
pub const DISPOSITION_NON_BLOCKING: &str = "non-blocking";
pub const DISPOSITION_MOVED_TO_BG: &str = "moved-to-bg";
pub const DISPOSITION_FAILED: &str = "failed";

pub fn describe_metrics() {
    describe_counter!(
        STORAGE_BG_TASK_SPAWNED,
        Unit::Count,
        "Number of background storage tasks spawned"
    );

    describe_counter!(
        STORAGE_IO_OP,
        Unit::Count,
        "Number of forground rocksdb operations, label 'disposition' defines how IO was actually handled.
Options are 'maybe-blocking', 'non-blocking', 'moved-to-bg'"
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
        STORAGE_BG_TASK_TOTAL_DURATION,
        Unit::Seconds,
        "Total time to queue+run a storage task, with 'priority' label"
    );
}
