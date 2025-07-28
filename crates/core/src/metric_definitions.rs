// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge, gauge};

// value of label `kind` in TC_SPAWN are defined in [`crate::TaskKind`].
pub const TC_SPAWN: &str = "restate.task_center.spawned.total";
pub const TC_FINISHED: &str = "restate.task_center.finished.total";
pub const PROCESS_START_TIME_SECONDS: &str = "restate.process_start_time_seconds";

// values of label `status` in METRICS
pub const STATUS_COMPLETED: &str = "completed";
pub const STATUS_FAILED: &str = "failed";

pub fn describe_metrics() {
    describe_counter!(
        TC_SPAWN,
        Unit::Count,
        "Total tasks spawned by the task center"
    );

    describe_counter!(
        TC_FINISHED,
        Unit::Count,
        "Number of tasks that finished with 'status'"
    );

    // restate_process_start_time_seconds is used for usage calculations.
    describe_gauge!(
        PROCESS_START_TIME_SECONDS,
        Unit::Seconds,
        "Unix timestamp when the server process was started"
    );

    gauge!(PROCESS_START_TIME_SECONDS).set(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as f64, // integer seconds to avoid comparison issues
    );
}
