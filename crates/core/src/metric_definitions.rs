// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(unused)]

use metrics::{Unit, describe_counter, describe_gauge};

// value of label `kind` in TC_SPAWN are defined in [`crate::TaskKind`].
pub const TC_SPAWN: &str = "restate.task_center.spawned.total";
pub const TC_FINISHED: &str = "restate.task_center.finished.total";

pub const SCHEMA_SIZE_BYTES: &str = "restate.schema.size.bytes";

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

    describe_gauge!(
        SCHEMA_SIZE_BYTES,
        Unit::Bytes,
        "Size of the latest schema this node decoded, after decompression. Decoding cost grows with this size"
    );
}
