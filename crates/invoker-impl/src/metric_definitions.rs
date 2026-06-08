// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub const INVOKER_ENQUEUE: &str = "restate.invoker.enqueue.total";
pub const INVOKER_INVOCATION_TASKS: &str = "restate.invoker.invocation_tasks.total";
pub const INVOKER_CONCURRENCY_SLOTS_ACQUIRED: &str = "restate.invoker.concurrency_slots.acquired";
pub const INVOKER_CONCURRENCY_SLOTS_RELEASED: &str = "restate.invoker.concurrency_slots.released";
pub const INVOKER_CONCURRENCY_LIMIT: &str = "restate.invoker.concurrency_limit";
pub const INVOKER_TASK_DURATION: &str = "restate.invoker.task_duration.seconds";
pub const INVOKER_EAGER_STATE_TRUNCATED: &str = "restate.invoker.eager_state_truncated.total";

pub const TASK_OP_STARTED: &str = "started";
pub const TASK_OP_SUSPENDED: &str = "suspended";
pub const TASK_OP_FAILED: &str = "failed";
pub const TASK_OP_COMPLETED: &str = "completed";

pub(crate) fn describe_metrics() {
    describe_counter!(
        INVOKER_ENQUEUE,
        Unit::Count,
        "Number of invocations that were added to the queue"
    );

    describe_gauge!(
        INVOKER_CONCURRENCY_LIMIT,
        Unit::Count,
        "Concurrency limit (slots) for invoker tasks"
    );

    describe_counter!(
        INVOKER_INVOCATION_TASKS,
        Unit::Count,
        "Invocation task operation"
    );

    describe_counter!(
        INVOKER_CONCURRENCY_SLOTS_ACQUIRED,
        Unit::Count,
        "Number of concurrency slots acquired"
    );

    describe_counter!(
        INVOKER_CONCURRENCY_SLOTS_RELEASED,
        Unit::Count,
        "Number of concurrency slots released"
    );

    describe_histogram!(
        INVOKER_TASK_DURATION,
        Unit::Seconds,
        "Time taken to complete an invoker task"
    );

    describe_counter!(
        INVOKER_EAGER_STATE_TRUNCATED,
        Unit::Count,
        "Number of invocations where eager state was truncated due to size limit"
    );
}
