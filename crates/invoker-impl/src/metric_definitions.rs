// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Optional to have but adds description/help message to the metrics emitted to
/// the metrics' sink.
use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub const INVOKER_ENQUEUE: &str = "restate.invoker.enqueue.total";
pub const INVOKER_PENDING_TASKS: &str = "restate.invoker.pending_tasks";
pub const INVOKER_INVOCATION_TASKS: &str = "restate.invoker.invocation_tasks.total";
pub const INVOKER_AVAILABLE_SLOTS: &str = "restate.invoker.available_slots";
pub const INVOKER_CONCURRENCY_LIMIT: &str = "restate.invoker.concurrency_limit";
pub const INVOKER_TASK_DURATION: &str = "restate.invoker.task_duration.seconds";
pub const INVOKER_TASKS_IN_FLIGHT: &str = "restate.invoker.inflight_tasks";
pub const INVOKER_DEPLOYMENT_UNREACHABLE_ERRORS: &str =
    "restate.invoker.deployment_unreachable_errors.total";

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
        INVOKER_PENDING_TASKS,
        Unit::Count,
        "Number of pending invocation tasks queued"
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

    describe_gauge!(
        INVOKER_AVAILABLE_SLOTS,
        Unit::Count,
        "Number of available slots to create new tasks"
    );

    describe_histogram!(
        INVOKER_TASK_DURATION,
        Unit::Seconds,
        "Time taken to complete an invoker task"
    );

    describe_gauge!(
        INVOKER_TASKS_IN_FLIGHT,
        Unit::Count,
        "Number of inflight invoker tasks"
    );

    describe_counter!(
        INVOKER_DEPLOYMENT_UNREACHABLE_ERRORS,
        Unit::Count,
        "Number of deployment down errors"
    );
}
