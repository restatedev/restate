// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use metrics::{describe_counter, describe_gauge, Unit};

pub const INVOKER_ENQUEUE: &str = "restate.invoker.enqueue.total";
pub const INVOKER_INVOCATION_TASK_STARTED: &str = "restate.invoker.invocation_task_started.total";
pub const INVOKER_INVOCATION_TASK_FAILED: &str = "restate.invoker.invocation_task_failed.total";
pub const INVOKER_INVOCATION_TASK_SUSPENDED: &str = "restate.invoker.invocation_task_suspended.total";
pub const INVOKER_INFLIGHT_INVOCATIONS: &str = "restate.invoker.inflight_invocations.total";

pub(crate) fn describe_metrics() {
    describe_counter!(
        INVOKER_ENQUEUE,
        Unit::Count,
        "Number of invocations that were added to the queue"
    );

    describe_counter!(
        INVOKER_INVOCATION_TASK_STARTED,
        Unit::Count,
        "Number of active invocation tasks started"
    );

    describe_counter!(
        INVOKER_INVOCATION_TASK_FAILED,
        Unit::Count,
        "Number of active invocation tasks failed"
    );

    describe_counter!(
        INVOKER_INVOCATION_TASK_SUSPENDED,
        Unit::Count,
        "Number of active invocation tasks suspended"
    );

    describe_gauge!(
        INVOKER_INFLIGHT_INVOCATIONS,
        Unit::Count,
        "Number of actively executing invocation tasks"
    );
}
