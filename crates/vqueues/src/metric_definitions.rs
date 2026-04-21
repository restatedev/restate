// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, counter, describe_counter};

pub const VQUEUE_ENQUEUE: &str = "restate.vqueue.scheduler.enqueue.total";
pub const VQUEUE_SCHEDULER_DECISION: &str = "restate.vqueue.scheduler.decision.total";
pub const VQUEUE_CONFIRMED: &str = "restate.vqueue.scheduler.decision_confirmed.total";
pub const VQUEUE_INVOKER_MEMORY_WAIT_MS: &str =
    "restate.vqueue.scheduler.invoker_memory_wait_ms.total";
pub const VQUEUE_INVOKER_CONCURRENCY_WAIT_MS: &str =
    "restate.vqueue.scheduler.invoker_concurrency_wait_ms.total";

// Node-level scheduler throttling (affects resume/start)
pub const VQUEUE_GLOBAL_THROTTLE_WAIT_MS: &str =
    "restate.vqueue.scheduler.global_throttle_ms.total";
// Per vqueue start throttling (affects starts only)
pub const VQUEUE_LOCAL_THROTTLE_WAIT_MS: &str = "restate.vqueue.scheduler.vqueue_throttle_ms.total";

pub const ACTION_YIELD: &str = "yield";
pub const ACTION_RUN: &str = "run";

pub fn describe_metrics() {
    describe_counter!(
        VQUEUE_ENQUEUE,
        Unit::Count,
        "Number of entries/invocations in vqueues added to the waiting inbox"
    );

    describe_counter!(
        VQUEUE_SCHEDULER_DECISION,
        Unit::Count,
        "Number of entries in vqueues scheduler, broken down by decision"
    );

    describe_counter!(
        VQUEUE_CONFIRMED,
        Unit::Count,
        "Number of entries/invocations in vqueues where the run request was confirmed"
    );

    describe_counter!(
        VQUEUE_INVOKER_CONCURRENCY_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent waiting for global invoker capacity"
    );

    describe_counter!(
        VQUEUE_INVOKER_MEMORY_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent waiting for invoker memory pool"
    );

    describe_counter!(
        VQUEUE_GLOBAL_THROTTLE_WAIT_MS,
        Unit::Count,
        "Cumulative number of seconds vqueues waited because of global start/resume throttling"
    );

    describe_counter!(
        VQUEUE_LOCAL_THROTTLE_WAIT_MS,
        Unit::Count,
        "Cumulative number of seconds vqueues waited because of their self-imposed start throttling"
    );
}

pub fn publish_scheduler_decision_metrics(num_run: u32, num_yield: u32) {
    counter!(VQUEUE_SCHEDULER_DECISION, "action" => ACTION_RUN).increment(num_run as u64);
    counter!(VQUEUE_SCHEDULER_DECISION, "action" => ACTION_YIELD).increment(num_yield as u64);
}
