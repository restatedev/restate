// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
pub const VQUEUE_RUN_CONFIRMED: &str = "restate.vqueue.scheduler.run_confirmed.total";
pub const VQUEUE_RUN_REJECTED: &str = "restate.vqueue.scheduler.run_rejected.total";

pub const ACTION_YIELD: &str = "yield";
pub const ACTION_RESUME: &str = "resume";
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
        VQUEUE_RUN_CONFIRMED,
        Unit::Count,
        "Number of entries/invocations in vqueues where the run request was confirmed"
    );

    describe_counter!(
        VQUEUE_RUN_REJECTED,
        Unit::Count,
        "Number of entries/invocations in vqueues where the run request was rejected"
    );
}

pub fn publish_scheduler_decision_metrics(num_run: usize, num_yield: usize, num_resume: usize) {
    counter!(VQUEUE_SCHEDULER_DECISION, "action" => ACTION_RUN).increment(num_run as u64);
    counter!(VQUEUE_SCHEDULER_DECISION, "action" => ACTION_RESUME).increment(num_resume as u64);
    counter!(VQUEUE_SCHEDULER_DECISION, "action" => ACTION_YIELD).increment(num_yield as u64);
}
