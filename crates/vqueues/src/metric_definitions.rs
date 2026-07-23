// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, counter, describe_counter, describe_gauge, describe_histogram};

pub const VQUEUE_ENQUEUE: &str = "restate.vqueue.scheduler.enqueue.total";
pub const VQUEUE_SCHEDULER_DECISION: &str = "restate.vqueue.scheduler.decision.total";
pub const VQUEUE_RUN_CONFIRMED: &str = "restate.vqueue.scheduler.run_confirmed.total";
pub const VQUEUE_INVOKER_MEMORY_WAIT_MS: &str =
    "restate.vqueue.scheduler.invoker_memory_wait_ms.total";
pub const VQUEUE_INVOKER_CONCURRENCY_WAIT_MS: &str =
    "restate.vqueue.scheduler.invoker_concurrency_wait_ms.total";

// Node-level invoker throttling (affects resume/start)
pub const VQUEUE_INVOKER_THROTTLING_WAIT_MS: &str =
    "restate.vqueue.scheduler.invoker_throttling_wait_ms.total";
// Per-vqueue user-defined throttling rules
pub const VQUEUE_THROTTLING_RULES_WAIT_MS: &str =
    "restate.vqueue.scheduler.throttling_rules_wait_ms.total";
pub const VQUEUE_CONCURRENCY_RULES_WAIT_MS: &str =
    "restate.vqueue.scheduler.concurrency_rules_wait_ms.total";
pub const VQUEUE_LOCK_WAIT_MS: &str = "restate.vqueue.scheduler.lock_wait_ms.total";
pub const VQUEUE_DEPLOYMENT_CONCURRENCY_WAIT_MS: &str =
    "restate.vqueue.scheduler.deployment_concurrency_wait_ms.total";

pub const ACTION_YIELD: &str = "yield";
pub const ACTION_RUN: &str = "run";

// -- Adaptive concurrency controller (Gradient2) per user rule --------------
// Labels: `pattern` (the rule pattern, ~handful of values) and `partition_id`.
pub const ADAPTIVE_LIMIT: &str = "restate.limiter.concurrency.limit";
pub const ADAPTIVE_IN_FLIGHT: &str = "restate.limiter.concurrency.in_flight";
pub const ADAPTIVE_GRADIENT: &str = "restate.limiter.concurrency.adaptive.gradient";
pub const ADAPTIVE_SHORT_RTT_MS: &str = "restate.limiter.concurrency.adaptive.short_rtt_ms";
pub const ADAPTIVE_LONG_RTT_MS: &str = "restate.limiter.concurrency.adaptive.long_rtt_ms";
pub const ADAPTIVE_SOJOURN_MIN_MS: &str = "restate.limiter.concurrency.adaptive.sojourn_min_ms";
pub const ADAPTIVE_UPDATES_TOTAL: &str = "restate.limiter.concurrency.adaptive.updates.total";
pub const ADAPTIVE_BACKSTOP_TOTAL: &str = "restate.limiter.concurrency.adaptive.backstop.total";
pub const ADAPTIVE_DRIFT_DECAY_TOTAL: &str =
    "restate.limiter.concurrency.adaptive.drift_decay.total";
pub const ADAPTIVE_SAMPLES_TOTAL: &str = "restate.limiter.concurrency.adaptive.samples.total";
pub const HOLD_TIME_SECONDS: &str = "restate.limiter.hold_time.seconds";

pub fn describe_metrics() {
    describe_counter!(
        VQUEUE_ENQUEUE,
        Unit::Count,
        "Number of entries/invocations in vqueues added to the inbox stage"
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
        VQUEUE_INVOKER_THROTTLING_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds vqueues waited on node-level invoker throttling"
    );

    describe_counter!(
        VQUEUE_THROTTLING_RULES_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds vqueues spent blocked on user-defined throttling rules"
    );

    describe_counter!(
        VQUEUE_CONCURRENCY_RULES_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent waiting on user-defined concurrency rules"
    );

    describe_counter!(
        VQUEUE_LOCK_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent waiting to acquire virtual object locks"
    );

    describe_counter!(
        VQUEUE_DEPLOYMENT_CONCURRENCY_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent blocked on deployment concurrency capacity"
    );
    describe_gauge!(
        ADAPTIVE_LIMIT,
        Unit::Count,
        "Current (learned or static) concurrency limit per user rule"
    );
    describe_gauge!(
        ADAPTIVE_IN_FLIGHT,
        Unit::Count,
        "Current in-flight concurrency usage per user rule"
    );
    describe_gauge!(
        ADAPTIVE_GRADIENT,
        Unit::Count,
        "Last Gradient2 gradient value (tolerance x long/short, clamped)"
    );
    describe_gauge!(
        ADAPTIVE_SHORT_RTT_MS,
        Unit::Milliseconds,
        "Last permit-hold-time sample fed to the adaptive controller"
    );
    describe_gauge!(
        ADAPTIVE_LONG_RTT_MS,
        Unit::Milliseconds,
        "Long (baseline) EMA of the permit hold time"
    );
    describe_gauge!(
        ADAPTIVE_SOJOURN_MIN_MS,
        Unit::Milliseconds,
        "Minimum acquire-side sojourn observed in the last backstop window"
    );
    describe_counter!(
        ADAPTIVE_UPDATES_TOTAL,
        Unit::Count,
        "Adaptive limit update decisions by outcome"
    );
    describe_counter!(
        ADAPTIVE_BACKSTOP_TOTAL,
        Unit::Count,
        "Sojourn backstop activations (freeze/trim)"
    );
    describe_counter!(
        ADAPTIVE_DRIFT_DECAY_TOTAL,
        Unit::Count,
        "Baseline drift-decay activations (long EMA pulled down)"
    );
    describe_counter!(
        ADAPTIVE_SAMPLES_TOTAL,
        Unit::Count,
        "Permit-hold-time samples fed to the adaptive controller (per-rule completions)"
    );
    describe_histogram!(
        HOLD_TIME_SECONDS,
        Unit::Seconds,
        "Permit hold time distribution per user rule (acquire to release)"
    );
}

pub fn publish_scheduler_decision_metrics(num_run: u32, num_yield: u32) {
    counter!(VQUEUE_SCHEDULER_DECISION, "action" => ACTION_RUN).increment(num_run as u64);
    counter!(VQUEUE_SCHEDULER_DECISION, "action" => ACTION_YIELD).increment(num_yield as u64);
}
