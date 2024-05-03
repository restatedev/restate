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
use metrics::{describe_counter, describe_histogram, Unit};

pub const PARTITION_APPLY_COMMAND: &str = "restate.partition.apply_command.total";
pub const PARTITION_ACTUATOR_HANDLED: &str = "restate.partition.actuator_handled.total";
pub const PARTITION_TIMER_DUE_HANDLED: &str = "restate.partition.timer_due_handled.total";
pub const PARTITION_STORAGE_TX_CREATED: &str = "restate.partition.storage_tx_created.total";
pub const PARTITION_STORAGE_TX_COMMITTED: &str = "restate.partition.storage_tx_committed.total";

pub const PP_LOG_READ_NEXT_DURATION: &str = "restate.partition.log_read_next_duration.seconds";

pub const PP_APPLY_RECORD_DURATION: &str = "restate.partition.apply_record_duration.seconds";
pub const PP_WAIT_OR_IDLE_DURATION: &str = "restate.partition.wait_or_idle_duration.seconds";
pub const PP_APPLY_EFFECTS_DURATION: &str = "restate.partition.apply_effects_duration.seconds";
pub const PP_APPLY_TIMERS_DURATION: &str = "restate.partition.apply_timers_duration.seconds";

pub const PARTITION_LABEL: &str = "partition";

pub(crate) fn describe_metrics() {
    describe_counter!(
        PARTITION_APPLY_COMMAND,
        Unit::Count,
        "Total consensus commands processed by partition processor"
    );
    describe_counter!(
        PARTITION_ACTUATOR_HANDLED,
        Unit::Count,
        "Number of actuator operation outputs processed"
    );
    describe_counter!(
        PARTITION_TIMER_DUE_HANDLED,
        Unit::Count,
        "Number of due timer instances processed"
    );
    describe_counter!(
        PARTITION_STORAGE_TX_CREATED,
        Unit::Count,
        "Storage transactions created by from processing state machine commands"
    );
    describe_counter!(
        PARTITION_STORAGE_TX_COMMITTED,
        Unit::Count,
        "Storage transactions committed by applying partition state machine commands"
    );
    describe_histogram!(
        PP_LOG_READ_NEXT_DURATION,
        Unit::Seconds,
        "Time spent attempting to read the next record off bifrost, this is inclusive of wait time if not records are available to read");
    describe_histogram!(
        PP_APPLY_RECORD_DURATION,
        Unit::Seconds,
        "Time spent processing a single bifrost message"
    );
    describe_histogram!(
        PP_WAIT_OR_IDLE_DURATION,
        Unit::Seconds,
        "Time spent since last activity on this partition processor"
    );
    describe_histogram!(
        PP_APPLY_EFFECTS_DURATION,
        Unit::Seconds,
        "Time spent applying effects in a single iteration"
    );
    describe_histogram!(
        PP_APPLY_TIMERS_DURATION,
        Unit::Seconds,
        "Time spent applying effects in a single iteration"
    );
}
