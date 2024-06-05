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
use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

pub const PARTITION_APPLY_COMMAND: &str = "restate.partition.apply_command.seconds";
pub const PARTITION_ACTUATOR_HANDLED: &str = "restate.partition.actuator_handled.total";
pub const PARTITION_TIMER_DUE_HANDLED: &str = "restate.partition.timer_due_handled.total";
pub const PARTITION_STORAGE_TX_CREATED: &str = "restate.partition.storage_tx_created.total";
pub const PARTITION_STORAGE_TX_COMMITTED: &str = "restate.partition.storage_tx_committed.total";
pub const PARTITION_HANDLE_LEADER_ACTIONS: &str = "restate.partition.handle_leader_action.total";

pub const NUM_PARTITIONS: &str = "restate.partitions_configured";
pub const PARTITION_STATUS: &str = "restate.partition.status";
// labels for PARTITION_STATUS
pub const EFFECTIVE_LEADERSHIP: &str = "effective_leadership";

pub const PP_APPLY_RECORD_DURATION: &str = "restate.partition.apply_record_duration.seconds";
pub const PARTITION_LEADER_HANDLE_ACTION_BATCH_DURATION: &str =
    "restate.partition.handle_action_batch_duration.seconds";
pub const PARTITION_HANDLE_INVOKER_EFFECT_COMMAND: &str =
    "restate.partition.handle_invoker_effect.seconds";

pub const PARTITION_LABEL: &str = "partition";

pub(crate) fn describe_metrics() {
    describe_histogram!(
        PARTITION_APPLY_COMMAND,
        Unit::Seconds,
        "Time spent applying partition processor command"
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
        PP_APPLY_RECORD_DURATION,
        Unit::Seconds,
        "Time spent processing a single bifrost message"
    );
    describe_histogram!(
        PARTITION_LEADER_HANDLE_ACTION_BATCH_DURATION,
        Unit::Seconds,
        "Time spent applying actions/effects in a single iteration"
    );
    describe_histogram!(
        PARTITION_HANDLE_LEADER_ACTIONS,
        Unit::Count,
        "Number of actions the leader has performed"
    );
    describe_histogram!(
        PARTITION_HANDLE_INVOKER_EFFECT_COMMAND,
        Unit::Seconds,
        "Time spent handling an invoker effect command"
    );

    describe_gauge!(
        NUM_PARTITIONS,
        Unit::Count,
        "Number of partitions planned to run on this node"
    );
    describe_gauge!(
        PARTITION_STATUS,
        Unit::Count,
        "Number of partitions in various states"
    );
}
