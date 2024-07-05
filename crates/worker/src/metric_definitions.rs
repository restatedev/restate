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
pub const PARTITION_STORAGE_TX_CREATED: &str = "restate.partition.storage_tx_created.total";
pub const PARTITION_STORAGE_TX_COMMITTED: &str = "restate.partition.storage_tx_committed.total";
pub const PARTITION_HANDLE_LEADER_ACTIONS: &str = "restate.partition.handle_leader_action.total";

pub const NUM_ACTIVE_PARTITIONS: &str = "restate.num_active_partitions";
pub const PARTITION_TIME_SINCE_LAST_STATUS_UPDATE: &str =
    "restate.partition.time_since_last_status_update";
pub const PARTITION_TIME_SINCE_LAST_RECORD: &str = "restate.partition.time_since_last_record";
pub const PARTITION_LAST_APPLIED_LOG_LSN: &str = "restate.partition.last_applied_lsn";
pub const PARTITION_LAST_PERSISTED_LOG_LSN: &str = "restate.partition.last_persisted_lsn";
pub const PARTITION_IS_EFFECTIVE_LEADER: &str = "restate.partition.is_effective_leader";
pub const PARTITION_IS_ACTIVE: &str = "restate.partition.is_active";

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
        NUM_ACTIVE_PARTITIONS,
        Unit::Count,
        "Number of partitions started by partition processor manager on this node"
    );

    describe_gauge!(
        PARTITION_IS_EFFECTIVE_LEADER,
        Unit::Count,
        "Set to 1 if the partition is an effective leader"
    );

    describe_gauge!(
        PARTITION_IS_ACTIVE,
        Unit::Count,
        "Set to 1 if the partition is an active replay (not catching up or starting)"
    );

    describe_gauge!(
        PARTITION_TIME_SINCE_LAST_STATUS_UPDATE,
        Unit::Seconds,
        "Number of seconds since the last partition status update"
    );

    describe_gauge!(
        PARTITION_LAST_APPLIED_LOG_LSN,
        Unit::Count,
        "Raw value of the last applied log LSN"
    );

    describe_gauge!(
        PARTITION_LAST_PERSISTED_LOG_LSN,
        Unit::Count,
        "Raw value of the LSN that can be trimmed"
    );

    describe_gauge!(
        PARTITION_TIME_SINCE_LAST_RECORD,
        Unit::Seconds,
        "Number of seconds since the last record was applied"
    );
}
