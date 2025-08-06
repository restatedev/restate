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
use metrics::{Unit, describe_gauge, describe_histogram};

pub const PARTITION_LABEL: &str = "partition";

pub const PARTITION_BLOCKED_FLARE: &str = "restate.partition.blocked_flare";

pub const PARTITION_APPLY_COMMAND: &str = "restate.partition.apply_command_duration.seconds";
pub const PARTITION_HANDLE_LEADER_ACTIONS: &str = "restate.partition.handle_leader_action.total";

pub const NUM_PARTITIONS: &str = "restate.num_partitions";
pub const NUM_ACTIVE_PARTITIONS: &str = "restate.num_active_partitions";
pub const PARTITION_TIME_SINCE_LAST_STATUS_UPDATE: &str =
    "restate.partition.time_since_last_status_update";
pub const PARTITION_APPLIED_LSN_LAG: &str = "restate.partition.applied_lsn_lag";
pub const PARTITION_IS_EFFECTIVE_LEADER: &str = "restate.partition.is_effective_leader";

pub const PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS: &str =
    "restate.partition.record_committed_to_read_latency.seconds";

pub(crate) fn describe_metrics() {
    describe_gauge!(
        PARTITION_BLOCKED_FLARE,
        Unit::Count,
        "A partition requires a higher restate-server version and is blocked from starting on this node"
    );
    describe_histogram!(
        PARTITION_APPLY_COMMAND,
        Unit::Seconds,
        "Time spent applying partition processor command"
    );
    describe_histogram!(
        PARTITION_HANDLE_LEADER_ACTIONS,
        Unit::Count,
        "Number of actions the leader has performed"
    );
    describe_histogram!(
        PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS,
        Unit::Seconds,
        "Duration between the record commit time to read time"
    );

    describe_gauge!(
        NUM_PARTITIONS,
        Unit::Count,
        "Total number of partitions in the partition table"
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
        PARTITION_TIME_SINCE_LAST_STATUS_UPDATE,
        Unit::Seconds,
        "Number of seconds since the last partition status update"
    );

    describe_gauge!(
        PARTITION_APPLIED_LSN_LAG,
        Unit::Count,
        "Number of records between last applied lsn and the log tail"
    );
}
