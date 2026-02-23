// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

pub const TYPE_LABEL: &str = "type";
pub const PARTITION_LABEL: &str = "partition";
pub const REASON_LABEL: &str = "reason";
pub const LEADER_LABEL: &str = "leader";
pub const LEADER_LABEL_LEADER: &str = "1";
pub const LEADER_LABEL_FOLLOWER: &str = "0";

// contains `reason' label and `partition`labels:
// - `version_barrier` indicates that the partition processor manager was unable to start
//                     the partition processor because the version of the running restate-server
//                     is not compatible with the version of the data in the partition store.
// - `snapshot-unavailable` indicates that the partition processor manager was unable to start
//                     the partition processor because the snapshot repository is not available or
//                     not configured.
pub const PARTITION_BLOCKED_FLARE: &str = "restate.partition.blocked_flare";
pub const FLARE_REASON_VERSION_BARRIER: &str = "version_barrier";
pub const FLARE_REASON_SNAPSHOT_UNAVAILABLE: &str = "snapshot-unavailable";

pub const PARTITION_APPLY_COMMAND: &str = "restate.partition.apply_command_duration.seconds";
pub const PARTITION_HANDLE_LEADER_ACTIONS: &str = "restate.partition.handle_leader_action.total";

pub const PARTITION_START: &str = "restate.partition.start.total";
// contains 'type' label
pub const PARTITION_STOP: &str = "restate.partition.stop.total";
// types of partition stop
pub const NORMAL_STOP: &str = "normal";
pub const STARTUP_ERROR_STOP: &str = "startup-error";
pub const GAP_STOP: &str = "log-gap-detected";
pub const ERROR_STOP: &str = "error";

pub const SNAPSHOT_AGE: &str = "restate.partition.snapshot_age.seconds";

pub const USAGE_LEADER_ACTION_COUNT: &str = "restate.usage.leader_action_count.total";

pub const USAGE_LEADER_JOURNAL_ENTRY_COUNT: &str = "restate.usage.leader_journal_entry_count.total";

pub const NUM_PARTITIONS: &str = "restate.num_partitions";
pub const NUM_ACTIVE_PARTITIONS: &str = "restate.num_active_partitions";
pub const PARTITION_TIME_SINCE_LAST_STATUS_UPDATE: &str =
    "restate.partition.time_since_last_status_update";
pub const PARTITION_APPLIED_LSN_LAG: &str = "restate.partition.applied_lsn_lag";
pub const PARTITION_IS_EFFECTIVE_LEADER: &str = "restate.partition.is_effective_leader";

pub const PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS: &str =
    "restate.partition.record_committed_to_read_latency.seconds";

pub const PARTITION_INGESTION_REQUEST_LEN: &str = "restate.partition.ingest.request.len";
pub const PARTITION_INGESTION_REQUEST_SIZE: &str = "restate.partition.ingest.request.size.bytes";
pub const PARTITION_SHUFFLE_MESSAGE_COUNT: &str = "restate.partition.shuffle.message.count";
pub const PARTITION_SHUFFLE_INFLIGHT_COUNT: &str = "restate.partition.shuffle.inflight.count";

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

    describe_counter!(
        PARTITION_START,
        Unit::Count,
        "Number of partition processor starts on this node"
    );

    describe_counter!(
        PARTITION_STOP,
        Unit::Count,
        "Number of partition processor stops on this node"
    );

    describe_counter!(
        USAGE_LEADER_ACTION_COUNT,
        Unit::Count,
        "Count of invocation actions processed by partition leaders"
    );

    describe_counter!(
        USAGE_LEADER_JOURNAL_ENTRY_COUNT,
        Unit::Count,
        "Count of specific journal entries processed by partition leaders"
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

    describe_gauge!(
        SNAPSHOT_AGE,
        Unit::Seconds,
        "The age of the latest partition snapshot in seconds"
    );

    describe_histogram!(
        PARTITION_INGESTION_REQUEST_LEN,
        Unit::Count,
        "Number of records in a single ingestion request"
    );

    describe_histogram!(
        PARTITION_INGESTION_REQUEST_SIZE,
        Unit::Bytes,
        "Total size of records in a single ingestion request"
    );

    describe_counter!(
        PARTITION_SHUFFLE_MESSAGE_COUNT,
        Unit::Count,
        "Number of records shuffled by source partition",
    );

    describe_histogram!(
        PARTITION_SHUFFLE_INFLIGHT_COUNT,
        Unit::Count,
        "Number of inflight records by source partition"
    );
}
