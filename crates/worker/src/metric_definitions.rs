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
pub const FLARE_REASON_AHEAD_OF_LOG: &str = "ahead_of_log";
pub const FLARE_REASON_MIGRATION_BARRIER: &str = "migration_barrier";
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

pub const PARTITION_CLEANER_PURGE_DELAY: &str = "restate.partition.cleaner_purge_delay.seconds";

pub const SNAPSHOT_AGE: &str = "restate.partition.snapshot_age.seconds";

pub const USAGE_LEADER_ACTION_COUNT: &str = "restate.usage.leader_action_count.total";

pub const USAGE_LEADER_JOURNAL_ENTRY_COUNT: &str = "restate.usage.leader_journal_entry_count.total";
pub const USAGE_LEADER_JOURNAL_ENTRY_BYTES: &str = "restate.usage.leader_journal_entry_bytes.total";

// Per-partition snapshot gauges carry only the `partition` label and are reported by the node that
// currently leads the partition. A node that stops leading it reports NaN age and lag, and 0 in
// progress, so `max by (partition)` picks the leader and node-level `sum`s of in-progress hold.
pub const PARTITION_SNAPSHOT_AGE: &str = "restate.partition.latest_snapshot.age.seconds";
pub const PARTITION_SNAPSHOT_LSN_LAG: &str = "restate.partition.latest_snapshot.lsn_lag";
pub const PARTITION_SNAPSHOT_IN_PROGRESS: &str = "restate.partition.snapshot_in_progress";
pub const NUM_ACTIVE_SNAPSHOTS: &str = "restate.num_active_snapshots";
pub const NUM_PARTITIONS: &str = "restate.num_partitions";
pub const NUM_ACTIVE_PARTITIONS: &str = "restate.num_active_partitions";
pub const NUM_ACTIVE_PARTITION_LEADERS: &str = "restate.num_active_partition_leaders";
pub const PARTITION_TIME_SINCE_LAST_STATUS_UPDATE: &str =
    "restate.partition.time_since_last_status_update.seconds";
pub const PARTITION_APPLIED_LSN_LAG: &str = "restate.partition.applied_lsn_lag";
pub const PARTITION_NUM_UNKNOWN_APPLIED_LSN_LAG: &str =
    "restate.partition.num_unknown_applied_lsn_lag";

pub const PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS: &str =
    "restate.partition.record_committed_to_read_latency.seconds";

pub const PARTITION_SHUFFLE_MESSAGE_COUNT: &str = "restate.partition.shuffle.message.total";
pub const PARTITION_SHUFFLE_INFLIGHT_RECORDS: &str = "restate.partition.shuffle.inflight";

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
        PARTITION_CLEANER_PURGE_DELAY,
        Unit::Seconds,
        "Delay between the retention expiry of a completed invocation (or its journal) and the application of the cleaner's purge"
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

    describe_counter!(
        USAGE_LEADER_JOURNAL_ENTRY_BYTES,
        Unit::Bytes,
        "Total number of bytes of journal entries processed by partition leaders"
    );

    describe_histogram!(
        PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS,
        Unit::Seconds,
        "Duration between the record commit time to read time"
    );

    describe_gauge!(
        PARTITION_SNAPSHOT_AGE,
        Unit::Seconds,
        "Seconds since the latest snapshot of a partition was created, reported by the node leading the partition. NaN on nodes that do not lead it, and while the partition has no snapshot"
    );

    describe_gauge!(
        PARTITION_SNAPSHOT_LSN_LAG,
        Unit::Count,
        "Number of log records applied by the partition leader since its latest snapshot (last applied LSN minus latest snapshot LSN). NaN on nodes that do not lead the partition, and while the applied LSN is unknown"
    );

    describe_gauge!(
        PARTITION_SNAPSHOT_IN_PROGRESS,
        Unit::Count,
        "1 while a snapshot of the partition is being created by this node, 0 otherwise, including on nodes that no longer lead the partition"
    );

    describe_gauge!(
        NUM_ACTIVE_SNAPSHOTS,
        Unit::Count,
        "Number of partition snapshots in progress on this node, whether automatic or requested"
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
        NUM_ACTIVE_PARTITION_LEADERS,
        Unit::Count,
        "Number of active partition leaders started by partition processor manager on this node"
    );

    describe_gauge!(
        PARTITION_TIME_SINCE_LAST_STATUS_UPDATE,
        Unit::Seconds,
        "Current quantiles of the number of seconds since the last status update, across the partitions on this node"
    );

    describe_gauge!(
        PARTITION_APPLIED_LSN_LAG,
        Unit::Count,
        "Current quantiles of the number of records between last applied lsn and the log tail, across the partitions on this node"
    );

    describe_gauge!(
        PARTITION_NUM_UNKNOWN_APPLIED_LSN_LAG,
        Unit::Count,
        "Number of partitions with unknown applied lsn lag"
    );

    describe_gauge!(
        SNAPSHOT_AGE,
        Unit::Seconds,
        "Current quantiles of the age in seconds of the latest snapshot, across the partitions on this node"
    );

    describe_counter!(
        PARTITION_SHUFFLE_MESSAGE_COUNT,
        Unit::Count,
        "Number of records shuffled by source partition",
    );

    describe_gauge!(
        PARTITION_SHUFFLE_INFLIGHT_RECORDS,
        Unit::Count,
        "Number of inflight records by source partition"
    );
}
