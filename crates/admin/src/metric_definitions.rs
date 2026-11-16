// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub(crate) const USAGE_STATE_STORAGE_BYTES: &str = "restate.usage.state.storage_bytes";
pub(crate) const USAGE_STATE_STORAGE_BYTE_SECONDS: &str =
    "restate.usage.state.storage_byte_seconds.total";
pub(crate) const USAGE_STATE_SIZE_ACCOUNTING_QUERY_DURATION_SECONDS: &str =
    "restate.usage.state_size_accounting.query_duration_seconds";

pub(crate) const CLUSTER_SAFETY_WRITE_UNAVAILABLE_LOGS: &str =
    "restate.cluster_safety.write_unavailable_logs";
pub(crate) const CLUSTER_SAFETY_READ_UNAVAILABLE_LOGS: &str =
    "restate.cluster_safety.read_unavailable_logs";
pub(crate) const CLUSTER_SAFETY_WRITE_AT_RISK_LOGS: &str =
    "restate.cluster_safety.write_at_risk_logs";
pub(crate) const CLUSTER_SAFETY_READ_AT_RISK_LOGS: &str =
    "restate.cluster_safety.read_at_risk_logs";
pub(crate) const CLUSTER_SAFETY_UNCHECKED_LOGS: &str = "restate.cluster_safety.unchecked_logs";
pub(crate) const CLUSTER_SAFETY_UNAVAILABLE_PARTITIONS: &str =
    "restate.cluster_safety.unavailable_partitions";
pub(crate) const CLUSTER_SAFETY_AT_RISK_PARTITIONS: &str =
    "restate.cluster_safety.at_risk_partitions";
pub(crate) const CLUSTER_SAFETY_UNCHECKED_PARTITIONS: &str =
    "restate.cluster_safety.unchecked_partitions";
pub(crate) const CLUSTER_SAFETY_LAST_CHECK_TIMESTAMP_SECONDS: &str =
    "restate.cluster_safety.last_check_timestamp_seconds";

pub(crate) fn describe_metrics() {
    describe_gauge!(
        USAGE_STATE_STORAGE_BYTES,
        Unit::Bytes,
        "Total bytes of state storage used"
    );

    describe_counter!(
        USAGE_STATE_STORAGE_BYTE_SECONDS,
        Unit::Count,
        "Cumulative state storage usage over the process lifetime"
    );

    describe_histogram!(
        USAGE_STATE_SIZE_ACCOUNTING_QUERY_DURATION_SECONDS,
        Unit::Seconds,
        "Accounting query execution duration"
    );

    // Cluster safety metrics are only reported by the cluster controller leader, other nodes
    // report 0.
    describe_gauge!(
        CLUSTER_SAFETY_WRITE_UNAVAILABLE_LOGS,
        Unit::Count,
        "Number of logs that cannot accept writes given the currently alive nodes"
    );
    describe_gauge!(
        CLUSTER_SAFETY_READ_UNAVAILABLE_LOGS,
        Unit::Count,
        "Number of logs that cannot be reliably read given the currently alive nodes"
    );
    describe_gauge!(
        CLUSTER_SAFETY_WRITE_AT_RISK_LOGS,
        Unit::Count,
        "Number of logs that would stop accepting writes if a single failure domain (at the greatest scope of the replication) failed"
    );
    describe_gauge!(
        CLUSTER_SAFETY_READ_AT_RISK_LOGS,
        Unit::Count,
        "Number of logs that could no longer be reliably read if a single failure domain (at the greatest scope of the replication) failed"
    );
    describe_gauge!(
        CLUSTER_SAFETY_UNCHECKED_LOGS,
        Unit::Count,
        "Number of logs that the safety check cannot evaluate because they use non-replicated loglets"
    );
    describe_gauge!(
        CLUSTER_SAFETY_UNAVAILABLE_PARTITIONS,
        Unit::Count,
        "Number of partitions without an alive replica that cannot be moved to other nodes"
    );
    describe_gauge!(
        CLUSTER_SAFETY_AT_RISK_PARTITIONS,
        Unit::Count,
        "Number of partitions that would become unavailable if a single failure domain (at the greatest scope of the partition replication) failed"
    );
    describe_gauge!(
        CLUSTER_SAFETY_UNCHECKED_PARTITIONS,
        Unit::Count,
        "Number of partitions whose placement is not yet known to the cluster controller"
    );
    describe_gauge!(
        CLUSTER_SAFETY_LAST_CHECK_TIMESTAMP_SECONDS,
        Unit::Seconds,
        "Unix timestamp of the last completed cluster safety check. Alert if it falls behind to detect that the other cluster safety metrics are stale"
    );
}
