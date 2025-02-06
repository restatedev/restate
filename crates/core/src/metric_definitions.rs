// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{describe_counter, describe_histogram, Unit};

// value of label `kind` in TC_SPAWN are defined in [`crate::TaskKind`].
pub const TC_SPAWN: &str = "restate.task_center.spawned.total";
pub const TC_FINISHED: &str = "restate.task_center.finished.total";

// values of label `status` in METRICS
pub const STATUS_COMPLETED: &str = "completed";
pub const STATUS_FAILED: &str = "failed";

pub(crate) const METADATA_CLIENT_GET_DURATION: &str = "restate.metadata_client.get.duration";
pub(crate) const METADATA_CLIENT_GET_VERSION_DURATION: &str =
    "restate.metadata_client.get_version.duration";
pub(crate) const METADATA_CLIENT_PUT_DURATION: &str = "restate.metadata_client.put.duration";
pub(crate) const METADATA_CLIENT_DELETE_DURATION: &str = "restate.metadata_client.delete.duration";

pub(crate) const METADATA_CLIENT_GET_TOTAL: &str = "restate.metadata_client.get.total";
pub(crate) const METADATA_CLIENT_GET_VERSION_TOTAL: &str =
    "restate.metadata_client.get_version.total";
pub(crate) const METADATA_CLIENT_PUT_TOTAL: &str = "restate.metadata_client.put.total";
pub(crate) const METADATA_CLIENT_DELETE_TOTAL: &str = "restate.metadata_client.delete.total";

pub fn describe_metrics() {
    describe_counter!(
        TC_SPAWN,
        Unit::Count,
        "Total tasks spawned by the task center"
    );

    describe_counter!(
        TC_FINISHED,
        Unit::Count,
        "Number of tasks that finished with 'status'"
    );

    describe_histogram!(
        METADATA_CLIENT_GET_DURATION,
        Unit::Seconds,
        "Metadata client get request duration in seconds"
    );

    describe_histogram!(
        METADATA_CLIENT_GET_VERSION_DURATION,
        Unit::Seconds,
        "Metadata client get_version request duration in seconds"
    );

    describe_histogram!(
        METADATA_CLIENT_PUT_DURATION,
        Unit::Seconds,
        "Metadata client put request duration in seconds"
    );

    describe_histogram!(
        METADATA_CLIENT_DELETE_DURATION,
        Unit::Seconds,
        "Metadata client delete request duration in seconds"
    );

    describe_counter!(
        METADATA_CLIENT_GET_TOTAL,
        Unit::Count,
        "Metadata client get request success count"
    );

    describe_counter!(
        METADATA_CLIENT_GET_VERSION_TOTAL,
        Unit::Count,
        "Metadata client get_version request success count"
    );

    describe_counter!(
        METADATA_CLIENT_PUT_TOTAL,
        Unit::Count,
        "Metadata client put request success count"
    );

    describe_counter!(
        METADATA_CLIENT_DELETE_TOTAL,
        Unit::Count,
        "Metadata client delete request success count"
    );
}
