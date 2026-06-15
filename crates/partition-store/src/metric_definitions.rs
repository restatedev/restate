// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub(crate) const SNAPSHOT_UPLOAD_SUCCESS: &str = "restate.partition_store.snapshots.upload.total";
pub(crate) const SNAPSHOT_UPLOAD_FAILED: &str =
    "restate.partition_store.snapshots.upload.failed.total";
pub(crate) const SNAPSHOT_UPLOAD_DURATION: &str =
    "restate.partition_store.snapshots.upload.duration.seconds";
pub(crate) const SNAPSHOT_DOWNLOAD_DURATION: &str =
    "restate.partition_store.snapshots.download.duration.seconds";
pub(crate) const SNAPSHOT_DOWNLOAD_FAILED: &str =
    "restate.partition_store.snapshots.download.failed.total";
pub(crate) const PARTITION_MEMTABLE_BUDGET: &str = "restate.partition_store.memtable_budget.bytes";
pub(crate) const NUM_OPEN_PARTITIONS: &str = "restate.partition_store.num_open";
pub(crate) const RECLAIM_FLUSH: &str = "restate.partition_store.reclaim_flush.total";

pub(crate) fn describe_metrics() {
    describe_counter!(
        SNAPSHOT_UPLOAD_SUCCESS,
        Unit::Count,
        "Number of successful partition snapshot uploads"
    );

    describe_counter!(
        SNAPSHOT_UPLOAD_FAILED,
        Unit::Count,
        "Number of failed partition snapshot uploads"
    );

    describe_histogram!(
        SNAPSHOT_UPLOAD_DURATION,
        Unit::Seconds,
        "Duration of partition snapshot upload operations"
    );

    describe_histogram!(
        SNAPSHOT_DOWNLOAD_DURATION,
        Unit::Seconds,
        "Duration of partition snapshot download operations"
    );

    describe_counter!(
        SNAPSHOT_DOWNLOAD_FAILED,
        Unit::Count,
        "Number of failed partition snapshot download operations"
    );

    describe_gauge!(
        PARTITION_MEMTABLE_BUDGET,
        Unit::Bytes,
        "The current memtable budget for a single partition on this node"
    );

    describe_gauge!(
        NUM_OPEN_PARTITIONS,
        Unit::Count,
        "The number of open partition stores on this node"
    );

    describe_counter!(
        RECLAIM_FLUSH,
        Unit::Count,
        "How many flushes were triggered by the partition store memory reclaimer"
    );
}
