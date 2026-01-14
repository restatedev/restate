// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_histogram};

pub(crate) const SNAPSHOT_UPLOAD_SUCCESS: &str = "restate.partition_store.snapshots.upload.total";
pub(crate) const SNAPSHOT_UPLOAD_FAILED: &str =
    "restate.partition_store.snapshots.upload.failed.total";
pub(crate) const SNAPSHOT_UPLOAD_DURATION: &str =
    "restate.partition_store.snapshots.upload.duration.seconds";
pub(crate) const SNAPSHOT_DOWNLOAD_DURATION: &str =
    "restate.partition_store.snapshots.download.duration.seconds";
pub(crate) const SNAPSHOT_DOWNLOAD_FAILED: &str =
    "restate.partition_store.snapshots.download.failed.total";
pub(crate) const SNAPSHOT_UPLOAD_BYTES: &str =
    "restate.partition_store.snapshots.upload.bytes.total";
pub(crate) const SNAPSHOT_UPLOAD_BYTES_DEDUPLICATED: &str =
    "restate.partition_store.snapshots.upload.bytes.deduplicated.total";
pub(crate) const SNAPSHOT_ORPHAN_SCAN_TOTAL: &str =
    "restate.partition_store.snapshots.orphan_scan.total";
pub(crate) const SNAPSHOT_ORPHAN_SCAN_FAILED: &str =
    "restate.partition_store.snapshots.orphan_scan.failed.total";
pub(crate) const SNAPSHOT_ORPHAN_FILES_DELETED: &str =
    "restate.partition_store.snapshots.orphan_files_deleted.total";

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

    describe_counter!(
        SNAPSHOT_UPLOAD_BYTES,
        Unit::Bytes,
        "Total bytes uploaded for partition snapshots"
    );

    describe_counter!(
        SNAPSHOT_UPLOAD_BYTES_DEDUPLICATED,
        Unit::Bytes,
        "Total bytes deduplicated (not uploaded) for partition snapshots"
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

    describe_counter!(
        SNAPSHOT_ORPHAN_SCAN_TOTAL,
        Unit::Count,
        "Number of orphan scan operations performed"
    );

    describe_counter!(
        SNAPSHOT_ORPHAN_SCAN_FAILED,
        Unit::Count,
        "Number of orphan scan failures"
    );

    describe_counter!(
        SNAPSHOT_ORPHAN_FILES_DELETED,
        Unit::Count,
        "Number of orphaned files deleted during cleanup"
    );
}
