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

// values of label `status` in METRICS
pub const STATUS_COMPLETED: &str = "completed";
pub const STATUS_FAILED: &str = "failed";

pub(crate) const METADATA_SERVER_GET_DURATION: &str = "restate.metadata_server.get.duration";
pub(crate) const METADATA_SERVER_GET_VERSION_DURATION: &str =
    "restate.metadata_server.get_version.duration";
pub(crate) const METADATA_SERVER_PUT_DURATION: &str = "restate.metadata_server.put.duration";
pub(crate) const METADATA_SERVER_DELETE_DURATION: &str = "restate.metadata_server.delete.duration";

pub(crate) const METADATA_SERVER_GET_TOTAL: &str = "restate.metadata_server.get.total";
pub(crate) const METADATA_SERVER_GET_VERSION_TOTAL: &str =
    "restate.metadata_server.get_version.total";
pub(crate) const METADATA_SERVER_PUT_TOTAL: &str = "restate.metadata_server.put.total";
pub(crate) const METADATA_SERVER_DELETE_TOTAL: &str = "restate.metadata_server.delete.total";

// Raft specific metrics
pub(crate) const METADATA_SERVER_REPLICATED_SENT_MESSAGE_TOTAL: &str =
    "restate.metadata_server.replicated.sent_messages.total";
pub(crate) const METADATA_SERVER_REPLICATED_RECV_MESSAGE_TOTAL: &str =
    "restate.metadata_server.replicated.received_messages.total";
pub(crate) const METADATA_SERVER_REPLICATED_SENT_MESSAGE_BYTES: &str =
    "restate.metadata_server.replicated.sent_messages.bytes";
pub(crate) const METADATA_SERVER_REPLICATED_RECV_MESSAGE_BYTES: &str =
    "restate.metadata_server.replicated.received_messages.bytes";

pub(crate) const METADATA_SERVER_REPLICATED_LEADER_ID: &str =
    "restate.metadata_server.replicated.leader.id";
pub(crate) const METADATA_SERVER_REPLICATED_SNAPSHOT_SIZE_BYTES: &str =
    "restate.metadata_server.replicated.snapshot_size.bytes";
pub(crate) const METADATA_SERVER_REPLICATED_TERM: &str =
    "restate.metadata_server.replicated.snapshot.bytes";
pub(crate) const METADATA_SERVER_REPLICATED_COMMITTED_LSN: &str =
    "restate.metadata_server.replicated.committed_lsn";
pub(crate) const METADATA_SERVER_REPLICATED_APPLIED_LSN: &str =
    "restate.metadata_server.replicated.applied_lsn";
pub(crate) const METADATA_SERVER_REPLICATED_FIRST_INDEX: &str =
    "restate.metadata_server.replicated.first_index";
pub(crate) const METADATA_SERVER_REPLICATED_LAST_INDEX: &str =
    "restate.metadata_server.replicated.last_index";

pub(crate) fn describe_metrics() {
    describe_histogram!(
        METADATA_SERVER_GET_DURATION,
        Unit::Seconds,
        "Metadata get request duration in seconds as measured by the metadata handler"
    );

    describe_histogram!(
        METADATA_SERVER_GET_VERSION_DURATION,
        Unit::Seconds,
        "Metadata get_version request duration in seconds as measured by the metadata handler"
    );

    describe_histogram!(
        METADATA_SERVER_PUT_DURATION,
        Unit::Seconds,
        "Metadata put request duration in seconds as measured by the metadata handler"
    );

    describe_histogram!(
        METADATA_SERVER_DELETE_DURATION,
        Unit::Seconds,
        "Metadata delete request duration in seconds as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_GET_TOTAL,
        Unit::Count,
        "Metadata get request success count as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_GET_VERSION_TOTAL,
        Unit::Count,
        "Metadata get_version success request count as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_PUT_TOTAL,
        Unit::Count,
        "Metadata put request success count as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_DELETE_TOTAL,
        Unit::Count,
        "Metadata delete request success count as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_REPLICATED_SENT_MESSAGE_TOTAL,
        Unit::Count,
        "Raft Metadata server sent messages count"
    );

    describe_counter!(
        METADATA_SERVER_REPLICATED_RECV_MESSAGE_TOTAL,
        Unit::Count,
        "Raft Metadata server received messages count"
    );

    describe_counter!(
        METADATA_SERVER_REPLICATED_SENT_MESSAGE_BYTES,
        Unit::Bytes,
        "Raft Metadata server sent messages size in bytes"
    );

    describe_counter!(
        METADATA_SERVER_REPLICATED_RECV_MESSAGE_BYTES,
        Unit::Bytes,
        "Raft Metadata server received messages size in bytes"
    );

    describe_gauge!(
        METADATA_SERVER_REPLICATED_LEADER_ID,
        "Raft Metadata server know leader id"
    );

    describe_gauge!(
        METADATA_SERVER_REPLICATED_SNAPSHOT_SIZE_BYTES,
        Unit::Bytes,
        "Raft Metadata snapshot size"
    );

    describe_gauge!(
        METADATA_SERVER_REPLICATED_TERM,
        Unit::Count,
        "Raft Metadata raft term number"
    );

    describe_gauge!(
        METADATA_SERVER_REPLICATED_APPLIED_LSN,
        Unit::Count,
        "Raft Metadata raft applied lsn"
    );

    describe_gauge!(
        METADATA_SERVER_REPLICATED_COMMITTED_LSN,
        Unit::Count,
        "Raft Metadata raft committed lsn"
    );

    describe_gauge!(
        METADATA_SERVER_REPLICATED_FIRST_INDEX,
        Unit::Count,
        "Raft Metadata raft first index"
    );
    describe_gauge!(
        METADATA_SERVER_REPLICATED_LAST_INDEX,
        Unit::Count,
        "Raft Metadata raft last index"
    );
}
