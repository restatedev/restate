// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_histogram};

pub const NETWORK_CONNECTION_CREATED: &str = "restate.network.connection_created.total";
pub const NETWORK_CONNECTION_DROPPED: &str = "restate.network.connection_dropped.total";
pub const NETWORK_MESSAGE_ACCEPTED_BYTES: &str = "restate.network.receive_accepted_bytes.total";
pub const NETWORK_MESSAGE_RECEIVED_REJECTED_BYTES: &str =
    "restate.network.receive_rejected_bytes.total";

pub const NETWORK_MESSAGE_PROCESSING_DURATION: &str =
    "restate.network.message_processing_duration.seconds";

pub fn describe_metrics() {
    describe_counter!(
        NETWORK_CONNECTION_CREATED,
        Unit::Count,
        "Number of connections created"
    );
    describe_counter!(
        NETWORK_CONNECTION_DROPPED,
        Unit::Count,
        "Number of connections dropped"
    );
    describe_counter!(
        NETWORK_MESSAGE_ACCEPTED_BYTES,
        Unit::Bytes,
        "Number of bytes accepted by service name"
    );

    describe_counter!(
        NETWORK_MESSAGE_RECEIVED_REJECTED_BYTES,
        Unit::Bytes,
        "Number of bytes received and dropped/rejected by service name"
    );

    describe_histogram!(
        NETWORK_MESSAGE_PROCESSING_DURATION,
        Unit::Seconds,
        "Latency of deserializing and processing incoming messages"
    );
}
