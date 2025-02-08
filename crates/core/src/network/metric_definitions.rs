// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{counter, describe_counter, describe_histogram, Counter, Unit};
use std::sync::LazyLock;

pub(crate) const NETWORK_CONNECTION_CREATED: &str = "restate.network.connection_created.total";
pub(crate) const NETWORK_CONNECTION_DROPPED: &str = "restate.network.connection_dropped.total";
pub(crate) const NETWORK_MESSAGE_RECEIVED: &str = "restate.network.message_received.total";
pub(crate) const NETWORK_MESSAGE_RECEIVED_BYTES: &str =
    "restate.network.message_received_bytes.total";

pub(crate) const NETWORK_MESSAGE_PROCESSING_DURATION: &str =
    "restate.network.message_processing_duration.seconds";

#[cfg(debug_assertions)]
pub(crate) const NETWORK_MESSAGE_DECODE_DURATION: &str =
    "restate.network.message_decode_duration.seconds";

pub static INCOMING_CONNECTION: LazyLock<Counter> =
    LazyLock::new(|| counter!(NETWORK_CONNECTION_CREATED, "direction" => "incoming"));

pub static OUTGOING_CONNECTION: LazyLock<Counter> =
    LazyLock::new(|| counter!(NETWORK_CONNECTION_CREATED, "direction" => "outgoing"));

pub static CONNECTION_DROPPED: LazyLock<Counter> =
    LazyLock::new(|| counter!(NETWORK_CONNECTION_DROPPED));

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
        NETWORK_MESSAGE_RECEIVED_BYTES,
        Unit::Bytes,
        "Number of bytes received by message name"
    );

    describe_counter!(
        NETWORK_MESSAGE_RECEIVED,
        Unit::Count,
        "Number of messages received by message type"
    );

    describe_histogram!(
        NETWORK_MESSAGE_PROCESSING_DURATION,
        Unit::Seconds,
        "Latency of deserializing and processing incoming messages"
    );
}
