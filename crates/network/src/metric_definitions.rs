// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram, Counter,
    Gauge, Histogram, Unit,
};
use once_cell::sync::Lazy;

const NETWORK_CONNECTION_CREATED: &str = "restate.network.connection_created.total";
const NETWORK_CONNECTION_DROPPED: &str = "restate.network.connection_dropped.total";
const NETWORK_ONGOING_DRAINS: &str = "restate.network.ongoing_drains";
const NETWORK_MESSAGE_SENT: &str = "restate.network.message_sent.total";
const NETWORK_MESSAGE_RECEIVED: &str = "restate.network.message_received.total";

const NETWORK_CONNECTION_SEND_DURATION: &str = "restate.network.connection_send_duration.seconds";
const NETWORK_MESSAGE_PROCESSING_DURATION: &str =
    "restate.network.message_processing_duration.seconds";

pub static INCOMING_CONNECTION: Lazy<Counter> =
    Lazy::new(|| counter!(NETWORK_CONNECTION_CREATED, "direction" => "incoming"));

pub static OUTGOING_CONNECTION: Lazy<Counter> =
    Lazy::new(|| counter!(NETWORK_CONNECTION_CREATED, "direction" => "outgoing"));

pub static CONNECTION_DROPPED: Lazy<Counter> = Lazy::new(|| counter!(NETWORK_CONNECTION_DROPPED));
pub static ONGOING_DRAIN: Lazy<Gauge> = Lazy::new(|| gauge!(NETWORK_ONGOING_DRAINS));

pub static MESSAGE_SENT: Lazy<Counter> = Lazy::new(|| counter!(NETWORK_MESSAGE_SENT));
pub static MESSAGE_RECEIVED: Lazy<Counter> = Lazy::new(|| counter!(NETWORK_MESSAGE_RECEIVED));

pub static CONNECTION_SEND_DURATION: Lazy<Histogram> =
    Lazy::new(|| histogram!(NETWORK_CONNECTION_SEND_DURATION));

pub static MESSAGE_PROCESSING_DURATION: Lazy<Histogram> =
    Lazy::new(|| histogram!(NETWORK_MESSAGE_PROCESSING_DURATION));

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
    describe_gauge!(
        NETWORK_ONGOING_DRAINS,
        Unit::Count,
        "Number of connections currently being drained"
    );

    describe_counter!(NETWORK_MESSAGE_SENT, Unit::Count, "Number of messages sent");

    describe_counter!(
        NETWORK_MESSAGE_RECEIVED,
        Unit::Count,
        "Number of messages received"
    );

    describe_histogram!(
        NETWORK_CONNECTION_SEND_DURATION,
        Unit::Seconds,
        "Latency of sending a message over a single connection stream"
    );
    describe_histogram!(
        NETWORK_MESSAGE_PROCESSING_DURATION,
        Unit::Seconds,
        "Latency of deserializing and processing incoming messages"
    );
}
