// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, counter, describe_counter, describe_histogram};

// connection metrics
pub(crate) const CONNECTION_POOL_CONNECTION_OPENED: &str =
    "restate.connection_pool.connection_opened.total";
pub(crate) const CONNECTION_POOL_CONNECTION_CLOSED: &str =
    "restate.connection_pool.connection_closed.total";
pub(crate) const CONNECTION_POOL_CONNECTION_OPEN_FAILED: &str =
    "restate.connection_pool.connection_open_failed.total";
pub(crate) const CONNECTION_POOL_CONNECTION_DURATION: &str =
    "restate.connection_pool.connection_duration.seconds";
pub(crate) const CONNECTION_POOL_CONNECTION_UTILIZATION: &str =
    "restate.connection_pool.connection_utilization";

// stream metrics
pub(crate) const CONNECTION_POOL_STREAM_OPENED: &str =
    "restate.connection_pool.stream_opened.total";
pub(crate) const CONNECTION_POOL_STREAM_CLOSED: &str =
    "restate.connection_pool.stream_closed.total";
pub(crate) const CONNECTION_POOL_STREAM_OPEN_FAILED: &str =
    "restate.connection_pool.stream_open_failed.total";
pub(crate) const CONNECTION_POOL_ACQUIRE_STREAM_DURATION: &str =
    "restate.connection_pool.acquire_stream_duration.seconds";
pub(crate) const CONNECTION_POOL_STREAM_DURATION: &str =
    "restate.connection_pool.stream_duration.seconds";

pub(crate) fn describe_metrics() {
    describe_counter!(
        CONNECTION_POOL_CONNECTION_OPENED,
        Unit::Count,
        "Number of opened h2 connection"
    );

    describe_counter!(
        CONNECTION_POOL_CONNECTION_CLOSED,
        Unit::Count,
        "Number of closed h2 connection"
    );

    describe_counter!(
        CONNECTION_POOL_CONNECTION_OPEN_FAILED,
        Unit::Count,
        "Number failed open connection attempts"
    );

    describe_histogram!(
        CONNECTION_POOL_CONNECTION_DURATION,
        Unit::Seconds,
        "Duration of an open h2 connection"
    );

    describe_histogram!(
        CONNECTION_POOL_CONNECTION_UTILIZATION,
        Unit::Percent,
        "Utilization of an h2 connection"
    );

    // stream metrics
    describe_counter!(
        CONNECTION_POOL_STREAM_OPENED,
        Unit::Count,
        "Number of opened h2 streams"
    );

    describe_counter!(
        CONNECTION_POOL_STREAM_CLOSED,
        Unit::Count,
        "Number of closed h2 streams"
    );

    describe_counter!(
        CONNECTION_POOL_STREAM_OPEN_FAILED,
        Unit::Count,
        "Number failed open stream attempts"
    );

    describe_histogram!(
        CONNECTION_POOL_ACQUIRE_STREAM_DURATION,
        Unit::Seconds,
        "Time a request was blocked waiting for a free stream"
    );

    describe_histogram!(
        CONNECTION_POOL_STREAM_DURATION,
        Unit::Seconds,
        "Duration of a reserved h2 stream until it's dropped"
    );

    // Reset counters so both series appear on dashboards even
    // if no connections has been opened or closed yet.
    counter!(CONNECTION_POOL_CONNECTION_CLOSED).absolute(0);
    counter!(CONNECTION_POOL_CONNECTION_OPENED).absolute(0);

    counter!(CONNECTION_POOL_STREAM_OPENED).absolute(0);
    counter!(CONNECTION_POOL_STREAM_CLOSED).absolute(0);
}
