// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{describe_counter, describe_histogram, Unit};

pub(crate) const LOG_SERVER_STORE: &str = "restate.log_server.store.total";

pub(crate) const LOG_SERVER_STORE_DURATION: &str = "restate.log_server.store_duration.seconds";

pub(crate) const LOG_SERVER_WRITE_BATCH_COUNT: &str = "restate.log_server.write_batch_count";

pub(crate) const LOG_SERVER_WRITE_BATCH_SIZE_BYTES: &str =
    "restate.log_server.write_batch_size_bytes";

pub(crate) fn describe_metrics() {
    describe_counter!(
        LOG_SERVER_STORE,
        Unit::Count,
        "Number of store requests to this log-server"
    );

    describe_histogram!(
        LOG_SERVER_WRITE_BATCH_COUNT,
        Unit::Count,
        "Histogram of the number of records in each batch written to log-store"
    );

    describe_histogram!(
        LOG_SERVER_WRITE_BATCH_SIZE_BYTES,
        Unit::Bytes,
        "Histogram of size in bytes of write batches written to log-store"
    );

    describe_histogram!(
        LOG_SERVER_STORE_DURATION,
        Unit::Seconds,
        "Histogram of store requests processing latency"
    );
}
