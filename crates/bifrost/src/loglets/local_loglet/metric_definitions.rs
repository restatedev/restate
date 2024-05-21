// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Optional to have but adds description/help message to the metrics emitted to
/// the metrics' sink.
use metrics::{describe_counter, describe_histogram, Unit};

pub(crate) const BIFROST_LOCAL_APPEND: &str = "restate.bifrost.localloglet.appends.total";

pub(crate) const BIFROST_LOCAL_APPEND_DURATION: &str =
    "restate.bifrost.localloglet.append_duration.seconds";

pub(crate) const BIFROST_LOCAL_WRITE_BATCH_COUNT: &str =
    "restate.bifrost.localloglet.write_batch_count";

pub(crate) const BIFROST_LOCAL_WRITE_BATCH_SIZE_BYTES: &str =
    "restate.bifrost.localloglet.write_batch_size_bytes";

pub(crate) fn describe_metrics() {
    describe_counter!(
        BIFROST_LOCAL_APPEND,
        Unit::Count,
        "Number of append requests to bifrost's local loglet"
    );

    describe_histogram!(
        BIFROST_LOCAL_WRITE_BATCH_COUNT,
        Unit::Count,
        "Number of records in each append request to bifrost's local loglet"
    );

    describe_histogram!(
        BIFROST_LOCAL_WRITE_BATCH_SIZE_BYTES,
        Unit::Count,
        "Size in bytes of local loglet write batches"
    );

    describe_histogram!(
        BIFROST_LOCAL_APPEND_DURATION,
        Unit::Seconds,
        "Total latency of bifrost's local loglet appends"
    );
}
