// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

pub const INGRESS_REQUESTS: &str = "restate.ingress.requests.total";
// values of label `status` in INGRESS_REQUEST
pub const REQUEST_ADMITTED: &str = "admitted";
pub const REQUEST_COMPLETED: &str = "completed";
pub const REQUEST_DENIED_THROTTLE: &str = "throttled";

pub const INGRESS_REQUEST_DURATION: &str = "restate.ingress.request_duration.seconds";

pub(crate) fn describe_metrics() {
    describe_counter!(
        INGRESS_REQUESTS,
        Unit::Count,
        "Number of ingress requests in different states, see label state to classify"
    );
    describe_histogram!(
        INGRESS_REQUEST_DURATION,
        Unit::Seconds,
        "Total latency of Ingress request processing in seconds"
    );
}
