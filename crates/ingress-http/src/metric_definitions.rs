// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use metrics::{Unit, describe_counter, describe_histogram};

pub const HTTP_CONNECTION_CREATED: &str = "restate.ingress.http.connection_created.total";
pub const HTTP_CONNECTION_DROPPED: &str = "restate.ingress.http.connection_dropped.total";
pub const INGESTION_COMMITTED_RECORDS: &str = "restate.ingress.ingestion.committed_records.total";
pub const INGESTION_INFLIGHT_BYTES: &str = "restate.ingress.ingestion.inflight_bytes.total";
pub const INGESTION_COMMITTED_BYTES: &str = "restate.ingress.ingestion.committed_bytes.total";

pub const INGRESS_REQUESTS: &str = "restate.ingress.requests.total";
// values of label `status` in INGRESS_REQUEST
pub const REQUEST_ADMITTED: &str = "admitted";
pub const REQUEST_COMPLETED: &str = "completed";
pub const REQUEST_ERROR: &str = "request_error";
pub const REQUEST_INGRESS_ERROR: &str = "ingress_error";
pub const REQUEST_INVOCATION_ERROR: &str = "invocation_error";
pub const REQUEST_RATE_LIMITED: &str = "rate-limited";

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

    describe_counter!(
        HTTP_CONNECTION_CREATED,
        Unit::Count,
        "Number of ingress incoming connections created"
    );

    describe_counter!(
        HTTP_CONNECTION_DROPPED,
        Unit::Count,
        "Number of ingress incoming connections dropped"
    );

    describe_counter!(
        INGESTION_COMMITTED_RECORDS,
        Unit::Count,
        "Number of ingested (committed) invocations via the ingestion API"
    );

    describe_counter!(
        INGESTION_INFLIGHT_BYTES,
        Unit::Count,
        "Number of inflight bytes by the ingestion API"
    );

    describe_counter!(
        INGESTION_COMMITTED_BYTES,
        Unit::Count,
        "Number of committed bytes by the ingestion API"
    );
}
