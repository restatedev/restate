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

pub(crate) const USAGE_STATE_STORAGE_BYTES: &str = "restate.usage.state.storage_bytes";
pub(crate) const USAGE_STATE_STORAGE_BYTE_SECONDS: &str =
    "restate.usage.state.storage_byte_seconds.total";
pub(crate) const USAGE_STATE_SIZE_ACCOUNTING_QUERY_DURATION_SECONDS: &str =
    "restate.usage.state_size_accounting.query_duration_seconds";

pub(crate) fn describe_metrics() {
    describe_gauge!(
        USAGE_STATE_STORAGE_BYTES,
        Unit::Bytes,
        "Total bytes of state storage used"
    );

    describe_counter!(
        USAGE_STATE_STORAGE_BYTE_SECONDS,
        Unit::Count,
        "Cumulative state storage usage over the process lifetime"
    );

    describe_histogram!(
        USAGE_STATE_SIZE_ACCOUNTING_QUERY_DURATION_SECONDS,
        Unit::Seconds,
        "Accounting query execution duration"
    )
}
