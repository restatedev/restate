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

pub(crate) const LOG_SERVER_WRITE_BATCH_SIZE_BYTES: &str =
    "restate.log_server.write_batch_size_bytes";

pub(crate) const LOG_SERVER_LOGLET_STARTED: &str = "restate.log_server.loglet_started.total";

pub(crate) const LOG_SERVER_LOGLET_STOPPED: &str = "restate.log_server.loglet_stopped.total";

// broken by label "status"
// - "ok"       (accepted, stored)
// - "duplicate" (accepted, duplicate not stored)
// - "repair" (accepted, but repair record)
//
// - "sealing"  (dropped, loglet is sealing)
// - "sealed"  (dropped, loglet is sealed)
// - "disabled"  (dropped, store isn't accepting writes)
// - "expired" (dropped, expired)
// - "malformed" (dropped, malformed)
pub(crate) const LOG_SERVER_STORE_RECORDS: &str = "restate.log_server.store.records.total";
pub(crate) const LOG_SERVER_STORE_BYTES: &str = "restate.log_server.store.bytes.total";

pub(crate) fn describe_metrics() {
    describe_histogram!(
        LOG_SERVER_WRITE_BATCH_SIZE_BYTES,
        Unit::Bytes,
        "Histogram of size in bytes of write batches written to log-store"
    );

    describe_counter!(
        LOG_SERVER_STORE_RECORDS,
        Unit::Count,
        "Number of records in STORE messages broken by status of processing"
    );

    describe_counter!(
        LOG_SERVER_STORE_BYTES,
        Unit::Bytes,
        "Bytes received in STOREs broken down by status"
    );
}
