// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_histogram};

pub(crate) const LOG_SERVER_WRITE_BATCH_SIZE_BYTES: &str =
    "restate.log_server.write_batch_size_bytes";

pub(crate) fn describe_metrics() {
    describe_histogram!(
        LOG_SERVER_WRITE_BATCH_SIZE_BYTES,
        Unit::Bytes,
        "Histogram of size in bytes of write batches written to log-store"
    );
}
