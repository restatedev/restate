// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::gauge;
use tracing::debug;

use crate::metric_definitions::{USAGE_STATE_STORAGE_BYTE_SECONDS, USAGE_STATE_STORAGE_BYTES};

pub(super) fn update_storage_bytes_metrics(total_bytes: u64, total_byte_seconds: f64) {
    gauge!(USAGE_STATE_STORAGE_BYTES).set(total_bytes as f64);
    gauge!(USAGE_STATE_STORAGE_BYTE_SECONDS).set(total_byte_seconds);
    debug!(
        "Updated storage metrics: {} bytes, {:.2} byte-seconds",
        total_bytes, total_byte_seconds
    );
}
