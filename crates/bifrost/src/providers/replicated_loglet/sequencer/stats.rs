// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use metrics::{Counter, Histogram, counter, histogram};

use crate::providers::replicated_loglet::metric_definitions::{
    BIFROST_SEQ_APPEND_DURATION, BIFROST_SEQ_APPEND_WAVE_DURATION, BIFROST_SEQ_APPEND_WAVES,
    BIFROST_SEQ_RECORDS_COMMITTED_BYTES, BIFROST_SEQ_RECORDS_COMMITTED_TOTAL,
};

pub struct SequencerStats {
    waves_per_append: Histogram,
    records_committed: Counter,
    bytes_committed: Counter,
    append_latency: Histogram,
    wave_latency: Histogram,
}

impl SequencerStats {
    pub fn record_waves_per_append(&self, count: u32) {
        self.waves_per_append.record(count);
    }

    pub fn increment_committed(&self, count: u64) {
        self.records_committed.increment(count);
    }

    pub fn increment_bytes_committed(&self, count: u64) {
        self.bytes_committed.increment(count);
    }

    pub fn record_append_latency(&self, duration: Duration) {
        self.append_latency.record(duration);
    }

    pub fn record_wave_latency(&self, duration: Duration) {
        self.wave_latency.record(duration);
    }
}

impl Default for SequencerStats {
    fn default() -> Self {
        Self {
            waves_per_append: histogram!(BIFROST_SEQ_APPEND_WAVES),
            records_committed: counter!(BIFROST_SEQ_RECORDS_COMMITTED_TOTAL),
            bytes_committed: counter!(BIFROST_SEQ_RECORDS_COMMITTED_BYTES),
            append_latency: histogram!(BIFROST_SEQ_APPEND_DURATION),
            wave_latency: histogram!(BIFROST_SEQ_APPEND_WAVE_DURATION),
        }
    }
}
