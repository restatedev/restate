// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Instant;

use metrics::{counter, histogram};
use restate_types::config::{Configuration, PerfStatsLevel};
use rocksdb::{PerfContext, PerfMetric};

use crate::{
    BLOCK_DECOMPRESS_DURATION, BLOCK_READ_BYTES, BLOCK_READ_DURATION, GET_FROM_MEMTABLE_DURATION,
    NEXT_ON_MEMTABLE, OP_NAME, SEEK_ON_MEMTABLE, TOTAL_DURATION, WRITE_ARTIFICIAL_DELAY_DURATION,
    WRITE_MEMTABLE_DURATION, WRITE_PRE_AND_POST_DURATION, WRITE_WAL_DURATION,
};

fn convert_perf_level(input: PerfStatsLevel) -> rocksdb::perf::PerfStatsLevel {
    use rocksdb::perf::PerfStatsLevel as RocksLevel;
    match input {
        PerfStatsLevel::Disable => RocksLevel::Disable,
        PerfStatsLevel::EnableCount => RocksLevel::EnableCount,
        PerfStatsLevel::EnableTimeExceptForMutex => RocksLevel::EnableTimeExceptForMutex,
        PerfStatsLevel::EnableTimeAndCPUTimeExceptForMutex => {
            RocksLevel::EnableTimeAndCPUTimeExceptForMutex
        }
        PerfStatsLevel::EnableTime => RocksLevel::EnableTime,
    }
}

/// This guard is !Send. It must be created and dropped on the same thread since
/// it's backed by a thread-local in rocksdb.
pub struct RocksDbPerfGuard {
    start: Instant,
    name: &'static str,
    context: PerfContext,
    should_report: bool,
}

static_assertions::assert_not_impl_any!(RocksDbPerfGuard: Send);

impl RocksDbPerfGuard {
    /// IMPORTANT NOTE: you MUST bind this value with a named variable (f) to ensure
    /// that the guard is not insta-dropped. Binding to something like `_x = ...` works, just don't
    /// use the special `_` variable and you'll be fine. Unfortunately, rust/clippy doesn't have a
    /// mechanism to enforce this at the moment.
    ///
    /// This guard should not be used across await points. Stats are thread local.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        let rocks_level = convert_perf_level(Configuration::pinned().common.rocksdb_perf_level);
        rocksdb::perf::set_perf_stats(rocks_level);
        // Behind the scenes, this "gets" the thread-local perf context and doesn't clear it up.
        let mut context = PerfContext::default();
        context.reset();
        RocksDbPerfGuard {
            name,
            start: Instant::now(),
            context,
            should_report: true,
        }
    }

    /// Drops the guard without reporting any metrics.
    pub fn forget(mut self) {
        self.should_report = false;
    }

    fn report(&self) {
        let labels = [(OP_NAME, self.name)];
        // Note to future visitors of this code. RocksDb reports times in nanoseconds in this
        // API compared to microseconds in Statistics/Properties. Use n_to_s() to convert to
        // standard prometheus unit (second).
        histogram!(TOTAL_DURATION, &labels).record(self.start.elapsed());

        // iterators-related
        let v = self.context.metric(PerfMetric::NextOnMemtableCount);
        if v != 0 {
            counter!(NEXT_ON_MEMTABLE, &labels).increment(v)
        };
        let v = self.context.metric(PerfMetric::SeekOnMemtableTime);
        if v != 0 {
            histogram!(SEEK_ON_MEMTABLE, &labels).record(n_to_s(v));
        }
        let v = self.context.metric(PerfMetric::FindNextUserEntryTime);
        if v != 0 {
            histogram!(SEEK_ON_MEMTABLE, &labels).record(n_to_s(v));
        }

        // read-related
        let v = self.context.metric(PerfMetric::BlockReadByte);
        if v != 0 {
            counter!(BLOCK_READ_BYTES, &labels).increment(v)
        };

        let v = self.context.metric(PerfMetric::BlockReadTime);
        if v != 0 {
            histogram!(BLOCK_READ_DURATION, &labels).record(n_to_s(v));
        }

        let v = self.context.metric(PerfMetric::GetFromMemtableTime);
        if v != 0 {
            histogram!(GET_FROM_MEMTABLE_DURATION, &labels).record(n_to_s(v));
        }

        let v = self.context.metric(PerfMetric::BlockDecompressTime);
        if v != 0 {
            histogram!(BLOCK_DECOMPRESS_DURATION, &labels).record(n_to_s(v));
        }

        // write-related
        let v = self.context.metric(PerfMetric::WriteWalTime);
        if v != 0 {
            histogram!(WRITE_WAL_DURATION, &labels).record(n_to_s(v));
        }
        let v = self.context.metric(PerfMetric::WriteMemtableTime);
        if v != 0 {
            histogram!(WRITE_MEMTABLE_DURATION, &labels).record(n_to_s(v));
        }

        let v = self.context.metric(PerfMetric::WritePreAndPostProcessTime);
        if v != 0 {
            histogram!(WRITE_PRE_AND_POST_DURATION, &labels).record(n_to_s(v));
        }

        let v = self.context.metric(PerfMetric::WriteDelayTime);
        if v != 0 {
            histogram!(WRITE_ARTIFICIAL_DELAY_DURATION, &labels).record(n_to_s(v));
        }
    }
}

impl Drop for RocksDbPerfGuard {
    fn drop(&mut self) {
        rocksdb::perf::set_perf_stats(rocksdb::perf::PerfStatsLevel::Disable);
        if self.should_report {
            // report collected metrics
            self.report();
        }
    }
}

#[inline]
/// nanos to seconds
fn n_to_s(v: u64) -> f64 {
    // Prometheus recommends base units, so we convert nanos to seconds
    // (fractions) when convenient.
    // See https://prometheus.io/docs/practices/naming/
    v as f64 / 1_000_000_000.0
}
