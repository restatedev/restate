// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Instant;

use metrics::histogram;
use rocksdb::{PerfContext, PerfMetric};

use restate_types::config::{Configuration, PerfStatsLevel};
use restate_util_random as rand;

use crate::metric_definitions::DB_MUTEX_DURATION;
use crate::{
    OP_NAME, TOTAL_DURATION, WRITE_DELAY_DURATION, WRITE_MEMTABLE_DURATION,
    WRITE_PRE_AND_POST_DURATION, WRITE_WAL_DURATION,
};

// Sample 33% of rocksdb write operations.
// Weight W = 1/P where p is the sampling rate.
const WEIGHT: usize = 3;
const DEFAULT_SAMPLE_RATE: u64 = const { u64::MAX / WEIGHT as u64 };

/// This guard is !Send. It must be created and dropped on the same thread since
/// it's backed by a thread-local in rocksdb.
pub struct RocksDbWritePerfGuard(Inner);

static_assertions::assert_not_impl_any!(RocksDbWritePerfGuard: Send);

impl RocksDbWritePerfGuard {
    /// IMPORTANT NOTE: you MUST bind this value with a named variable (f) to ensure
    /// that the guard is not insta-dropped. Binding to something like `_x = ...` works, just don't
    /// use the special `_` variable and you'll be fine. Unfortunately, rust/clippy doesn't have a
    /// mechanism to enforce this at the moment.
    ///
    /// This guard should not be used across await points. Stats are thread local.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        if should_sample() {
            Self(Inner::Real(RealRocksGuard::new(name)))
        } else {
            Self(Inner::Noop)
        }
    }

    /// Drops the guard without reporting any metrics.
    pub fn forget(mut self) {
        if let Inner::Real(guard) = &mut self.0 {
            guard.should_report = false;
        }
    }

    fn report(&self) {
        if let Inner::Real(guard) = &self.0 {
            guard.report();
        }
    }
}

impl Drop for RocksDbWritePerfGuard {
    fn drop(&mut self) {
        if let Inner::Real(guard) = &mut self.0 {
            rocksdb::perf::set_perf_stats(rocksdb::perf::PerfStatsLevel::Disable);
            if guard.should_report {
                // report collected metrics
                self.report();
            }
        }
    }
}

#[inline]
fn should_sample() -> bool {
    rand::pseudo_random() < DEFAULT_SAMPLE_RATE
}

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

struct RealRocksGuard {
    start: Instant,
    name: &'static str,
    context: PerfContext,
    should_report: bool,
}

impl RealRocksGuard {
    fn new(name: &'static str) -> Self {
        let rocks_level = convert_perf_level(Configuration::pinned().common.rocksdb_perf_level);
        rocksdb::perf::set_perf_stats(rocks_level);
        // Behind the scenes, this "gets" the thread-local perf context and doesn't clear it up.
        let mut context = PerfContext::default();
        context.reset();
        Self {
            name,
            start: Instant::now(),
            context,
            should_report: true,
        }
    }

    fn report(&self) {
        let labels = [(OP_NAME, self.name)];
        // Note to future visitors of this code. RocksDb reports times in nanoseconds in this
        // API compared to microseconds in Statistics/Properties. Use n_to_s() to convert to
        // standard prometheus unit (second).
        histogram!(TOTAL_DURATION, &labels).record_many(self.start.elapsed(), WEIGHT);

        // write-related
        let v = self.context.metric(PerfMetric::WriteWalTime);
        if v != 0 {
            histogram!(WRITE_WAL_DURATION, &labels).record_many(n_to_s(v), WEIGHT);
        }
        let v = self.context.metric(PerfMetric::WriteMemtableTime);
        if v != 0 {
            histogram!(WRITE_MEMTABLE_DURATION, &labels).record_many(n_to_s(v), WEIGHT);
        }

        let v = self.context.metric(PerfMetric::WritePreAndPostProcessTime);
        if v != 0 {
            histogram!(WRITE_PRE_AND_POST_DURATION, &labels).record_many(n_to_s(v), WEIGHT);
        }

        let v = self.context.metric(PerfMetric::WriteDelayTime);
        if v != 0 {
            histogram!(WRITE_DELAY_DURATION, &labels).record_many(n_to_s(v), WEIGHT);
        }

        let v = self.context.metric(PerfMetric::DbMutexLockNanos);
        if v != 0 {
            histogram!(DB_MUTEX_DURATION, &labels).record_many(n_to_s(v), WEIGHT);
        }
    }
}

enum Inner {
    Noop,
    Real(RealRocksGuard),
}

#[inline]
/// nanos to seconds
fn n_to_s(v: u64) -> f64 {
    // Prometheus recommends base units, so we convert nanos to seconds
    // (fractions) when convenient.
    // See https://prometheus.io/docs/practices/naming/
    v as f64 / 1_000_000_000.0
}
