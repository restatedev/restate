// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::PerfMetric;

use super::collector::{MetricSpec, PerfSpec, SyncGuard, Unit};
use crate::metric_definitions::{
    DB_MUTEX_DURATION, WRITE_DELAY_DURATION, WRITE_MEMTABLE_DURATION, WRITE_PRE_AND_POST_DURATION,
    WRITE_WAL_DURATION,
};

const fn m(metric: PerfMetric, name: &'static str) -> MetricSpec {
    MetricSpec {
        metric,
        name,
        unit: Unit::Nanos,
    }
}

// Sample 33% of rocksdb write operations.
pub(super) static WRITE_SPEC: PerfSpec = PerfSpec::new(
    3,
    &[
        m(PerfMetric::WriteWalTime, WRITE_WAL_DURATION),
        m(PerfMetric::WriteMemtableTime, WRITE_MEMTABLE_DURATION),
        m(
            PerfMetric::WritePreAndPostProcessTime,
            WRITE_PRE_AND_POST_DURATION,
        ),
        m(PerfMetric::WriteDelayTime, WRITE_DELAY_DURATION),
        m(PerfMetric::DbMutexLockNanos, DB_MUTEX_DURATION),
    ],
);

/// Measures the RocksDB writes done on this thread until it's dropped.
///
/// This guard is !Send. It must be created and dropped on the same thread since
/// it's backed by a thread-local in rocksdb. Don't hold it across an `.await`:
/// wrap the future with [`RocksDbPerfExt::with_write_perf`](super::RocksDbPerfExt::with_write_perf)
/// instead.
pub struct RocksDbWritePerfGuard(Option<SyncGuard>);

static_assertions::assert_not_impl_any!(RocksDbWritePerfGuard: Send);

impl RocksDbWritePerfGuard {
    /// IMPORTANT NOTE: you MUST bind this value with a named variable to ensure
    /// that the guard is not insta-dropped. Binding to something like `_x = ...` works, just don't
    /// use the special `_` variable.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self(SyncGuard::new(&WRITE_SPEC, name))
    }

    /// Drops the guard without reporting any metrics.
    pub fn forget(mut self) {
        if let Some(guard) = &mut self.0 {
            guard.forget();
        }
    }
}
