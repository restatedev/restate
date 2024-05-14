// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;

use metrics::{counter, histogram};
use rocksdb::{PerfContext, PerfMetric, PerfStatsLevel};

use crate::background::StorageTaskKind;
use crate::{
    BLOCK_READ_BYTES, BLOCK_READ_COUNT, OP_TYPE, WRITE_ARTIFICIAL_DELAY_DURATION,
    WRITE_MEMTABLE_DURATION, WRITE_PRE_AND_POST_DURATION, WRITE_WAL_DURATION,
};

thread_local! {
    static ROCKSDB_PERF_CONTEXT: RefCell<PerfContext>  = RefCell::new(PerfContext::default());
}

/// This guard must be created and dropped in the same thread, you should never use the same
/// guard across .await points. This should strictly be used within the bounds of the sync
/// RocksAccess layer.
pub struct RocksDbPerfGuard {
    kind: StorageTaskKind,
}

impl RocksDbPerfGuard {
    /// IMPORTANT NOTE: you MUST bind this value with a named variable (f) to ensure
    /// that the guard is not insta-dropped. Binding to something like `_x = ...` works, just don't
    /// use the special `_` variable and you'll be fine. Unfortunately, rust/clippy doesn't have a
    /// mechanism to enforce this at the moment.
    #[must_use]
    pub fn new(kind: StorageTaskKind) -> Self {
        rocksdb::perf::set_perf_stats(PerfStatsLevel::EnableTimeExceptForMutex);
        ROCKSDB_PERF_CONTEXT.with(|context| {
            context.borrow_mut().reset();
        });
        RocksDbPerfGuard { kind }
    }
}

impl Drop for RocksDbPerfGuard {
    fn drop(&mut self) {
        rocksdb::perf::set_perf_stats(PerfStatsLevel::Disable);
        // report collected metrics
        ROCKSDB_PERF_CONTEXT.with(|context| {
            // Note to future visitors of this code. RocksDb reports times in nanoseconds in this
            // API compared to microseconds in Statistics/Properties. Use n_to_s() to convert to
            // standard prometheus unit (second).
            let context = context.borrow();
            let v = context.metric(PerfMetric::BlockReadCount);
            if v != 0 {
                counter!(BLOCK_READ_COUNT,
                     OP_TYPE => self.kind.as_static_str(),
                )
                .increment(v);
            }
            let v = context.metric(PerfMetric::BlockReadByte);
            if v != 0 {
                counter!(BLOCK_READ_BYTES,
                     OP_TYPE => self.kind.as_static_str(),
                )
                .increment(v)
            };

            let v = context.metric(PerfMetric::WriteWalTime);
            if v != 0 {
                histogram!(WRITE_WAL_DURATION,
                     OP_TYPE => self.kind.as_static_str(),
                )
                .record(n_to_s(v));
            }
            let v = context.metric(PerfMetric::WriteMemtableTime);
            if v != 0 {
                histogram!(WRITE_MEMTABLE_DURATION,
                     OP_TYPE => self.kind.as_static_str(),
                )
                .record(n_to_s(v));
            }

            let v = context.metric(PerfMetric::WritePreAndPostProcessTime);
            if v != 0 {
                histogram!(WRITE_PRE_AND_POST_DURATION,
                     OP_TYPE => self.kind.as_static_str(),
                )
                .record(n_to_s(v));
            }

            let v = context.metric(PerfMetric::WriteDelayTime);
            if v != 0 {
                histogram!(WRITE_ARTIFICIAL_DELAY_DURATION,
                     OP_TYPE => self.kind.as_static_str(),
                )
                .record(n_to_s(v));
            }
        });
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
