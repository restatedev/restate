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
use std::time::Instant;

use metrics::{counter, histogram};
use restate_types::config::{Configuration, PerfStatsLevel};
use rocksdb::{PerfContext, PerfMetric};

use crate::{
    BLOCK_DECOMPRESS_DURATION, BLOCK_READ_BYTES, BLOCK_READ_DURATION, GET_FROM_MEMTABLE_DURATION,
    NEXT_ON_MEMTABLE, OP_NAME, SEEK_ON_MEMTABLE, TOTAL_DURATION, WRITE_ARTIFICIAL_DELAY_DURATION,
    WRITE_MEMTABLE_DURATION, WRITE_PRE_AND_POST_DURATION, WRITE_WAL_DURATION,
};

thread_local! {
    static ROCKSDB_PERF_CONTEXT: RefCell<PerfContext>  = RefCell::new(PerfContext::default());
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

/// This guard must be created and dropped in the same thread, you should never use the same
/// guard across .await points. This should strictly be used within the bounds of the sync
/// RocksAccess layer.
pub struct RocksDbPerfGuard {
    start: Instant,
    name: &'static str,
}

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
        ROCKSDB_PERF_CONTEXT.with(|context| {
            context.borrow_mut().reset();
        });
        RocksDbPerfGuard {
            name,
            start: Instant::now(),
        }
    }

    /// Drops the guard without reporting any metrics.
    pub fn forget(self) {
        rocksdb::perf::set_perf_stats(rocksdb::perf::PerfStatsLevel::Disable);
        let _ = std::mem::ManuallyDrop::new(self);
    }
}

impl Drop for RocksDbPerfGuard {
    fn drop(&mut self) {
        rocksdb::perf::set_perf_stats(rocksdb::perf::PerfStatsLevel::Disable);
        // report collected metrics
        ROCKSDB_PERF_CONTEXT.with(|context| {
            // Note to future visitors of this code. RocksDb reports times in nanoseconds in this
            // API compared to microseconds in Statistics/Properties. Use n_to_s() to convert to
            // standard prometheus unit (second).
            let context = context.borrow();
            histogram!(TOTAL_DURATION,
                 OP_NAME => self.name,
            )
            .record(self.start.elapsed());

            // iterators-related
            let v = context.metric(PerfMetric::NextOnMemtableCount);
            if v != 0 {
                counter!(NEXT_ON_MEMTABLE,
                     OP_NAME => self.name,
                )
                .increment(v)
            };
            let v = context.metric(PerfMetric::SeekOnMemtableTime);
            if v != 0 {
                histogram!(SEEK_ON_MEMTABLE,
                     OP_NAME => self.name,
                )
                .record(n_to_s(v));
            }
            let v = context.metric(PerfMetric::FindNextUserEntryTime);
            if v != 0 {
                histogram!(SEEK_ON_MEMTABLE,
                     OP_NAME => self.name,
                )
                .record(n_to_s(v));
            }

            // read-related
            let v = context.metric(PerfMetric::BlockReadByte);
            if v != 0 {
                counter!(BLOCK_READ_BYTES,
                     OP_NAME => self.name,
                )
                .increment(v)
            };

            let v = context.metric(PerfMetric::BlockReadTime);
            if v != 0 {
                histogram!(BLOCK_READ_DURATION,
                     OP_NAME => self.name,
                )
                .record(n_to_s(v));
            }

            let v = context.metric(PerfMetric::GetFromMemtableTime);
            if v != 0 {
                histogram!(GET_FROM_MEMTABLE_DURATION,
                     OP_NAME => self.name,
                )
                .record(n_to_s(v));
            }

            let v = context.metric(PerfMetric::BlockDecompressTime);
            if v != 0 {
                histogram!(BLOCK_DECOMPRESS_DURATION,
                     OP_NAME => self.name,
                )
                .record(n_to_s(v));
            }

            // write-related
            let v = context.metric(PerfMetric::WriteWalTime);
            if v != 0 {
                histogram!(WRITE_WAL_DURATION,
                     OP_NAME => self.name,
                )
                .record(n_to_s(v));
            }
            let v = context.metric(PerfMetric::WriteMemtableTime);
            if v != 0 {
                histogram!(WRITE_MEMTABLE_DURATION,
                     OP_NAME => self.name,
                )
                .record(n_to_s(v));
            }

            let v = context.metric(PerfMetric::WritePreAndPostProcessTime);
            if v != 0 {
                histogram!(WRITE_PRE_AND_POST_DURATION,
                     OP_NAME => self.name,
                )
                .record(n_to_s(v));
            }

            let v = context.metric(PerfMetric::WriteDelayTime);
            if v != 0 {
                histogram!(WRITE_ARTIFICIAL_DELAY_DURATION,
                     OP_NAME => self.name,
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
