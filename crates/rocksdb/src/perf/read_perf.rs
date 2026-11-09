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
    BLOCK_CACHE_HIT, BLOCK_DECOMPRESS_DURATION, BLOCK_READ_BYTES, BLOCK_READ_DURATION,
    BLOCK_SEEK_DURATION, BLOOM_MEMTABLE_HIT, BLOOM_MEMTABLE_MISS, BLOOM_SST_HIT, BLOOM_SST_MISS,
    DATA_BLOCK_READ_BYTES, FILTER_BLOCK_READ_BYTES, FILTER_BLOCK_READ_DURATION,
    FIND_NEXT_USER_ENTRY, GET_FROM_MEMTABLE_DURATION, INDEX_BLOCK_READ_BYTES,
    INDEX_BLOCK_READ_DURATION, MERGE_OPERATOR_DURATION, NEXT_ON_MEMTABLE, SEEK_ON_MEMTABLE,
};

const fn m(metric: PerfMetric, name: &'static str, unit: Unit) -> MetricSpec {
    MetricSpec { metric, name, unit }
}

// Sample 10% of rocksdb read operations.
pub(super) static READ_SPEC: PerfSpec = PerfSpec::new(
    10,
    &[
        // iterators-related
        m(
            PerfMetric::NextOnMemtableCount,
            NEXT_ON_MEMTABLE,
            Unit::Count,
        ),
        m(
            PerfMetric::SeekOnMemtableTime,
            SEEK_ON_MEMTABLE,
            Unit::Nanos,
        ),
        m(
            PerfMetric::FindNextUserEntryTime,
            FIND_NEXT_USER_ENTRY,
            Unit::Nanos,
        ),
        // read-related
        m(PerfMetric::BlockReadByte, BLOCK_READ_BYTES, Unit::Count),
        m(
            PerfMetric::ReadIndexBlockNanos,
            INDEX_BLOCK_READ_DURATION,
            Unit::Nanos,
        ),
        m(
            PerfMetric::ReadFilterBlockNanos,
            FILTER_BLOCK_READ_DURATION,
            Unit::Nanos,
        ),
        m(
            PerfMetric::DataBlockReadByte,
            DATA_BLOCK_READ_BYTES,
            Unit::Count,
        ),
        m(
            PerfMetric::IndexBlockReadByte,
            INDEX_BLOCK_READ_BYTES,
            Unit::Count,
        ),
        m(
            PerfMetric::FilterBlockReadByte,
            FILTER_BLOCK_READ_BYTES,
            Unit::Count,
        ),
        m(PerfMetric::BlockSeekNanos, BLOCK_SEEK_DURATION, Unit::Nanos),
        m(PerfMetric::BlockReadTime, BLOCK_READ_DURATION, Unit::Nanos),
        m(
            PerfMetric::GetFromMemtableTime,
            GET_FROM_MEMTABLE_DURATION,
            Unit::Nanos,
        ),
        m(
            PerfMetric::BlockDecompressTime,
            BLOCK_DECOMPRESS_DURATION,
            Unit::Nanos,
        ),
        m(PerfMetric::BloomSstHitCount, BLOOM_SST_HIT, Unit::Count),
        m(PerfMetric::BloomSstMissCount, BLOOM_SST_MISS, Unit::Count),
        m(
            PerfMetric::BloomMemtableHitCount,
            BLOOM_MEMTABLE_HIT,
            Unit::Count,
        ),
        m(
            PerfMetric::BloomMemtableMissCount,
            BLOOM_MEMTABLE_MISS,
            Unit::Count,
        ),
        m(
            PerfMetric::MergeOperatorTimeNanos,
            MERGE_OPERATOR_DURATION,
            Unit::Nanos,
        ),
        m(PerfMetric::BlockCacheHitCount, BLOCK_CACHE_HIT, Unit::Count),
    ],
);

/// Measures the RocksDB reads done on this thread until it's dropped.
///
/// This guard is !Send. It must be created and dropped on the same thread since
/// it's backed by a thread-local in rocksdb. Don't hold it across an `.await`:
/// wrap the future with [`RocksDbPerfExt::with_read_perf`](super::RocksDbPerfExt::with_read_perf)
/// instead.
pub struct RocksDbReadPerfGuard(Option<SyncGuard>);

static_assertions::assert_not_impl_any!(RocksDbReadPerfGuard: Send);

impl RocksDbReadPerfGuard {
    /// IMPORTANT NOTE: you MUST bind this value with a named variable to ensure
    /// that the guard is not insta-dropped. Binding to something like `_x = ...` works, just don't
    /// use the special `_` variable.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self(SyncGuard::new(&READ_SPEC, name))
    }

    /// Drops the guard without reporting any metrics.
    pub fn forget(mut self) {
        if let Some(guard) = &mut self.0 {
            guard.forget();
        }
    }
}
