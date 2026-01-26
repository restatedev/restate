// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;

use axum::extract::State;
use metrics_exporter_prometheus::formatting;
use rocksdb::statistics::{Histogram, Ticker};

use restate_core::task_center::TaskCenterMonitoring;
use restate_rocksdb::{CfName, RocksDbManager};

use crate::network_server::prometheus_helpers::{
    MetricUnit, format_rocksdb_histogram_for_prometheus, format_rocksdb_property_for_prometheus,
    format_rocksdb_stat_ticker_for_prometheus,
};
use crate::network_server::state::NodeCtrlHandlerState;

const ROCKSDB_TICKERS: &[Ticker] = &[
    Ticker::BlockCacheBytesRead,
    Ticker::BlockCacheBytesWrite,
    Ticker::BlockCacheHit,
    Ticker::BlockCacheMiss,
    Ticker::BloomFilterUseful,
    Ticker::BytesRead,
    Ticker::BytesWritten,
    Ticker::CompactReadBytes,
    Ticker::CompactWriteBytes,
    Ticker::FlushWriteBytes,
    Ticker::IterBytesRead,
    Ticker::MemtableHit,
    Ticker::MemtableMiss,
    Ticker::NoIteratorCreated,
    Ticker::NoIteratorDeleted,
    Ticker::NumberDbNext,
    Ticker::NumberDbSeek,
    Ticker::NumberIterSkip,
    Ticker::NumberKeysRead,
    Ticker::NumberKeysUpdated,
    Ticker::NumberKeysWritten,
    Ticker::NumberOfReseeksInIteration,
    Ticker::StallMicros,
    Ticker::WalFileBytes,
    Ticker::WalFileSynced,
    Ticker::WriteWithWal,
];

const ROCKSDB_HISTOGRAMS: &[(Histogram, &str, MetricUnit)] = &[
    (Histogram::DbGet, "rocksdb.db.get", MetricUnit::Micros),
    (
        Histogram::DbMultiget,
        "rocksdb.db.multiget",
        MetricUnit::Micros,
    ),
    (Histogram::DbWrite, "rocksdb.db.write", MetricUnit::Micros),
    (Histogram::DbSeek, "rocksdb.db.seek", MetricUnit::Micros),
    (Histogram::FlushTime, "rocksdb.db.flush", MetricUnit::Micros),
    (
        Histogram::ReadBlockGetMicros,
        "rocksdb.read.block.get",
        MetricUnit::Micros,
    ),
    (
        Histogram::SstReadMicros,
        "rocksdb.sst.read",
        MetricUnit::Micros,
    ),
    (
        Histogram::SstWriteMicros,
        "rocksdb.sst.write",
        MetricUnit::Micros,
    ),
    (
        Histogram::ReadNumMergeOperands,
        Histogram::ReadNumMergeOperands.name(),
        MetricUnit::Count,
    ),
    (
        Histogram::NumSstReadPerLevel,
        Histogram::NumSstReadPerLevel.name(),
        MetricUnit::Count,
    ),
    (
        Histogram::WalFileSyncMicros,
        "rocksdb.wal.file.sync",
        MetricUnit::Micros,
    ),
    (
        Histogram::AsyncReadBytes,
        "rocksdb.async.read",
        MetricUnit::Bytes,
    ),
    (
        Histogram::PollWaitMicros,
        "rocksdb.poll.wait",
        MetricUnit::Micros,
    ),
    (
        Histogram::CompactionTime,
        "rocksdb.compaction.times",
        MetricUnit::Micros,
    ),
    (
        Histogram::SstBatchSize,
        Histogram::SstBatchSize.name(),
        MetricUnit::Bytes,
    ),
    (
        Histogram::BytesPerWrite,
        Histogram::BytesPerWrite.name(),
        MetricUnit::Bytes,
    ),
    (
        Histogram::BytesPerRead,
        Histogram::BytesPerRead.name(),
        MetricUnit::Bytes,
    ),
    (
        Histogram::BytesPerMultiget,
        Histogram::BytesPerMultiget.name(),
        MetricUnit::Bytes,
    ),
];

// Per database properties
const ROCKSDB_DB_PROPERTIES: &[(&str, MetricUnit)] = &[
    ("rocksdb.block-cache-capacity", MetricUnit::Bytes),
    ("rocksdb.block-cache-usage", MetricUnit::Bytes),
    ("rocksdb.block-cache-pinned-usage", MetricUnit::Bytes),
    ("rocksdb.num-running-flushes", MetricUnit::Count),
];

// Per column-family properties
const ROCKSDB_CF_PROPERTIES: &[(&str, MetricUnit)] = &[
    ("rocksdb.num-immutable-mem-table", MetricUnit::Count),
    ("rocksdb.mem-table-flush-pending", MetricUnit::Count),
    ("rocksdb.is-write-stopped", MetricUnit::Count),
    ("rocksdb.compaction-pending", MetricUnit::Count),
    ("rocksdb.background-errors", MetricUnit::Count),
    ("rocksdb.cur-size-active-mem-table", MetricUnit::Bytes),
    ("rocksdb.cur-size-all-mem-tables", MetricUnit::Bytes),
    ("rocksdb.size-all-mem-tables", MetricUnit::Bytes),
    ("rocksdb.num-entries-active-mem-table", MetricUnit::Count),
    ("rocksdb.num-entries-imm-mem-tables", MetricUnit::Count),
    ("rocksdb.num-deletes-active-mem-table", MetricUnit::Count),
    ("rocksdb.num-deletes-imm-mem-tables", MetricUnit::Count),
    ("rocksdb.estimate-num-keys", MetricUnit::Count),
    ("rocksdb.estimate-table-readers-mem", MetricUnit::Bytes),
    ("rocksdb.num-live-versions", MetricUnit::Count),
    ("rocksdb.estimate-live-data-size", MetricUnit::Bytes),
    ("rocksdb.min-log-number-to-keep", MetricUnit::Count),
    ("rocksdb.live-sst-files-size", MetricUnit::Bytes),
    (
        "rocksdb.estimate-pending-compaction-bytes",
        MetricUnit::Bytes,
    ),
    ("rocksdb.num-running-compactions", MetricUnit::Count),
    ("rocksdb.actual-delayed-write-rate", MetricUnit::Count),
    ("rocksdb.num-files-at-level0", MetricUnit::Count),
    ("rocksdb.num-files-at-level1", MetricUnit::Count),
    // Add more as needed.
    ("rocksdb.num-files-at-level2", MetricUnit::Count),
    ("rocksdb.num-files-at-level3", MetricUnit::Count),
    ("rocksdb.num-files-at-level4", MetricUnit::Count),
    ("rocksdb.num-files-at-level5", MetricUnit::Count),
    ("rocksdb.num-files-at-level6", MetricUnit::Count),
];

// -- Direct HTTP Handlers --

pub async fn render_metrics(State(state): State<NodeCtrlHandlerState>) -> String {
    let default_cf = CfName::new("default");
    let mut out = String::new();

    // Response content type is plain/text and that's expected.
    if let Some(prometheus_handle) = state.prometheus_handle.handle() {
        // Default tokio runtime metrics
        state.task_center.submit_metrics();
        // jemalloc memory statistics
        #[cfg(not(target_env = "msvc"))]
        super::jemalloc::submit_metrics();
        // Internal system metrics
        let _ = write!(&mut out, "{}", prometheus_handle.render());
    }

    let manager = RocksDbManager::get();
    let all_dbs = manager.get_all_dbs();

    let mut labels = state.prometheus_handle.global_labels().clone();

    // Overall write buffer manager stats
    format_rocksdb_property_for_prometheus(
        &mut out,
        &labels,
        MetricUnit::Bytes,
        "rocksdb.memory.write_buffer_manager_capacity",
        manager.get_total_write_buffer_capacity(),
    );

    format_rocksdb_property_for_prometheus(
        &mut out,
        &labels,
        MetricUnit::Bytes,
        "rocksdb.memory.write_buffer_manager_usage",
        manager.get_total_write_buffer_usage(),
    );

    for db in &all_dbs {
        labels.insert("db".to_owned(), formatting::sanitize_label_value(db.name()));

        // Tickers (Counters)
        for ticker in ROCKSDB_TICKERS {
            format_rocksdb_stat_ticker_for_prometheus(&mut out, db, &labels, *ticker);
        }
        // Histograms
        for (histogram, name, unit) in ROCKSDB_HISTOGRAMS {
            format_rocksdb_histogram_for_prometheus(
                &mut out,
                name,
                db.get_histogram_data(*histogram),
                *unit,
                &labels,
            );
        }

        // Memory Usage Stats (Gauges)
        let memory_usage = manager
            .get_memory_usage_stats(&[])
            .expect("get_memory_usage_stats");

        format_rocksdb_property_for_prometheus(
            &mut out,
            &labels,
            MetricUnit::Bytes,
            "rocksdb.memory.approx-memtable",
            memory_usage.approximate_mem_table_total(),
        );

        format_rocksdb_property_for_prometheus(
            &mut out,
            &labels,
            MetricUnit::Bytes,
            "rocksdb.memory.approx-memtable-unflushed",
            memory_usage.approximate_mem_table_unflushed(),
        );

        format_rocksdb_property_for_prometheus(
            &mut out,
            &labels,
            MetricUnit::Bytes,
            "rocksdb.memory.approx-memtable-readers",
            memory_usage.approximate_mem_table_readers_total(),
        );

        // Other per-database properties
        for (property, unit) in ROCKSDB_DB_PROPERTIES {
            format_rocksdb_property_for_prometheus(
                &mut out,
                &labels,
                *unit,
                property,
                db.inner()
                    .get_property_int_cf(&default_cf, property)
                    .unwrap_or_default()
                    .unwrap_or_default(),
            );
        }

        // Properties (Gauges)
        // For properties, we need to get them for each column family.
        for cf in &db.cfs() {
            labels.insert("cf".to_owned(), formatting::sanitize_label_value(cf));
            for (property, unit) in ROCKSDB_CF_PROPERTIES {
                format_rocksdb_property_for_prometheus(
                    &mut out,
                    &labels,
                    *unit,
                    property,
                    db.inner()
                        .get_property_int_cf(cf, property)
                        .unwrap_or_default()
                        .unwrap_or_default(),
                );
            }
        }
    }

    out
}
