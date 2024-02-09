// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod cluster_controller;
pub mod node_ctrl;
pub mod worker;

use std::fmt::Write;
use std::iter::once;

use axum::extract::State;
use axum::response::IntoResponse;
use metrics_exporter_prometheus::formatting;
use restate_storage_rocksdb::{TableKind, DB};
use rocksdb::statistics::{Histogram, Ticker};
use rocksdb::AsColumnFamilyRef;

use crate::prometheus_helpers::{
    format_rocksdb_histogram_for_prometheus, format_rocksdb_property_for_prometheus,
    format_rocksdb_stat_ticker_for_prometheus, MetricUnit,
};
use crate::state::HandlerState;

static ROCKSDB_TICKERS: &[Ticker] = &[
    Ticker::BlockCacheDataBytesInsert,
    Ticker::BlockCacheDataHit,
    Ticker::BlockCacheDataMiss,
    Ticker::BloomFilterUseful,
    Ticker::BytesRead,
    Ticker::BytesWritten,
    Ticker::CompactReadBytes,
    Ticker::CompactWriteBytes,
    Ticker::FlushWriteBytes,
    Ticker::MemtableHit,
    Ticker::MemtableMiss,
    Ticker::NoIteratorCreated,
    Ticker::NoIteratorDeleted,
    Ticker::NumberKeysRead,
    Ticker::NumberKeysUpdated,
    Ticker::NumberKeysWritten,
    Ticker::StallMicros,
    Ticker::WalFileBytes,
    Ticker::WalFileSynced,
    Ticker::WriteWithWal,
];

static ROCKSDB_HISTOGRAMS: &[(Histogram, &str, MetricUnit)] = &[
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
        Histogram::WalFileSyncMicros,
        "rocksdb.wal.file.sync",
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

// Per column-family properties
static ROCKSDB_PROPERTIES: &[(&str, MetricUnit)] = &[
    ("rocksdb.num-immutable-mem-table", MetricUnit::Count),
    ("rocksdb.mem-table-flush-pending", MetricUnit::Count),
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
    ("rocksdb.block-cache-capacity", MetricUnit::Bytes),
    ("rocksdb.block-cache-usage", MetricUnit::Bytes),
    ("rocksdb.block-cache-pinned-usage", MetricUnit::Bytes),
    ("rocksdb.num-files-at-level0", MetricUnit::Count),
    ("rocksdb.num-files-at-level1", MetricUnit::Count),
    // Add more as needed.
    ("rocksdb.num-files-at-level2", MetricUnit::Count),
];

// -- Direct HTTP Handlers --
pub async fn render_metrics(State(state): State<HandlerState>) -> String {
    let mut out = String::new();

    // Response content type is plain/text and that's expected.
    if let Some(prometheus_handle) = state.prometheus_handle {
        // Internal system metrics
        let _ = write!(&mut out, "{}", prometheus_handle.render());
    }

    // Load metrics from rocksdb (if the node runs rocksdb, and rocksdb
    // stat collection is enabled)
    let Some(db) = state.rocksdb_storage else {
        return out;
    };

    let raw_db = db.inner();
    let options = db.options();
    // Tickers (Counters)
    for ticker in ROCKSDB_TICKERS {
        format_rocksdb_stat_ticker_for_prometheus(&mut out, &options, *ticker);
    }
    // Histograms
    for (histogram, name, unit) in ROCKSDB_HISTOGRAMS {
        format_rocksdb_histogram_for_prometheus(
            &mut out,
            name,
            options.get_histogram_data(*histogram),
            *unit,
        );
    }

    // Properties (Gauges)
    // For properties, we need to get them for each column family.
    let all_cfs = TableKind::all()
        .map(TableKind::cf_name)
        // Include the default column family
        .chain(once("default"));

    for cf in all_cfs {
        let Some(cf_handle) = raw_db.cf_handle(cf) else {
            continue;
        };
        let sanitized_cf_name = formatting::sanitize_label_value(cf);
        let labels = [format!("cf=\"{}\"", sanitized_cf_name)];
        for (property, unit) in ROCKSDB_PROPERTIES {
            format_rocksdb_property_for_prometheus(
                &mut out,
                &labels,
                *unit,
                property,
                get_property(&raw_db, &cf_handle, property),
            );
        }
    }

    // Memory Usage Stats (Gauges)
    let cache = db.cache();
    let memory_usage =
        get_memory_usage_stats(Some(&[&raw_db]), cache).expect("get_memory_usage_stats");

    format_rocksdb_property_for_prometheus(
        &mut out,
        &[],
        MetricUnit::Bytes,
        "rocksdb.memory.approximate-cache",
        memory_usage.approximate_cache_total(),
    );

    format_rocksdb_property_for_prometheus(
        &mut out,
        &[],
        MetricUnit::Bytes,
        "rocksdb.memory.approx-memtable",
        memory_usage.approximate_mem_table_total(),
    );

    format_rocksdb_property_for_prometheus(
        &mut out,
        &[],
        MetricUnit::Bytes,
        "rocksdb.memory.approx-memtable-unflushed",
        memory_usage.approximate_mem_table_unflushed(),
    );

    format_rocksdb_property_for_prometheus(
        &mut out,
        &[],
        MetricUnit::Bytes,
        "rocksdb.memory.approx-memtable-readers",
        memory_usage.approximate_mem_table_readers_total(),
    );
    out
}

pub async fn rocksdb_stats(State(state): State<HandlerState>) -> impl IntoResponse {
    let Some(db) = state.rocksdb_storage else {
        return String::new();
    };

    let options = db.options();
    options.get_statistics().unwrap_or_default()
}

// -- Local Helpers
#[inline]
fn get_property(db: &DB, cf_handle: &impl AsColumnFamilyRef, name: &str) -> u64 {
    db.property_int_value_cf(cf_handle, name)
        .unwrap_or_default()
        .unwrap_or_default()
}

fn get_memory_usage_stats(
    dbs: Option<&[&DB]>,
    cache: Option<rocksdb::Cache>,
) -> Result<rocksdb::perf::MemoryUsage, rocksdb::Error> {
    let mut builder = rocksdb::perf::MemoryUsageBuilder::new()?;
    if let Some(dbs_) = dbs {
        dbs_.iter().for_each(|db| builder.add_db(db));
    }
    if let Some(cache) = cache {
        builder.add_cache(&cache);
    }

    builder.build()
}
