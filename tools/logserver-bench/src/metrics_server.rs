// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lightweight Prometheus metrics HTTP server that exposes both standard metrics and
//! RocksDB-specific statistics for real-time observability during benchmark runs.

use std::fmt::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use indexmap::IndexMap;
use metrics_exporter_prometheus::PrometheusHandle;
use metrics_exporter_prometheus::formatting;
use rocksdb::statistics::{Histogram, Ticker};
use tracing::info;

use restate_rocksdb::{CfName, RocksDbManager};

// Duplicated from crates/node/src/network_server/metrics.rs and prometheus_helpers.rs
// to avoid pulling in the entire node crate. These could be extracted into a shared
// crate in the future.

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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum MetricUnit {
    Micros,
    Bytes,
    Count,
}

impl MetricUnit {
    fn normalize_value(self, value: f64) -> f64 {
        match self {
            MetricUnit::Micros => value / 1_000_000.0,
            MetricUnit::Bytes | MetricUnit::Count => value,
        }
    }

    fn normalized_unit(self) -> metrics::Unit {
        match self {
            MetricUnit::Micros => metrics::Unit::Seconds,
            MetricUnit::Bytes => metrics::Unit::Bytes,
            MetricUnit::Count => metrics::Unit::Count,
        }
    }
}

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
        Histogram::BytesPerWrite,
        "rocksdb.bytes.per.write",
        MetricUnit::Bytes,
    ),
    (
        Histogram::BytesPerRead,
        "rocksdb.bytes.per.read",
        MetricUnit::Bytes,
    ),
];

const ROCKSDB_DB_PROPERTIES: &[(&str, MetricUnit)] = &[
    ("rocksdb.block-cache-capacity", MetricUnit::Bytes),
    ("rocksdb.block-cache-usage", MetricUnit::Bytes),
    ("rocksdb.block-cache-pinned-usage", MetricUnit::Bytes),
    ("rocksdb.num-running-flushes", MetricUnit::Count),
];

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
    ("rocksdb.live-sst-files-size", MetricUnit::Bytes),
    (
        "rocksdb.estimate-pending-compaction-bytes",
        MetricUnit::Bytes,
    ),
    ("rocksdb.num-running-compactions", MetricUnit::Count),
    ("rocksdb.actual-delayed-write-rate", MetricUnit::Count),
    ("rocksdb.num-files-at-level0", MetricUnit::Count),
    ("rocksdb.num-files-at-level1", MetricUnit::Count),
    ("rocksdb.num-files-at-level2", MetricUnit::Count),
    ("rocksdb.num-files-at-level3", MetricUnit::Count),
    ("rocksdb.num-files-at-level4", MetricUnit::Count),
    ("rocksdb.num-files-at-level5", MetricUnit::Count),
    ("rocksdb.num-files-at-level6", MetricUnit::Count),
];

#[derive(Clone)]
struct MetricsState {
    prometheus_handle: Arc<PrometheusHandle>,
}

pub fn start_metrics_server(port: u16, prometheus_handle: PrometheusHandle) {
    if port == 0 {
        return;
    }
    let state = MetricsState {
        prometheus_handle: Arc::new(prometheus_handle),
    };
    let app = axum::Router::new()
        .route("/metrics", get(render_metrics))
        .with_state(state);

    tokio::spawn(async move {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                restate_cli_util::c_warn!("Failed to bind metrics server on port {port}: {e}");
                return;
            }
        };
        info!("Metrics server listening on http://{addr}/metrics");
        if let Err(e) = axum::serve(listener, app).await {
            restate_cli_util::c_error!("Metrics server error: {e}");
        }
    });
}

async fn render_metrics(State(state): State<MetricsState>) -> String {
    let mut out = state.prometheus_handle.render();
    append_rocksdb_metrics(&mut out);
    out
}

fn append_rocksdb_metrics(out: &mut String) {
    let manager = RocksDbManager::get();
    let all_dbs = manager.get_all_dbs();
    let default_cf = CfName::new("default");
    let mut labels = IndexMap::new();

    // Write buffer manager stats
    format_property(
        out,
        &labels,
        MetricUnit::Bytes,
        "rocksdb.memory.write_buffer_manager_capacity",
        manager.get_total_write_buffer_capacity(),
    );
    format_property(
        out,
        &labels,
        MetricUnit::Bytes,
        "rocksdb.memory.write_buffer_manager_usage",
        manager.get_total_write_buffer_usage(),
    );

    for db in &all_dbs {
        labels.insert("db".to_owned(), formatting::sanitize_label_value(db.name()));

        // Tickers
        for ticker in ROCKSDB_TICKERS {
            format_ticker(out, db, &labels, *ticker);
        }

        // Histograms
        for (histogram, name, unit) in ROCKSDB_HISTOGRAMS {
            format_histogram(out, name, db.get_histogram_data(*histogram), *unit, &labels);
        }

        // Memory usage
        if let Ok(memory) = manager.get_memory_usage_stats(&[]) {
            format_property(
                out,
                &labels,
                MetricUnit::Bytes,
                "rocksdb.memory.approx-memtable",
                memory.approximate_mem_table_total(),
            );
            format_property(
                out,
                &labels,
                MetricUnit::Bytes,
                "rocksdb.memory.approx-memtable-unflushed",
                memory.approximate_mem_table_unflushed(),
            );
            format_property(
                out,
                &labels,
                MetricUnit::Bytes,
                "rocksdb.memory.approx-memtable-readers",
                memory.approximate_mem_table_readers_total(),
            );
        }

        // Per-DB properties
        for (property, unit) in ROCKSDB_DB_PROPERTIES {
            format_property(
                out,
                &labels,
                *unit,
                property,
                db.inner()
                    .get_property_int_cf(&default_cf, property)
                    .unwrap_or_default()
                    .unwrap_or_default(),
            );
        }

        // Per-CF properties
        for cf in &db.cfs() {
            labels.insert("cf".to_owned(), formatting::sanitize_label_value(cf));
            for (property, unit) in ROCKSDB_CF_PROPERTIES {
                format_property(
                    out,
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
}

fn format_ticker(
    out: &mut String,
    db: &restate_rocksdb::RocksDb,
    labels: &IndexMap<String, String>,
    ticker: Ticker,
) {
    let name = format!(
        "restate_{}",
        formatting::sanitize_metric_name(ticker.name())
    );
    formatting::write_type_line(out, &name, None, Some("total"), "counter");
    formatting::write_metric_line::<&str, u64>(
        out,
        &name,
        Some("total"),
        &metrics_exporter_prometheus::LabelSet::from_key_and_global(
            &metrics::Key::from_static_name("x"),
            labels,
        ),
        None,
        db.get_ticker_count(ticker),
        None,
    );
    let _ = writeln!(out);
}

fn format_property(
    out: &mut String,
    labels: &IndexMap<String, String>,
    unit: MetricUnit,
    property_name: &str,
    value: u64,
) {
    let name = format!(
        "restate_{}",
        formatting::sanitize_metric_name(property_name),
    );
    formatting::write_type_line(out, &name, Some(unit.normalized_unit()), None, "gauge");
    formatting::write_metric_line::<&str, u64>(
        out,
        &name,
        None,
        &metrics_exporter_prometheus::LabelSet::from_key_and_global(
            &metrics::Key::from_static_name("x"),
            labels,
        ),
        None,
        value,
        Some(unit.normalized_unit()),
    );
    let _ = writeln!(out);
}

fn format_histogram(
    out: &mut String,
    name: &str,
    data: rocksdb::statistics::HistogramData,
    unit: MetricUnit,
    labels: &IndexMap<String, String>,
) {
    let name = format!("restate_{}", formatting::sanitize_metric_name(name));
    formatting::write_type_line(out, &name, Some(unit.normalized_unit()), None, "summary");

    let labels = metrics_exporter_prometheus::LabelSet::from_key_and_global(
        &metrics::Key::from_static_name("x"),
        labels,
    );
    for (quantile, value) in [
        ("0.5", data.median()),
        ("0.95", data.p95()),
        ("0.99", data.p99()),
        ("1.0", data.max()),
    ] {
        formatting::write_metric_line::<&str, f64>(
            out,
            &name,
            None,
            &labels,
            Some(("quantile", quantile)),
            unit.normalize_value(value),
            Some(unit.normalized_unit()),
        );
    }
    formatting::write_metric_line::<&str, f64>(
        out,
        &name,
        Some("sum"),
        &labels,
        None,
        unit.normalize_value(data.sum() as f64),
        Some(unit.normalized_unit()),
    );
    formatting::write_metric_line::<&str, u64>(
        out,
        &name,
        Some("count"),
        &labels,
        None,
        data.count(),
        Some(unit.normalized_unit()),
    );
    let _ = writeln!(out);
}
