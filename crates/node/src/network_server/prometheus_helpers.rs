// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;

use metrics::gauge;
use metrics_exporter_prometheus::formatting;
use restate_rocksdb::RocksDb;
use rocksdb::statistics::{HistogramData, Ticker};
use tokio::runtime::RuntimeMetrics;

static PREFIX: &str = "restate";

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MetricUnit {
    Micros,
    Bytes,
    Count,
}

impl MetricUnit {
    fn normalize_value(&self, value: f64) -> f64 {
        match self {
            // Prometheus recommends base units, so we convert micros to seconds
            // (fractions) when convenient.
            // See https://prometheus.io/docs/practices/naming/
            MetricUnit::Micros => value / 1_000_000.0,
            MetricUnit::Bytes => value,
            MetricUnit::Count => value,
        }
    }
    fn normalized_unit(&self) -> &str {
        match self {
            MetricUnit::Micros => "seconds",
            MetricUnit::Bytes => "bytes",
            MetricUnit::Count => "count",
        }
    }
}

pub fn format_rocksdb_stat_ticker_for_prometheus(
    out: &mut String,
    db: &RocksDb,
    labels: &[String],
    ticker: Ticker,
) {
    let sanitized_name = format!(
        "{}_{}_total",
        PREFIX,
        formatting::sanitize_metric_name(ticker.name())
    );
    formatting::write_type_line(out, &sanitized_name, "counter");
    formatting::write_metric_line::<&str, u64>(
        out,
        &sanitized_name,
        None,
        labels,
        None,
        db.get_ticker_count(ticker),
    );
    let _ = writeln!(out);
}

pub fn format_rocksdb_property_for_prometheus(
    out: &mut String,
    labels: &[String],
    unit: MetricUnit,
    property_name: &str,
    property_value: u64,
) {
    let sanitized_name = format!(
        "{}_{}_{}",
        PREFIX,
        formatting::sanitize_metric_name(property_name),
        unit.normalized_unit()
    );

    formatting::write_type_line(out, &sanitized_name, "gauge");
    formatting::write_metric_line::<&str, u64>(
        out,
        &sanitized_name,
        None,
        labels,
        None,
        property_value,
    );
    let _ = writeln!(out);
}

//
// Follows prometheus exposition format like following:
//    rpc_duration_seconds{quantile="0.01"} 3102
//    rpc_duration_seconds{quantile="0.05"} 3272
//    rpc_duration_seconds{quantile="0.5"} 4773
//    rpc_duration_seconds{quantile="0.9"} 9001
//    rpc_duration_seconds{quantile="0.99"} 76656
//    rpc_duration_seconds_sum 1.7560473e+07
//    rpc_duration_seconds_count 2693
//
pub fn format_rocksdb_histogram_for_prometheus(
    out: &mut String,
    name: &str,
    data: HistogramData,
    unit: MetricUnit,
    labels: &[String],
) {
    let base_sanitized_name = format!(
        "{}_{}_{}",
        PREFIX,
        formatting::sanitize_metric_name(name),
        unit.normalized_unit()
    );
    formatting::write_type_line(out, &base_sanitized_name, "summary");

    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        labels,
        Some(("quantile", "0.5")),
        unit.normalize_value(data.median()),
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        labels,
        Some(("quantile", "0.95")),
        unit.normalize_value(data.p95()),
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        labels,
        Some(("quantile", "0.99")),
        unit.normalize_value(data.p99()),
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        labels,
        Some(("quantile", "1.0")),
        unit.normalize_value(data.max()),
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        Some("sum"),
        labels,
        None,
        unit.normalize_value(data.sum() as f64),
    );
    formatting::write_metric_line::<&str, u64>(
        out,
        &base_sanitized_name,
        Some("count"),
        labels,
        None,
        data.count(),
    );
    let _ = writeln!(out);
}

pub fn submit_tokio_metrics(runtime: &'static str, stats: RuntimeMetrics) {
    gauge!("restate.tokio.num_workers", "runtime" => runtime).set(stats.num_workers() as f64);
    gauge!("restate.tokio.blocking_threads", "runtime" => runtime)
        .set(stats.num_blocking_threads() as f64);
    gauge!("restate.tokio.blocking_queue_depth", "runtime" => runtime)
        .set(stats.blocking_queue_depth() as f64);
    gauge!("restate.tokio.active_tasks_count", "runtime" => runtime)
        .set(stats.active_tasks_count() as f64);
    gauge!("restate.tokio.io_driver_ready_count", "runtime" => runtime)
        .set(stats.io_driver_ready_count() as f64);
    gauge!("restate.tokio.remote_schedule_count", "runtime" => runtime)
        .set(stats.remote_schedule_count() as f64);
    // per worker stats
    for idx in 0..stats.num_workers() {
        gauge!("restate.tokio.worker_overflow_count", "runtime" => runtime, "worker" =>
            idx.to_string())
        .set(stats.worker_overflow_count(idx) as f64);
        gauge!("restate.tokio.worker_poll_count", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_poll_count(idx) as f64);
        gauge!("restate.tokio.worker_park_count", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_park_count(idx) as f64);
        gauge!("restate.tokio.worker_noop_count", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_noop_count(idx) as f64);
        gauge!("restate.tokio.worker_steal_count", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_steal_count(idx) as f64);
        gauge!("restate.tokio.worker_total_busy_duration_seconds", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_total_busy_duration(idx).as_secs_f64());
        gauge!("restate.tokio.worker_mean_poll_time", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_mean_poll_time(idx).as_secs_f64());
    }
}
