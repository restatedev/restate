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

use indexmap::IndexMap;
use metrics::{Key, Unit};
use metrics_exporter_prometheus::{LabelSet, formatting};
use rocksdb::statistics::{HistogramData, Ticker};

use restate_rocksdb::RocksDb;

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

    fn normalized_unit(&self) -> Unit {
        match self {
            MetricUnit::Micros => Unit::Seconds,
            MetricUnit::Bytes => Unit::Bytes,
            MetricUnit::Count => Unit::Count,
        }
    }
}

pub fn format_rocksdb_stat_ticker_for_prometheus(
    out: &mut String,
    db: &RocksDb,
    labels: &IndexMap<String, String>,
    ticker: Ticker,
) {
    let sanitized_name = format!(
        "{}_{}",
        PREFIX,
        formatting::sanitize_metric_name(ticker.name())
    );
    formatting::write_type_line(out, &sanitized_name, None, Some("total"), "counter");
    formatting::write_metric_line::<&str, u64>(
        out,
        &sanitized_name,
        None,
        &LabelSet::from_key_and_global(&Key::from_static_name("non-existent"), labels),
        None,
        db.get_ticker_count(ticker),
        None,
    );
    let _ = writeln!(out);
}

pub fn format_rocksdb_property_for_prometheus(
    out: &mut String,
    labels: &IndexMap<String, String>,
    unit: MetricUnit,
    property_name: &str,
    property_value: u64,
) {
    let sanitized_name = format!(
        "{}_{}",
        PREFIX,
        formatting::sanitize_metric_name(property_name),
    );

    formatting::write_type_line(
        out,
        &sanitized_name,
        Some(unit.normalized_unit()),
        None,
        "gauge",
    );
    formatting::write_metric_line::<&str, u64>(
        out,
        &sanitized_name,
        None,
        &LabelSet::from_key_and_global(&Key::from_static_name("non-existent"), labels),
        None,
        property_value,
        None,
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
    labels: &IndexMap<String, String>,
) {
    let base_sanitized_name = format!("{}_{}", PREFIX, formatting::sanitize_metric_name(name),);
    formatting::write_type_line(
        out,
        &base_sanitized_name,
        Some(unit.normalized_unit()),
        None,
        "summary",
    );

    let labels = LabelSet::from_key_and_global(&Key::from_static_name("non-existent"), labels);
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        &labels,
        Some(("quantile", "0.5")),
        unit.normalize_value(data.median()),
        None,
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        &labels,
        Some(("quantile", "0.95")),
        unit.normalize_value(data.p95()),
        None,
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        &labels,
        Some(("quantile", "0.99")),
        unit.normalize_value(data.p99()),
        None,
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        &labels,
        Some(("quantile", "1.0")),
        unit.normalize_value(data.max()),
        None,
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        Some("sum"),
        &labels,
        None,
        unit.normalize_value(data.sum() as f64),
        None,
    );
    formatting::write_metric_line::<&str, u64>(
        out,
        &base_sanitized_name,
        Some("count"),
        &labels,
        None,
        data.count(),
        None,
    );
    let _ = writeln!(out);
}
