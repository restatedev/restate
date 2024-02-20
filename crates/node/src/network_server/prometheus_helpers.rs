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

use metrics_exporter_prometheus::formatting;
use rocksdb::statistics::{HistogramData, Ticker};

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
    options: &rocksdb::Options,
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
        &[],
        None,
        options.get_ticker_count(ticker),
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
        &[],
        Some(("quantile", "0.5")),
        unit.normalize_value(data.median()),
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        &[],
        Some(("quantile", "0.95")),
        unit.normalize_value(data.p95()),
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        &[],
        Some(("quantile", "0.99")),
        unit.normalize_value(data.p99()),
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        None,
        &[],
        Some(("quantile", "1.0")),
        unit.normalize_value(data.max()),
    );
    formatting::write_metric_line::<&str, f64>(
        out,
        &base_sanitized_name,
        Some("sum"),
        &[],
        None,
        unit.normalize_value(data.sum() as f64),
    );
    formatting::write_metric_line::<&str, u64>(
        out,
        &base_sanitized_name,
        Some("count"),
        &[],
        None,
        data.count(),
    );
    let _ = writeln!(out);
}
