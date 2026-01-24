// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::time::Duration;

use hdrhistogram::Histogram;
use metrics_exporter_prometheus::PrometheusHandle;
use restate_rocksdb::{DbName, RocksDbManager};
use restate_types::flexbuffers_storage_encode_decode;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DummyPayload {
    pub precise_ts: u64,
    pub blob: bytes::Bytes,
}

flexbuffers_storage_encode_decode!(DummyPayload);

pub fn print_latencies(title: &str, histogram: Histogram<u64>) {
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(&mut stdout, "Histogram of {title}");
    let _ = writeln!(
        &mut stdout,
        "P50: {:?}",
        Duration::from_nanos(histogram.value_at_percentile(50.0))
    );
    let _ = writeln!(
        &mut stdout,
        "P90: {:?}",
        Duration::from_nanos(histogram.value_at_percentile(90.0))
    );
    let _ = writeln!(
        &mut stdout,
        "P99: {:?}",
        Duration::from_nanos(histogram.value_at_percentile(99.0))
    );
    let _ = writeln!(
        &mut stdout,
        "P999: {:?}",
        Duration::from_nanos(histogram.value_at_percentile(99.9))
    );
    let _ = writeln!(
        &mut stdout,
        "P9999: {:?}",
        Duration::from_nanos(histogram.value_at_percentile(99.99))
    );
    let _ = writeln!(
        &mut stdout,
        "P100: {:?}",
        Duration::from_nanos(histogram.value_at_percentile(100.0))
    );
    let _ = writeln!(&mut stdout, "# of samples: {}", histogram.len());
    let _ = writeln!(&mut stdout);
}

pub fn print_rocksdb_stats(db_name: &str) {
    let db_manager = RocksDbManager::get();
    let db = db_manager.get_db(DbName::new(db_name)).unwrap();
    let stats = db.get_statistics_str();
    let total_wb_usage = db_manager.get_total_write_buffer_usage();
    let wb_capacity = db_manager.get_total_write_buffer_capacity();
    let memory = db_manager
        .get_memory_usage_stats(&[DbName::new(db_name)])
        .unwrap();
    let mut stdout = std::io::stdout().lock();

    let _ = writeln!(&mut stdout);
    let _ = writeln!(&mut stdout, "==========================");
    let _ = writeln!(&mut stdout, "Rocksdb Stats of {db_name}");
    let _ = writeln!(&mut stdout, "{}", stats.unwrap());

    let _ = writeln!(
        &mut stdout,
        "RocksDB approximate memory usage for all mem-tables: {}",
        memory.approximate_mem_table_total(),
    );
    let _ = writeln!(
        &mut stdout,
        "RocksDB approximate memory usage of un-flushed mem-tables: {}",
        memory.approximate_mem_table_unflushed(),
    );

    let _ = writeln!(
        &mut stdout,
        "RocksDB approximate memory usage of all the table readers: {}",
        memory.approximate_mem_table_readers_total(),
    );

    let _ = writeln!(
        &mut stdout,
        "RocksDB approximate memory usage by cache: {}",
        memory.approximate_cache_total(),
    );

    let _ = writeln!(
        &mut stdout,
        "RocksDB total write buffers usage (from write buffers manager): {total_wb_usage}"
    );

    let _ = writeln!(
        &mut stdout,
        "RocksDB total write buffers capacity (from write buffers manager): {wb_capacity}"
    );
}

pub fn print_prometheus_stats(handle: &PrometheusHandle) {
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(&mut stdout);
    let _ = writeln!(&mut stdout, "==========================");
    let _ = writeln!(&mut stdout, "Prometheus Stats");
    let metrics = handle.render();

    // print the metrics to the terminal.
    let _ = writeln!(&mut stdout, "{metrics}");
}
