// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Range;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::StreamExt;
use restate_bifrost::{Bifrost, BifrostService};
use restate_core::metadata;
use restate_rocksdb::{DbName, RocksDbManager};
use restate_types::config::{
    BifrostOptionsBuilder, CommonOptionsBuilder, ConfigurationBuilder, LocalLogletOptionsBuilder,
};
use restate_types::live::Live;
use restate_types::logs::LogId;
use tracing::info;
use tracing_subscriber::EnvFilter;
mod util;

async fn append_records_multi_log(bifrost: Bifrost, log_id_range: Range<u32>, count_per_log: u64) {
    let mut appends = FuturesUnordered::new();
    for log_id in log_id_range {
        for _ in 0..count_per_log {
            let bifrost = bifrost.clone();
            appends.push(async move {
                let _ = bifrost
                    .create_appender(LogId::new(log_id))
                    .expect("log exists")
                    .append("")
                    .await
                    .unwrap();
            })
        }
    }
    while appends.next().await.is_some() {}
}

async fn append_records_concurrent_single_log(bifrost: Bifrost, log_id: LogId, count_per_log: u64) {
    let mut appends = FuturesOrdered::new();
    for _ in 0..count_per_log {
        let bifrost = bifrost.clone();
        appends.push_back(async move {
            bifrost
                .create_appender(log_id)
                .expect("log exists")
                .append("")
                .await
                .unwrap()
        })
    }
    while appends.next().await.is_some() {}
}

async fn append_seq(bifrost: Bifrost, log_id: LogId, count: u64) {
    let mut appender = bifrost.create_appender(log_id).expect("log exists");
    for _ in 1..=count {
        let _ = appender.append("").await.expect("bifrost accept record");
    }
}

fn write_throughput_local_loglet(c: &mut Criterion) {
    let test_runner_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current thread runtime must build");

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let tmp_base = tempfile::tempdir().unwrap();

    let provider = restate_types::logs::metadata::ProviderKind::Local;
    let total_num_logs = 64;
    let num_logs_per_run: u32 = 10;
    info!("Benchmark starting in {}", tmp_base.path().display());
    let local_opts = LocalLogletOptionsBuilder::default().build().unwrap();
    let config = ConfigurationBuilder::default()
        .common(
            CommonOptionsBuilder::default()
                .base_dir(tmp_base.path().to_path_buf())
                .build()
                .expect("config"),
        )
        .bifrost(
            BifrostOptionsBuilder::default()
                .local(local_opts)
                .build()
                .unwrap(),
        )
        .build()
        .expect("config");
    let tc = test_runner_rt.block_on(util::spawn_environment(
        config.clone(),
        total_num_logs,
        provider,
    ));

    let bifrost = tc.block_on("bifrost-init", None, async {
        let metadata = metadata();
        let bifrost_svc = BifrostService::new(restate_core::task_center(), metadata)
            .enable_local_loglet(&Live::from_value(config));
        let bifrost = bifrost_svc.handle();

        // start bifrost service in the background
        bifrost_svc.start().await.expect("bifrost starts");
        bifrost
    });

    let mut group = c.benchmark_group("Bifrost");

    let count_per_run = 100;
    // Sequential single log
    group
        .sample_size(10)
        .throughput(Throughput::Elements(count_per_run))
        .bench_function("sequential_single_log", |bencher| {
            bencher.to_async(&test_runner_rt).iter(|| {
                tc.run_in_scope(
                    "bench",
                    None,
                    append_seq(bifrost.clone(), LogId::new(1), count_per_run),
                )
            });
        });

    // Concurrent single log
    for count_per_run in [10, 100, 1000].iter() {
        group
            .sample_size(10)
            .throughput(Throughput::Elements(*count_per_run))
            .bench_with_input(
                BenchmarkId::new("concurrent_single_log", count_per_run),
                count_per_run,
                |bencher, &count_per_run| {
                    bencher.to_async(&test_runner_rt).iter(|| {
                        tc.run_in_scope(
                            "bench",
                            None,
                            append_records_concurrent_single_log(
                                bifrost.clone(),
                                LogId::new(1),
                                count_per_run,
                            ),
                        )
                    });
                },
            );
    }

    // Concurrent multi-log
    for count_per_run in [10, 100, 1000].iter() {
        group
            .sample_size(10)
            .throughput(Throughput::Elements(
                count_per_run * num_logs_per_run as u64,
            ))
            .bench_with_input(
                BenchmarkId::new("concurrent_multi_log", count_per_run),
                count_per_run,
                |bencher, &count_per_run| {
                    bencher.to_async(&test_runner_rt).iter(|| {
                        tc.run_in_scope(
                            "bench",
                            None,
                            append_records_multi_log(
                                bifrost.clone(),
                                0..num_logs_per_run,
                                count_per_run,
                            ),
                        )
                    });
                },
            );
    }

    group.finish();
    let db_manager = RocksDbManager::get();

    let db = db_manager.get_db(DbName::new("local-loglet")).unwrap();
    let stats = db.get_statistics_str();
    let total_wb_usage = db_manager.get_total_write_buffer_usage();
    let wb_capacity = db_manager.get_total_write_buffer_capacity();
    let memory = db_manager
        .get_memory_usage_stats(&[DbName::new("local-loglet")])
        .unwrap();
    test_runner_rt.block_on(tc.shutdown_node("completed", 0));
    test_runner_rt.block_on(RocksDbManager::get().shutdown());

    info!("RocksDB stats: {}", stats.unwrap());

    info!(
        "RocksDB approximate memory usage for all mem-tables: {}",
        memory.approximate_mem_table_total(),
    );

    info!(
        "RocksDB approximate memory usage of un-flushed mem-tables: {}",
        memory.approximate_mem_table_unflushed(),
    );

    info!(
        "RocksDB approximate memory usage of all the table readers: {}",
        memory.approximate_mem_table_readers_total(),
    );

    info!(
        "RocksDB approximate memory usage by cache: {}",
        memory.approximate_cache_total(),
    );

    info!(
        "RocksDB total write buffers usage (from write buffers manager): {}",
        total_wb_usage
    );

    info!(
        "RocksDB total write buffers capacity (from write buffers manager): {}",
        wb_capacity
    );

    if std::env::var("RESTATE_BENCHMARK_KEEP").unwrap_or("false".to_owned()) == "true" {
        let path = tmp_base.into_path();
        info!(
            "Benchmark completed in {}, keeping the database",
            path.display()
        );
    }
}

criterion_group!(benches, write_throughput_local_loglet);
criterion_main!(benches);
