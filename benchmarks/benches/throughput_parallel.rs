// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This benchmark requires the [counter.Counter service](https://github.com/restatedev/e2e/blob/a500164a31d58c0ee65ae77a7f99a8a2ef1825cb/services/node-services/src/counter.ts)
//! running on localhost:9080 in order to run.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use http::Uri;
use http::header::CONTENT_TYPE;
#[cfg(unix)]
use pprof::criterion::{Output, PProfProfiler};
use rand::distr::{Alphanumeric, SampleString};
use restate_benchmarks::{BenchmarkSettings, parse_benchmark_settings};
use restate_rocksdb::RocksDbManager;
use tokio::runtime::Builder;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

fn throughput_benchmark(criterion: &mut Criterion) {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let config = restate_benchmarks::restate_configuration();
    let tc = restate_benchmarks::spawn_restate(config);
    restate_benchmarks::spawn_mock_service_endpoint(&tc);

    let BenchmarkSettings {
        num_requests,
        num_parallel_requests,
        sample_size,
    } = parse_benchmark_settings();

    let current_thread_rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current thread runtime must build");

    restate_benchmarks::discover_deployment(
        &current_thread_rt,
        Uri::from_static("http://localhost:9080"),
    );

    let client = reqwest::Client::builder()
        .build()
        .expect("build reqwest client");

    let mut group = criterion.benchmark_group("throughput");
    group
        .sample_size(sample_size)
        .throughput(Throughput::Elements(u64::from(num_requests)))
        .bench_function("parallel", |bencher| {
            bencher.to_async(&current_thread_rt).iter(|| {
                send_parallel_counter_requests(client.clone(), num_requests, num_parallel_requests)
            })
        });

    group.finish();
    current_thread_rt.block_on(tc.shutdown_node("completed", 0));
    current_thread_rt.block_on(RocksDbManager::get().shutdown());
}

async fn send_parallel_counter_requests(
    client: reqwest::Client,
    num_requests: u32,
    num_parallel_requests: usize,
) {
    let mut pending_requests = FuturesUnordered::new();
    let mut completed_requests = 0;

    while completed_requests < num_requests {
        if pending_requests.len() < num_parallel_requests
            && completed_requests as usize + pending_requests.len() < num_requests as usize
        {
            let client = client.clone();
            let counter_name = Alphanumeric.sample_string(&mut rand::rng(), 8);
            pending_requests.push(async move {
                client
                    .post(format!("http://localhost:8080/Counter/{counter_name}/add"))
                    .header(CONTENT_TYPE, "application/json")
                    .body("10")
                    .send()
                    .await
            });
        } else {
            let result = pending_requests
                .next()
                .await
                .expect("pending requests should not be empty");

            match result {
                Ok(response) => {
                    if response.status().is_success() {
                        completed_requests += 1;
                    } else {
                        panic!("request failed: {response:?}");
                    }
                }
                Err(err) => {
                    panic!("request failed: {err}")
                }
            }
        }
    }
}

#[cfg(unix)]
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(997, Output::Flamegraph(Some(restate_benchmarks::flamegraph_options()))));
    targets = throughput_benchmark
);

#[cfg(not(unix))]
criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = throughput_benchmark
);

criterion_main!(benches);
