#![allow(clippy::async_yields_async)]
// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for benchmarking the Restate runtime
use drain::Signal;
use futures_util::{future, TryFutureExt};
use hyper::header::CONTENT_TYPE;
use hyper::{Body, Uri};
use pprof::flamegraph::Options;
use restate_server::config::{
    ConfigurationBuilder, MetaOptionsBuilder, RocksdbOptionsBuilder, WorkerOptionsBuilder,
};
use restate_server::{Application, ApplicationError, Configuration};
use restate_types::retries::RetryPolicy;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

pub mod counter {
    include!(concat!(env!("OUT_DIR"), "/counter.rs"));
}

pub fn discover_deployment(current_thread_rt: &Runtime, address: Uri) {
    let discovery_payload = serde_json::json!({"uri": address.to_string()}).to_string();
    let discovery_result = current_thread_rt.block_on(async {
        RetryPolicy::fixed_delay(Duration::from_millis(200), 50)
            .retry_operation(|| {
                hyper::Client::new()
                    .request(
                        hyper::Request::post("http://localhost:9070/deployments")
                            .header(CONTENT_TYPE, "application/json")
                            .body(Body::from(discovery_payload.clone()))
                            .expect("building discovery request should not fail"),
                    )
                    .map_err(anyhow::Error::from)
                    .and_then(|response| {
                        if response.status().is_success() {
                            future::ready(Ok(response))
                        } else {
                            future::ready(Err(anyhow::anyhow!("Discovery was unsuccessful.")))
                        }
                    })
            })
            .await
    });

    assert!(discovery_result
        .expect("Discovery must be successful")
        .status()
        .is_success(),);
}

pub fn spawn_restate(
    config: Configuration,
) -> (Runtime, Signal, JoinHandle<Result<(), ApplicationError>>) {
    let rt = config
        .tokio_runtime
        .build()
        .expect("Tokio runtime must build");

    let (signal, drain) = drain::channel();
    let app_handle = rt.block_on(async move {
        let app = Application::new(config.meta, config.worker).expect("Application must build");
        tokio::task::spawn(app.run(drain))
    });

    (rt, signal, app_handle)
}

pub fn flamegraph_options<'a>() -> Options<'a> {
    #[allow(unused_mut)]
    let mut options = Options::default();
    if cfg!(target_os = "macos") {
        // Ignore different thread origins to merge traces. This seems not needed on Linux.
        options.base = vec!["__pthread_joiner_wake".to_string(), "_main".to_string()];
    }
    options
}

pub fn restate_configuration() -> Configuration {
    let meta_options = MetaOptionsBuilder::default()
        .storage_path(
            tempfile::tempdir()
                .expect("tempdir failed")
                .into_path()
                .into_os_string()
                .into_string()
                .unwrap(),
        )
        .build()
        .expect("building meta options should work");

    let rocksdb_options = RocksdbOptionsBuilder::default()
        .path(
            tempfile::tempdir()
                .expect("tempdir failed")
                .into_path()
                .into_os_string()
                .into_string()
                .unwrap(),
        )
        .build()
        .expect("building rocksdb options should work");

    let worker_options = WorkerOptionsBuilder::default()
        .partitions(10)
        .storage_rocksdb(rocksdb_options)
        .build()
        .expect("building worker options should work");

    let default_config = ConfigurationBuilder::default()
        .worker(worker_options)
        .meta(meta_options)
        .build()
        .expect("building the configuration should work");

    Configuration::load_with_default(default_config, None)
        .expect("configuration loading should not fail")
}

pub struct BenchmarkSettings {
    pub num_requests: u32,
    pub num_parallel_requests: usize,
    pub sample_size: usize,
}

pub fn parse_benchmark_settings() -> BenchmarkSettings {
    let num_requests = std::env::var("BENCHMARK_REQUESTS")
        .ok()
        .and_then(|requests| requests.parse().ok())
        .unwrap_or(4000);
    let num_parallel_requests = std::env::var("BENCHMARK_PARALLEL_REQUESTS")
        .ok()
        .and_then(|parallel_requests| parallel_requests.parse().ok())
        .unwrap_or(1000);
    let sample_size = std::env::var("BENCHMARK_SAMPLE_SIZE")
        .ok()
        .and_then(|parallel_requests| parallel_requests.parse().ok())
        .unwrap_or(20);

    BenchmarkSettings {
        num_requests,
        num_parallel_requests,
        sample_size,
    }
}
