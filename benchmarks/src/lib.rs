// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::async_yields_async)]

//! Utilities for benchmarking the Restate runtime
use anyhow::anyhow;
use futures_util::{TryFutureExt, future};
use http::Uri;
use http::header::CONTENT_TYPE;
#[cfg(unix)]
use pprof::flamegraph::Options;
use restate_core::{TaskCenter, TaskCenterBuilder, TaskKind, cancellation_token, task_center};
use restate_node::Node;
use restate_rocksdb::RocksDbManager;
use restate_tracing_instrumentation::prometheus_metrics::Prometheus;
use restate_types::config::{
    BifrostOptionsBuilder, CommonOptionsBuilder, Configuration, ConfigurationBuilder,
    MetadataServerOptionsBuilder, WorkerOptionsBuilder,
};
use restate_types::config_loader::ConfigLoaderBuilder;
use restate_types::live::Constant;
use restate_types::logs::metadata::ProviderKind;
use restate_types::retries::RetryPolicy;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::warn;

pub fn discover_deployment(current_thread_rt: &Runtime, address: Uri) {
    let client = reqwest::Client::builder()
        .build()
        .expect("client should build");
    let discovery_payload = serde_json::json!({"uri": address.to_string()}).to_string();
    let discovery_result = current_thread_rt.block_on(async {
        RetryPolicy::fixed_delay(Duration::from_millis(200), Some(50))
            .retry(|| {
                client
                    .post("http://localhost:9070/deployments")
                    .header(CONTENT_TYPE, "application/json")
                    .body(discovery_payload.clone())
                    .send()
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

    assert!(
        discovery_result
            .expect("Discovery must be successful")
            .status()
            .is_success(),
    );

    // wait for ingress being available
    // todo replace with node get_ident/status once it signals that the node is fully up and running
    let health_response = current_thread_rt.block_on(async {
        RetryPolicy::fixed_delay(Duration::from_millis(200), Some(50))
            .retry(|| {
                client
                    .get("http://localhost:8080/restate/health")
                    .send()
                    .map_err(anyhow::Error::from)
                    .and_then(|response| {
                        if response.status().is_success() {
                            future::ready(Ok(response))
                        } else {
                            future::ready(Err(anyhow::anyhow!("health request was unsuccessful.")))
                        }
                    })
            })
            .await
    });

    assert!(
        health_response
            .expect("health check must be successful")
            .status()
            .is_success(),
    );
}

pub fn spawn_restate(config: Configuration) -> task_center::Handle {
    #[cfg(unix)]
    if rlimit::increase_nofile_limit(u64::MAX).is_err() {
        warn!("Failed to increase the number of open file descriptors limit.");
    }

    let tc = TaskCenterBuilder::default()
        .options(config.common.clone())
        .build()
        .expect("task_center builds")
        .into_handle();
    let mut prometheus = Prometheus::install(&config.common);
    restate_types::config::set_current_config(config.clone());
    let live_config = Configuration::live();

    tc.block_on(async {
        RocksDbManager::init(Constant::new(config.common));
        prometheus.start_upkeep_task();

        TaskCenter::spawn(TaskKind::SystemBoot, "restate", async move {
            let node = Node::create(live_config, prometheus)
                .await
                .expect("Restate node must build");
            node.start().await
        })
        .unwrap();
    });

    tc
}

pub fn spawn_mock_service_endpoint(task_center_handle: &task_center::Handle) {
    task_center_handle.block_on(async {
        let (running_tx, running_rx) = oneshot::channel();
        TaskCenter::spawn(TaskKind::TestRunner, "mock-service-endpoint", async move {
            cancellation_token()
                .run_until_cancelled(mock_service_endpoint::listener::run_listener(
                    "127.0.0.1:9080".parse().expect("valid socket addr"),
                    || {
                        let _ = running_tx.send(());
                    },
                ))
                .await
                .map(|result| result.map_err(|err| anyhow!("mock service endpoint failed: {err}")))
                .unwrap_or(Ok(()))
        })
        .expect("spawn mock service endpoint task");

        running_rx
            .await
            .expect("mock service endpoint should start");
    });
}

#[cfg(unix)]
pub fn flamegraph_options<'a>() -> Options<'a> {
    #[allow(unused_mut)]
    let mut options = Options::default();
    if cfg!(target_os = "macos") {
        // Ignore different thread origins to merge traces. This seems not needed on Linux.
        options.base = vec!["_pthread_key_init_np".to_string(), "_main".to_string()];
    }
    options
}

pub fn restate_configuration() -> Configuration {
    let common_options = CommonOptionsBuilder::default()
        .base_dir(tempfile::tempdir().expect("tempdir failed").keep())
        .default_num_partitions(10)
        .build()
        .expect("building common options should work");

    let worker_options = WorkerOptionsBuilder::default()
        .build()
        .expect("building worker options should work");

    let metadata_server_options = MetadataServerOptionsBuilder::default()
        .build()
        .expect("building metadata server options should work");

    let bifrost_options = BifrostOptionsBuilder::default()
        .default_provider(ProviderKind::Replicated)
        .build()
        .expect("building bifrost options should work");

    let config = ConfigurationBuilder::default()
        .common(common_options)
        .worker(worker_options)
        .metadata_server(metadata_server_options)
        .bifrost(bifrost_options)
        .build()
        .expect("building the configuration should work");

    ConfigLoaderBuilder::default()
        .load_env(true)
        .custom_default(config)
        .build()
        .expect("builder should build")
        .load_once()
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
