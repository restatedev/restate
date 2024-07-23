// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use std::num::NonZeroU64;
use std::time::Duration;

use futures_util::{future, TryFutureExt};
use http::header::CONTENT_TYPE;
use http::Uri;
use pprof::flamegraph::Options;
use restate_rocksdb::RocksDbManager;
use restate_server::config_loader::ConfigLoaderBuilder;
use restate_types::config::{
    CommonOptionsBuilder, Configuration, ConfigurationBuilder, WorkerOptionsBuilder,
};
use restate_types::live::Constant;
use tokio::runtime::Runtime;

use restate_core::{TaskCenter, TaskCenterBuilder, TaskKind};
use restate_node::Node;
use restate_types::retries::RetryPolicy;

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

    assert!(discovery_result
        .expect("Discovery must be successful")
        .status()
        .is_success(),);

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

    assert!(health_response
        .expect("Discovery must be successful")
        .status()
        .is_success(),);
}

pub fn spawn_restate(config: Configuration) -> TaskCenter {
    let tc = TaskCenterBuilder::default()
        .options(config.common.clone())
        .build()
        .expect("task_center builds");
    let cloned_tc = tc.clone();
    restate_types::config::set_current_config(config.clone());
    let updateable_config = Configuration::updateable();

    tc.run_in_scope_sync("db-manager-init", None, || {
        RocksDbManager::init(Constant::new(config.common))
    });
    tc.spawn(TaskKind::TestRunner, "benchmark", None, async move {
        let node = Node::create(updateable_config)
            .await
            .expect("Restate node must build");
        cloned_tc.run_in_scope("startup", None, node.start()).await
    })
    .unwrap();

    tc
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
    let common_options = CommonOptionsBuilder::default()
        .base_dir(tempfile::tempdir().expect("tempdir failed").into_path())
        .bootstrap_num_partitions(NonZeroU64::new(10).unwrap())
        .build()
        .expect("building common options should work");

    let worker_options = WorkerOptionsBuilder::default()
        .build()
        .expect("building worker options should work");

    let config = ConfigurationBuilder::default()
        .common(common_options)
        .worker(worker_options)
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
