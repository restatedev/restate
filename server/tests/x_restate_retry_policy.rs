// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enumset::enum_set;
use futures_util::StreamExt;
use googletest::IntoTestResult;
use http::header::CONTENT_TYPE;
use restate_core::{TaskCenter, TaskKind, cancellation_token};
use restate_local_cluster_runner::cluster::Cluster;
use restate_local_cluster_runner::node::{BinarySource, Node};
use restate_types::config::Configuration;
use restate_types::config::LogFormat;
use restate_types::logs::metadata::ProviderKind::Replicated;
use restate_types::logs::metadata::{NodeSetSize, ProviderConfiguration, ReplicatedLogletConfig};
use restate_types::nodes_config::Role;
use restate_types::replication::ReplicationProperty;
use restate_types::retries::RetryPolicy;
use std::thread::sleep;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tracing::info;
use url::Url;

#[test_log::test(restate_core::test)]
async fn test_x_restate_retry_policy_header() -> googletest::Result<()> {
    //
    // Cluster
    //

    let mut base_config = Configuration::default();
    base_config.common.default_num_partitions = 1.try_into()?;
    base_config.bifrost.default_provider = Replicated;
    base_config.common.log_filter = "restate=debug,warn".to_owned();
    base_config.common.log_format = LogFormat::Compact;
    base_config.common.log_disable_ansi_codes = true;
    base_config.worker.invoker.retry_policy = RetryPolicy::FixedDelay {
        interval: humantime::Duration::from(std::time::Duration::from_millis(250)),
        max_attempts: Some(std::num::NonZeroUsize::new(4).unwrap()),
    };

    let snapshots_dir = TempDir::new()?;
    base_config.worker.snapshots.destination = Some(
        Url::from_file_path(snapshots_dir.path())
            .unwrap()
            .to_string(),
    );

    let nodes = Node::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(
            Role::MetadataServer | Role::Admin | Role::Worker | Role::LogServer | Role::HttpIngress
        ),
        1,
        false,
    );

    let mut cluster = Cluster::builder()
        .cluster_name("x-restate-retry-policy")
        .nodes(nodes)
        .temp_base_dir("x_restate_retry_policy")
        .build()
        .start()
        .await?;

    let replicated_loglet_config = ReplicatedLogletConfig {
        target_nodeset_size: NodeSetSize::default(),
        replication_property: ReplicationProperty::new_unchecked(1),
    };

    info!("Provisioning the cluster");
    cluster.nodes[0]
        .provision_cluster(
            None,
            ReplicationProperty::new_unchecked(1),
            Some(ProviderConfiguration::Replicated(replicated_loglet_config)),
        )
        .await
        .into_test_result()?;

    {
        // Taken from trim_gap_handling, not sure if necessary.
        let worker_1 = &cluster.nodes[0];
        let mut worker_1_ready = worker_1.lines("Partition [0-9]+ started".parse()?);
        info!("Waiting until the cluster is healthy");
        cluster.wait_healthy(Duration::from_secs(60)).await?;
        info!("Waiting until node-1 has started the partition processor");
        worker_1_ready.next().await;
        drop(worker_1_ready);
    }

    //
    // Service
    //

    let admin_address = cluster
        .nodes
        .iter()
        .flat_map(|node| node.admin_address().cloned())
        .next()
        .expect("at least one admin node to be present");

    let service_endpoint_address = restate_local_cluster_runner::random_socket_address()?;

    let (running_tx, running_rx) = oneshot::channel();

    let service_handle = TaskCenter::spawn_unmanaged(TaskKind::TestRunner, "failing-service", {
        async move {
            info!("Running the failing service endpoint");
            cancellation_token()
                .run_until_cancelled(mock_service_endpoint::listener::run_listener(
                    service_endpoint_address,
                    move || {
                        // if the test program is gone than this task will soon be stopped as well
                        let _ = running_tx.send(());
                    },
                ))
                .await
        }
    })?;

    running_rx.await?;

    info!(
        "Service endpoint is now running on: {}",
        service_endpoint_address
    );

    //
    // Register
    //

    let client = reqwest::Client::builder().build()?;

    info!("Registering service endpoint with admin...");
    let discovery_response = client
        .post(format!("http://{admin_address}/deployments"))
        .header(CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({"uri": format!("http://{}", service_endpoint_address)}).to_string(),
        )
        .send()
        .await?;

    info!("Discovery response status: {}", discovery_response.status());
    let discovery_body = discovery_response.text().await?;
    info!("Discovery response body: {}", discovery_body);

    // Let's also check what services are available
    info!("Checking available services...");
    let services_response = client
        .get(format!("http://{admin_address}/services"))
        .send()
        .await?;

    info!("Services response status: {}", services_response.status());
    let services_body = services_response.text().await?;
    info!("Available services: {}", services_body);

    info!("Waiting for service registration to propagate...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    //
    // Invokes
    //

    let ingress_address = cluster.nodes[0].ingress_address().unwrap();

    info!("Ingress address: {}", ingress_address);

    {
        let policy = "---";

        info!("----------------------");
        info!("X-Restate-Retry-Policy: {}", policy);
        let start_time = std::time::Instant::now();
        let response = client
            .post(format!("http://{ingress_address}/Counter/1/add"))
            .header(CONTENT_TYPE, "application/json")
            .header("X-Restate-Retry-Policy", policy)
            .body("1")
            .send()
            .await?;
        let elapsed = start_time.elapsed();
        info!("status: {}", response.status());
        let response_body = response.text().await?;
        info!("body: {}", response_body);
        info!("ms: {}", elapsed.as_millis());
    }

    {
        let policy = "max-attempts=16";

        info!("----------------------");
        info!("X-Restate-Retry-Policy: {}", policy);
        let start_time = std::time::Instant::now();
        let response = client
            .post(format!("http://{ingress_address}/Counter/1/add"))
            .header(CONTENT_TYPE, "application/json")
            .header("X-Restate-Retry-Policy", policy)
            .body("1")
            .send()
            .await?;
        let elapsed = start_time.elapsed();
        info!("status: {}", response.status());
        let response_body = response.text().await?;
        info!("body: {}", response_body);
        info!("ms: {}", elapsed.as_millis());
    }

    //
    // Teardown
    //

    info!("waiting before shutdown");
    sleep(Duration::from_secs(30));

    service_handle.cancel();
    cluster.graceful_shutdown(Duration::from_secs(3)).await?;
    Ok(())
}
