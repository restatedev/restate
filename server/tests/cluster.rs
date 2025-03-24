// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use enumset::enum_set;
use futures_util::StreamExt;
use googletest::IntoTestResult;
use http::header::CONTENT_TYPE;
use rand::prelude::IndexedMutRandom;
use rand::seq::IndexedRandom;
use regex::Regex;
use restate_core::{TaskCenter, TaskKind, cancellation_token};
use restate_local_cluster_runner::cluster::StartedCluster;
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
};
use restate_types::config::{MetadataServerKind, RaftOptions};
use restate_types::logs::metadata::{
    NodeSetSize, ProviderConfiguration, ProviderKind, ReplicatedLogletConfig,
};
use restate_types::partition_table::PartitionReplication;
use restate_types::replication::ReplicationProperty;
use restate_types::{config::Configuration, nodes_config::Role};
use std::convert::Infallible;
use std::num::{NonZeroU8, NonZeroUsize};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};
use tracing::{debug, info};

mod common;

#[test_log::test(restate_core::test)]
async fn replicated_loglet() -> googletest::Result<()> {
    let mut base_config = Configuration::default();
    // require an explicit provision step to configure the replication property to 2
    base_config.common.auto_provision = false;
    base_config.common.default_num_partitions = 1;

    let nodes = Node::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Admin | Role::MetadataServer | Role::Worker | Role::LogServer),
        3,
        false,
    );

    let regex: Regex = "Partition [0-9]+ started".parse()?;
    let mut partition_processors_starting_up: Vec<_> =
        nodes.iter().map(|node| node.lines(regex.clone())).collect();

    let cluster = Cluster::builder()
        .cluster_name("cluster-1")
        .nodes(nodes)
        .temp_base_dir()
        .build()
        .start()
        .await?;

    let replicated_loglet_config = ReplicatedLogletConfig {
        target_nodeset_size: NodeSetSize::default(),
        replication_property: ReplicationProperty::new(NonZeroU8::new(2).expect("to be non-zero")),
    };

    cluster.nodes[0]
        .provision_cluster(
            None,
            PartitionReplication::Everywhere,
            Some(ProviderConfiguration::Replicated(replicated_loglet_config)),
        )
        .await
        .into_test_result()?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    for partition_processor in &mut partition_processors_starting_up {
        assert!(partition_processor.next().await.is_some())
    }

    Ok(())
}

#[test_log::test(restate_core::test)]
async fn cluster_chaos_test() -> googletest::Result<()> {
    let num_nodes = 3;
    let chaos_duration = Duration::from_secs(20);
    let expected_recovery_interval = Duration::from_secs(10);
    let mut base_config = Configuration::default();
    base_config
        .metadata_server
        .set_kind(MetadataServerKind::Raft(RaftOptions {
            raft_election_tick: NonZeroUsize::new(5).expect("5 to be non zero"),
            raft_heartbeat_tick: NonZeroUsize::new(2).expect("2 to be non zero"),
            ..RaftOptions::default()
        }));
    base_config.common.default_num_partitions = 4;
    base_config.bifrost.default_provider = ProviderKind::Replicated;
    base_config
        .bifrost
        .replicated_loglet
        .default_log_replication =
        ReplicationProperty::new(NonZeroU8::new(2).expect("to be non-zero"));

    let nodes = Node::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::Admin | Role::Worker | Role::LogServer | Role::MetadataServer),
        num_nodes,
        true,
    );
    let mut cluster = Cluster::builder()
        .cluster_name("cluster_chaos_test")
        .nodes(nodes)
        .temp_base_dir()
        .build()
        .start()
        .await?;

    let ingress_addresses: Vec<_> = cluster
        .nodes
        .iter()
        .flat_map(|node| node.ingress_address().cloned())
        .map(|socket_address| format!("http://{}/Counter/1/add", socket_address))
        .collect();
    let admin_address = cluster
        .nodes
        .iter()
        .flat_map(|node| node.admin_address().cloned())
        .next()
        .expect("at least one admin node to be present");

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let service_endpoint_address = restate_local_cluster_runner::random_socket_address()?;

    let (running_tx, running_rx) = oneshot::channel();

    let service_handle = TaskCenter::spawn_unmanaged(TaskKind::TestRunner, "mock-service", {
        async move {
            info!("Running the mock service endpoint");
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

    // await that the mock service endpoint is running
    running_rx.await?;

    let client = reqwest::Client::builder()
        .build()
        .expect("reqwest client should build");

    let discovery_response = client
        .post(format!("http://{}/deployments", admin_address))
        .header(CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({"uri": format!("http://{}", service_endpoint_address)}).to_string(),
        )
        .send()
        .await?;

    assert!(
        discovery_response.status().is_success(),
        "discovery should be successful"
    );

    info!("Successfully registered service endpoint at cluster");

    let start_chaos = Instant::now();

    let (success_tx, success_rx) = watch::channel(0);

    let chaos_handle = TaskCenter::spawn_unmanaged(TaskKind::TestRunner, "chaos", async move {
        let cancellation = cancellation_token();

        async fn restart_nodes(
            cluster: &mut StartedCluster,
            mut success_rx: watch::Receiver<i32>,
            expected_recovery_interval: Duration,
        ) -> Result<Infallible, anyhow::Error> {
            loop {
                let node = cluster
                    .nodes
                    .choose_mut(&mut rand::rng())
                    .expect("at least one node being present");

                node.restart().await?;

                success_rx.mark_unchanged();
                cluster.wait_healthy(Duration::from_secs(10)).await?;
                tokio::time::timeout(expected_recovery_interval, success_rx.changed())
                    .await
                    .map_err(|_| {
                        anyhow!("Cluster did not recover in time to accept new invocations")
                    })??;
            }
        }

        if let Some(result) = cancellation
            .run_until_cancelled(restart_nodes(
                &mut cluster,
                success_rx,
                expected_recovery_interval,
            ))
            .await
        {
            result?;
        }

        Ok::<_, anyhow::Error>(cluster)
    })?;

    info!("Starting the cluster chaos test");

    let mut successes = 0;
    let mut failures = 0;
    let mut rng = rand::rng();
    while start_chaos.elapsed() < chaos_duration {
        let ingress = ingress_addresses
            .choose(&mut rng)
            .expect("at least one address to be present");

        debug!("Send request {successes} to {ingress}");
        match client
            .post(ingress)
            .header(CONTENT_TYPE, "application/json")
            .header("idempotency-key", successes.to_string())
            .body("1")
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    successes += 1;
                    success_tx.send_replace(successes);
                    let counter_value: i32 =
                        serde_json::from_slice(response.bytes().await?.as_ref())?;
                    assert_eq!(successes, counter_value);
                } else {
                    failures += 1;
                }
            }
            Err(err) => {
                failures += 1;
                // request failed, let's retry
                debug!(%err, "failed sending request {successes} to {ingress}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }

    // make sure that we have written at least some values
    assert!(
        successes > 1,
        "successful writes: {successes} failed writes: {failures}",
    );

    chaos_handle.cancel();
    service_handle.cancel();
    let mut cluster = chaos_handle.await?.into_test_result()?;
    cluster.graceful_shutdown(Duration::from_secs(3)).await?;

    info!("Finished metadata cluster chaos test with #{successes} successful writes");

    Ok(())
}
