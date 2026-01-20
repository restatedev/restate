// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use enumset::EnumSet;
use futures_util::StreamExt;
use googletest::{IntoTestResult, fail};
use serde::Deserialize;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tonic::transport::Channel;
use tracing::info;
use url::Url;

use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::protobuf::cluster_ctrl_svc::{
    ClusterStateRequest, CreatePartitionSnapshotRequest,
    cluster_ctrl_svc_client::ClusterCtrlSvcClient, new_cluster_ctrl_client,
};
use restate_local_cluster_runner::cluster::Cluster;
use restate_local_cluster_runner::node::{BinarySource, HealthCheck, NodeSpec, TerminationSignal};
use restate_types::config::{Configuration, LogFormat, NetworkingOptions, SnapshotType};
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::ProviderKind::Replicated;
use restate_types::logs::metadata::{NodeSetSize, ProviderConfiguration, ReplicatedLogletConfig};
use restate_types::net::address::PeerNetAddress;
use restate_types::protobuf::cluster::RunMode;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::replication::ReplicationProperty;

#[derive(Deserialize)]
struct LatestSnapshotRef {
    retained_snapshots: Vec<SnapshotRef>,
}

#[derive(Deserialize)]
struct SnapshotRef {
    path: String,
}

#[derive(Deserialize)]
struct SnapshotMetadata {
    #[serde(default)]
    file_keys: std::collections::BTreeMap<String, String>,
}

fn corrupt_latest_snapshot(snapshots_dir: &Path, partition_id: u32) -> anyhow::Result<bool> {
    let partition_dir = snapshots_dir.join(partition_id.to_string());
    let latest_json = partition_dir.join("latest.json");

    let latest: LatestSnapshotRef = serde_json::from_str(&std::fs::read_to_string(&latest_json)?)?;

    if latest.retained_snapshots.len() < 2 {
        return Ok(false);
    }

    let latest_metadata_path = partition_dir
        .join(&latest.retained_snapshots[0].path)
        .join("metadata.json");
    let latest_metadata: SnapshotMetadata =
        serde_json::from_str(&std::fs::read_to_string(&latest_metadata_path)?)?;
    let latest_ssts: HashSet<_> = latest_metadata.file_keys.values().collect();

    let mut older_ssts = HashSet::new();
    for older_ref in &latest.retained_snapshots[1..] {
        let metadata_path = partition_dir.join(&older_ref.path).join("metadata.json");
        if let Ok(content) = std::fs::read_to_string(&metadata_path)
            && let Ok(metadata) = serde_json::from_str::<SnapshotMetadata>(&content)
        {
            older_ssts.extend(metadata.file_keys.values().cloned());
        }
    }

    let exclusive_ssts: Vec<_> = latest_ssts
        .iter()
        .filter(|sst| !older_ssts.contains(**sst))
        .collect();

    if exclusive_ssts.is_empty() {
        return Ok(false);
    }

    let sst_to_delete = partition_dir.join(exclusive_ssts[0]);
    std::fs::remove_file(&sst_to_delete)?;
    info!("Corrupted latest snapshot by deleting: {:?}", sst_to_delete);

    Ok(true)
}

#[test_log::test(restate_core::test(flavor = "multi_thread", worker_threads = 4))]
async fn incremental_snapshot_chaos() -> googletest::Result<()> {
    const NUM_NODES: u32 = 3;
    const NUM_PARTITIONS: u32 = 3;
    const DEFAULT_CHAOS_DURATION_SECS: u64 = 30;
    const EXPECTED_RECOVERY_DURATION: Duration = Duration::from_secs(15);
    const RETAIN_SNAPSHOTS: u8 = 3;
    const MAX_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

    let chaos_duration_secs: u64 = std::env::var("CHAOS_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CHAOS_DURATION_SECS);
    let chaos_duration = Duration::from_secs(chaos_duration_secs);
    info!("Chaos test duration: {} seconds", chaos_duration_secs);

    let mut base_config = Configuration::new_unix_sockets();
    base_config.common.default_num_partitions = NUM_PARTITIONS.try_into()?;
    base_config.bifrost.default_provider = Replicated;
    base_config.common.log_filter = "warn,restate=info,restate_partition_store=debug".to_owned();
    base_config.common.log_format = LogFormat::Compact;
    base_config.common.log_disable_ansi_codes = true;

    let snapshots_dir = TempDir::new()?;
    base_config.worker.snapshots.destination = Some(
        Url::from_file_path(snapshots_dir.path())
            .unwrap()
            .to_string(),
    );
    base_config.worker.snapshots.experimental_snapshot_type = SnapshotType::Incremental;
    base_config.worker.snapshots.experimental_num_retained =
        Some(NonZeroU8::new(RETAIN_SNAPSHOTS).unwrap());
    base_config.worker.snapshots.check_interval = Some(Duration::from_millis(100).into());

    let nodes = NodeSpec::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        EnumSet::all(),
        NUM_NODES,
        false,
    );

    let mut cluster = Cluster::builder()
        .cluster_name("incremental-snapshot-chaos")
        .nodes(nodes)
        .temp_base_dir("incremental_snapshot_chaos")
        .build()
        .start()
        .await?;

    let replicated_loglet_config = ReplicatedLogletConfig {
        target_nodeset_size: NodeSetSize::default(),
        replication_property: ReplicationProperty::new_unchecked(2),
    };

    info!("Provisioning the cluster");
    cluster.nodes[0]
        .provision_cluster(
            None,
            ReplicationProperty::new_unchecked(2),
            Some(ProviderConfiguration::Replicated(replicated_loglet_config)),
        )
        .await
        .into_test_result()?;

    let worker_1 = &cluster.nodes[0];
    let mut worker_1_ready = worker_1.lines("Partition [0-9]+ started".parse()?);

    info!("Waiting until the cluster is healthy");
    cluster.wait_healthy(Duration::from_secs(60)).await?;

    info!("Waiting until node-1 has started the partition processor");
    worker_1_ready.next().await;
    drop(worker_1_ready);

    let mut client = new_cluster_ctrl_client(
        create_tonic_channel(
            cluster.nodes[0].advertised_address().clone(),
            &NetworkingOptions::default(),
            DNSResolution::Gai,
        ),
        &base_config.networking,
    );

    info!("Waiting until partition processors have become leaders");
    wait_all_partitions_active(&mut client, NUM_PARTITIONS, MAX_WAIT_TIMEOUT).await?;

    let (running_tx, running_rx) = oneshot::channel();
    let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    let mock_svc_port = addr.port();

    tokio::spawn(async move {
        info!("Starting mock service on http://{}", addr);
        if let Err(e) = mock_service_endpoint::listener::run_listener(listener, || {
            let _ = running_tx.send(());
        })
        .await
        {
            panic!("Error running listener: {e:?}");
        }
    });

    running_rx.await?;

    let worker_1 = &cluster.nodes[0];
    let admin_uds = worker_1
        .admin_address()
        .clone()
        .unwrap()
        .into_address()
        .unwrap();
    let PeerNetAddress::Uds(admin_uds) = admin_uds else {
        panic!("admin address must be a unix domain socket");
    };
    let admin_http_client = reqwest::Client::builder().unix_socket(admin_uds).build()?;
    let registration_response = admin_http_client
        .post("http://localhost/deployments")
        .header("content-type", "application/json")
        .json(&serde_json::json!({ "uri": format!("http://127.0.0.1:{mock_svc_port}") }))
        .send()
        .await?;
    assert!(registration_response.status().is_success());

    let ingress_clients: Vec<_> = cluster
        .nodes
        .iter()
        .map(|node| {
            let ingress_uds = node
                .ingress_address()
                .clone()
                .unwrap()
                .into_address()
                .unwrap();
            let PeerNetAddress::Uds(ingress_uds) = ingress_uds else {
                panic!("ingress address must be a unix domain socket");
            };
            reqwest::Client::builder()
                .unix_socket(ingress_uds)
                .build()
                .unwrap()
        })
        .collect();

    // make sure we have at least one snapshot per partition before chaos starts
    info!("Creating initial snapshots for all partitions");
    for partition_id in 0..NUM_PARTITIONS {
        let partition_dir = snapshots_dir.path().join(partition_id.to_string());
        let latest_json = partition_dir.join("latest.json");
        let start = Instant::now();
        while !latest_json.exists() && start.elapsed() < Duration::from_secs(5) {
            let _ = client
                .create_partition_snapshot(CreatePartitionSnapshotRequest {
                    partition_id,
                    min_target_lsn: None,
                    trim_log: false,
                })
                .await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(
            latest_json.exists(),
            "Initial snapshot should have been created for partition {}",
            partition_id
        );
    }
    info!("Initial snapshots created for all partitions");

    let (success_tx, success_rx) = watch::channel(0u32);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    info!("Starting incremental snapshot chaos test");

    let health_check_task = {
        let ingress_clients = ingress_clients.clone();
        let success_tx = success_tx.clone();
        let num_nodes = ingress_clients.len();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = shutdown_rx => {}
                _ = async {
                    let mut counter = 0u64;
                    loop {
                        let partition = (counter as u32) % NUM_PARTITIONS;
                        let node_idx = (counter as usize) % num_nodes;
                        counter = counter.wrapping_add(1);

                        let url = format!("http://localhost/Counter/{partition}/get");
                        let mut success = false;
                        let result: Result<reqwest::Response, _> =
                            ingress_clients[node_idx].post(&url).send().await;
                        if result.is_ok_and(|r| r.status().is_success()) {
                            success = true;
                        } else {
                            for (i, client) in ingress_clients.iter().enumerate() {
                                if i == node_idx {
                                    continue;
                                }
                                let result: Result<reqwest::Response, _> = client.post(&url).send().await;
                                if result.is_ok_and(|r| r.status().is_success()) {
                                    success = true;
                                    break;
                                }
                            }
                        }
                        if success {
                            success_tx.send_modify(|v| *v += 1);
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                } => {}
            }
        })
    };

    let chaos_start = Instant::now();
    let num_nodes = cluster.nodes.len();
    let mut invocation_counter = 0u64;
    let mut iteration = 0u64;
    let mut success_rx = success_rx.clone();

    while chaos_start.elapsed() < chaos_duration {
        info!("Chaos iteration {}", iteration);

        for i in 0..10u64 {
            let partition = ((invocation_counter + i) % NUM_PARTITIONS as u64) as u32;
            let node_idx = (iteration as usize) % num_nodes;
            let url = format!("http://localhost/Counter/{partition}/add");
            let client = if cluster.nodes[node_idx].pid().is_some() {
                &ingress_clients[node_idx]
            } else {
                cluster
                    .nodes
                    .iter()
                    .position(|n| n.pid().is_some())
                    .map(|idx| &ingress_clients[idx])
                    .unwrap_or(&ingress_clients[0])
            };
            let result: Result<reqwest::Response, _> = client
                .post(&url)
                .header("content-type", "application/json")
                .body(invocation_counter.to_string())
                .send()
                .await;
            if result.is_ok_and(|r| r.status().is_success()) {
                success_tx.send_modify(|v| *v += 1);
            }
            invocation_counter += 1;
        }

        let partition_id = (iteration % NUM_PARTITIONS as u64) as u32;

        let client_node_idx = cluster
            .nodes
            .iter()
            .position(|n| n.pid().is_some())
            .unwrap_or(0);

        let mut client = new_cluster_ctrl_client(
            create_tonic_channel(
                cluster.nodes[client_node_idx].advertised_address().clone(),
                &NetworkingOptions::default(),
                DNSResolution::Gai,
            ),
            &base_config.networking,
        );

        let snapshot_result = client
            .create_partition_snapshot(CreatePartitionSnapshotRequest {
                partition_id,
                min_target_lsn: None,
                trim_log: true,
            })
            .await;

        if let Ok(response) = snapshot_result {
            info!(
                "Snapshot created for partition {} at LSN {}",
                partition_id,
                response.into_inner().min_applied_lsn
            );

            if iteration >= 2 && iteration.is_multiple_of(2) {
                match corrupt_latest_snapshot(snapshots_dir.path(), partition_id) {
                    Ok(true) => {
                        info!(
                            "Corrupted latest snapshot for partition {}, fallback should be used",
                            partition_id
                        );
                    }
                    Ok(false) => {
                        info!(
                            "Could not corrupt partition {} (no exclusive SSTs or < 2 retained snapshots)",
                            partition_id
                        );
                    }
                    Err(e) => {
                        info!("Failed to corrupt partition {}: {}", partition_id, e);
                    }
                }
            }
        }

        let node_to_restart = (iteration as usize) % num_nodes;
        let node_name;
        let db_dir;
        {
            let node = &mut cluster.nodes[node_to_restart];
            node_name = node.node_name().to_owned();
            db_dir = node.config().common.base_dir().join(&node_name).join("db");

            info!("Restarting node '{}' with data wipe", node_name);

            node.terminate()?;
            let _ = node.status().await;

            if db_dir.exists() {
                std::fs::remove_dir_all(&db_dir)?;
            }

            // killing a node can potentially leave stale leases lying around, causing flakiness
            if let Err(e) = node.restart(TerminationSignal::SIGTERM).await {
                fail!("Failed to restart node: {e}")?;
            }
        }

        // Set up watcher for snapshot restore log message (needs fresh borrow after restart)
        let mut snapshot_restore_watcher = cluster.nodes[node_to_restart].lines(
            "(Found partition snapshot, restoring it|Restored from fallback snapshot)".parse()?,
        );

        if let Err(e) = cluster
            .wait_check_healthy(HealthCheck::Worker, Duration::from_secs(30))
            .await
        {
            fail!("Failed to wait for cluster health: {e}")?;
        }

        let restore_result =
            tokio::time::timeout(Duration::from_secs(5), snapshot_restore_watcher.next()).await;
        if restore_result.is_err() {
            fail!(
                "Node '{}' did not restore any partition from snapshot after data wipe",
                node_name
            )?;
        }
        drop(snapshot_restore_watcher);
        info!("Node '{}' restored from snapshot", node_name);

        success_rx.mark_unchanged();
        tokio::time::timeout(EXPECTED_RECOVERY_DURATION, success_rx.changed())
            .await
            .map_err(|_| {
                anyhow!("Failed to process requests within the expected recovery duration")
            })
            .into_test_result()??;

        iteration += 1;
    }

    let _ = shutdown_tx.send(());
    let _ = health_check_task.await;

    info!("Waiting for applied LSN convergence");
    let mut final_client = new_cluster_ctrl_client(
        create_tonic_channel(
            cluster.nodes[0].advertised_address().clone(),
            &NetworkingOptions::default(),
            DNSResolution::Gai,
        ),
        &base_config.networking,
    );

    const PARTITION_REPLICATION: usize = 2;
    for partition_id in 0..NUM_PARTITIONS {
        applied_lsn_converged(
            &mut final_client,
            PARTITION_REPLICATION,
            PartitionId::from(partition_id as u16),
            MAX_WAIT_TIMEOUT,
        )
        .await?;
        info!("Partition {} converged", partition_id);
    }

    let total_ops = *success_rx.borrow();
    info!(
        "Chaos test completed. Total successful operations: {}",
        total_ops
    );
    assert!(
        total_ops > 0,
        "Should have processed at least some operations successfully"
    );

    for partition_id in 0..NUM_PARTITIONS {
        let partition_dir = snapshots_dir.path().join(partition_id.to_string());
        let latest_json = partition_dir.join("latest.json");
        assert!(
            latest_json.exists(),
            "Snapshot metadata should exist for partition {}",
            partition_id
        );
    }

    info!("Incremental snapshot chaos test passed!");
    cluster.graceful_shutdown(Duration::from_secs(10)).await?;

    Ok(())
}

async fn wait_all_partitions_active(
    client: &mut ClusterCtrlSvcClient<Channel>,
    expected_partitions: u32,
    timeout: Duration,
) -> googletest::Result<()> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            fail!(
                "Timeout waiting for {} partitions to become active",
                expected_partitions
            )?;
        }

        let cluster_state = client
            .get_cluster_state(ClusterStateRequest {})
            .await?
            .into_inner()
            .cluster_state
            .unwrap();

        let active_partitions: u32 = cluster_state
            .nodes
            .values()
            .map(|n| {
                n.state
                    .as_ref()
                    .map(|s| match s {
                        State::Alive(s) => s
                            .partitions
                            .values()
                            .filter(|p| {
                                RunMode::try_from(p.effective_mode)
                                    .is_ok_and(|m| m == RunMode::Leader)
                            })
                            .count() as u32,
                        _ => 0,
                    })
                    .unwrap_or(0)
            })
            .sum();

        if active_partitions >= expected_partitions {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}

async fn applied_lsn_converged(
    client: &mut ClusterCtrlSvcClient<Channel>,
    expected_processors: usize,
    partition_id: PartitionId,
    timeout: Duration,
) -> googletest::Result<()> {
    assert!(expected_processors > 0);
    info!(
        "Waiting for {} partition processor(s) to converge on the same applied LSN for partition {}",
        expected_processors, partition_id
    );

    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            fail!(
                "Timeout waiting for {} processors to converge on partition {}",
                expected_processors,
                partition_id
            )?;
        }

        let cluster_state = match client.get_cluster_state(ClusterStateRequest {}).await {
            Ok(response) => response.into_inner().cluster_state.unwrap(),
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
        };

        let applied_lsns: Vec<_> = cluster_state
            .nodes
            .values()
            .filter_map(|n| {
                n.state.as_ref().and_then(|s| match s {
                    State::Alive(s) => s
                        .partitions
                        .get(&partition_id.into())
                        .and_then(|p| p.last_applied_log_lsn)
                        .map(|lsn| lsn.value),
                    _ => None,
                })
            })
            .collect();

        if applied_lsns.len() >= expected_processors
            && !applied_lsns.is_empty()
            && applied_lsns.iter().all(|lsn| *lsn == applied_lsns[0])
        {
            info!(
                "Partition {} converged at LSN {}",
                partition_id, applied_lsns[0]
            );
            break;
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}
