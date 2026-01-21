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
use std::num::NonZeroU64;
use std::time::Duration;

use enumset::EnumSet;
use futures_util::StreamExt;
use googletest::{IntoTestResult, fail};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tracing::info;
use url::Url;

use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::protobuf::cluster_ctrl_svc::{
    ClusterStateRequest, CreatePartitionSnapshotRequest,
    cluster_ctrl_svc_client::ClusterCtrlSvcClient, new_cluster_ctrl_client,
};
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, NodeSpec},
};
use restate_types::config::Configuration;
use restate_types::config::{LogFormat, NetworkingOptions, SnapshotType};
use restate_types::logs::metadata::ProviderKind::Replicated;
use restate_types::logs::metadata::{NodeSetSize, ProviderConfiguration, ReplicatedLogletConfig};
use restate_types::net::address::PeerNetAddress;
use restate_types::protobuf::cluster::RunMode;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::replication::ReplicationProperty;
use restate_types::retries::RetryPolicy;

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_snapshot_cleanup() -> googletest::Result<()> {
    const RETAIN_SNAPSHOTS: usize = 5;
    const TOTAL_SNAPSHOTS: usize = 10;
    // Each invocation generates ~3 log records
    const RECORDS_PER_SNAPSHOT: u64 = 10;
    // Send enough invocations to exceed the threshold
    const INVOCATIONS_PER_BURST: usize = 5; // ~15 records, exceeds threshold of 10

    let mut base_config = Configuration::new_unix_sockets();
    base_config.common.default_num_partitions = 1.try_into()?;
    base_config.bifrost.default_provider = Replicated;
    base_config.common.log_filter = "warn,restate=debug".to_owned();
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
        Some(std::num::NonZeroU8::new(RETAIN_SNAPSHOTS as u8).unwrap());
    // Enable automatic snapshots based on record count
    base_config.worker.snapshots.snapshot_interval_num_records =
        Some(NonZeroU64::new(RECORDS_PER_SNAPSHOT).unwrap());
    // Fast check interval for responsive snapshot triggering (jitter will be 10% = 5ms)
    base_config.worker.snapshots.check_interval = Some(Duration::from_millis(50).into());

    let nodes = NodeSpec::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        EnumSet::all(),
        1,
        false,
    );

    let mut cluster = Cluster::builder()
        .cluster_name("incremental-snapshot-cleanup")
        .nodes(nodes)
        .temp_base_dir("incremental_snapshot_cleanup")
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

    info!("Waiting until the partition processor has become the leader");
    any_partition_active(&mut client).await?;

    // Start mock service and register it
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

    let ingress_uds = worker_1
        .ingress_address()
        .clone()
        .unwrap()
        .into_address()
        .unwrap();
    let PeerNetAddress::Uds(ingress_uds) = ingress_uds else {
        panic!("ingress address must be a unix domain socket");
    };
    let ingress_http_client = reqwest::Client::builder()
        .unix_socket(ingress_uds)
        .build()?;

    // Initial request to warm up the service
    info!("Sending initial request");
    let mut retry = RetryPolicy::fixed_delay(Duration::from_millis(500), None).into_iter();
    loop {
        let response = ingress_http_client
            .post("http://localhost/Counter/0/get")
            .send()
            .await?;
        if response.status().is_success() {
            break;
        }
        if let Some(delay) = retry.next() {
            tokio::time::sleep(delay).await;
        } else {
            fail!("Failed to invoke worker")?;
        }
    }

    let partition_dir = snapshots_dir.path().join("0");

    info!(
        "Creating {} automatic snapshots with {} invocations per burst",
        TOTAL_SNAPSHOTS, INVOCATIONS_PER_BURST
    );
    let mut invocation_counter = 0;
    for snapshot_num in 0..TOTAL_SNAPSHOTS {
        let snapshot_id_before = get_latest_snapshot_id(&partition_dir);

        for _ in 0..INVOCATIONS_PER_BURST {
            let response = ingress_http_client
                .post("http://localhost/Counter/0/add")
                .header("content-type", "application/json")
                .body(invocation_counter.to_string())
                .send()
                .await?;
            assert!(
                response.status().is_success(),
                "Invocation {} failed: {}",
                invocation_counter,
                response.status()
            );
            invocation_counter += 1;
        }

        // Wait for automatic snapshot to be created (detected by snapshot_id change)
        let snapshot_created = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let current_id = get_latest_snapshot_id(&partition_dir);
                if let Some(id) = current_id
                    && Some(&id) != snapshot_id_before.as_ref()
                {
                    return id;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;

        match snapshot_created {
            Ok(snapshot_id) => {
                let retained_count = count_retained_snapshots(&partition_dir);
                info!(
                    "Snapshot {}/{} created: {} (retained: {})",
                    snapshot_num + 1,
                    TOTAL_SNAPSHOTS,
                    snapshot_id,
                    retained_count
                );
            }
            Err(_) => {
                fail!(
                    "Timeout waiting for snapshot {}/{} to be created",
                    snapshot_num + 1,
                    TOTAL_SNAPSHOTS
                )?;
            }
        }
    }

    // Wait for cleanup
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (retained_snapshots, referenced_ssts) =
        verify_snapshot_state(&partition_dir, RETAIN_SNAPSHOTS)?;

    info!(
        "After {} automatic snapshots: {} retained, {} SSTs referenced",
        TOTAL_SNAPSHOTS,
        retained_snapshots.len(),
        referenced_ssts.len()
    );

    info!("Incremental snapshot cleanup test passed!");
    cluster.graceful_shutdown(Duration::from_secs(10)).await?;

    Ok(())
}

fn get_latest_snapshot_id(partition_dir: &std::path::Path) -> Option<String> {
    let latest_json_path = partition_dir.join("latest.json");
    if !latest_json_path.exists() {
        return None;
    }

    let content = std::fs::read_to_string(&latest_json_path).ok()?;
    let latest_json: serde_json::Value = serde_json::from_str(&content).ok()?;

    latest_json["snapshot_id"].as_str().map(|s| s.to_string())
}

fn count_retained_snapshots(partition_dir: &std::path::Path) -> usize {
    let latest_json_path = partition_dir.join("latest.json");
    if !latest_json_path.exists() {
        return 0;
    }

    let Ok(content) = std::fs::read_to_string(&latest_json_path) else {
        return 0;
    };
    let Ok(latest_json) = serde_json::from_str::<serde_json::Value>(&content) else {
        return 0;
    };

    latest_json["retained_snapshots"]
        .as_array()
        .map(|arr| arr.len())
        .unwrap_or(0)
}

fn verify_snapshot_state(
    partition_dir: &std::path::Path,
    expected_retained: usize,
) -> googletest::Result<(HashSet<String>, HashSet<String>)> {
    let latest_json_path = partition_dir.join("latest.json");
    assert!(
        latest_json_path.exists(),
        "latest.json should exist in partition directory"
    );

    let latest_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&latest_json_path)?)?;

    // Check retained snapshots count
    let retained_snapshots = latest_json["retained_snapshots"]
        .as_array()
        .expect("retained_snapshots should be an array");
    assert_eq!(
        retained_snapshots.len(),
        expected_retained,
        "Should retain exactly {} snapshots, found {}",
        expected_retained,
        retained_snapshots.len()
    );

    // Check pending deletions is empty
    let empty_vec = vec![];
    let pending_deletions = latest_json["pending_deletions"]
        .as_array()
        .unwrap_or(&empty_vec);
    assert!(
        pending_deletions.is_empty(),
        "Pending deletions should be empty after cleanup, found: {:?}",
        pending_deletions
    );

    // Collect retained snapshot IDs
    let retained_ids: HashSet<String> = retained_snapshots
        .iter()
        .map(|s| s["snapshot_id"].as_str().unwrap().to_string())
        .collect();

    // Collect all SSTs referenced by retained snapshots
    let mut referenced_ssts: HashSet<String> = HashSet::new();
    for retained in retained_snapshots {
        let snapshot_path = retained["path"].as_str().unwrap();
        let metadata_path = partition_dir.join(snapshot_path).join("metadata.json");

        assert!(
            metadata_path.exists(),
            "Retained snapshot metadata should exist: {}",
            metadata_path.display()
        );

        let metadata: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&metadata_path)?)?;

        // Collect SST keys from file_keys (incremental snapshots)
        if let Some(file_keys) = metadata["file_keys"].as_object() {
            for sst_key in file_keys.values() {
                if let Some(key) = sst_key.as_str() {
                    // Extract just the filename from "ssts/N1_000001.sst"
                    if let Some(filename) = key.split('/').next_back() {
                        referenced_ssts.insert(filename.to_string());
                    }
                }
            }
        }
    }

    // Check for orphaned SSTs in the ssts/ directory
    let ssts_dir = partition_dir.join("ssts");
    if ssts_dir.exists() {
        let sst_files: Vec<_> = std::fs::read_dir(&ssts_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
            .collect();

        for sst_file in &sst_files {
            let filename = sst_file.file_name().to_string_lossy().to_string();
            assert!(
                referenced_ssts.contains(&filename),
                "Orphaned SST file found: {} (not referenced by any retained snapshot)",
                filename
            );
        }

        info!(
            "Verified {} SST files in ssts/ directory, all referenced",
            sst_files.len()
        );
    }

    Ok((retained_ids, referenced_ssts))
}

async fn any_partition_active(
    client: &mut ClusterCtrlSvcClient<Channel>,
) -> googletest::Result<()> {
    loop {
        let cluster_state = client
            .get_cluster_state(ClusterStateRequest {})
            .await?
            .into_inner()
            .cluster_state
            .unwrap();

        if cluster_state.nodes.values().any(|n| {
            n.state.as_ref().is_some_and(|s| match s {
                State::Alive(s) => s.partitions.values().any(|p| {
                    RunMode::try_from(p.effective_mode).is_ok_and(|m| m == RunMode::Leader)
                }),
                _ => false,
            })
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn sequential_snapshots() -> googletest::Result<()> {
    const RETAIN_SNAPSHOTS: usize = 3;
    const NUM_SNAPSHOTS: usize = 4;

    let mut base_config = Configuration::new_unix_sockets();
    base_config.common.default_num_partitions = 1.try_into()?;
    base_config.bifrost.default_provider = Replicated;
    base_config.common.log_filter = "warn,restate=debug".to_owned();
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
        Some(std::num::NonZeroU8::new(RETAIN_SNAPSHOTS as u8).unwrap());
    // No snapshot_interval_num_records - we trigger manually via RPC
    // Fast check interval for responsive snapshot triggering
    base_config.worker.snapshots.check_interval = Some(Duration::from_millis(50).into());

    let nodes = NodeSpec::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        EnumSet::all(),
        1,
        false,
    );

    let mut cluster = Cluster::builder()
        .cluster_name("back-to-back-manual-snapshots")
        .nodes(nodes)
        .temp_base_dir("back_to_back_manual_snapshots")
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

    info!("Waiting until the partition processor has become the leader");
    any_partition_active(&mut client).await?;

    // Start mock service and register it
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

    let ingress_uds = worker_1
        .ingress_address()
        .clone()
        .unwrap()
        .into_address()
        .unwrap();
    let PeerNetAddress::Uds(ingress_uds) = ingress_uds else {
        panic!("ingress address must be a unix domain socket");
    };
    let ingress_http_client = reqwest::Client::builder()
        .unix_socket(ingress_uds)
        .build()?;

    info!("Sending initial request");
    let mut retry = RetryPolicy::fixed_delay(Duration::from_millis(500), None).into_iter();
    loop {
        let response = ingress_http_client
            .post("http://localhost/Counter/0/get")
            .send()
            .await?;
        if response.status().is_success() {
            break;
        }
        if let Some(delay) = retry.next() {
            tokio::time::sleep(delay).await;
        } else {
            fail!("Failed to invoke worker")?;
        }
    }

    let partition_dir = snapshots_dir.path().join("0");

    // Trigger rapid back-to-back snapshots via RPC
    info!("Triggering {} back-to-back manual snapshots", NUM_SNAPSHOTS);
    let mut snapshot_ids: Vec<String> = Vec::new();

    for i in 0..NUM_SNAPSHOTS {
        // Send a request to generate state changes before snapshot
        let response = ingress_http_client
            .post("http://localhost/Counter/0/add")
            .header("content-type", "application/json")
            .body(i.to_string())
            .send()
            .await?;
        assert!(response.status().is_success());

        // Trigger snapshot via RPC with retry logic for lease contention.
        // The previous snapshot's lease may not have been released yet (async release),
        // so retry with a short delay if we get a "same node" lease error.
        let mut retry = RetryPolicy::fixed_delay(Duration::from_millis(50), Some(20)).into_iter();
        let snapshot_response = loop {
            match client
                .create_partition_snapshot(CreatePartitionSnapshotRequest {
                    partition_id: 0,
                    min_target_lsn: None,
                    trim_log: false,
                })
                .await
            {
                Ok(response) => break response.into_inner(),
                Err(status) => {
                    let msg = status.message().to_lowercase();
                    // Retry if the previous lease hasn't been released yet
                    if (msg.contains("same node") || msg.contains("concurrent"))
                        && let Some(delay) = retry.next()
                    {
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(status.into());
                }
            }
        };

        info!(
            "Snapshot {}/{} created: id={}, lsn={}",
            i + 1,
            NUM_SNAPSHOTS,
            snapshot_response.snapshot_id,
            snapshot_response.min_applied_lsn
        );

        snapshot_ids.push(snapshot_response.snapshot_id);
    }

    // Verify all snapshot IDs are unique
    let unique_ids: HashSet<_> = snapshot_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        snapshot_ids.len(),
        "All snapshot IDs should be unique"
    );

    // Wait for cleanup to complete
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify snapshot state
    let (retained_snapshots, referenced_ssts) =
        verify_snapshot_state(&partition_dir, RETAIN_SNAPSHOTS)?;

    info!(
        "Verified: {} retained snapshots, {} SSTs referenced",
        retained_snapshots.len(),
        referenced_ssts.len()
    );

    info!("Back-to-back manual snapshots test passed!");
    cluster.graceful_shutdown(Duration::from_secs(10)).await?;

    Ok(())
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_snapshot_requests() -> googletest::Result<()> {
    const RETAIN_SNAPSHOTS: usize = 3;
    const CONCURRENT_REQUESTS: usize = 3;

    let mut base_config = Configuration::new_unix_sockets();
    base_config.common.default_num_partitions = 1.try_into()?;
    base_config.bifrost.default_provider = Replicated;
    base_config.common.log_filter = "warn,restate=debug".to_owned();
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
        Some(std::num::NonZeroU8::new(RETAIN_SNAPSHOTS as u8).unwrap());
    base_config.worker.snapshots.check_interval = Some(Duration::from_millis(50).into());

    let nodes = NodeSpec::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        EnumSet::all(),
        1,
        false,
    );

    let mut cluster = Cluster::builder()
        .cluster_name("concurrent-snapshot-requests")
        .nodes(nodes)
        .temp_base_dir("concurrent_snapshot_requests")
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

    let worker_1 = &cluster.nodes[0];
    let mut worker_1_ready = worker_1.lines("Partition [0-9]+ started".parse()?);

    info!("Waiting until the cluster is healthy");
    cluster.wait_healthy(Duration::from_secs(60)).await?;

    info!("Waiting until node-1 has started the partition processor");
    worker_1_ready.next().await;
    drop(worker_1_ready);

    let client = new_cluster_ctrl_client(
        create_tonic_channel(
            cluster.nodes[0].advertised_address().clone(),
            &NetworkingOptions::default(),
            DNSResolution::Gai,
        ),
        &base_config.networking,
    );

    let mut leader_client = client.clone();
    info!("Waiting until the partition processor has become the leader");
    any_partition_active(&mut leader_client).await?;

    // Start mock service and register it
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

    let ingress_uds = worker_1
        .ingress_address()
        .clone()
        .unwrap()
        .into_address()
        .unwrap();
    let PeerNetAddress::Uds(ingress_uds) = ingress_uds else {
        panic!("ingress address must be a unix domain socket");
    };
    let ingress_http_client = reqwest::Client::builder()
        .unix_socket(ingress_uds)
        .build()?;

    // Initial request to warm up the service
    info!("Sending initial request");
    let mut retry = RetryPolicy::fixed_delay(Duration::from_millis(500), None).into_iter();
    loop {
        let response = ingress_http_client
            .post("http://localhost/Counter/0/get")
            .send()
            .await?;
        if response.status().is_success() {
            break;
        }
        if let Some(delay) = retry.next() {
            tokio::time::sleep(delay).await;
        } else {
            fail!("Failed to invoke worker")?;
        }
    }

    let partition_dir = snapshots_dir.path().join("0");

    // Record initial state (may be None if no snapshot exists yet)
    let snapshot_id_before = get_latest_snapshot_id(&partition_dir);
    let retained_before = count_retained_snapshots(&partition_dir);
    info!(
        "Initial state: snapshot_id={:?}, retained_count={}",
        snapshot_id_before, retained_before
    );

    // Spawn concurrent snapshot requests
    info!(
        "Spawning {} concurrent snapshot requests",
        CONCURRENT_REQUESTS
    );

    let mut handles = Vec::new();
    for i in 0..CONCURRENT_REQUESTS {
        let mut client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let result = client_clone
                .create_partition_snapshot(CreatePartitionSnapshotRequest {
                    partition_id: 0,
                    min_target_lsn: None,
                    trim_log: false,
                })
                .await;
            (i, result)
        });
        handles.push(handle);
    }

    // Collect results
    let mut successes = Vec::new();
    let mut in_progress_errors = 0;
    let mut other_errors = Vec::new();

    for handle in handles {
        let (i, result) = handle.await?;
        match result {
            Ok(response) => {
                let snapshot_id = response.into_inner().snapshot_id;
                info!("Request {}: succeeded with snapshot_id={}", i, snapshot_id);
                successes.push(snapshot_id);
            }
            Err(status) => {
                let msg = status.message().to_lowercase();
                // Check if it's a SnapshotInProgress error (message: "Snapshot export in progress")
                if msg.contains("in progress") {
                    info!("Request {}: blocked (snapshot in progress)", i);
                    in_progress_errors += 1;
                } else {
                    info!("Request {}: failed with error: {}", i, status);
                    other_errors.push(status.to_string());
                }
            }
        }
    }

    info!(
        "Results: {} successes, {} in-progress rejections, {} other errors",
        successes.len(),
        in_progress_errors,
        other_errors.len()
    );

    // Verify at least one snapshot was created
    assert!(
        !successes.is_empty(),
        "At least one snapshot request should succeed"
    );

    // Verify no unexpected errors
    assert!(
        other_errors.is_empty(),
        "No unexpected errors should occur: {:?}",
        other_errors
    );

    // All concurrent requests should either succeed or get SnapshotInProgress.
    // Multiple successes are possible if requests serialize (first completes before
    // second arrives), but typically we expect 1 success and rest blocked.
    assert_eq!(
        successes.len() + in_progress_errors,
        CONCURRENT_REQUESTS,
        "All requests should either succeed or be blocked"
    );

    // The single-writer guarantee means concurrent requests hitting during an
    // in-progress snapshot should be blocked. Verify we got some blocking.
    assert!(
        in_progress_errors > 0 || successes.len() == 1,
        "Either some requests should be blocked, or only one request succeeded"
    );

    // Wait for cleanup
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify only one new snapshot was created
    let snapshot_id_after = get_latest_snapshot_id(&partition_dir);
    assert!(
        snapshot_id_after.is_some(),
        "A snapshot should exist after concurrent requests"
    );

    let retained_after = count_retained_snapshots(&partition_dir);
    info!(
        "After state: snapshot_id={:?}, retained_count={}",
        snapshot_id_after, retained_after
    );

    // Retained count should increase by the number of successful requests, capped by
    // the retention limit. With RETAIN_SNAPSHOTS=3 and starting from 0, all 3 concurrent
    // requests succeeding would give us 3 retained. But typically only 1 succeeds.
    let new_snapshots = retained_after.saturating_sub(retained_before);
    assert!(
        new_snapshots <= successes.len(),
        "Retained count increase ({}) should not exceed successful requests ({})",
        new_snapshots,
        successes.len()
    );

    // Verify snapshot state is consistent
    let (retained_ids, referenced_ssts) = verify_snapshot_state(&partition_dir, retained_after)?;

    info!(
        "Verified: {} retained snapshots, {} SSTs referenced",
        retained_ids.len(),
        referenced_ssts.len()
    );

    info!("Concurrent snapshot requests test passed!");
    cluster.graceful_shutdown(Duration::from_secs(10)).await?;

    Ok(())
}
