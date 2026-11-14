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
use std::num::{NonZeroU8, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use enumset::EnumSet;
use futures_util::StreamExt;
use googletest::{IntoTestResult, fail};
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
    cluster::{Cluster, StartedCluster},
    node::{BinarySource, NodeSpec},
};
use restate_test_util::tempdir;
use restate_types::config::Configuration;
use restate_types::config::{LogFormat, NetworkingOptions, SnapshotType};
use restate_types::logs::metadata::ProviderKind::Replicated;
use restate_types::logs::metadata::{NodeSetSize, ProviderConfiguration, ReplicatedLogletConfig};
use restate_types::net::address::PeerNetAddress;
use restate_types::protobuf::cluster::RunMode;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::replication::ReplicationProperty;
use restate_types::retries::RetryPolicy;

const TEST_LOG_FILTER: &str = "warn,restate=debug";

/// A single-node cluster that writes incremental snapshots to a local directory, with the mock
/// service registered.
struct SnapshotCluster {
    cluster: StartedCluster,
    client: ClusterCtrlSvcClient<Channel>,
    ingress: reqwest::Client,
    /// The snapshot repository directory of partition 0.
    partition_dir: PathBuf,
}

impl SnapshotCluster {
    async fn start(
        name: &str,
        num_retained: u8,
        configure: impl FnOnce(&mut Configuration),
    ) -> googletest::Result<Self> {
        let mut base_config = Configuration::new_unix_sockets();
        base_config.common.default_num_partitions = 1.try_into()?;
        base_config.bifrost.default_provider = Replicated;
        TEST_LOG_FILTER.clone_into(&mut base_config.common.log_filter);
        base_config.common.log_format = LogFormat::Compact;
        base_config.common.log_disable_ansi_codes = true;

        let snapshots_dir = tempdir()?.keep();
        base_config.worker.snapshots.destination =
            Some(Url::from_file_path(&snapshots_dir).unwrap().to_string());
        base_config.worker.snapshots.experimental_snapshot_type = SnapshotType::Incremental;
        base_config.worker.snapshots.num_retained = NonZeroU8::new(num_retained).unwrap();
        base_config.worker.snapshots.check_interval = Some(Duration::from_millis(50).into());
        configure(&mut base_config);

        let nodes = NodeSpec::new_test_nodes(
            base_config.clone(),
            BinarySource::CargoTest,
            EnumSet::all(),
            1,
            false,
        );
        let cluster = Box::pin(
            Cluster::builder()
                .cluster_name(name)
                .nodes(nodes)
                .temp_base_dir(&name.replace('-', "_"))
                .build()
                .start(),
        )
        .await?;

        info!("Provisioning the cluster");
        let replicated_loglet_config = ReplicatedLogletConfig {
            target_nodeset_size: NodeSetSize::default(),
            replication_property: ReplicationProperty::new_unchecked(1),
        };
        cluster.nodes[0]
            .provision_cluster(
                None,
                ReplicationProperty::new_unchecked(1),
                Some(ProviderConfiguration::Replicated(replicated_loglet_config)),
                EnumSet::empty(),
            )
            .await
            .into_test_result()?;

        let node = &cluster.nodes[0];
        let mut partition_started = node.lines("Partition [0-9]+ started".parse()?);
        info!("Waiting until the cluster is healthy");
        cluster.wait_healthy(Duration::from_secs(60)).await?;
        info!("Waiting until the partition processor has started");
        partition_started.next().await;
        drop(partition_started);

        let mut client = new_cluster_ctrl_client(
            create_tonic_channel(
                node.advertised_address().clone(),
                &NetworkingOptions::default(),
                DNSResolution::Gai,
            ),
            &base_config.networking,
        );
        info!("Waiting until the partition processor has become the leader");
        any_partition_active(&mut client).await?;

        let (running_tx, running_rx) = oneshot::channel();
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let mock_svc_port = listener.local_addr()?.port();
        tokio::spawn(async move {
            if let Err(e) = mock_service_endpoint::listener::run_listener(listener, || {
                let _ = running_tx.send(());
            })
            .await
            {
                panic!("Error running listener: {e:?}");
            }
        });
        running_rx.await?;

        let admin = reqwest::Client::builder()
            .unix_socket(uds(node
                .admin_address()
                .clone()
                .unwrap()
                .into_address()
                .unwrap()))
            .build()?;
        let registration_response = admin
            .post("http://localhost/deployments")
            .header("content-type", "application/json")
            .json(&serde_json::json!({ "uri": format!("http://127.0.0.1:{mock_svc_port}") }))
            .send()
            .await?;
        assert!(registration_response.status().is_success());

        let ingress = reqwest::Client::builder()
            .unix_socket(uds(node
                .ingress_address()
                .clone()
                .unwrap()
                .into_address()
                .unwrap()))
            .build()?;
        info!("Sending initial request");
        let mut retry = RetryPolicy::fixed_delay(Duration::from_millis(500), None).into_iter();
        loop {
            let response = ingress
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

        Ok(Self {
            cluster,
            client,
            ingress,
            partition_dir: snapshots_dir.join("0"),
        })
    }

    async fn add(&self, value: usize) -> googletest::Result<()> {
        let response = self
            .ingress
            .post("http://localhost/Counter/0/add")
            .header("content-type", "application/json")
            .body(value.to_string())
            .send()
            .await?;
        if !response.status().is_success() {
            fail!("Invocation {value} failed: {}", response.status())?;
        }
        Ok(())
    }

    /// Waits until eviction cleanup has caught up: `expected_retained` snapshots are retained,
    /// and the repository holds no files that they don't reference.
    async fn wait_for_snapshot_state(&self, expected_retained: usize) -> googletest::Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match verify_snapshot_state(&self.partition_dir, expected_retained) {
                Ok(()) => return Ok(()),
                Err(err) if tokio::time::Instant::now() >= deadline => return Err(err),
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    }
}

fn uds(address: PeerNetAddress) -> PathBuf {
    let PeerNetAddress::Uds(path) = address else {
        panic!("address must be a unix domain socket");
    };
    path
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_snapshot_cleanup() -> googletest::Result<()> {
    const RETAIN_SNAPSHOTS: u8 = 5;
    const TOTAL_SNAPSHOTS: usize = 10;
    // Each invocation generates ~3 log records, so a burst exceeds the snapshot threshold.
    const RECORDS_PER_SNAPSHOT: u64 = 10;
    const INVOCATIONS_PER_BURST: usize = 5;

    let mut cluster =
        SnapshotCluster::start("incremental-snapshot-cleanup", RETAIN_SNAPSHOTS, |config| {
            config.worker.snapshots.snapshot_interval_num_records =
                Some(NonZeroU64::new(RECORDS_PER_SNAPSHOT).unwrap());
        })
        .await?;

    info!("Creating {TOTAL_SNAPSHOTS} automatic snapshots");
    let mut invocation_counter = 0;
    for snapshot_num in 1..=TOTAL_SNAPSHOTS {
        let snapshot_id_before = get_latest_snapshot_id(&cluster.partition_dir);
        for _ in 0..INVOCATIONS_PER_BURST {
            cluster.add(invocation_counter).await?;
            invocation_counter += 1;
        }

        // An automatic snapshot changes the latest snapshot id.
        let snapshot_created = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(id) = get_latest_snapshot_id(&cluster.partition_dir)
                    && Some(&id) != snapshot_id_before.as_ref()
                {
                    return id;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        match snapshot_created {
            Ok(snapshot_id) => info!("Snapshot {snapshot_num}/{TOTAL_SNAPSHOTS}: {snapshot_id}"),
            Err(_) => fail!("Timeout waiting for snapshot {snapshot_num}/{TOTAL_SNAPSHOTS}")?,
        }
    }

    cluster
        .wait_for_snapshot_state(RETAIN_SNAPSHOTS.into())
        .await?;
    cluster
        .cluster
        .graceful_shutdown(Duration::from_secs(10))
        .await?;
    Ok(())
}

/// Manual snapshots requested back to back, each waiting for the previous one's cleanup to
/// release the partition's snapshot lease.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn sequential_snapshots() -> googletest::Result<()> {
    const RETAIN_SNAPSHOTS: u8 = 3;
    const NUM_SNAPSHOTS: usize = 4;

    let mut cluster =
        SnapshotCluster::start("back-to-back-manual-snapshots", RETAIN_SNAPSHOTS, |_| {}).await?;

    info!("Triggering {NUM_SNAPSHOTS} back-to-back manual snapshots");
    let mut snapshot_ids = HashSet::new();
    for i in 0..NUM_SNAPSHOTS {
        cluster.add(i).await?;
        let snapshot = cluster
            .client
            .create_partition_snapshot(CreatePartitionSnapshotRequest {
                partition_id: 0,
                min_target_lsn: None,
                trim_log: false,
            })
            .await?
            .into_inner();
        info!(
            "Snapshot {}/{NUM_SNAPSHOTS}: id={}, lsn={}",
            i + 1,
            snapshot.snapshot_id,
            snapshot.min_applied_lsn
        );
        assert!(
            snapshot_ids.insert(snapshot.snapshot_id),
            "snapshot ids should be unique"
        );
    }

    cluster
        .wait_for_snapshot_state(RETAIN_SNAPSHOTS.into())
        .await?;
    cluster
        .cluster
        .graceful_shutdown(Duration::from_secs(10))
        .await?;
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

/// Checks that `expected_retained` snapshots are retained, that evicted snapshots left no files
/// behind, and that every SST in `ssts/` is referenced by a retained snapshot.
fn verify_snapshot_state(
    partition_dir: &std::path::Path,
    expected_retained: usize,
) -> googletest::Result<()> {
    let latest_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(partition_dir.join("latest.json"))?)?;
    let Some(retained_snapshots) = latest_json["retained_snapshots"].as_array() else {
        return fail!("retained_snapshots should be an array");
    };
    if retained_snapshots.len() != expected_retained {
        return fail!(
            "expected {expected_retained} retained snapshots, found {}",
            retained_snapshots.len()
        );
    }

    // Evicted snapshots' files are deleted: only retained snapshot directories hold files. The
    // local object store leaves the emptied directories behind.
    let retained_paths: HashSet<&str> = retained_snapshots
        .iter()
        .filter_map(|s| s["path"].as_str())
        .collect();
    for entry in std::fs::read_dir(partition_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("lsn_") && !retained_paths.contains(name.as_str()) {
            let leftover: Vec<_> = std::fs::read_dir(entry.path())?.collect();
            if !leftover.is_empty() {
                return fail!("evicted snapshot {name} still has files: {leftover:?}");
            }
        }
    }

    // Every SST in ssts/ is referenced by a retained snapshot.
    let mut referenced_ssts = HashSet::new();
    for path in &retained_paths {
        let metadata: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
            partition_dir.join(path).join("metadata.json"),
        )?)?;
        if let Some(file_keys) = metadata["file_keys"].as_object() {
            referenced_ssts.extend(
                file_keys
                    .values()
                    .filter_map(|key| key.as_str()?.rsplit('/').next())
                    .map(str::to_owned),
            );
        }
    }
    let ssts_dir = partition_dir.join("ssts");
    if ssts_dir.exists() {
        for entry in std::fs::read_dir(&ssts_dir)? {
            let name = entry?.file_name().to_string_lossy().to_string();
            if name.ends_with(".sst") && !referenced_ssts.contains(&name) {
                return fail!("orphaned SST {name} is not referenced by any retained snapshot");
            }
        }
    }
    Ok(())
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
