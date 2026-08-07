// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, this software will be governed
// by the Apache License, Version 2.0.

#![cfg(unix)]

use std::{net::SocketAddr, num::NonZeroU8, process::Command, time::Duration};

use anyhow::{anyhow, bail};
use enumset::EnumSet;
use futures_util::{StreamExt, TryStreamExt};
use regex::Regex;
use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::protobuf::cluster_ctrl_svc::{
    ClusterStateRequest, CreatePartitionSnapshotRequest,
    cluster_ctrl_svc_client::ClusterCtrlSvcClient, new_cluster_ctrl_client,
};
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, NodeSpec, TerminationSignal},
};
use restate_types::config::{Configuration, NetworkingOptions};
use restate_types::logs::metadata::{
    NodeSetSize, ProviderConfiguration, ProviderKind, ReplicatedLogletConfig,
};
use restate_types::net::address::PeerNetAddress;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::replication::ReplicationProperty;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{info, warn};
use url::Url;

/// Reproduces the characteristic `gossip-age=29` observation from the rollout
/// incident. This is deliberately a process pause, not a network fault: while the
/// node is stopped it cannot process its own periodic gossip tick. On resume, its
/// next tick accounts for all missed 100-ms intervals. The detector's paused-time
/// unit test covers the immediate state transition this causes: both previously
/// Alive peers become Dead at the accumulated age. A network-only loss normally
/// reaches the ordinary threshold at age 11 instead.
///
/// This is ignored because it starts three real server processes and intentionally
/// freezes one. Run with:
///
/// `cargo nextest run -p restate-server --run-ignored ignored-only failure_detector_process_pause`
#[test_log::test(restate_core::test)]
#[ignore = "manual local-cluster reproducer; pauses a server process"]
async fn failure_detector_process_pause_accumulates_gossip_intervals() -> anyhow::Result<()> {
    let mut base_config = Configuration::new_unix_sockets();
    base_config.common.default_num_partitions = 1;
    base_config.common.log_filter =
        "info,restate_node::failure_detector::node_state=info".to_owned();

    let nodes = NodeSpec::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        EnumSet::all(),
        3,
        true,
    );
    let mut cluster = Cluster::builder()
        .cluster_name("failure_detector_process_pause")
        .nodes(nodes)
        .temp_base_dir("failure_detector_process_pause")
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;
    // The health endpoint reports role readiness, while the detector still needs
    // regular gossip and the suspect interval to establish its initial view.
    tokio::time::sleep(Duration::from_secs(7)).await;

    let failure_significant_tick = Regex::new(r"intervals_passed[:=] ?(2[0-9]|[1-9][0-9]{2,})")?;
    let observed = {
        let paused_node = &cluster.nodes[1];
        let mut significant_ticks = paused_node.lines(failure_significant_tick);
        let pid = paused_node
            .pid()
            .ok_or_else(|| anyhow!("node exited before the test could pause it"))?;

        signal(pid, "STOP")?;
        tokio::time::sleep(Duration::from_millis(2900)).await;
        signal(pid, "CONT")?;

        tokio::time::timeout(Duration::from_secs(10), async {
            significant_ticks
                .next()
                .await
                .ok_or_else(|| anyhow!("paused node exited before a delayed gossip tick arrived"))
        })
        .await
        .map_err(|_| anyhow!("timed out waiting for a high-age gossip tick after SIGCONT"))??
    };

    assert!(
        observed.contains("intervals_passed:"),
        "the resumed node should account for the missed gossip intervals: {observed}"
    );

    cluster.graceful_shutdown(Duration::from_secs(5)).await?;
    Ok(())
}

/// Investigates whether ordinary recovery fan-out can make a survivor miss a
/// failure-significant number of gossip intervals. Unlike the pause reproducer,
/// this uses only graceful rolling restarts and real partition processor starts.
///
/// It is intentionally ignored: a negative result is useful evidence about the
/// local machine's scheduler and resource regime, not a test failure. The same
/// workload should be run in a CPU-constrained Cloud environment if it does not
/// reproduce locally.
///
/// Run with:
///
/// `cargo nextest run -p restate-server --run-ignored ignored-only failure_detector_rollout_fanout_probe`
#[test_log::test(restate_core::test)]
#[ignore = "manual local-cluster investigation probe; exercises real rolling recovery"]
async fn failure_detector_rollout_fanout_probe() -> anyhow::Result<()> {
    const NUM_PARTITIONS: u16 = 128;

    let mut base_config = Configuration::new_unix_sockets();
    base_config.common.auto_provision = false;
    base_config.common.default_num_partitions = NUM_PARTITIONS;
    base_config.bifrost.default_provider = restate_types::logs::metadata::ProviderKind::Replicated;
    base_config.common.log_filter = "info,restate_node::failure_detector=debug".to_owned();

    let nodes = NodeSpec::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        EnumSet::all(),
        3,
        false,
    )
    .into_iter()
    .map(|node| {
        node.with_env_var("RESTATE_DEFAULT_THREAD_POOL_SIZE", "1")
            .with_env_var("RESTATE_STORAGE_HIGH_PRIORITY_BG_THREADS", "1")
            .with_env_var("RESTATE_STORAGE_LOW_PRIORITY_BG_THREADS", "1")
            .with_env_var("RESTATE_ROCKSDB_HIGH_PRIORITY_THREADS", "1")
            .with_env_var("RESTATE_ROCKSDB_LOW_PRIORITY_THREADS", "1")
    })
    .collect();

    let mut cluster = Cluster::builder()
        .cluster_name("failure_detector_rollout_fanout_probe")
        .nodes(nodes)
        .temp_base_dir("failure_detector_rollout_fanout_probe")
        .build()
        .start()
        .await?;

    let replicated_loglet_config = ReplicatedLogletConfig {
        target_nodeset_size: NodeSetSize::default(),
        replication_property: ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
    };
    cluster.nodes[0]
        .provision_cluster(
            None,
            ReplicationProperty::new_unchecked(2),
            Some(ProviderConfiguration::Replicated(replicated_loglet_config)),
            EnumSet::empty(),
        )
        .await?;

    cluster.wait_healthy(Duration::from_secs(60)).await?;
    tokio::time::sleep(Duration::from_secs(7)).await;

    let significant_tick = Regex::new(
        r"Failure detector processed a failure-significant number of gossip intervals in one tick",
    )?;
    let mut observations = Vec::new();

    for restarted_node in 0..cluster.nodes.len() {
        let restarted_name = cluster.nodes[restarted_node].node_name().to_owned();
        info!(%restarted_name, NUM_PARTITIONS, "Restarting node for failure-detector recovery probe");
        cluster.nodes[restarted_node]
            .restart(TerminationSignal::Sigterm)
            .await?;
        cluster.wait_healthy(Duration::from_secs(60)).await?;

        // Let the new node start its processors and the survivors react to the rollout.
        tokio::time::sleep(Duration::from_secs(7)).await;

        for (node_index, node) in cluster.nodes.iter().enumerate() {
            if node_index == restarted_node {
                continue;
            }
            for line in node.last_n_lines(2_000).await? {
                if significant_tick.is_match(&line) {
                    observations.push(format!(
                        "survivor={} after_restart={} {line}",
                        node.node_name(),
                        restarted_name
                    ));
                }
            }
        }
    }

    if observations.is_empty() {
        warn!(
            NUM_PARTITIONS,
            "No failure-significant failure-detector tick observed during local recovery fan-out probe; this is a negative result for this host, not a regression assertion"
        );
    } else {
        info!(
            NUM_PARTITIONS,
            observations = ?observations,
            "Observed natural failure-significant failure-detector ticks during recovery fan-out probe"
        );
    }

    cluster.graceful_shutdown(Duration::from_secs(10)).await?;
    Ok(())
}

fn signal(pid: u32, signal: &str) -> anyhow::Result<()> {
    let status = Command::new("kill")
        .args([format!("-{signal}"), pid.to_string()])
        .status()?;
    if !status.success() {
        bail!("kill -{signal} {pid} exited with {status}");
    }
    Ok(())
}

/// Exercises the real cold-recovery path of a replacement worker during a rolling restart: the
/// node's partition-store volume is empty, so every processor restores a shared partition
/// snapshot and then replays a post-snapshot log tail. It keeps VQueues disabled so any leader
/// promotion follows the legacy invoked-invocation resumption path, although this probe creates
/// completed invocations and therefore does not itself load that scan. It deliberately does not
/// assert a failure-detector stall: a negative result is evidence that this recovery load alone
/// is not sufficient on the machine running the probe.
///
/// Run with:
///
/// `cargo nextest run -p restate-server --run-ignored ignored-only failure_detector_snapshot_recovery_rollout_probe --nocapture`
///
/// For a long record-heavy run, use `cargo test` to avoid nextest's default timeout:
///
/// `RESTATE_FD_REPLAY_RECORDS_PER_PARTITION=100000 RESTATE_FD_RECOVERY_TIMEOUT_SECS=600 cargo test -p restate-server --test failure_detector failure_detector_snapshot_recovery_rollout_probe -- --ignored --exact --nocapture`
#[test_log::test(restate_core::test)]
#[ignore = "manual local-cluster probe; writes 256 MiB of real virtual-object state"]
async fn failure_detector_snapshot_recovery_rollout_probe() -> anyhow::Result<()> {
    const NUM_PARTITIONS: u16 = 32;
    const DEFAULT_BLOB_KEYS_PER_PARTITION: usize = 16;
    const DEFAULT_BLOB_SIZE_BYTES: usize = 512 * 1024;
    const DEFAULT_REPLAY_RECORDS_PER_PARTITION: usize = 256;
    const TAIL_WRITES_PER_PARTITION: usize = 2;

    let blob_keys_per_partition = scale_env(
        "RESTATE_FD_SNAPSHOT_BLOB_KEYS_PER_PARTITION",
        DEFAULT_BLOB_KEYS_PER_PARTITION,
    );
    let blob_size_bytes = scale_env(
        "RESTATE_FD_SNAPSHOT_BLOB_SIZE_BYTES",
        DEFAULT_BLOB_SIZE_BYTES,
    );
    let replay_records_per_partition = scale_env(
        "RESTATE_FD_REPLAY_RECORDS_PER_PARTITION",
        DEFAULT_REPLAY_RECORDS_PER_PARTITION,
    );
    let recovery_timeout = Duration::from_secs(
        scale_env("RESTATE_FD_RECOVERY_TIMEOUT_SECS", 120)
            .try_into()
            .unwrap_or(u64::MAX),
    );

    let snapshots_dir = TempDir::new()?;
    let mut base_config = Configuration::new_unix_sockets();
    base_config.common.auto_provision = false;
    base_config.common.default_num_partitions = NUM_PARTITIONS;
    base_config.bifrost.default_provider = ProviderKind::Replicated;
    base_config.common.experimental.set_vqueues(false);
    base_config.common.log_filter = "info,restate_node::failure_detector=debug".to_owned();
    base_config.worker.snapshots.destination = Some(
        Url::from_file_path(snapshots_dir.path())
            .map_err(|_| anyhow!("snapshot temporary directory is not a file URL"))?
            .to_string(),
    );

    let nodes = NodeSpec::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        EnumSet::all(),
        3,
        false,
    )
    .into_iter()
    .enumerate()
    .map(|(index, node)| {
        // Make the replacement node sensitive to real control-plane contention without adding
        // an artificial CPU load. Repeat with the host default to distinguish sensitivity from
        // a defect that is independent of default-runtime width.
        if index == 2 {
            node.with_env_var("RESTATE_DEFAULT_THREAD_POOL_SIZE", "1")
                .with_env_var("RESTATE_STORAGE_HIGH_PRIORITY_BG_THREADS", "1")
                .with_env_var("RESTATE_STORAGE_LOW_PRIORITY_BG_THREADS", "1")
                .with_env_var("RESTATE_ROCKSDB_HIGH_PRIORITY_THREADS", "1")
                .with_env_var("RESTATE_ROCKSDB_LOW_PRIORITY_THREADS", "1")
        } else {
            node
        }
    })
    .collect();

    let mut cluster = Cluster::builder()
        .cluster_name("failure_detector_snapshot_recovery_rollout_probe")
        .nodes(nodes)
        .temp_base_dir("failure_detector_snapshot_recovery_rollout_probe")
        .build()
        .start()
        .await?;

    let replicated_loglet_config = ReplicatedLogletConfig {
        target_nodeset_size: NodeSetSize::default(),
        replication_property: ReplicationProperty::new_unchecked(2),
    };
    cluster.nodes[0]
        .provision_cluster(
            None,
            ReplicationProperty::new_unchecked(3),
            Some(ProviderConfiguration::Replicated(replicated_loglet_config)),
            EnumSet::empty(),
        )
        .await?;
    cluster.wait_healthy(Duration::from_secs(60)).await?;

    let (mock_service_addr, mock_service) = start_mock_service().await?;
    let ingress_client = ingress_client(&cluster.nodes[0])?;
    deploy_mock_service(&cluster.nodes[0], mock_service_addr).await?;

    write_blobs(
        &ingress_client,
        "snapshot",
        usize::from(NUM_PARTITIONS) * blob_keys_per_partition,
        blob_size_bytes,
    )
    .await?;

    let mut cluster_ctrl = new_cluster_ctrl_client(
        create_tonic_channel(
            cluster.nodes[0].advertised_address().clone(),
            &NetworkingOptions::default(),
            DNSResolution::Gai,
        ),
        &base_config.networking,
    );
    create_and_trim_snapshots(&mut cluster_ctrl, NUM_PARTITIONS).await?;

    write_blobs(
        &ingress_client,
        "tail",
        usize::from(NUM_PARTITIONS) * TAIL_WRITES_PER_PARTITION,
        blob_size_bytes,
    )
    .await?;
    write_journal_bursts(
        &ingress_client,
        NUM_PARTITIONS,
        replay_records_per_partition,
    )
    .await?;

    let replacement_name = cluster.nodes[2].node_name().to_owned();
    let replacement_db = cluster.base_dir().join(&replacement_name).join("db");
    let replacement = &mut cluster.nodes[2];
    replacement.kill().await?;
    std::fs::remove_dir_all(&replacement_db).map_err(|err| {
        anyhow!(
            "failed to remove stopped replacement partition store at {}: {err}",
            replacement_db.display()
        )
    })?;
    replacement.restart(TerminationSignal::Sigkill).await?;

    // A replacement with an empty store can reach these post-snapshot LSNs only by importing the
    // snapshot we just made: the preceding log records were trimmed. Use the control-plane state
    // as the assertion; individual import logs are useful diagnostics but are not a stable API.
    tokio::time::timeout(
        recovery_timeout,
        replacement_recovered(&mut cluster_ctrl, NUM_PARTITIONS),
    )
    .await
    .map_err(|_| anyhow!("timed out waiting for replacement processors to recover"))??;

    let recovery_lines = replacement.last_n_lines(20_000).await?;
    let imported_snapshots = recovery_lines
        .iter()
        .filter(|line| line.contains("Importing partition store snapshot"))
        .count();
    if imported_snapshots < usize::from(NUM_PARTITIONS) {
        warn!(
            observed = imported_snapshots,
            expected = NUM_PARTITIONS,
            "Replacement recovered, but retained diagnostics did not contain every snapshot-import log"
        );
    }

    let failure_significant_ticks: Vec<_> = recovery_lines
        .iter()
        .filter(|line| {
            line.contains(
                "Failure detector processed a failure-significant number of gossip intervals",
            )
        })
        .collect();
    let slow_scheduler_passes: Vec<_> = recovery_lines
        .iter()
        .filter(|line| line.contains("Partition-wide scheduler pass exceeded"))
        .collect();
    info!(
        imported_snapshots,
        blob_keys_per_partition,
        blob_size_bytes,
        replay_records_per_partition,
        ?recovery_timeout,
        failure_significant_ticks = ?failure_significant_ticks,
        slow_scheduler_passes = ?slow_scheduler_passes,
        "Completed cold replacement snapshot-recovery probe"
    );

    mock_service.abort();
    cluster.graceful_shutdown(Duration::from_secs(10)).await?;
    Ok(())
}

fn scale_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(default)
}

async fn start_mock_service() -> anyhow::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        if let Err(err) = mock_service_endpoint::listener::run_listener(listener, || {
            let _ = ready_tx.send(());
        })
        .await
        {
            panic!("mock service endpoint failed: {err:?}");
        }
    });
    ready_rx.await?;
    Ok((addr, task))
}

fn ingress_client(
    node: &restate_local_cluster_runner::node::StartedNode,
) -> anyhow::Result<reqwest::Client> {
    let ingress_address = node
        .ingress_address()
        .clone()
        .ok_or_else(|| anyhow!("node has no ingress listener"))?
        .into_address()?;
    let PeerNetAddress::Uds(ingress_uds) = ingress_address else {
        bail!("local-cluster probe expects a Unix ingress socket");
    };
    Ok(reqwest::Client::builder()
        .unix_socket(ingress_uds)
        .build()?)
}

async fn deploy_mock_service(
    node: &restate_local_cluster_runner::node::StartedNode,
    mock_service_addr: SocketAddr,
) -> anyhow::Result<()> {
    let admin_address = node
        .admin_address()
        .clone()
        .ok_or_else(|| anyhow!("node has no admin listener"))?
        .into_address()?;
    let PeerNetAddress::Uds(admin_uds) = admin_address else {
        bail!("local-cluster probe expects a Unix admin socket");
    };
    let response = reqwest::Client::builder()
        .unix_socket(admin_uds)
        .build()?
        .post("http://localhost/deployments")
        .header("content-type", "application/json")
        .json(&serde_json::json!({ "uri": format!("http://{mock_service_addr}") }))
        .send()
        .await?;
    if !response.status().is_success() {
        bail!("mock service deployment failed with {}", response.status());
    }
    Ok(())
}

async fn write_blobs(
    ingress_client: &reqwest::Client,
    prefix: &str,
    count: usize,
    blob_size_bytes: usize,
) -> anyhow::Result<()> {
    const CONCURRENCY: usize = 16;
    futures_util::stream::iter(0..count)
        .map(|index| {
            let ingress_client = ingress_client.clone();
            let url = format!("http://localhost/Blob/{prefix}-{index}/write");
            async move {
                let mut state = (index as u64) ^ 0x9e37_79b9_7f4a_7c15;
                let mut blob = vec![0; blob_size_bytes];
                for byte in &mut blob {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    *byte = state as u8;
                }
                let response = ingress_client
                    .post(url)
                    .header("content-type", "application/octet-stream")
                    .header("idempotency-key", format!("{prefix}-{index}"))
                    .body(blob)
                    .send()
                    .await?;
                if !response.status().is_success() {
                    bail!("blob invocation failed with {}", response.status());
                }
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn write_journal_bursts(
    ingress_client: &reqwest::Client,
    num_partitions: u16,
    records_per_partition: usize,
) -> anyhow::Result<()> {
    const INVOCATIONS_PER_PARTITION: usize = 4;
    const CONCURRENCY: usize = 8;

    let invocation_count = usize::from(num_partitions) * INVOCATIONS_PER_PARTITION;
    let commands_per_invocation =
        (usize::from(num_partitions) * records_per_partition).div_ceil(invocation_count);
    futures_util::stream::iter(0..invocation_count)
        .map(|index| {
            let ingress_client = ingress_client.clone();
            let url = format!("http://localhost/JournalBurst/tail-{index}/write");
            async move {
                let response = ingress_client
                    .post(url)
                    .header("content-type", "application/json")
                    .header("idempotency-key", format!("journal-burst-{index}"))
                    .body(commands_per_invocation.to_string())
                    .send()
                    .await?;
                if !response.status().is_success() {
                    bail!("journal burst invocation failed with {}", response.status());
                }
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn create_and_trim_snapshots(
    cluster_ctrl: &mut ClusterCtrlSvcClient<tonic::transport::Channel>,
    num_partitions: u16,
) -> anyhow::Result<()> {
    for partition_id in 0..num_partitions {
        cluster_ctrl
            .create_partition_snapshot(CreatePartitionSnapshotRequest {
                partition_id: partition_id.into(),
                min_target_lsn: None,
                trim_log: true,
            })
            .await?;
    }
    Ok(())
}

async fn replacement_recovered(
    cluster_ctrl: &mut ClusterCtrlSvcClient<tonic::transport::Channel>,
    num_partitions: u16,
) -> anyhow::Result<()> {
    loop {
        let cluster_state = cluster_ctrl
            .get_cluster_state(ClusterStateRequest {})
            .await?
            .into_inner()
            .cluster_state
            .ok_or_else(|| anyhow!("cluster controller returned no cluster state"))?;

        let mut applied_lsns_per_partition = vec![Vec::new(); usize::from(num_partitions)];
        for node in cluster_state.nodes.values() {
            let Some(State::Alive(status)) = node.state.as_ref() else {
                continue;
            };
            for partition_id in 0..num_partitions {
                if let Some(lsn) = status
                    .partitions
                    .get(&u32::from(partition_id))
                    .and_then(|partition| partition.last_applied_log_lsn)
                {
                    applied_lsns_per_partition[usize::from(partition_id)].push(lsn.value);
                }
            }
        }

        if applied_lsns_per_partition
            .iter()
            .all(|lsns| lsns.len() == 3 && lsns.iter().all(|lsn| *lsn == lsns[0]))
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
