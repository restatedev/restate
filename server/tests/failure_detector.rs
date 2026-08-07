// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, this software will be governed
// by the Apache License, Version 2.0.

#![cfg(unix)]

use std::{
    fs::File,
    io::{BufRead, BufReader, Seek, SeekFrom},
    net::SocketAddr,
    num::{NonZeroU8, NonZeroU32},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

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
use restate_types::GenerationalNodeId;
use restate_types::config::{Configuration, NetworkingOptions};
use restate_types::logs::metadata::{
    NodeSetSize, ProviderConfiguration, ProviderKind, ReplicatedLogletConfig,
};
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::address::PeerNetAddress;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::protobuf::cluster::{ReplayStatus, node_state::State};
use restate_types::replication::ReplicationProperty;
use restate_util_time::NonZeroFriendlyDuration;
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
///
/// `RESTATE_FD_GOSSIP_TICK_INTERVAL_MS`, `RESTATE_FD_GOSSIP_FAILURE_THRESHOLD`, and
/// `RESTATE_FD_REPLACEMENT_DEFAULT_THREAD_POOL_SIZE` support controlled A/B runs of detector
/// sensitivity and default-runtime width. `RESTATE_FD_JOURNAL_BURST_INVOCATIONS_PER_PARTITION`
/// and `RESTATE_FD_JOURNAL_BURST_CONCURRENCY` tune the fixed and recovery JournalBurst load.
/// Set `RESTATE_FD_RECOVERY_LOAD_RECORDS_PER_PARTITION` to a non-zero value to keep pressure
/// running through replacement recovery.
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
    let gossip_tick_interval_ms = scale_env("RESTATE_FD_GOSSIP_TICK_INTERVAL_MS", 100);
    let gossip_failure_threshold = scale_env("RESTATE_FD_GOSSIP_FAILURE_THRESHOLD", 10);
    let replacement_default_thread_pool_size =
        scale_env("RESTATE_FD_REPLACEMENT_DEFAULT_THREAD_POOL_SIZE", 1);
    let journal_burst_invocations_per_partition =
        scale_env("RESTATE_FD_JOURNAL_BURST_INVOCATIONS_PER_PARTITION", 4);
    let journal_burst_concurrency = scale_env("RESTATE_FD_JOURNAL_BURST_CONCURRENCY", 8);
    let recovery_load_records_per_partition =
        scale_env_allow_zero("RESTATE_FD_RECOVERY_LOAD_RECORDS_PER_PARTITION", 0);

    let snapshots_dir = TempDir::new()?;
    let mut base_config = Configuration::new_unix_sockets();
    base_config.common.auto_provision = false;
    base_config.common.default_num_partitions = NUM_PARTITIONS;
    base_config.bifrost.default_provider = ProviderKind::Replicated;
    base_config.common.experimental.set_vqueues(false);
    base_config.common.gossip.gossip_tick_interval =
        NonZeroFriendlyDuration::from_millis_unchecked(gossip_tick_interval_ms as u64);
    base_config.common.gossip.gossip_failure_threshold =
        NonZeroU32::new(gossip_failure_threshold.try_into().unwrap_or(u32::MAX))
            .expect("scale_env rejects zero");
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
            node.with_env_var(
                "RESTATE_DEFAULT_THREAD_POOL_SIZE",
                replacement_default_thread_pool_size.to_string(),
            )
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
        "tail",
        NUM_PARTITIONS,
        replay_records_per_partition,
        journal_burst_invocations_per_partition,
        journal_burst_concurrency,
        None,
    )
    .await?;

    let replacement_name = cluster.nodes[2].node_name().to_owned();
    let pre_restart_generation =
        current_node_generation(&cluster.nodes[0], &replacement_name).await?;
    let recovery_targets = tokio::time::timeout(
        recovery_timeout,
        capture_recovery_targets(&mut cluster_ctrl, pre_restart_generation, NUM_PARTITIONS),
    )
    .await
    .map_err(|_| anyhow!("timed out capturing a converged pre-restart recovery target"))??;
    let replacement_db = cluster.base_dir().join(&replacement_name).join("db");
    let replacement_log = cluster
        .base_dir()
        .join(&replacement_name)
        .join("restate.log");
    let pre_load_false_dead_events =
        false_dead_events(&cluster.nodes[2].last_n_lines(20_000).await?);

    let mut recovery_load = (recovery_load_records_per_partition > 0).then(|| {
        let records = recovery_load_records_per_partition;
        let (stop_tx, stop_rx) = oneshot::channel();
        let (first_attempt_tx, first_attempt_rx) = oneshot::channel();
        let ingress_client = ingress_client.clone();
        let task = tokio::spawn(run_continuous_journal_load(
            ingress_client,
            NUM_PARTITIONS,
            records,
            journal_burst_invocations_per_partition,
            journal_burst_concurrency,
            stop_rx,
            first_attempt_tx,
        ));
        (stop_tx, first_attempt_rx, task)
    });

    if let Some((_, first_attempt_rx, _)) = &mut recovery_load {
        tokio::time::timeout(recovery_timeout, first_attempt_rx)
            .await
            .map_err(|_| {
                anyhow!("timed out waiting for recovery JournalBurst load's first request attempt")
            })?
            .map_err(|_| {
                anyhow!("recovery JournalBurst load exited before its first request attempt")
            })?;
    }

    info!(%replacement_name, "Cold recovery phase marker: killing replacement");
    let replacement_killed_at = Instant::now();
    cluster.nodes[2].kill().await?;
    // The runner appends both process lifetimes to this file. Snapshot it while the old process
    // is stopped so later entries can only have been emitted by the fresh replacement.
    let pre_restart_log_false_dead_events =
        false_dead_events(&cluster.nodes[2].last_n_lines(20_000).await?);
    let pre_restart_false_dead_events = new_events_since(
        &pre_restart_log_false_dead_events,
        &pre_load_false_dead_events,
    );
    let post_restart_log_offset = std::fs::metadata(&replacement_log)?.len();
    std::fs::remove_dir_all(&replacement_db).map_err(|err| {
        anyhow!(
            "failed to remove stopped replacement partition store at {}: {err}",
            replacement_db.display()
        )
    })?;
    cluster.nodes[2].restart(TerminationSignal::Sigkill).await?;
    info!(%replacement_name, "Cold recovery phase marker: replacement restarted");
    let replacement_restarted_at = Instant::now();
    let replacement_generation = tokio::time::timeout(
        recovery_timeout,
        wait_for_new_generation(&cluster.nodes[0], &replacement_name, pre_restart_generation),
    )
    .await
    .map_err(|_| anyhow!("timed out waiting for replacement to register a new generation"))??;

    // A replacement with an empty store can reach these post-snapshot LSNs only by importing the
    // snapshot we just made: the preceding log records were trimmed. Use the control-plane state
    // as the assertion; individual import logs are useful diagnostics but are not a stable API.
    tokio::time::timeout(
        recovery_timeout,
        replacement_recovered(&mut cluster_ctrl, replacement_generation, &recovery_targets),
    )
    .await
    .map_err(|_| anyhow!("timed out waiting for replacement processors to recover"))??;
    let replacement_recovered_at = Instant::now();
    info!(%replacement_name, "Cold recovery phase marker: replacement recovered");

    let recovery_load_waves = if let Some((stop_tx, _, task)) = recovery_load.take() {
        let _ = stop_tx.send(());
        task.await
            .map_err(|err| anyhow!("recovery JournalBurst load task failed: {err}"))??
    } else {
        Vec::new()
    };
    let recovery_load_attempted_overlapping_waves: Vec<_> = recovery_load_waves
        .iter()
        .filter(|wave| {
            wave.started_at <= replacement_recovered_at
                && wave.ended_at >= replacement_killed_at
                && wave.requests_attempted > 0
        })
        .collect();
    let recovery_load_successful_responses_during_recovery = recovery_load_waves
        .iter()
        .map(|wave| {
            instants_between(
                &wave.successful_response_completions,
                replacement_killed_at,
                replacement_recovered_at,
            )
        })
        .sum::<usize>();
    if recovery_load_records_per_partition > 0
        && recovery_load_successful_responses_during_recovery == 0
    {
        bail!(
            "recovery JournalBurst load did not receive a successful response while the replacement was down or recovering"
        );
    }

    let recovery_lines = cluster.nodes[2].last_n_lines(20_000).await?;
    let post_restart_false_dead_events =
        false_dead_events_after_offset(&replacement_log, post_restart_log_offset)?;
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
    mock_service.abort();
    cluster.graceful_shutdown(Duration::from_secs(10)).await?;

    info!(
        imported_snapshots,
        blob_keys_per_partition,
        blob_size_bytes,
        replay_records_per_partition,
        journal_burst_invocations_per_partition,
        journal_burst_concurrency,
        recovery_load_records_per_partition,
        recovery_load_started_waves = recovery_load_waves.len(),
        recovery_load_completed_waves = recovery_load_waves.iter().filter(|wave| !wave.cancelled).count(),
        recovery_load_cancelled_waves = recovery_load_waves.iter().filter(|wave| wave.cancelled).count(),
        recovery_load_attempted_requests = recovery_load_waves.iter().map(|wave| wave.requests_attempted).sum::<usize>(),
        recovery_load_successful_responses = recovery_load_waves.iter().map(|wave| wave.successful_response_completions.len()).sum::<usize>(),
        recovery_load_failed_requests = recovery_load_waves.iter().map(|wave| wave.failed_requests).sum::<usize>(),
        recovery_load_failure_samples = ?recovery_load_failure_samples(&recovery_load_waves),
        recovery_load_attempted_overlapping_waves = recovery_load_attempted_overlapping_waves.len(),
        recovery_load_attempted_requests_in_overlapping_waves = recovery_load_attempted_overlapping_waves.iter().map(|wave| wave.requests_attempted).sum::<usize>(),
        recovery_load_successful_responses_during_recovery,
        replacement_killed_to_restarted = ?replacement_restarted_at.duration_since(replacement_killed_at),
        replacement_restarted_to_recovered = ?replacement_recovered_at.duration_since(replacement_restarted_at),
        gossip_tick_interval_ms,
        gossip_failure_threshold,
        replacement_default_thread_pool_size,
        ?recovery_timeout,
        pre_restart_false_dead_events = ?pre_restart_false_dead_events,
        post_restart_false_dead_events = ?post_restart_false_dead_events,
        failure_significant_ticks = ?failure_significant_ticks,
        slow_scheduler_passes = ?slow_scheduler_passes,
        "Completed cold replacement snapshot-recovery probe"
    );
    Ok(())
}

fn scale_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(default)
}

fn scale_env_allow_zero(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn false_dead_events(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .filter(|line| line.contains(" transitioned from Alive to Dead"))
        .cloned()
        .collect()
}

fn new_events_since(events: &[String], baseline: &[String]) -> Vec<String> {
    let mut remaining_baseline = baseline.to_vec();
    events
        .iter()
        .filter_map(|event| {
            remaining_baseline
                .iter()
                .position(|baseline_event| baseline_event == event)
                .map(|index| remaining_baseline.swap_remove(index))
                .is_none()
                .then(|| event.clone())
        })
        .collect()
}

fn instants_between(instants: &[Instant], start: Instant, end: Instant) -> usize {
    instants
        .iter()
        .filter(|instant| start <= **instant && **instant <= end)
        .count()
}

fn false_dead_events_after_offset(
    log_path: &std::path::Path,
    offset: u64,
) -> anyhow::Result<Vec<String>> {
    let mut reader = BufReader::new(File::open(log_path)?);
    reader.seek(SeekFrom::Start(offset))?;
    let events = reader
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.contains(" transitioned from Alive to Dead") => Some(Ok(line)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    Ok(events)
}

#[test]
fn false_dead_events_after_offset_keeps_repeated_events() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let log = dir.path().join("restate.log");
    let old = "old transitioned from Alive to Dead\n";
    std::fs::write(&log, old)?;
    let offset = std::fs::metadata(&log)?.len();
    let repeated = "new transitioned from Alive to Dead\n";
    std::fs::write(&log, format!("{old}{repeated}{repeated}"))?;

    assert_eq!(
        false_dead_events_after_offset(&log, offset)?,
        vec![
            repeated.trim_end().to_owned(),
            repeated.trim_end().to_owned()
        ]
    );
    Ok(())
}

#[test]
fn new_events_since_keeps_event_multiplicity() {
    let event = "node transitioned from Alive to Dead".to_owned();
    assert_eq!(
        new_events_since(
            &[event.clone(), event.clone(), event.clone()],
            &[event.clone(), event]
        ),
        vec!["node transitioned from Alive to Dead".to_owned()]
    );
}

#[test]
fn instants_between_includes_interval_boundaries() {
    let start = Instant::now();
    let end = start + Duration::from_millis(2);
    let before = start.checked_sub(Duration::from_millis(1)).unwrap();
    let middle = start + Duration::from_millis(1);
    let after = end + Duration::from_millis(1);

    assert_eq!(
        instants_between(&[before, start, middle, end, after], start, end),
        3
    );
}

#[test]
fn per_partition_maxima_uses_the_most_advanced_active_replica_per_partition() {
    assert_eq!(
        per_partition_maxima(&[vec![101, 202, 303], vec![111, 201, 333], vec![99, 222, 300]]),
        Some(vec![111, 222, 333])
    );
}

#[test]
fn per_partition_maxima_rejects_inconsistent_partition_sets() {
    assert_eq!(per_partition_maxima(&[]), None);
    assert_eq!(per_partition_maxima(&[vec![101, 202], vec![111]]), None);
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
    prefix: &str,
    num_partitions: u16,
    records_per_partition: usize,
    invocations_per_partition: usize,
    concurrency: usize,
    request_tracker: Option<RequestTracker>,
) -> anyhow::Result<()> {
    let invocation_count = usize::from(num_partitions) * invocations_per_partition;
    let commands_per_invocation =
        (usize::from(num_partitions) * records_per_partition).div_ceil(invocation_count);
    let results = futures_util::stream::iter(0..invocation_count)
        .map(|index| {
            let ingress_client = ingress_client.clone();
            let request_tracker = request_tracker.clone();
            let url = format!("http://localhost/JournalBurst/{prefix}-{index}/write");
            let idempotency_key = format!("journal-burst-{prefix}-{index}");
            async move {
                let request = ingress_client
                    .post(url)
                    .header("content-type", "application/json")
                    .header("idempotency-key", idempotency_key)
                    .body(commands_per_invocation.to_string());
                if let Some(request_tracker) = &request_tracker {
                    request_tracker.request_attempted();
                }
                let response = request.send().await?;
                if !response.status().is_success() {
                    bail!("journal burst invocation failed with {}", response.status());
                }
                if let Some(request_tracker) = &request_tracker {
                    request_tracker.response_succeeded();
                }
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    if let Some(request_tracker) = request_tracker {
        for err in results.into_iter().filter_map(Result::err) {
            request_tracker.request_failed(err);
        }
    } else {
        results.into_iter().collect::<anyhow::Result<Vec<_>>>()?;
    }
    Ok(())
}

#[derive(Debug)]
struct RecoveryLoadWave {
    started_at: Instant,
    ended_at: Instant,
    cancelled: bool,
    requests_attempted: usize,
    successful_response_completions: Vec<Instant>,
    failed_requests: usize,
    failure_samples: Vec<String>,
}

#[derive(Clone)]
struct RequestTracker {
    requests_attempted: Arc<AtomicUsize>,
    successful_response_completions: Arc<Mutex<Vec<Instant>>>,
    failures: Arc<Mutex<RequestFailures>>,
    first_attempt_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[derive(Default)]
struct RequestFailures {
    count: usize,
    samples: Vec<String>,
}

impl RequestTracker {
    fn new(first_attempt_tx: Option<oneshot::Sender<()>>) -> Self {
        Self {
            requests_attempted: Arc::new(AtomicUsize::new(0)),
            successful_response_completions: Arc::new(Mutex::new(Vec::new())),
            failures: Arc::new(Mutex::new(RequestFailures::default())),
            first_attempt_tx: Arc::new(Mutex::new(first_attempt_tx)),
        }
    }

    fn request_attempted(&self) {
        self.requests_attempted.fetch_add(1, Ordering::Relaxed);
        if let Some(first_attempt_tx) = self
            .first_attempt_tx
            .lock()
            .expect("request tracker mutex should not be poisoned")
            .take()
        {
            let _ = first_attempt_tx.send(());
        }
    }

    fn response_succeeded(&self) {
        self.successful_response_completions
            .lock()
            .expect("request tracker mutex should not be poisoned")
            .push(Instant::now());
    }

    fn request_failed(&self, err: anyhow::Error) {
        const MAX_SAMPLES: usize = 3;

        let mut failures = self
            .failures
            .lock()
            .expect("request tracker mutex should not be poisoned");
        failures.count += 1;
        if failures.samples.len() < MAX_SAMPLES {
            failures.samples.push(err.to_string());
        }
    }

    fn requests_attempted(&self) -> usize {
        self.requests_attempted.load(Ordering::Relaxed)
    }

    fn successful_response_completions(&self) -> Vec<Instant> {
        self.successful_response_completions
            .lock()
            .expect("request tracker mutex should not be poisoned")
            .clone()
    }

    fn failures(&self) -> RequestFailures {
        let failures = self
            .failures
            .lock()
            .expect("request tracker mutex should not be poisoned");
        RequestFailures {
            count: failures.count,
            samples: failures.samples.clone(),
        }
    }
}

async fn run_continuous_journal_load(
    ingress_client: reqwest::Client,
    num_partitions: u16,
    records_per_partition: usize,
    invocations_per_partition: usize,
    concurrency: usize,
    mut stop_rx: oneshot::Receiver<()>,
    first_attempt_tx: oneshot::Sender<()>,
) -> anyhow::Result<Vec<RecoveryLoadWave>> {
    let mut waves = Vec::new();
    let mut first_attempt_tx = Some(first_attempt_tx);

    for wave in 0usize.. {
        let started_at = Instant::now();
        let prefix = format!("recovery-wave-{wave}");
        let request_tracker = RequestTracker::new(first_attempt_tx.take());
        tokio::select! {
            _ = &mut stop_rx => {
                let failures = request_tracker.failures();
                waves.push(RecoveryLoadWave {
                    started_at,
                    ended_at: Instant::now(),
                    cancelled: true,
                    requests_attempted: request_tracker.requests_attempted(),
                    successful_response_completions: request_tracker.successful_response_completions(),
                    failed_requests: failures.count,
                    failure_samples: failures.samples,
                });
                return Ok(waves);
            }
            result = write_journal_bursts(
                &ingress_client,
                &prefix,
                num_partitions,
                records_per_partition,
                invocations_per_partition,
                concurrency,
                Some(request_tracker.clone()),
            ) => {
                result?;
                let failures = request_tracker.failures();
                waves.push(RecoveryLoadWave {
                    started_at,
                    ended_at: Instant::now(),
                    cancelled: false,
                    requests_attempted: request_tracker.requests_attempted(),
                    successful_response_completions: request_tracker.successful_response_completions(),
                    failed_requests: failures.count,
                    failure_samples: failures.samples,
                });
            }
        }
    }

    unreachable!("unbounded recovery load exits only through cancellation")
}

fn recovery_load_failure_samples(waves: &[RecoveryLoadWave]) -> Vec<String> {
    const MAX_SAMPLES: usize = 5;

    waves
        .iter()
        .flat_map(|wave| wave.failure_samples.iter().cloned())
        .take(MAX_SAMPLES)
        .collect()
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

async fn current_node_generation(
    node: &restate_local_cluster_runner::node::StartedNode,
    node_name: &str,
) -> anyhow::Result<GenerationalNodeId> {
    let nodes_config = node
        .metadata_client()
        .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone())
        .await?
        .ok_or_else(|| anyhow!("nodes configuration was not available"))?;
    nodes_config
        .find_node_by_name(node_name)
        .map(|node| node.current_generation)
        .ok_or_else(|| anyhow!("replacement node {node_name} was absent from nodes configuration"))
}

async fn wait_for_new_generation(
    node: &restate_local_cluster_runner::node::StartedNode,
    node_name: &str,
    previous_generation: GenerationalNodeId,
) -> anyhow::Result<GenerationalNodeId> {
    let mut logged_transient_read_error = false;
    loop {
        match current_node_generation(node, node_name).await {
            Ok(generation) if generation.is_newer_than(previous_generation) => {
                return Ok(generation);
            }
            Ok(_) => {}
            Err(err) if is_retryable_control_plane_read_error(&err) => {
                if !logged_transient_read_error {
                    warn!(%err, %node_name, "Retrying replacement generation read during control-plane churn");
                    logged_transient_read_error = true;
                }
            }
            Err(err) => return Err(err),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn is_retryable_control_plane_read_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(|status| status.code() == tonic::Code::Unavailable)
            || cause
                .to_string()
                .to_ascii_lowercase()
                .contains("service unavailable")
    })
}

async fn capture_recovery_targets(
    cluster_ctrl: &mut ClusterCtrlSvcClient<tonic::transport::Channel>,
    replacement_generation: GenerationalNodeId,
    num_partitions: u16,
) -> anyhow::Result<Vec<u64>> {
    let mut logged_transient_read_error = false;

    let targets = loop {
        let Some(cluster_state) = read_cluster_state_or_retry(
            cluster_ctrl,
            &mut logged_transient_read_error,
            "capturing pre-restart recovery targets",
        )
        .await?
        else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };

        if active_partition_lsns(&cluster_state, replacement_generation, num_partitions).is_none() {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }

        let Some(targets) = max_active_replica_lsns(&cluster_state, num_partitions) else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        break targets;
    };

    loop {
        let Some(cluster_state) = read_cluster_state_or_retry(
            cluster_ctrl,
            &mut logged_transient_read_error,
            "waiting for pre-restart recovery target convergence",
        )
        .await?
        else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };

        if active_partition_lsns(&cluster_state, replacement_generation, num_partitions).is_some()
            && all_active_replicas_reached(&cluster_state, &targets, num_partitions)
        {
            return Ok(targets);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn replacement_recovered(
    cluster_ctrl: &mut ClusterCtrlSvcClient<tonic::transport::Channel>,
    replacement_generation: GenerationalNodeId,
    targets: &[u64],
) -> anyhow::Result<()> {
    let mut logged_transient_read_error = false;

    loop {
        let Some(cluster_state) = read_cluster_state_or_retry(
            cluster_ctrl,
            &mut logged_transient_read_error,
            "waiting for replacement recovery",
        )
        .await?
        else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };

        if active_partition_lsns(&cluster_state, replacement_generation, targets.len() as u16)
            .is_some_and(|lsns| {
                lsns.iter()
                    .zip(targets)
                    .all(|(applied_lsn, target)| applied_lsn >= target)
            })
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn read_cluster_state_or_retry(
    cluster_ctrl: &mut ClusterCtrlSvcClient<tonic::transport::Channel>,
    logged_transient_read_error: &mut bool,
    operation: &'static str,
) -> anyhow::Result<Option<restate_types::protobuf::cluster::ClusterState>> {
    match cluster_ctrl.get_cluster_state(ClusterStateRequest {}).await {
        Ok(response) => Ok(response.into_inner().cluster_state),
        Err(err) => {
            let err = anyhow::Error::from(err);
            if is_retryable_control_plane_read_error(&err) {
                if !*logged_transient_read_error {
                    warn!(%err, %operation, "Retrying cluster-state read during control-plane churn");
                    *logged_transient_read_error = true;
                }
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}

fn max_active_replica_lsns(
    cluster_state: &restate_types::protobuf::cluster::ClusterState,
    num_partitions: u16,
) -> Option<Vec<u64>> {
    let replica_lsns: Vec<_> = cluster_state
        .nodes
        .values()
        .map(|node| {
            let State::Alive(status) = node.state.as_ref()? else {
                return None;
            };
            active_partition_lsns_from_status(status, num_partitions)
        })
        .collect::<Option<_>>()?;
    (replica_lsns.len() == 3)
        .then(|| per_partition_maxima(&replica_lsns))
        .flatten()
}

fn all_active_replicas_reached(
    cluster_state: &restate_types::protobuf::cluster::ClusterState,
    targets: &[u64],
    num_partitions: u16,
) -> bool {
    cluster_state.nodes.len() == 3
        && targets.len() == usize::from(num_partitions)
        && cluster_state.nodes.values().all(|node| {
            let Some(State::Alive(status)) = node.state.as_ref() else {
                return false;
            };
            active_partition_lsns_from_status(status, num_partitions)
                .is_some_and(|lsns| lsns.iter().zip(targets).all(|(lsn, target)| lsn >= target))
        })
}

fn per_partition_maxima(replica_lsns: &[Vec<u64>]) -> Option<Vec<u64>> {
    let (first, rest) = replica_lsns.split_first()?;
    let mut maxima = first.clone();
    for replica in rest {
        if replica.len() != maxima.len() {
            return None;
        }
        for (maximum, lsn) in maxima.iter_mut().zip(replica) {
            *maximum = (*maximum).max(*lsn);
        }
    }
    Some(maxima)
}

fn active_partition_lsns(
    cluster_state: &restate_types::protobuf::cluster::ClusterState,
    expected_generation: GenerationalNodeId,
    num_partitions: u16,
) -> Option<Vec<u64>> {
    let node_id = u32::from(expected_generation.as_plain());
    let State::Alive(status) = cluster_state.nodes.get(&node_id)?.state.as_ref()? else {
        return None;
    };
    if status
        .generational_node_id
        .as_ref()
        .map(|generation| GenerationalNodeId::from(generation.clone()))
        != Some(expected_generation)
    {
        return None;
    }

    active_partition_lsns_from_status(status, num_partitions)
}

fn active_partition_lsns_from_status(
    status: &restate_types::protobuf::cluster::AliveNode,
    num_partitions: u16,
) -> Option<Vec<u64>> {
    (0..num_partitions)
        .map(|partition_id| {
            let partition = status.partitions.get(&u32::from(partition_id))?;
            (ReplayStatus::try_from(partition.replay_status).ok()? == ReplayStatus::Active)
                .then_some(partition.last_applied_log_lsn?.value)
        })
        .collect()
}
