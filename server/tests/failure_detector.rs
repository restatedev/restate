// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, this software will be governed
// by the Apache License, Version 2.0.

#![cfg(unix)]

use std::{num::NonZeroU8, process::Command, time::Duration};

use anyhow::{anyhow, bail};
use enumset::EnumSet;
use futures_util::StreamExt;
use regex::Regex;
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, NodeSpec, TerminationSignal},
};
use restate_types::config::Configuration;
use restate_types::logs::metadata::{NodeSetSize, ProviderConfiguration, ReplicatedLogletConfig};
use restate_types::replication::ReplicationProperty;
use tracing::{info, warn};

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

    let failure_significant_tick = Regex::new(r"intervals_passed: (2[0-9]|[1-9][0-9]{2,})")?;
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
