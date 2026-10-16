// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::large_futures)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::Duration;

use anyhow::{Context, ensure};
use enumset::EnumSet;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::oneshot;
use url::Url;

use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::protobuf::cluster_ctrl_svc::{
    CreatePartitionSnapshotRequest, new_cluster_ctrl_client,
};
use restate_limiter::RuleBook;
use restate_local_cluster_runner::cluster::{Cluster, StartedCluster};
use restate_local_cluster_runner::node::{
    BinarySource, HealthCheck, NodeSpec, StartedNode, TerminationSignal,
};
use restate_types::config::{Configuration, MetadataClientKind, ObjectStoreOptions};
use restate_types::config_loader::ConfigLoaderBuilder;
use restate_types::identifiers::{PartitionId, ServiceId};
use restate_types::logs::metadata::{Logs, NodeSetSize, ProviderConfiguration, ProviderKind};
use restate_types::metadata_store::keys::{BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, RULE_BOOK_KEY};
use restate_types::net::address::{AdvertisedAddress, ListenerPort, PeerNetAddress};
use restate_types::nodes_config::{NodesConfiguration, Role};
use restate_types::partition_table::PartitionTable;
use restate_types::replication::ReplicationProperty;
use restate_types::sharding::WithPartitionKey;

// Authoring contract: the real CLI must produce a directory usable by the unchanged server,
// preserve recovered state, permit new writes after lost metadata, and survive restart. This
// catches bad log bases, stale schema authority, incorrect node generations, and activation order;
// no parser-only test or mocked metadata writer can prove that contract.
#[test_log::test(tokio::test)]
async fn recovery_without_metadata_and_with_missing_partition() -> anyhow::Result<()> {
    let snapshots = TempDir::new()?;
    let url = Url::from_directory_path(snapshots.path()).unwrap();
    let mut source = Fixture::new(url.clone(), None, ObjectStoreOptions::default()).await?;
    // Keep Unix socket paths below the platform limit even for the longer takeover directory.
    let work = TempDir::new_in("/tmp")?;
    let config_path = work.path().join("target.toml");
    // Reusing an operator's config must not reuse a separate source WAL mount.
    let source_wal = work.path().join("source-wal");
    std::fs::create_dir(&source_wal)?;
    std::fs::write(source_wal.join("sentinel"), "untouched")?;
    let mut target = serde_json::to_value(Configuration::new_unix_sockets())?;
    target["log-server"]["rocksdb-wal-dir"] = serde_json::json!(source_wal);
    let target: Configuration = serde_json::from_value(target)?;
    std::fs::write(
        &config_path,
        target.dump().map_err(anyhow::Error::from_boxed)?,
    )?;

    // Make the recorded position deliberately conservative. The imported SST still contains the
    // actual position. Recovery must read it, not start fresh logs at this lower bound.
    let files = files_in(snapshots.path())?;
    for path in files.keys().filter(|p| p.ends_with("metadata.json")) {
        let path = snapshots.path().join(path);
        let mut metadata: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        metadata["min_applied_lsn"] = serde_json::json!(0);
        std::fs::write(path, serde_json::to_vec(&metadata)?)?;
    }
    // A missing latest pointer must not hide otherwise complete immutable snapshots.
    for id in 0..2 {
        std::fs::remove_file(snapshots.path().join(format!("{id}/latest.json")))?;
    }
    source
        .cluster
        .graceful_shutdown(Duration::from_secs(30))
        .await?;
    let original_files = files_in(snapshots.path())?;

    let output = work.path().join("data-only");
    let common = [
        "--config-file",
        config_path.to_str().unwrap(),
        "--controlled-idempotent-sharding",
        "true",
    ];
    let dry_run = run_tool(&url, &output, &common, &["--dry-run"]).await?;
    assert_success(&dry_run)?;
    let plan: serde_json::Value = serde_json::from_slice(&dry_run.stdout)?;
    assert!(!output.exists(), "dry-run created an output directory");
    assert_ne!(
        plan["source_cluster_fingerprint"],
        plan["target_cluster_fingerprint"]
    );
    let bad_count = run_tool(
        &url,
        &output,
        &common,
        &["--num-partitions", "1", "--accept-data-loss"],
    )
    .await?;
    assert!(
        !bad_count.status.success(),
        "under-counted topology was accepted"
    );
    assert!(!output.exists(), "invalid plan wrote target data");
    let selected = original_files
        .keys()
        .find(|path| path.starts_with("0") && path.ends_with("metadata.json"))
        .unwrap();
    let selected = format!("0={}", selected.display());
    assert_success(
        &run_tool(
            &url,
            &output,
            &common,
            &["--snapshot", &selected, "--accept-data-loss"],
        )
        .await?,
    )?;

    let generated = ConfigLoaderBuilder::default()
        .path(Some(output.join("restate.toml")))
        .build()?
        .load_once()?;
    assert_ne!(
        generated.log_server.wal_dir(),
        source_wal,
        "recovery retained the source WAL mount"
    );
    let mut recovered = start_recovered(&output).await?;
    let admin = unix_client(recovered.admin_address().as_ref().unwrap())?;
    let deployments: serde_json::Value = admin
        .get("http://localhost/deployments")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(deployments["deployments"].as_array().unwrap().len(), 0);
    // State is accessible even before customers reconstruct deployment registrations.
    let state = recovered_state(&admin).await?;
    assert_eq!(state["rows"].as_array().unwrap().len(), 2);
    let rules: RuleBook = recovered
        .metadata_client()
        .get(RULE_BOOK_KEY.clone())
        .await?
        .unwrap();
    assert!(
        rules.is_empty(),
        "lost metadata must not leave hidden source rules"
    );
    upsert_rule(&admin, "recovered", false).await?;
    wait_rule_version(&recovered, 1).await?;
    register(&admin, &source.service_url, false).await?;
    let ingress = unix_client(recovered.ingress_address().as_ref().unwrap())?;
    assert_eq!(invoke(&ingress, &source.keys[0], "get", None).await?, 3);
    assert_eq!(invoke(&ingress, &source.keys[1], "get", None).await?, 7);
    assert_eq!(invoke(&ingress, &source.keys[0], "add", Some(1)).await?, 4);
    recovered.restart(TerminationSignal::Sigterm).await?;
    HealthCheck::Worker
        .wait_healthy(&recovered, Duration::from_secs(60))
        .await?;
    assert_eq!(invoke(&ingress, &source.keys[0], "get", None).await?, 4);
    assert_eq!(invoke(&ingress, &source.keys[0], "add", Some(1)).await?, 5);
    recovered.graceful_shutdown(Duration::from_secs(30)).await?;
    // Existing output is refused, including after successful recovery, without changing it.
    assert!(
        !run_tool(&url, &output, &common, &["--accept-data-loss"])
            .await?
            .status
            .success()
    );

    let partial = work.path().join("partial-takeover");
    let mut local_config = Configuration::new_unix_sockets();
    local_config.bifrost.default_provider = ProviderKind::Local;
    std::fs::write(
        &config_path,
        local_config.dump().map_err(anyhow::Error::from_boxed)?,
    )?;
    assert_success(
        &run_tool(
            &url,
            &partial,
            &common,
            &[
                "--num-partitions",
                "2",
                "--skip-partition",
                "1",
                "--schema-from-partition",
                "0",
                "--preserve-cluster-id",
                "--original-cluster-stopped",
                "--accept-data-loss",
            ],
        )
        .await?,
    )?;
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(partial.join("recovery.json"))?)?;
    assert_eq!(
        report["source_cluster_fingerprint"],
        report["target_cluster_fingerprint"]
    );
    assert_eq!(report["empty_partitions"], serde_json::json!([1]));
    let mut recovered = start_recovered(&partial).await?;
    let ingress = unix_client(recovered.ingress_address().as_ref().unwrap())?;
    assert_eq!(invoke(&ingress, &source.keys[0], "get", None).await?, 3);
    assert_eq!(invoke(&ingress, &source.keys[1], "get", None).await?, 0);
    assert_eq!(invoke(&ingress, &source.keys[1], "add", Some(1)).await?, 1);
    recovered.graceful_shutdown(Duration::from_secs(30)).await?;
    // Public CLI boundary guards: accepting data loss must never mean following filesystem
    // traversal from metadata, nor handing off a runnable config for a partial SST download.
    let sentinel = work.path().join("sentinel.sst");
    std::fs::write(&sentinel, "unchanged")?;
    let invalid = snapshots.path().join("0/invalid");
    std::fs::create_dir(&invalid)?;
    let original = original_files
        .iter()
        .find(|(path, _)| path.starts_with("0") && path.ends_with("metadata.json"))
        .unwrap()
        .1;
    let mut malicious: serde_json::Value = serde_json::from_slice(original)?;
    malicious["files"][0]["name"] = serde_json::json!("../../sentinel.sst");
    std::fs::write(
        invalid.join("metadata.json"),
        serde_json::to_vec(&malicious)?,
    )?;
    let unsafe_output = work.path().join("unsafe");
    let failure = run_tool(
        &url,
        &unsafe_output,
        &common,
        &[
            "--snapshot",
            "0=0/invalid/metadata.json",
            "--accept-data-loss",
        ],
    )
    .await?;
    assert!(!failure.status.success());
    assert!(
        String::from_utf8_lossy(&failure.stderr).contains("unsafe snapshot SST filename"),
        "{}",
        String::from_utf8_lossy(&failure.stderr)
    );
    assert!(!unsafe_output.exists());
    assert_eq!(std::fs::read_to_string(sentinel)?, "unchanged");
    // Valid metadata with absent SSTs represents a real interrupted upload / retention race.
    std::fs::write(invalid.join("metadata.json"), original)?;
    let failed_output = work.path().join("partial-download");
    let failure = run_tool(
        &url,
        &failed_output,
        &common,
        &[
            "--snapshot",
            "0=0/invalid/metadata.json",
            "--accept-data-loss",
        ],
    )
    .await?;
    assert!(!failure.status.success());
    let incomplete: serde_json::Value =
        serde_json::from_slice(&std::fs::read(failed_output.join("recovery.json"))?)?;
    assert_eq!(incomplete["complete"], false);
    assert!(!failed_output.join("restate.toml").exists());
    std::fs::remove_file(invalid.join("metadata.json"))?;
    std::fs::remove_dir(invalid)?;
    assert_eq!(
        original_files,
        files_in(snapshots.path())?,
        "recovery modified source snapshots"
    );
    assert_eq!(files_in(&source_wal)?.len(), 1);
    assert_eq!(
        std::fs::read_to_string(source_wal.join("sentinel"))?,
        "untouched"
    );
    Ok(())
}

// This is an additional transport/backend contract, not a duplicate of the file recovery proof:
// real S3 snapshot downloads plus conditional external-metadata writes/read-only source metadata.
#[test_log::test(tokio::test)]
#[ignore = "requires minio in PATH (or MINIO_BIN) and cargo build -p restate-server"]
async fn recovery_from_s3_with_surviving_external_metadata() -> anyhow::Result<()> {
    let minio = Minio::start().await?;
    let options = ObjectStoreOptions {
        aws_region: Some("us-east-1".into()),
        aws_endpoint_url: Some(minio.endpoint.clone()),
        aws_access_key_id: Some("recovery-test".into()),
        aws_secret_access_key: Some("recovery-test-secret".into()),
        aws_allow_http: Some(true),
        ..Default::default()
    };
    let source_backend = external_backend("s3://recovery/source-metadata", &options);
    let url = Url::parse("s3://recovery/snapshots")?;
    let mut source = Fixture::new(url.clone(), Some(source_backend), options.clone()).await?;
    let work = TempDir::new_in("/tmp")?;
    let source_config_path = work.path().join("source.toml");
    std::fs::write(
        &source_config_path,
        source.cluster.nodes[0]
            .config()
            .dump()
            .map_err(anyhow::Error::from_boxed)?,
    )?;
    let options_path = work.path().join("object-store.json");
    std::fs::write(&options_path, serde_json::to_vec(&options)?)?;
    let mut target_config = Configuration::new_unix_sockets();
    target_config.common.metadata_client.kind =
        external_backend("s3://recovery/target-metadata", &options);
    let config_path = work.path().join("target.toml");
    std::fs::write(
        &config_path,
        target_config.dump().map_err(anyhow::Error::from_boxed)?,
    )?;
    let source_store = source.cluster.nodes[0].metadata_client();
    let before: NodesConfiguration = source_store.get(NODES_CONFIG_KEY.clone()).await?.unwrap();
    source
        .cluster
        .graceful_shutdown(Duration::from_secs(30))
        .await?;

    let source_objects = objects_in("s3://recovery/source-metadata", &options).await?;
    let output = work.path().join("external");
    let common = [
        "--config-file",
        config_path.to_str().unwrap(),
        "--object-store-config",
        options_path.to_str().unwrap(),
        "--source-metadata-config",
        source_config_path.to_str().unwrap(),
        "--restore-schema",
    ];
    assert_success(&run_tool(&url, &output, &common, &["--dry-run"]).await?)?;
    assert!(!output.exists());
    assert_success(&run_tool(&url, &output, &common, &["--accept-data-loss"]).await?)?;
    let mut recovered = start_recovered(&output).await?;
    assert!(!recovered.config().has_role(Role::MetadataServer));
    let ingress = unix_client(recovered.ingress_address().as_ref().unwrap())?;
    let rules: RuleBook = recovered
        .metadata_client()
        .get(RULE_BOOK_KEY.clone())
        .await?
        .unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(
        rules.iter().next().unwrap().1.description.as_deref(),
        Some("source-2")
    );
    assert_eq!(invoke(&ingress, &source.keys[0], "get", None).await?, 3);
    assert_eq!(invoke(&ingress, &source.keys[0], "add", Some(1)).await?, 4);
    recovered.graceful_shutdown(Duration::from_secs(30)).await?;
    // Reusing populated external metadata must fail and must not supply a startup config.
    let rejected = work.path().join("rejected");
    let failure = run_tool(&url, &rejected, &common, &["--accept-data-loss"]).await?;
    assert!(!failure.status.success());
    assert!(String::from_utf8_lossy(&failure.stderr).contains("target metadata already contains"));
    assert!(!rejected.join("restate.toml").exists());
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("recovery.json"))?)?;
    assert_eq!(
        report["source_cluster_fingerprint"],
        serde_json::to_value(before.cluster_fingerprint())?
    );
    assert_eq!(
        source_objects,
        objects_in("s3://recovery/source-metadata", &options).await?,
        "source metadata was modified"
    );
    Ok(())
}

// Independent lifecycle contract: all replicas/logs disappear, but a real multinode source can
// still be salvaged in all four metadata/identity combinations. Expected state comes from
// acknowledged requests, not from interpreting the recovery tool's own report or SSTs.
#[test_log::test(tokio::test)]
#[ignore = "requires local MinIO and cargo build -p restate-server; four disaster recovery cases"]
async fn multinode_s3_disaster_recovery_matrix() -> anyhow::Result<()> {
    const PARTITIONS: u16 = 6;
    let minio = Minio::start().await?;
    let options = ObjectStoreOptions {
        aws_region: Some("us-east-1".into()),
        aws_endpoint_url: Some(minio.endpoint.clone()),
        aws_access_key_id: Some("recovery-test".into()),
        aws_secret_access_key: Some("recovery-test-secret".into()),
        aws_allow_http: Some(true),
        ..Default::default()
    };
    let work = TempDir::new_in("/tmp")?;
    let options_path = work.path().join("objects.json");
    std::fs::write(&options_path, serde_json::to_vec(&options)?)?;
    for surviving_metadata in [true, false] {
        let scenario = if surviving_metadata {
            "external"
        } else {
            "raft-lost"
        };
        let source_namespace = format!("s3://recovery/{scenario}/metadata");
        let repository = Url::parse(&format!("s3://recovery/{scenario}/snapshots"))?;
        let metadata = surviving_metadata.then(|| external_backend(&source_namespace, &options));
        let mut source =
            Fixture::new_sized(repository.clone(), metadata, options.clone(), 3, PARTITIONS)
                .await?;
        let original_dir = source.cluster.base_dir().to_owned();
        let before: NodesConfiguration = source.cluster.nodes[0]
            .metadata_client()
            .get(NODES_CONFIG_KEY.clone())
            .await?
            .unwrap();
        let source_config = work.path().join(format!("{scenario}-source.toml"));
        std::fs::write(
            &source_config,
            source.cluster.nodes[0]
                .config()
                .dump()
                .map_err(anyhow::Error::from_boxed)?,
        )?;
        let table =
            PartitionTable::with_equally_sized_partitions(restate_types::Version::MIN, PARTITIONS);
        let counters: Vec<_> = table
            .iter()
            .flat_map(|(id, partition)| {
                (0..10000)
                    .map(|key| format!("stress-{key}"))
                    .filter(|key| {
                        partition
                            .key_range
                            .contains(&ServiceId::new(None, "Counter", key.clone()).partition_key())
                    })
                    .take(4)
                    .map(|key| (u16::from(*id), key))
                    .collect::<Vec<_>>()
            })
            .collect();
        let clients: Vec<_> = source
            .cluster
            .nodes
            .iter()
            .map(|node| unix_client(node.ingress_address().as_ref().unwrap()))
            .collect::<anyhow::Result<_>>()?;
        let mut expected = vec![0; counters.len()];
        // Several concurrent rounds and an older complete generation guard discovery's newest
        // selection; each key has one writer so acknowledged values form an independent oracle.
        counter_round(&clients, &counters, &mut expected).await?;
        let mut ctrl = new_cluster_ctrl_client(
            create_tonic_channel(
                source.cluster.nodes[0].advertised_address().clone(),
                &source.cluster.nodes[0].config().networking,
                DNSResolution::Gai,
            ),
            &source.cluster.nodes[0].config().networking,
        );
        for id in 0..u32::from(PARTITIONS) {
            snapshot(&mut ctrl, id).await?;
        }
        // Exercise a changed source node generation and possible leader failover before the
        // selected generation. Read readiness before issuing non-idempotent writes again.
        source.cluster.nodes[2]
            .restart(TerminationSignal::Sigkill)
            .await?;
        source.cluster.wait_healthy(Duration::from_secs(60)).await?;
        for ((_, key), value) in counters.iter().zip(&expected) {
            assert_eq!(invoke(&clients[0], key, "get", None).await?, *value);
        }
        for _ in 0..3 {
            counter_round(&clients, &counters, &mut expected).await?;
        }
        let mut selected = expected.clone();
        for id in 0..PARTITIONS {
            snapshot(&mut ctrl, u32::from(id)).await?;
            for (i, (partition, _)) in counters.iter().enumerate() {
                if *partition == id {
                    selected[i] = expected[i];
                }
            }
            // Writes cross each snapshot boundary. The selected values intentionally differ
            // across partitions, and all post-cut acknowledged writes must disappear.
            counter_round(&clients, &counters, &mut expected).await?;
        }
        let service_url = source.service_url.clone();
        source.cluster.kill().await?;
        drop(ctrl);
        drop(clients);
        drop(source);
        assert!(
            !original_dir.exists(),
            "original data/log/raft directories survived disaster"
        );
        let original_snapshots = objects_in(repository.as_str(), &options).await?;
        let original_metadata = if surviving_metadata {
            Some(objects_in(&source_namespace, &options).await?)
        } else {
            None
        };
        for preserve in [true, false] {
            let mode = if preserve { "takeover" } else { "new" };
            let output = work.path().join(format!("{scenario}-{mode}"));
            let config_path = work.path().join(format!("{scenario}-{mode}.toml"));
            let mut config = Configuration::new_unix_sockets();
            if surviving_metadata {
                config.common.metadata_client.kind = external_backend(
                    &format!("s3://recovery/{scenario}/{mode}-metadata"),
                    &options,
                );
            }
            std::fs::write(
                &config_path,
                config.dump().map_err(anyhow::Error::from_boxed)?,
            )?;
            let mut args = vec![
                "--config-file",
                config_path.to_str().unwrap(),
                "--object-store-config",
                options_path.to_str().unwrap(),
                "--accept-data-loss",
            ];
            if surviving_metadata {
                args.extend([
                    "--source-metadata-config",
                    source_config.to_str().unwrap(),
                    "--restore-schema",
                ]);
            } else {
                args.extend(["--controlled-idempotent-sharding", "true"]);
            }
            if preserve {
                args.extend(["--preserve-cluster-id", "--original-cluster-stopped"]);
            }
            assert_success(&run_tool(&repository, &output, &[], &args).await?)?;
            let report: serde_json::Value =
                serde_json::from_slice(&std::fs::read(output.join("recovery.json"))?)?;
            assert_eq!(report["complete"], true);
            assert!(
                !output.join("data/recovery/log-store").exists(),
                "recovery copied source log files"
            );
            let mut recovered = start_recovered(&output).await?;
            let metadata = recovered.metadata_client();
            let identity: NodesConfiguration =
                metadata.get(NODES_CONFIG_KEY.clone()).await?.unwrap();
            assert_eq!(
                identity.cluster_fingerprint() == before.cluster_fingerprint(),
                preserve
            );
            assert_eq!(identity.cluster_name() == before.cluster_name(), preserve);
            assert_eq!(
                identity.features(),
                before.features(),
                "cluster-wide routing features changed"
            );
            let logs: Logs = metadata.get(BIFROST_CONFIG_KEY.clone()).await?.unwrap();
            assert_eq!(logs.iter().count(), usize::from(PARTITIONS));
            for (id, chain) in logs.iter() {
                assert_eq!(chain.iter().count(), 1, "old log history was retained");
                let position = report["applied_lsns"][id.to_string()].as_u64().unwrap();
                assert_eq!(u64::from(chain.tail().base_lsn), position + 1);
            }
            let admin = unix_client(recovered.admin_address().as_ref().unwrap())?;
            let state = recovered_state(&admin).await?;
            assert_eq!(
                state["rows"].as_array().unwrap().len(),
                counters.len() + usize::from(PARTITIONS)
            );
            let deployments: serde_json::Value = admin
                .get("http://localhost/deployments")
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            assert_eq!(
                deployments["deployments"].as_array().unwrap().is_empty(),
                !surviving_metadata
            );
            if !surviving_metadata {
                register(&admin, &service_url, false).await?;
            }
            let ingress = unix_client(recovered.ingress_address().as_ref().unwrap())?;
            for ((_, key), value) in counters.iter().zip(&selected) {
                assert_eq!(
                    invoke(&ingress, key, "get", None).await?,
                    *value,
                    "{scenario}/{mode}/{key}"
                );
            }
            let mut after = selected.clone();
            counter_round(std::slice::from_ref(&ingress), &counters, &mut after).await?;
            recovered.restart(TerminationSignal::Sigkill).await?;
            HealthCheck::Worker
                .wait_healthy(&recovered, Duration::from_secs(60))
                .await?;
            for ((_, key), value) in counters.iter().zip(&after) {
                assert_eq!(
                    invoke(&ingress, key, "get", None).await?,
                    *value,
                    "lost new writes on restart"
                );
            }
            counter_round(std::slice::from_ref(&ingress), &counters, &mut after).await?;
            recovered.graceful_shutdown(Duration::from_secs(30)).await?;
            eprintln!(
                "verified {scenario}/{mode}: 3 lost source nodes, {PARTITIONS} partitions, {} counters, new writes and crash restart",
                counters.len()
            );
        }
        assert_eq!(
            original_snapshots,
            objects_in(repository.as_str(), &options).await?,
            "source snapshots changed"
        );
        if let Some(original) = original_metadata {
            assert_eq!(
                original,
                objects_in(&source_namespace, &options).await?,
                "source metadata changed"
            );
        }
    }
    Ok(())
}

async fn counter_round(
    clients: &[reqwest::Client],
    counters: &[(u16, String)],
    expected: &mut [i32],
) -> anyhow::Result<()> {
    let values = futures::future::try_join_all(
        counters
            .iter()
            .enumerate()
            .map(|(i, (_, key))| invoke(&clients[i % clients.len()], key, "add", Some(1))),
    )
    .await?;
    for (expected, actual) in expected.iter_mut().zip(values) {
        *expected += 1;
        assert_eq!(actual, *expected);
    }
    Ok(())
}

struct Fixture {
    cluster: StartedCluster,
    service_url: String,
    keys: Vec<String>,
}

impl Fixture {
    async fn new(
        repository: Url,
        metadata: Option<MetadataClientKind>,
        options: ObjectStoreOptions,
    ) -> anyhow::Result<Self> {
        Self::new_sized(repository, metadata, options, 1, 2).await
    }

    async fn new_sized(
        repository: Url,
        metadata: Option<MetadataClientKind>,
        options: ObjectStoreOptions,
        node_count: u32,
        partition_count: u16,
    ) -> anyhow::Result<Self> {
        let mut config = Configuration::new_unix_sockets();
        config.common.default_num_partitions = partition_count;
        config.bifrost.default_provider = ProviderKind::Replicated;
        config.worker.snapshots.destination = Some(repository.to_string());
        config.worker.snapshots.object_store = options;
        let mut roles = EnumSet::all();
        if let Some(metadata) = metadata {
            config.common.metadata_client.kind = metadata;
            roles.remove(Role::MetadataServer);
        }
        let nodes = NodeSpec::new_test_nodes(
            config.clone(),
            BinarySource::CargoTest,
            roles,
            node_count,
            false,
        );
        let cluster = Cluster::builder()
            .cluster_name("step-zero-source")
            .nodes(nodes)
            .temp_base_dir("recovery_source")
            .build()
            .start()
            .await?;
        cluster.nodes[0]
            .provision_cluster(
                None,
                ReplicationProperty::new_unchecked(node_count.min(2) as u8),
                Some(ProviderConfiguration::from((
                    ProviderKind::Replicated,
                    ReplicationProperty::new_unchecked(node_count.min(2) as u8),
                    NodeSetSize::new(node_count as u16).unwrap(),
                ))),
                EnumSet::empty(),
            )
            .await?;
        cluster.wait_healthy(Duration::from_secs(60)).await?;
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let service_url = format!("http://{}", listener.local_addr()?);
        let (started_tx, started_rx) = oneshot::channel();
        tokio::spawn(async move {
            mock_service_endpoint::listener::run_listener(listener, || {
                let _ = started_tx.send(());
            })
            .await
            .unwrap();
        });
        started_rx.await?;
        let admin = unix_client(cluster.nodes[0].admin_address().as_ref().unwrap())?;
        register(&admin, &service_url, false).await?;
        // High cached schema versions make the lost-metadata path exercise normalization: the
        // recovered registry's first registration must reach workers, not be ignored as stale.
        register(&admin, &service_url, true).await?;
        register(&admin, &service_url, true).await?;
        for i in 0..3 {
            upsert_rule(&admin, &format!("source-{i}"), true).await?;
        }
        let table = PartitionTable::with_equally_sized_partitions(
            restate_types::Version::MIN,
            partition_count,
        );
        let keys: Vec<String> = (0..partition_count)
            .map(|id| {
                (0..100u16)
                    .map(|key| key.to_string())
                    .find(|key| {
                        table
                            .get(&PartitionId::new_unchecked(id))
                            .unwrap()
                            .key_range
                            .contains(&ServiceId::new(None, "Counter", key.clone()).partition_key())
                    })
                    .unwrap()
            })
            .collect();
        let ingress = unix_client(cluster.nodes[0].ingress_address().as_ref().unwrap())?;
        assert_eq!(invoke(&ingress, &keys[0], "add", Some(3)).await?, 3);
        for (id, key) in keys.iter().enumerate().skip(1) {
            let value = 3 + 4 * id as i32;
            assert_eq!(invoke(&ingress, key, "add", Some(value)).await?, value);
        }
        wait_rule_version_for(&cluster.nodes[0], 3, usize::from(partition_count)).await?;
        let mut client = new_cluster_ctrl_client(
            create_tonic_channel(
                cluster.nodes[0].advertised_address().clone(),
                &config.networking,
                DNSResolution::Gai,
            ),
            &config.networking,
        );
        snapshot(&mut client, 0).await?;
        // Snapshots intentionally do not form a common cut, and recovery knowingly loses this
        // later update to partition 0 while retaining partition 1's state.
        assert_eq!(invoke(&ingress, &keys[0], "add", Some(10)).await?, 13);
        for id in 1..u32::from(partition_count) {
            snapshot(&mut client, id).await?;
        }
        Ok(Self {
            cluster,
            service_url,
            keys,
        })
    }
}

async fn snapshot(
    client: &mut restate_core::protobuf::cluster_ctrl_svc::cluster_ctrl_svc_client::ClusterCtrlSvcClient<tonic::transport::Channel>,
    partition_id: u32,
) -> anyhow::Result<()> {
    // The Admin controller learns snapshot-capable nodes through an eventual status refresh,
    // which can lag successful worker invocations at initial startup.
    for attempt in 0..120 {
        match client
            .create_partition_snapshot(CreatePartitionSnapshotRequest {
                partition_id,
                min_target_lsn: None,
                trim_log: false,
            })
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if error.code() == tonic::Code::Internal && attempt < 119 => {
                tokio::time::sleep(Duration::from_millis(250)).await
            }
            Err(error) => return Err(error.into()),
        }
    }
    unreachable!()
}

async fn run_tool(
    repository: &Url,
    output: &Path,
    common: &[&str],
    args: &[&str],
) -> anyhow::Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_restate-doctor"))
        .arg("recover")
        .arg(repository.as_str())
        .arg("--output")
        .arg(output)
        .args(common)
        .args(args)
        .kill_on_drop(true)
        .output()
        .await?)
}

fn assert_success(output: &Output) -> anyhow::Result<()> {
    ensure!(
        output.status.success(),
        "recovery failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

async fn start_recovered(output: &Path) -> anyhow::Result<StartedNode> {
    let config = ConfigLoaderBuilder::default()
        .path(Some(output.join("restate.toml")))
        .build()?
        .load_once()?;
    let roles = config.common.roles;
    let node = NodeSpec::new_test_node("recovery", config, BinarySource::CargoTest, roles)
        .start()
        .await?;
    HealthCheck::Worker
        .wait_healthy(&node, Duration::from_secs(60))
        .await?;
    HealthCheck::Admin
        .wait_healthy(&node, Duration::from_secs(60))
        .await?;
    Ok(node)
}

fn unix_client<P: ListenerPort>(address: &AdvertisedAddress<P>) -> anyhow::Result<reqwest::Client> {
    let PeerNetAddress::Uds(path) = address.clone().into_address()? else {
        anyhow::bail!("expected unix socket")
    };
    Ok(reqwest::Client::builder()
        .unix_socket(path)
        .timeout(Duration::from_secs(10))
        .build()?)
}

async fn register(admin: &reqwest::Client, uri: &str, force: bool) -> anyhow::Result<()> {
    admin
        .post("http://localhost/deployments")
        .json(&serde_json::json!({"uri": uri, "force": force}))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn upsert_rule(
    admin: &reqwest::Client,
    description: &str,
    disabled: bool,
) -> anyhow::Result<()> {
    admin.put("http://localhost/limits/rules").json(&serde_json::json!([{
        "pattern": "*", "limits": {"concurrency": 1}, "disabled": disabled, "description": description,
    }])).send().await?.error_for_status()?;
    Ok(())
}

async fn recovered_state(admin: &reqwest::Client) -> anyhow::Result<serde_json::Value> {
    // Worker health means the role has started, not that every partition store is open yet.
    // Retrying a read across startup is safe; persistent errors still fail this bounded proof.
    for attempt in 0..120 {
        let response = admin.post("http://localhost/query").header("accept", "application/json")
            .json(&serde_json::json!({"query": "SELECT value_utf8 FROM state WHERE service_name = 'Counter'"}))
            .send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if status.is_success() {
            return Ok(serde_json::from_slice(&bytes)?);
        }
        ensure!(
            status.is_server_error() && attempt < 119,
            "state query failed ({status}): {}",
            String::from_utf8_lossy(&bytes)
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    unreachable!()
}

async fn wait_rule_version(node: &StartedNode, version: u32) -> anyhow::Result<()> {
    wait_rule_version_for(node, version, 2).await
}

async fn wait_rule_version_for(
    node: &StartedNode,
    version: u32,
    count: usize,
) -> anyhow::Result<()> {
    let mut client = new_cluster_ctrl_client(
        create_tonic_channel(
            node.advertised_address().clone(),
            &node.config().networking,
            DNSResolution::Gai,
        ),
        &node.config().networking,
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let state = client
                .get_cluster_state(
                    restate_core::protobuf::cluster_ctrl_svc::ClusterStateRequest::default(),
                )
                .await?
                .into_inner()
                .cluster_state;
            if let Some(state) = state {
                let partitions: BTreeMap<_, _> = state
                    .nodes
                    .values()
                    .filter_map(|node| match &node.state {
                        Some(restate_types::protobuf::cluster::node_state::State::Alive(alive)) => {
                            Some(alive)
                        }
                        _ => None,
                    })
                    .flat_map(|alive| alive.partitions.iter())
                    .collect();
                if partitions.len() == count
                    && partitions.values().all(|p| {
                        p.last_applied_rule_book_version
                            .as_ref()
                            .is_some_and(|v| v.value == version)
                    })
                {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .context("limiter rule change did not reach recovered partition processors")?
}

async fn invoke(
    client: &reqwest::Client,
    key: &str,
    handler: &str,
    value: Option<i32>,
) -> anyhow::Result<i32> {
    let url = format!("http://localhost/Counter/{key}/{handler}");
    // Only retry connection/availability errors on READS; retrying non-idempotent add would hide
    // an ambiguous write outcome and could make the expected value meaningless.
    for attempt in 0..60 {
        let mut request = client.post(&url);
        if let Some(value) = value {
            request = request.json(&value);
        }
        let response = request.send().await?;
        if response.status().is_success() {
            return Ok(response.json().await?);
        }
        if value.is_some() || attempt == 59 {
            return Err(response.error_for_status().unwrap_err().into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    unreachable!()
}

fn files_in(root: &Path) -> anyhow::Result<BTreeMap<PathBuf, Vec<u8>>> {
    let mut result = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                result.insert(path.strip_prefix(root)?.to_path_buf(), std::fs::read(path)?);
            }
        }
    }
    Ok(result)
}

fn external_backend(path: &str, options: &ObjectStoreOptions) -> MetadataClientKind {
    let mut value = serde_json::to_value(options).unwrap();
    value["type"] = serde_json::json!("object-store");
    value["path"] = serde_json::json!(path);
    serde_json::from_value(value).unwrap()
}

async fn objects_in(
    url: &str,
    options: &ObjectStoreOptions,
) -> anyhow::Result<BTreeMap<String, Vec<u8>>> {
    let url = Url::parse(url)?;
    let prefix = object_store::path::Path::from(url.path());
    let store = restate_object_store_util::create_object_store_client(
        url,
        options,
        &restate_types::config::SnapshotsOptions::default().object_store_retry_policy,
    )
    .await?;
    let objects = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
    let mut result = BTreeMap::new();
    for object in objects {
        result.insert(
            object.location.to_string(),
            store.get(&object.location).await?.bytes().await?.to_vec(),
        );
    }
    Ok(result)
}

struct Minio {
    _process: tokio::process::Child,
    _data: TempDir,
    endpoint: String,
}

impl Minio {
    async fn start() -> anyhow::Result<Self> {
        let data = TempDir::new()?;
        std::fs::create_dir(data.path().join("recovery"))?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        drop(listener);
        let process = Command::new(std::env::var_os("MINIO_BIN").unwrap_or_else(|| "minio".into()))
            .arg("server")
            .arg(data.path())
            .arg("--address")
            .arg(address.to_string())
            .arg("--console-address")
            .arg("127.0.0.1:0")
            .env("MINIO_ROOT_USER", "recovery-test")
            .env("MINIO_ROOT_PASSWORD", "recovery-test-secret")
            .env("MINIO_BROWSER", "off")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .context("install minio or set MINIO_BIN")?;
        let endpoint = format!("http://{address}");
        let client = reqwest::Client::new();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Ok(response) = client
                    .get(format!("{endpoint}/minio/health/ready"))
                    .send()
                    .await
                    && response.status().is_success()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;
        Ok(Self {
            _process: process,
            _data: data,
            endpoint,
        })
    }
}
