// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use bytestring::ByteString;
use enumset::enum_set;
use futures_util::never::Never;
use googletest::prelude::err;
use googletest::{IntoTestResult, assert_that, pat};
use parking_lot::Mutex;
use rand::Rng;
use rand::seq::IndexedMutRandom;
use serde::{Deserialize, Serialize};
use spectroscope::{History, Op, SetFullChecker, Validity};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use restate_core::{TaskCenter, TaskKind, cancellation_token};
use restate_local_cluster_runner::cluster::{Cluster, StartedCluster};
use restate_local_cluster_runner::node::{BinarySource, HealthCheck, NodeSpec, TerminationSignal};
use restate_metadata_providers::create_client;
use restate_metadata_server::tests::Value;
use restate_metadata_store::{MetadataStoreClient, WriteError, retry_on_retryable_error};
use restate_types::config::{
    Configuration, MetadataClientKind, MetadataClientOptions, RaftOptions, set_current_config,
};
use restate_types::metadata::Precondition;
use restate_types::nodes_config::Role;
use restate_types::retries::RetryPolicy;
use restate_types::{PlainNodeId, Version, Versioned, flexbuffers_storage_encode_decode};

#[test_log::test(restate_core::test)]
async fn raft_metadata_cluster_smoke_test() -> googletest::Result<()> {
    let base_config = Configuration::new_unix_sockets();

    let nodes = NodeSpec::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::MetadataServer),
        3,
        true,
    );
    let mut cluster = Cluster::builder()
        .cluster_name("raft_metadata_cluster_smoke_test")
        .nodes(nodes)
        .temp_base_dir("raft_metadata_cluster_smoke_test")
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let addresses = cluster
        .nodes
        .iter()
        .map(|node| node.advertised_address().clone())
        .collect();

    // Set a valid configuration with the cluster name for the GrpcMetadataServerClient to pick it up
    let mut configuration = Configuration::default();
    configuration
        .common
        .set_cluster_name(cluster.cluster_name().to_owned());
    set_current_config(configuration);

    let metadata_store_client_options = MetadataClientOptions {
        kind: MetadataClientKind::Replicated { addresses },
        ..MetadataClientOptions::default()
    };
    let client = create_client(metadata_store_client_options)
        .await
        .expect("to not fail");

    let value = Value::new(42);
    let value_version = value.version();
    let key = ByteString::from_static("my-key");

    let retry_policy = RetryPolicy::fixed_delay(Duration::from_millis(100), Some(10));
    // While all metadata servers are members of the cluster, not every server might have fully caught up and
    // therefore does not know about the current leader. In this case, the request can fail and requires
    // a retry.
    retry_on_retryable_error(retry_policy.clone(), || {
        client.put(key.clone(), &value, Precondition::DoesNotExist)
    })
    .await?;

    let stored_value =
        retry_on_retryable_error(retry_policy.clone(), || client.get::<Value>(key.clone())).await?;
    assert_eq!(stored_value, Some(value));

    let stored_value_version =
        retry_on_retryable_error(retry_policy.clone(), || client.get_version(key.clone())).await?;
    assert_eq!(stored_value_version, Some(value_version));

    let new_value = Value::new(1337);
    let new_value_version = new_value.version();
    assert_that!(
        retry_on_retryable_error(retry_policy.clone(), || client.put(
            key.clone(),
            &new_value,
            Precondition::MatchesVersion(value_version.next()),
        ))
        .await
        .map_err(|err| err.into_inner()),
        err(pat!(WriteError::FailedPrecondition(_)))
    );
    assert_that!(
        retry_on_retryable_error(retry_policy.clone(), || client.put(
            key.clone(),
            &new_value,
            Precondition::DoesNotExist
        ))
        .await
        .map_err(|err| err.into_inner()),
        err(pat!(WriteError::FailedPrecondition(_)))
    );

    retry_on_retryable_error(retry_policy.clone(), || {
        client.put(
            key.clone(),
            &new_value,
            Precondition::MatchesVersion(value_version),
        )
    })
    .await?;
    let stored_new_value =
        retry_on_retryable_error(retry_policy.clone(), || client.get::<Value>(key.clone())).await?;
    assert_eq!(stored_new_value, Some(new_value));

    retry_on_retryable_error(retry_policy.clone(), || {
        client.delete(key.clone(), Precondition::MatchesVersion(new_value_version))
    })
    .await?;
    assert!(
        retry_on_retryable_error(retry_policy.clone(), || client.get::<Value>(key.clone()))
            .await?
            .is_none()
    );

    cluster.graceful_shutdown(Duration::from_secs(3)).await?;

    Ok(())
}

#[test_log::test(restate_core::test)]
async fn raft_metadata_cluster_chaos_test() -> googletest::Result<()> {
    let num_nodes = 3;
    let chaos_duration = Duration::from_secs(20);
    let expected_recovery_duration = Duration::from_secs(10);
    let mut base_config = Configuration::new_unix_sockets();
    base_config.metadata_server.set_raft_options(RaftOptions {
        raft_election_tick: NonZeroUsize::new(5).expect("5 to be non zero"),
        raft_heartbeat_tick: NonZeroUsize::new(2).expect("2 to be non zero"),
        ..RaftOptions::default()
    });

    let nodes = NodeSpec::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::MetadataServer),
        num_nodes,
        true,
    );
    let mut cluster = Cluster::builder()
        .cluster_name("raft_metadata_cluster_chaos_test")
        .nodes(nodes)
        .temp_base_dir("raft_metadata_cluster_chaos_test")
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let addresses = cluster
        .nodes
        .iter()
        .map(|node| node.advertised_address().clone())
        .collect();

    // Set a valid configuration with the cluster name for the GrpcMetadataServerClient to pick it up
    let mut configuration = Configuration::default();
    configuration
        .common
        .set_cluster_name(cluster.cluster_name().to_owned());
    set_current_config(configuration);

    let metadata_store_client_options = MetadataClientOptions {
        kind: MetadataClientKind::Replicated { addresses },
        ..MetadataClientOptions::default()
    };
    let client = create_client(metadata_store_client_options)
        .await
        .expect("to not fail");

    let start_chaos = Instant::now();

    let (success_tx, success_rx) = watch::channel(0);

    let chaos_handle = TaskCenter::spawn_unmanaged(TaskKind::TestRunner, "chaos", async move {
        async fn restart_node(
            cluster: &mut StartedCluster,
            mut success_rx: watch::Receiver<i32>,
            expected_recovery_duration: Duration,
        ) -> anyhow::Result<Infallible> {
            loop {
                let node = cluster
                    .nodes
                    .choose_mut(&mut rand::rng())
                    .expect("at least one node being present");

                node.restart(TerminationSignal::random()).await?;
                success_rx.mark_unchanged();
                cluster
                    .wait_check_healthy(HealthCheck::MetadataServer, Duration::from_secs(10))
                    .await?;
                tokio::time::timeout(expected_recovery_duration, success_rx.changed())
                    .await
                    .map_err(|_| {
                        anyhow!("Failed to accept new writes within the expected recovery duration")
                    })??;
            }
        }

        if let Some(result) = cancellation_token()
            .run_until_cancelled(restart_node(
                &mut cluster,
                success_rx,
                expected_recovery_duration,
            ))
            .await
        {
            result?;
        }

        Ok::<_, anyhow::Error>(cluster)
    })?;

    let key = ByteString::from_static("my-key");
    let mut current_version = None;
    let mut next_value = Value::new(1);
    let mut test_state = State::Write;

    info!("Starting the metadata cluster chaos test");

    let mut successes = 0;
    let mut failures = 0;
    let mut last_err = None;
    while start_chaos.elapsed() < chaos_duration {
        match test_state {
            State::Write => {
                let result = client
                    .put(
                        key.clone(),
                        &next_value,
                        current_version
                            .map(Precondition::MatchesVersion)
                            .unwrap_or(Precondition::DoesNotExist),
                    )
                    .await;
                if result.is_err() {
                    failures += 1;
                    last_err = Some(result.err().unwrap());
                    test_state = State::Reconcile;
                } else {
                    successes += 1;
                    success_tx.send_replace(successes);
                    current_version = Some(next_value.version());
                    next_value = Value {
                        value: next_value.value + 1,
                        version: next_value.version.next(),
                    };
                }
            }
            State::Reconcile => {
                let result = client.get::<Value>(key.clone()).await;

                if let Ok(value) = result {
                    // assert that read value is next_value or next_value - 1
                    match value {
                        None => {
                            assert_eq!(current_version, None);
                            assert_eq!(next_value, Value::new(1));
                        }
                        Some(read_value) => {
                            let previous_value = Value {
                                value: next_value.value - 1,
                                version: next_value.version().prev(),
                            };
                            assert!(read_value == next_value || read_value == previous_value);

                            current_version = Some(read_value.version());
                            next_value = Value {
                                value: read_value.value + 1,
                                version: read_value.version.next(),
                            };
                        }
                    }

                    test_state = State::Write;
                } else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    chaos_handle.cancel();
    let mut cluster = chaos_handle.await?.into_test_result()?;

    // make sure that we have written at least some values
    assert!(
        next_value.value > 1,
        "successful writes: {successes} failed writes: {failures}, last error: {}",
        last_err
            .map(|e| e.to_string())
            .unwrap_or_else(|| "none".to_owned())
    );

    info!(
        "Finished metadata cluster chaos test with value: {}",
        next_value.value
    );

    cluster.graceful_shutdown(Duration::from_secs(3)).await?;

    Ok(())
}

enum State {
    Write,
    Reconcile,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SetValue {
    version: Version,
    elements: BTreeSet<u64>,
}

impl Default for SetValue {
    fn default() -> Self {
        Self {
            version: Version::MIN,
            elements: BTreeSet::new(),
        }
    }
}

impl SetValue {
    fn next_version(mut self) -> Self {
        self.version = self.version.next();
        self
    }
}

impl Versioned for SetValue {
    fn version(&self) -> Version {
        self.version
    }
}

flexbuffers_storage_encode_decode!(SetValue);

struct HistoryRecorder {
    history: Mutex<History<u64>>,
    index: AtomicUsize,
    start_time: Instant,
}

impl HistoryRecorder {
    fn new() -> Self {
        Self {
            history: Mutex::new(History::new()),
            index: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }

    fn next_index(&self) -> usize {
        self.index.fetch_add(1, Ordering::SeqCst)
    }

    fn elapsed(&self) -> spectroscope::Timestamp {
        self.start_time.elapsed().into()
    }

    fn add_invoke(&self, process: u64, element: u64) -> usize {
        let idx = self.next_index();
        let op = Op::add_invoke(idx, process, element).at(self.elapsed());
        self.history.lock().push(op);
        idx
    }

    fn add_ok(&self, process: u64, element: u64) {
        let idx = self.next_index();
        let op = Op::add_ok(idx, process, element).at(self.elapsed());
        self.history.lock().push(op);
    }

    fn add_info(&self, process: u64, element: u64) {
        let idx = self.next_index();
        let op = Op::add_info(idx, process, element).at(self.elapsed());
        self.history.lock().push(op);
    }

    fn read_invoke(&self, process: u64) -> usize {
        let idx = self.next_index();
        let op = Op::read_invoke(idx, process).at(self.elapsed());
        self.history.lock().push(op);
        idx
    }

    fn read_ok(&self, process: u64, elements: impl IntoIterator<Item = u64>) {
        let idx = self.next_index();
        let op = Op::read_ok(idx, process, elements).at(self.elapsed());
        self.history.lock().push(op);
    }

    fn read_info(&self, process: u64) {
        let idx = self.next_index();
        let op = Op::read_info(idx, process).at(self.elapsed());
        self.history.lock().push(op);
    }

    fn into_history(self) -> History<u64> {
        self.history.into_inner()
    }
}

async fn run_set_workload(
    client: MetadataStoreClient,
    key: ByteString,
    recorder: Arc<HistoryRecorder>,
    process_id: u64,
    next_element: Arc<AtomicU64>,
    cancel: CancellationToken,
    success_tx: watch::Sender<u64>,
) {
    use rand::SeedableRng;
    let mut rng = rand::rngs::SmallRng::seed_from_u64(process_id);

    while !cancel.is_cancelled() {
        let is_add: bool = rng.random_bool(0.2);

        if is_add {
            let element = next_element.fetch_add(1, Ordering::Relaxed);
            recorder.add_invoke(process_id, element);

            let result = client
                .read_modify_write::<SetValue, _, _>(key.clone(), |value| {
                    let mut set = value.map(|v| v.next_version()).unwrap_or_default();
                    set.elements.insert(element);
                    Ok::<_, Infallible>(set)
                })
                .await;

            match result {
                Ok(_) => {
                    recorder.add_ok(process_id, element);
                    success_tx.send_modify(|v| *v += 1);
                }
                Err(_) => recorder.add_info(process_id, element),
            }
        } else {
            recorder.read_invoke(process_id);

            let result = client.get::<SetValue>(key.clone()).await;

            match result {
                Ok(Some(value)) => {
                    recorder.read_ok(process_id, value.elements);
                    success_tx.send_modify(|v| *v += 1);
                }
                Ok(None) => {
                    recorder.read_ok(process_id, std::iter::empty());
                    success_tx.send_modify(|v| *v += 1);
                }
                Err(_) => recorder.read_info(process_id),
            }
        }
    }
}

#[test_log::test(restate_core::test)]
async fn raft_metadata_cluster_linearizability_test() -> googletest::Result<()> {
    let num_nodes = 3;
    let num_workers = 5;
    let chaos_duration = Duration::from_secs(20);
    let expected_recovery_duration = Duration::from_secs(10);
    let mut base_config = Configuration::new_unix_sockets();
    base_config.metadata_server.set_raft_options(RaftOptions {
        raft_election_tick: NonZeroUsize::new(5).expect("5 to be non zero"),
        raft_heartbeat_tick: NonZeroUsize::new(2).expect("2 to be non zero"),
        ..RaftOptions::default()
    });

    let nodes = NodeSpec::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::MetadataServer),
        num_nodes,
        true,
    );
    let mut cluster = Cluster::builder()
        .cluster_name("raft_metadata_cluster_linearizability_test")
        .nodes(nodes)
        .temp_base_dir("raft_metadata_cluster_linearizability_test")
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let addresses: Vec<_> = cluster
        .nodes
        .iter()
        .map(|node| node.advertised_address().clone())
        .collect();

    let mut configuration = Configuration::default();
    configuration
        .common
        .set_cluster_name(cluster.cluster_name().to_owned());
    set_current_config(configuration);

    let recorder = Arc::new(HistoryRecorder::new());
    let next_element = Arc::new(AtomicU64::new(0));
    let workload_cancel = CancellationToken::new();
    let key = ByteString::from_static("jepsen-set");

    let (success_tx, success_rx) = watch::channel(0u64);

    let mut workload_handles = Vec::new();
    for process_id in 0..num_workers {
        use rand::SeedableRng;
        use rand::seq::SliceRandom;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(process_id as u64);
        let mut shuffled_addresses = addresses.clone();
        shuffled_addresses.shuffle(&mut rng);

        let metadata_store_client_options = MetadataClientOptions {
            kind: MetadataClientKind::Replicated {
                addresses: shuffled_addresses,
            },
            ..MetadataClientOptions::default()
        };
        let client = create_client(metadata_store_client_options)
            .await
            .expect("to not fail");

        let key = key.clone();
        let recorder = Arc::clone(&recorder);
        let next_element = Arc::clone(&next_element);
        let cancel = workload_cancel.clone();
        let success_tx = success_tx.clone();

        let handle =
            TaskCenter::spawn_unmanaged(TaskKind::TestRunner, "set-workload", async move {
                run_set_workload(
                    client,
                    key,
                    recorder,
                    process_id as u64,
                    next_element,
                    cancel,
                    success_tx,
                )
                .await;
                Ok::<_, anyhow::Error>(())
            })?;
        workload_handles.push(handle);
    }

    // Drop the sender so we can detect when all workers complete
    drop(success_tx);

    // Run chaos in background
    let chaos_handle = TaskCenter::spawn_unmanaged(TaskKind::TestRunner, "chaos", async move {
        async fn restart_node(
            cluster: &mut StartedCluster,
            mut success_rx: watch::Receiver<u64>,
            expected_recovery_duration: Duration,
        ) -> anyhow::Result<Infallible> {
            loop {
                let node = cluster
                    .nodes
                    .choose_mut(&mut rand::rng())
                    .expect("at least one node being present");

                node.restart(TerminationSignal::random()).await?;
                success_rx.mark_unchanged();
                cluster
                    .wait_check_healthy(HealthCheck::MetadataServer, Duration::from_secs(10))
                    .await?;
                tokio::time::timeout(expected_recovery_duration, success_rx.changed())
                    .await
                    .map_err(|_| {
                        anyhow!(
                            "Failed to accept new operations within the expected recovery duration"
                        )
                    })??;
            }
        }

        if let Some(result) = cancellation_token()
            .run_until_cancelled(restart_node(
                &mut cluster,
                success_rx,
                expected_recovery_duration,
            ))
            .await
        {
            result?;
        }

        Ok::<_, anyhow::Error>(cluster)
    })?;

    info!("Starting linearizability test with {} workers", num_workers);

    tokio::time::sleep(chaos_duration).await;

    workload_cancel.cancel();
    for handle in workload_handles {
        handle.await?.into_test_result()?;
    }

    chaos_handle.cancel();
    let mut cluster = chaos_handle.await?.into_test_result()?;

    info!(
        "Test duration elapsed. Recorded {} operations.",
        recorder.index.load(Ordering::Relaxed)
    );

    // Verify linearizability
    let history = Arc::try_unwrap(recorder)
        .map_err(|_| anyhow!("all workers should have completed"))
        .into_test_result()?
        .into_history();

    let result = SetFullChecker::linearizable().check(&history);

    info!(
        "Linearizability check: {:?} (stable: {}, lost: {}, stale: {}, never-read: {})",
        result.valid,
        result.stable_count,
        result.lost_count,
        result.stale_count,
        result.never_read_count
    );

    if !result.lost.is_empty() {
        info!("Lost elements: {:?}", result.lost);
    }
    if !result.stale.is_empty() {
        info!("Stale elements: {:?}", result.stale);
    }

    assert_eq!(
        result.valid,
        Validity::Valid,
        "Linearizability violation: {} lost, {} stale. Lost: {:?}, Stale: {:?}",
        result.lost_count,
        result.stale_count,
        result.lost,
        result.stale
    );

    // Ensure we actually did some work
    assert!(
        result.stable_count > 0 || result.never_read_count > 0,
        "No elements were added during the test"
    );

    info!(
        "Linearizability verified: {} elements stable, {} never-read",
        result.stable_count, result.never_read_count
    );

    cluster.graceful_shutdown(Duration::from_secs(3)).await?;

    Ok(())
}

#[test_log::test(restate_core::test)]
async fn raft_metadata_cluster_reconfiguration() -> googletest::Result<()> {
    let num_nodes = 3;
    let test_duration = Duration::from_secs(20);
    let expected_recovery_duration = Duration::from_secs(10);
    let mut base_config = Configuration::new_unix_sockets();
    base_config.metadata_server.set_raft_options(RaftOptions {
        raft_election_tick: NonZeroUsize::new(5).expect("5 to be non zero"),
        raft_heartbeat_tick: NonZeroUsize::new(2).expect("2 to be non zero"),
        ..RaftOptions::default()
    });

    let nodes = NodeSpec::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        // we need to run the admin role to exchange metadata information between nodes, this can
        // be removed once we have gossip support on every node
        enum_set!(Role::MetadataServer | Role::Admin),
        num_nodes,
        true,
    );
    let mut cluster = Cluster::builder()
        .cluster_name("raft_metadata_cluster_reconfiguration")
        .nodes(nodes)
        .temp_base_dir("raft_metadata_cluster_reconfiguration")
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    // Set a valid configuration with the cluster name for the GrpcMetadataServerClient to pick it up
    let mut configuration = Configuration::default();
    configuration
        .common
        .set_cluster_name(cluster.cluster_name().to_owned());
    set_current_config(configuration);

    let addresses = cluster
        .nodes
        .iter()
        .map(|node| node.advertised_address().clone())
        .collect();

    let metadata_store_client_options = MetadataClientOptions {
        kind: MetadataClientKind::Replicated { addresses },
        ..MetadataClientOptions::default()
    };
    let client = create_client(metadata_store_client_options)
        .await
        .expect("to not fail");

    let (status_tx, mut status_rx) = watch::channel(0);

    let read_modify_write_task =
        TaskCenter::spawn_unmanaged(TaskKind::TestRunner, "read-modify-write", async {
            async fn read_modify_write(
                client: MetadataStoreClient,
                status_tx: watch::Sender<u32>,
            ) -> anyhow::Result<Never> {
                let retry_policy = RetryPolicy::exponential(
                    Duration::from_millis(50),
                    2.0,
                    None,
                    Some(Duration::from_secs(1)),
                );
                let lower_bound = AtomicU32::new(0);
                let upper_bound = AtomicU32::new(0);

                loop {
                    retry_on_retryable_error(retry_policy.clone(), || {
                        client.read_modify_write::<Value, _, _>("my_key".into(), |value| {
                            Ok::<_, Infallible>(if let Some(value) = value {
                                // make sure that we don't miss any writes as part of the reconfiguration
                                assert!(
                                    lower_bound.load(Ordering::Relaxed) <= value.value
                                        && value.value <= upper_bound.load(Ordering::Relaxed),
                                    "value should lie within the bounds"
                                );
                                let mut value = value.next_version();
                                value.value += 1;
                                upper_bound.store(value.value, Ordering::Relaxed);
                                value
                            } else {
                                Value::new(lower_bound.load(Ordering::Relaxed))
                            })
                        })
                    })
                    .await?;

                    // write was successful; update the lower bound wrt the upper one
                    let new_value = upper_bound.load(Ordering::Relaxed);
                    lower_bound.store(new_value, Ordering::Relaxed);
                    status_tx.send_replace(new_value);
                }
            }

            if let Some(result) = cancellation_token()
                .run_until_cancelled(read_modify_write(client, status_tx))
                .await
            {
                result?;
            }

            Ok(())
        })?;

    let start_reconfiguration = Instant::now();
    let mut rng = rand::rng();
    let mut successful_reconfigurations = 0;

    let mut step = async || -> anyhow::Result<()> {
        let cluster_status = cluster
            .get_metadata_cluster_status()
            .await
            .ok_or(anyhow!("failed to retrieve the cluster status"))?;

        let (leader, configuration) = cluster_status.into_inner();

        let leader = leader.ok_or(anyhow!("unknown metadata server leader"))?;

        // switch a random node from member to standby and standby to member
        let mut chosen_node = PlainNodeId::from(rng.random_range(1..=num_nodes));

        if configuration.num_members() == 1 && configuration.contains(chosen_node) {
            // we cannot remove the only remaining metadata server from the cluster; choose the next one
            chosen_node = PlainNodeId::from(u32::from(chosen_node) % num_nodes + 1);
        }

        if configuration.contains(chosen_node) {
            // remove metadata member
            info!(
                "Remove node {} from the metadata cluster at leader {}",
                chosen_node, leader
            );
            cluster.nodes[usize::try_from(u32::from(leader)).expect("to fit into usize") - 1]
                .remove_metadata_member(chosen_node)
                .await?;
        } else {
            // add metadata member
            info!("Add node {} to the metadata cluster", chosen_node);
            cluster.nodes[usize::try_from(u32::from(chosen_node)).expect("to fit into usize") - 1]
                .add_as_metadata_member()
                .await?;
        }

        status_rx.mark_unchanged();

        cluster
            .wait_check_healthy(HealthCheck::MetadataServer, expected_recovery_duration)
            .await
            .expect("the cluster to be healthy within the recovery duration");

        // wait for a successful read-modify-write operation
        tokio::time::timeout(expected_recovery_duration, status_rx.changed()).await.expect("we should be able to perform our read-modify-write operation within the recovery duration").expect("the read-modify-write task should not fail");

        Ok(())
    };

    while start_reconfiguration.elapsed() < test_duration {
        if let Err(err) = step().await {
            debug!("Test step failed with {err}. Retrying");
            // let's wait until we obtain the metadata cluster status with a known leader
            tokio::time::sleep(Duration::from_millis(50)).await;
        } else {
            successful_reconfigurations += 1;
        }
    }

    read_modify_write_task.cancel();
    read_modify_write_task.await?.into_test_result()?;

    assert!(
        successful_reconfigurations > 2,
        "We expect successfully reconfigure the metadata cluster"
    );

    // check one last time that all nodes are healthy
    cluster.wait_healthy(expected_recovery_duration).await?;
    cluster.graceful_shutdown(Duration::from_secs(3)).await?;

    Ok(())
}
