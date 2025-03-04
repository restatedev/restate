// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytestring::ByteString;
use enumset::EnumSet;
use googletest::prelude::err;
use googletest::{IntoTestResult, assert_that, pat};
use rand::seq::IndexedMutRandom;
use restate_core::metadata_store::{Precondition, WriteError};
use restate_core::{TaskCenter, TaskKind, cancellation_watcher};
use restate_local_cluster_runner::cluster::Cluster;
use restate_local_cluster_runner::node::{BinarySource, HealthCheck, Node};
use restate_metadata_server::create_client;
use restate_metadata_server::tests::Value;
use restate_types::Versioned;
use restate_types::config::{
    Configuration, MetadataClientKind, MetadataClientOptions, MetadataServerKind, RaftOptions,
};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tracing::info;

#[test_log::test(restate_core::test)]
async fn raft_metadata_cluster_smoke_test() -> googletest::Result<()> {
    let base_config = Configuration::default();

    let nodes = Node::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        EnumSet::empty(),
        3,
        true,
    );
    let mut cluster = Cluster::builder()
        .cluster_name("raft_metadata_cluster_smoke_test")
        .nodes(nodes)
        .temp_base_dir()
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let addresses = cluster
        .nodes
        .iter()
        .map(|node| node.node_address().clone())
        .collect();

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
    client
        .put(key.clone(), &value, Precondition::DoesNotExist)
        .await?;

    let stored_value = client.get::<Value>(key.clone()).await?;
    assert_eq!(stored_value, Some(value));

    let stored_value_version = client.get_version(key.clone()).await?;
    assert_eq!(stored_value_version, Some(value_version));

    let new_value = Value::new(1337);
    let new_value_version = new_value.version();
    assert_that!(
        client
            .put(
                key.clone(),
                &new_value,
                Precondition::MatchesVersion(value_version.next()),
            )
            .await,
        err(pat!(WriteError::FailedPrecondition(_)))
    );
    assert_that!(
        client
            .put(key.clone(), &new_value, Precondition::DoesNotExist)
            .await,
        err(pat!(WriteError::FailedPrecondition(_)))
    );

    client
        .put(
            key.clone(),
            &new_value,
            Precondition::MatchesVersion(value_version),
        )
        .await?;
    let stored_new_value = client.get::<Value>(key.clone()).await?;
    assert_eq!(stored_new_value, Some(new_value));

    client
        .delete(key.clone(), Precondition::MatchesVersion(new_value_version))
        .await?;
    assert!(client.get::<Value>(key.clone()).await?.is_none());

    cluster.graceful_shutdown(Duration::from_secs(3)).await?;

    Ok(())
}

#[test_log::test(restate_core::test)]
async fn raft_metadata_cluster_chaos_test() -> googletest::Result<()> {
    let num_nodes = 3;
    let chaos_duration = Duration::from_secs(20);
    let mut base_config = Configuration::default();
    base_config
        .metadata_server
        .set_kind(MetadataServerKind::Raft(RaftOptions {
            raft_election_tick: NonZeroUsize::new(5).expect("5 to be non zero"),
            raft_heartbeat_tick: NonZeroUsize::new(2).expect("2 to be non zero"),
            ..RaftOptions::default()
        }));

    let nodes = Node::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        EnumSet::empty(),
        num_nodes,
        true,
    );
    let mut cluster = Cluster::builder()
        .cluster_name("raft_metadata_cluster_smoke_test")
        .nodes(nodes)
        .temp_base_dir()
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let addresses = cluster
        .nodes
        .iter()
        .map(|node| node.node_address().clone())
        .collect();

    let metadata_store_client_options = MetadataClientOptions {
        kind: MetadataClientKind::Replicated { addresses },
        ..MetadataClientOptions::default()
    };
    let client = create_client(metadata_store_client_options)
        .await
        .expect("to not fail");

    let start_chaos = Instant::now();

    let chaos_handle = TaskCenter::spawn_unmanaged(TaskKind::Background, "chaos", async move {
        let mut shutdown = std::pin::pin!(cancellation_watcher());

        loop {
            let node = cluster
                .nodes
                .choose_mut(&mut rand::rng())
                .expect("at least one node being present");

            tokio::select! {
                _ = &mut shutdown => {
                    break;
                },
                result = node.restart() => {
                    result?;
                    // wait until the cluster is healthy again
                    tokio::select! {
                        _ = &mut shutdown => {
                            break;
                        }
                        result = cluster.wait_check_healthy(HealthCheck::MetadataServer, Duration::from_secs(10)) => {
                            result?;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
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
                }
            }
        }
    }

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

    chaos_handle.cancel();
    let mut cluster = chaos_handle.await?.into_test_result()?;
    cluster.graceful_shutdown(Duration::from_secs(3)).await?;

    Ok(())
}

enum State {
    Write,
    Reconcile,
}
