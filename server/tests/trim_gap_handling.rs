// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::time::Duration;

use enumset::enum_set;
use futures_util::StreamExt;
use googletest::{IntoTestResult, fail};
use restate_core::protobuf::cluster_ctrl_svc::{
    GetClusterConfigurationRequest, SetClusterConfigurationRequest,
};
use restate_types::logs::metadata::{NodeSetSize, ProviderConfiguration, ReplicatedLogletConfig};
use restate_types::replication::ReplicationProperty;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::try_join;
use tonic::transport::Channel;
use tracing::info;
use url::Url;

use restate_core::network::net_util::create_tonic_channel;
use restate_core::protobuf::cluster_ctrl_svc::{
    ClusterStateRequest, CreatePartitionSnapshotRequest,
    cluster_ctrl_svc_client::ClusterCtrlSvcClient, new_cluster_ctrl_client,
};
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
};
use restate_types::config::{LogFormat, MetadataClientKind, NetworkingOptions};
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::ProviderKind::Replicated;
use restate_types::protobuf::cluster::RunMode;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::retries::RetryPolicy;
use restate_types::{config::Configuration, nodes_config::Role};

mod common;

#[test_log::test(tokio::test)]
async fn fast_forward_over_trim_gap() -> googletest::Result<()> {
    let mut base_config = Configuration::default();
    base_config.common.default_num_partitions = 1.try_into()?;
    base_config.bifrost.default_provider = Replicated;
    base_config.common.log_filter = "restate=debug,warn".to_owned();
    base_config.common.log_format = LogFormat::Compact;
    base_config.common.log_disable_ansi_codes = true;

    let no_snapshot_repository_config = base_config.clone();

    let snapshots_dir = TempDir::new()?;
    base_config.worker.snapshots.destination = Some(
        Url::from_file_path(snapshots_dir.path())
            .unwrap()
            .to_string(),
    );

    let nodes = Node::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::MetadataServer | Role::Admin | Role::Worker | Role::LogServer),
        1,
        false,
    );

    let mut cluster = Cluster::builder()
        .cluster_name("fast-forward-over-trim-gap")
        .nodes(nodes)
        .temp_base_dir("fast_forward_over_trim_gap")
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

    let mut client = new_cluster_ctrl_client(create_tonic_channel(
        cluster.nodes[0].node_address().clone(),
        &NetworkingOptions::default(),
    ));

    info!("Waiting until the partition processor has become the leader");
    any_partition_active(&mut client).await?;

    let (running_tx, running_rx) = oneshot::channel();

    let addr: SocketAddr = "127.0.0.1:9080".parse()?;
    tokio::spawn(async move {
        info!("Starting mock service on http://{}", addr);
        if let Err(e) = mock_service_endpoint::listener::run_listener(addr, || {
            let _ = running_tx.send(());
        })
        .await
        {
            panic!("Error running listener: {e:?}");
        }
    });

    // await that the mock service endpoint is running
    running_rx.await?;

    let http_client = reqwest::Client::new();
    let registration_response = http_client
        .post(format!(
            "http://{}/deployments",
            worker_1.config().admin.bind_address
        ))
        .header("content-type", "application/json")
        .json(&serde_json::json!({ "uri": "http://localhost:9080" }))
        .send()
        .await?;
    assert!(registration_response.status().is_success());

    let ingress_url = format!(
        "http://{}/Counter/0/get",
        worker_1.config().ingress.bind_address
    );

    info!("Send Counter/0/get request to node-1");
    // It takes a little bit for the service to become available for invocations
    let mut retry = RetryPolicy::fixed_delay(Duration::from_millis(500), None).into_iter();
    loop {
        let invoke_response = http_client.post(ingress_url.clone()).send().await?;
        if invoke_response.status().is_success() {
            break;
        }
        if let Some(delay) = retry.next() {
            tokio::time::sleep(delay).await;
        } else {
            fail!("Failed to invoke worker")?;
        }
    }

    let snapshot_response = client
        .create_partition_snapshot(CreatePartitionSnapshotRequest {
            partition_id: 0,
            min_target_lsn: Some(3),
            trim_log: true,
        })
        .await?
        .into_inner();
    info!(
        "Snapshot created including changes up to LSN {}",
        snapshot_response.min_applied_lsn
    );

    let mut worker_2 = Node::new_test_node(
        "node-2",
        no_snapshot_repository_config,
        BinarySource::CargoTest,
        enum_set!(Role::Worker),
    );
    *worker_2.metadata_store_client_mut() = MetadataClientKind::Replicated {
        addresses: vec![cluster.nodes[0].node_address().clone()],
    };

    let mut trim_gap_encountered =
        worker_2.lines("Partition processor stopped due to a log trim gap, and no snapshot repository is configured".parse()?);
    let mut joined_cluster = worker_2.lines("My Node ID is".parse()?);

    let mut worker_2 = worker_2
        .start_clustered(cluster.base_dir(), cluster.cluster_name())
        .await?;

    info!("Waiting until node-2 has joined the cluster");
    assert!(
        joined_cluster.next().await.is_some(),
        "node-2 should join the cluster"
    );

    let mut current_cluster_config = client
        .get_cluster_configuration(GetClusterConfigurationRequest {})
        .await
        .unwrap()
        .into_inner();

    // reconfigure partition replication to include node-2
    current_cluster_config
        .cluster_configuration
        .as_mut()
        .unwrap()
        .partition_replication = Some(ReplicationProperty::new_unchecked(2).into());

    client
        .set_cluster_configuration(SetClusterConfigurationRequest {
            cluster_configuration: Some(current_cluster_config.cluster_configuration.unwrap()),
        })
        .await
        .unwrap();

    info!("Waiting for partition processor to encounter log trim gap");
    assert!(
        trim_gap_encountered.next().await.is_some(),
        "Trim gap was never encountered"
    );
    worker_2.graceful_shutdown(Duration::from_secs(30)).await?;

    info!("Re-starting additional node with snapshot repository configured");
    let mut worker_2 = Node::new_test_node(
        "node-2",
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Worker),
    );
    *worker_2.metadata_store_client_mut() = MetadataClientKind::Replicated {
        addresses: vec![cluster.nodes[0].node_address().clone()],
    };

    let ingress_url = format!(
        "http://{}/Counter/0/get",
        worker_2.config().ingress.bind_address
    );
    let mut worker_2_imported_snapshot = worker_2.lines(
        format!(
            "Importing partition store snapshot.*{}",
            snapshot_response.snapshot_id
        )
        .parse()?,
    );
    let mut worker_2 = worker_2
        .start_clustered(cluster.base_dir(), cluster.cluster_name())
        .await?;

    assert!(
        worker_2_imported_snapshot.next().await.is_some(),
        "Importing partition store snapshot never happened"
    );

    info!("Send Counter/0/get request to node-2");
    // todo(pavel): promote node 2 to be the leader for partition 0 and invoke the service again
    //  right now, all we are asserting is that the new node is applying newly appended log records
    assert!(
        http_client
            .post(ingress_url)
            .send()
            .await?
            .status()
            .is_success()
    );

    applied_lsn_converged(&mut client, 2, PartitionId::from(0)).await?;

    try_join!(
        worker_2.graceful_shutdown(Duration::from_secs(10)),
        cluster.graceful_shutdown(Duration::from_secs(10))
    )?;

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

async fn applied_lsn_converged(
    client: &mut ClusterCtrlSvcClient<Channel>,
    expected_processors: usize,
    partition_id: PartitionId,
) -> googletest::Result<()> {
    assert!(expected_processors > 0);
    info!(
        "Waiting for {} partition processors to converge on the same applied LSN",
        expected_processors
    );
    loop {
        let cluster_state = client
            .get_cluster_state(ClusterStateRequest {})
            .await?
            .into_inner()
            .cluster_state
            .unwrap();

        let applied_lsn: Vec<_> = cluster_state
            .nodes
            .values()
            .filter_map(|n| {
                n.state
                    .as_ref()
                    .map(|s| match s {
                        State::Alive(s) => s
                            .partitions
                            .get(&partition_id.into())
                            .map(|p| p.last_applied_log_lsn)
                            .unwrap_or_default()
                            .map(|lsn| (partition_id, lsn.value)),
                        _ => None,
                    })
                    .unwrap_or_default()
            })
            .collect();

        if applied_lsn.len() == expected_processors
            && applied_lsn.iter().all(|(_, lsn)| *lsn == applied_lsn[0].1)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}
