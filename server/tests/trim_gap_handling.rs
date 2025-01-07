// Copyright (c) 2024 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use enumset::enum_set;
use futures_util::StreamExt;
use googletest::fail;
use tempfile::TempDir;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::{error, info};
use url::Url;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::{
    ChainExtension, ClusterStateRequest, CreatePartitionSnapshotRequest, ListLogsRequest,
    ListNodesRequest, SealAndExtendChainRequest, TrimLogRequest,
};
use restate_core::network::net_util::{
    create_tonic_channel_from_advertised_address, CommonClientConnectionOptions,
};
use restate_local_cluster_runner::cluster::StartedCluster;
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
};
use restate_types::cluster_controller::SchedulingPlan;
use restate_types::config::{LogFormat, MetadataStoreClient};
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::{Logs, ProviderKind};
use restate_types::logs::{LogId, LogletId};
use restate_types::metadata_store::keys::SCHEDULING_PLAN_KEY;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::protobuf::cluster::RunMode;
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::retries::RetryPolicy;
use restate_types::storage::StorageCodec;
use restate_types::{config::Configuration, nodes_config::Role, PlainNodeId};

mod common;

#[test_log::test(tokio::test)]
async fn fast_forward_over_trim_gap() -> googletest::Result<()> {
    let mut base_config = Configuration::default();
    base_config.common.bootstrap_num_partitions = 1.try_into()?;
    base_config.bifrost.default_provider = ProviderKind::Replicated;
    base_config.common.log_filter = "restate=debug,warn".to_owned();
    base_config.common.log_format = LogFormat::Compact;

    let no_snapshot_repository_config = base_config.clone();

    let snapshots_dir = TempDir::new()?;
    base_config.worker.snapshots.destination = Some(
        Url::from_file_path(snapshots_dir.path())
            .unwrap()
            .to_string(),
    );

    let nodes = Node::new_test_nodes_with_metadata(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Worker | Role::LogServer),
        2,
    );
    let admin_node = &nodes[0];

    let worker_1 = &nodes[1];
    let worker_2 = &nodes[2];

    let mut worker_1_ready = worker_1.lines("PartitionProcessor starting event loop".parse()?);
    let mut worker_2_ready = worker_2.lines("PartitionProcessor starting event loop".parse()?);

    let mut cluster = Cluster::builder()
        .temp_base_dir()
        .nodes(nodes.clone())
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(10)).await?;
    tokio::time::timeout(Duration::from_secs(10), worker_1_ready.next()).await?;
    tokio::time::timeout(Duration::from_secs(10), worker_2_ready.next()).await?;

    let mut client = ClusterCtrlSvcClient::new(create_tonic_channel_from_advertised_address(
        cluster.nodes[0].node_address().clone(),
        &TestNetworkOptions::default(),
    ))
    .accept_compressed(CompressionEncoding::Gzip);

    any_partition_active(&mut client, Duration::from_secs(5)).await?;

    let metadata_config = MetadataStoreClient::Embedded {
        address: cluster.nodes[0].node_address().clone(),
    };

    let addr: SocketAddr = "127.0.0.1:9080".parse()?;
    tokio::spawn(async move {
        info!("Starting mock service on http://{}", addr);
        if let Err(e) = mock_service_endpoint::listener::run_listener(addr).await {
            error!("Error running listener: {:?}", e);
        }
    });

    let http_client = reqwest::Client::new();
    let registration_response = http_client
        .post(format!(
            "http://{}/deployments",
            admin_node.config().admin.bind_address
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

    // It takes a little bit for the service to become available for invocations
    let mut retry = RetryPolicy::fixed_delay(Duration::from_millis(500), Some(10)).into_iter();
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
        .create_partition_snapshot(CreatePartitionSnapshotRequest { partition_id: 0 })
        .await?
        .into_inner();

    // todo(pavel): if create snapshot returned an LSN, we could trim the log to that specific LSN instead of guessing
    trim_log(&mut client, 3, Duration::from_secs(3)).await?;

    let mut worker_3 = Node::new_test_node(
        "node-3",
        no_snapshot_repository_config,
        BinarySource::CargoTest,
        enum_set!(Role::HttpIngress | Role::Worker),
    );
    *worker_3.metadata_store_client_mut() = metadata_config.clone();

    let mut trim_gap_encountered =
        worker_3.lines("Partition processor stopped due to a log trim gap, and no snapshot repository is configured".parse()?);

    info!("Waiting for partition processor to encounter log trim gap");
    let mut worker_3 = worker_3
        .start_clustered(cluster.base_dir(), cluster.cluster_name())
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(20), trim_gap_encountered.next())
            .await
            .is_ok()
    );
    worker_3.graceful_shutdown(Duration::from_secs(1)).await?;

    info!("Re-starting additional node with snapshot repository configured");
    let mut worker_3 = Node::new_test_node(
        "node-3",
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::HttpIngress | Role::Worker),
    );
    *worker_3.metadata_store_client_mut() = metadata_config.clone();

    let mut worker_3_imported_snapshot = worker_3.lines(
        format!(
            "Importing partition store snapshot.*{}",
            snapshot_response.snapshot_id
        )
        .parse()?,
    );
    let mut worker_3 = worker_3
        .start_clustered(cluster.base_dir(), cluster.cluster_name())
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(20), worker_3_imported_snapshot.next())
            .await
            .is_ok()
    );

    // Make the new node the sequencer for the partition log - this makes it the
    // preferred candidate to become the next partition processor leader.
    let extension =
        Some(replicated_loglet_extension(&mut client, LogId::new(0), PlainNodeId::new(3)).await?);
    let reconfigure_response = client
        .seal_and_extend_chain(SealAndExtendChainRequest {
            log_id: 0,
            min_version: None,
            extension,
        })
        .await?
        .into_inner();
    assert!(reconfigure_response.sealed_segment.is_some());

    force_promote_partition_leader(
        &cluster,
        &mut client,
        PartitionId::from(0),
        PlainNodeId::new(3),
        Duration::from_secs(10),
    )
    .await?;

    // Verify that node 3 can process the incoming request while being the partition leader:
    let ingress_url = format!(
        "http://{}/Counter/0/get",
        worker_3.config().ingress.bind_address
    );
    assert!(http_client
        .post(ingress_url)
        .send()
        .await?
        .status()
        .is_success());
    assert_eq!(
        effective_partition_leader(&mut client, PartitionId::from(0)).await?,
        Some(PlainNodeId::from(3))
    );

    worker_3.graceful_shutdown(Duration::from_secs(1)).await?;
    cluster.graceful_shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

async fn any_partition_active(
    client: &mut ClusterCtrlSvcClient<Channel>,
    timeout: Duration,
) -> googletest::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
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
        if tokio::time::Instant::now() > deadline {
            fail!(
                "Partition processor did not become ready within {:?}",
                timeout
            )?;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}

async fn trim_log(
    client: &mut ClusterCtrlSvcClient<Channel>,
    trim_point: u64,
    timeout: Duration,
) -> googletest::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let response = client
            .trim_log(TrimLogRequest {
                log_id: 0,
                trim_point,
            })
            .await?
            .into_inner();

        if response.trim_point.is_some_and(|tp| tp == trim_point) {
            break;
        }

        if tokio::time::Instant::now() > deadline {
            fail!(
                "Failed to trim log to LSN {} within {:?}",
                trim_point,
                timeout
            )?;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}

async fn force_promote_partition_leader(
    cluster: &StartedCluster,
    client: &mut ClusterCtrlSvcClient<Channel>,
    partition_id: PartitionId,
    leader: PlainNodeId,
    timeout: Duration,
) -> googletest::Result<()> {
    let metadata_client = cluster.nodes[0]
        .metadata_client()
        .await
        .expect("can create metadata client");

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let plan = metadata_client
            .get::<SchedulingPlan>(SCHEDULING_PLAN_KEY.clone())
            .await?;
        if plan.is_none_or(|p| {
            p.get(&partition_id)
                .is_none_or(|p| p.leader != Some(leader))
        }) {
            metadata_client
                .read_modify_write(
                    SCHEDULING_PLAN_KEY.clone(),
                    |scheduling_plan: Option<SchedulingPlan>| {
                        let mut plan_builder = scheduling_plan.unwrap().into_builder();
                        plan_builder.modify_partition(&partition_id, |partition| {
                            if partition.leader == Some(leader) {
                                return false;
                            }
                            partition.leader = Some(leader);
                            true
                        });
                        anyhow::Ok::<SchedulingPlan>(plan_builder.build())
                    },
                )
                .await?;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let cluster_state = client
            .get_cluster_state(ClusterStateRequest {})
            .await?
            .into_inner()
            .cluster_state
            .unwrap();

        if cluster_state
            .nodes
            .get(&leader.into())
            .is_some_and(|ns| match ns.state.as_ref() {
                Some(State::Alive(n)) => n.partitions.get(&partition_id.into()).is_some_and(|p| {
                    RunMode::try_from(p.effective_mode).is_ok_and(|m| m == RunMode::Leader)
                }),
                _ => false,
            })
        {
            info!(
                "Node {:#} became leader for partition {:#}",
                leader, partition_id
            );
            break;
        }

        if tokio::time::Instant::now() > deadline {
            fail!(
                "Node {:#} did not become leader for partition {:#} within {:?}",
                leader,
                partition_id,
                timeout
            )?;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}

async fn effective_partition_leader(
    client: &mut ClusterCtrlSvcClient<Channel>,
    partition_id: PartitionId,
) -> googletest::Result<Option<PlainNodeId>> {
    let cluster_state = client
        .get_cluster_state(ClusterStateRequest {})
        .await?
        .into_inner()
        .cluster_state
        .unwrap();

    Ok(cluster_state
        .nodes
        .iter()
        .find(|(_, ns)| match &ns.state {
            Some(State::Alive(n)) => n
                .partitions
                .get(&partition_id.into())
                .map(|p| RunMode::try_from(p.effective_mode).is_ok_and(|m| m == RunMode::Leader))
                .unwrap_or(false),
            _ => false,
        })
        .map(|(id, _)| PlainNodeId::from(*id)))
}

async fn replicated_loglet_extension(
    client: &mut ClusterCtrlSvcClient<Channel>,
    log_id: LogId,
    new_sequencer: PlainNodeId,
) -> googletest::Result<ChainExtension> {
    let mut logs_response = client.list_logs(ListLogsRequest {}).await?.into_inner();

    let logs = StorageCodec::decode::<Logs, _>(&mut logs_response.logs)?;
    let chain = logs.chain(&log_id).expect("log exists");

    let tail_index = chain.tail_index();
    let loglet_id = LogletId::new(log_id, tail_index.next());
    let tail_segment = chain.tail();

    let last_params =
        ReplicatedLogletParams::deserialize_from(tail_segment.config.params.as_bytes())?;

    // --- get nodes ---
    let mut nodes_response = client.list_nodes(ListNodesRequest {}).await?.into_inner();
    let nodes_configuration =
        StorageCodec::decode::<NodesConfiguration, _>(&mut nodes_response.nodes_configuration)?;
    let nodes = nodes_configuration.iter().collect::<BTreeMap<_, _>>();

    // --- construct new replicated log params ---
    let mut nodeset = last_params.nodeset.clone();
    nodeset.insert(new_sequencer); // just in case

    let params = ReplicatedLogletParams {
        loglet_id,
        nodeset,
        replication: last_params.replication.clone(),
        sequencer: nodes
            .get(&new_sequencer)
            .expect("proposed sequencer node exists")
            .current_generation,
    };

    Ok(ChainExtension {
        provider: ProviderKind::Replicated.to_string(),
        segment_index: None,
        params: params.serialize()?,
    })
}

struct TestNetworkOptions {
    connect_timeout: u64,
    request_timeout: u64,
}

impl Default for TestNetworkOptions {
    fn default() -> Self {
        Self {
            connect_timeout: 1000,
            request_timeout: 5000,
        }
    }
}

impl CommonClientConnectionOptions for TestNetworkOptions {
    fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout)
    }

    fn keep_alive_interval(&self) -> Duration {
        Duration::from_secs(60)
    }

    fn keep_alive_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout)
    }

    fn http2_adaptive_window(&self) -> bool {
        true
    }
}
