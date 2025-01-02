// Copyright (c) 2024 - 2025 Restate Software, Inc., Restate GmbH.
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
use googletest::fail;
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::io;
use tokio::net::UnixStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use url::Url;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::{
    ClusterStateRequest, CreatePartitionSnapshotRequest, TrimLogRequest,
};
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
};
use restate_types::config::{LogFormat, MetadataStoreClient};
use restate_types::logs::metadata::ProviderKind::Replicated;
use restate_types::net::AdvertisedAddress;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::retries::RetryPolicy;
use restate_types::{config::Configuration, nodes_config::Role};

mod common;

#[tokio::test]
async fn fast_forward_over_trim_gap() -> googletest::Result<()> {
    tracing_subscriber::fmt()
        .event_format(tracing_subscriber::fmt::format().compact())
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let mut base_config = Configuration::default();
    base_config.common.bootstrap_num_partitions = 1.try_into()?;
    base_config.bifrost.default_provider = Replicated;
    base_config.common.log_filter = "restate=debug,warn".to_owned();
    base_config.common.log_format = LogFormat::Compact;

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

    let mut client =
        ClusterCtrlSvcClient::new(grpc_connect(cluster.nodes[0].node_address().clone()).await?)
            .accept_compressed(CompressionEncoding::Gzip);

    any_partition_active(&mut client, Duration::from_secs(5)).await?;

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
        info!("Invoke response: {:?}", invoke_response);
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
    trim_log(&mut client, 3).await?;

    let mut no_snapshot_config = base_config.clone();
    no_snapshot_config.worker.snapshots.destination = None;
    let node_3_name = "node-3";
    let mut node_3 = Node::new_test_node(
        node_3_name,
        no_snapshot_config,
        BinarySource::CargoTest,
        enum_set!(Role::HttpIngress | Role::Worker),
    );
    *node_3.metadata_store_client_mut() = MetadataStoreClient::Embedded {
        address: cluster.nodes[0].node_address().clone(),
    };

    let mut trim_gap_encountered =
        node_3.lines("Partition processor stopped due to a log trim gap, and no snapshot repository is configured".parse()?);
    cluster.push_node(node_3).await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(20), trim_gap_encountered.next())
            .await
            .is_ok()
    );

    let worker_3 = &mut cluster.nodes[3];
    assert_eq!(worker_3.config().node_name(), node_3_name);
    worker_3.graceful_shutdown(Duration::from_secs(2)).await?;

    // Restart node 3 with snapshot repository configured
    let mut worker_3 = Node::new_test_node(
        node_3_name,
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::HttpIngress | Role::Worker),
    );
    *worker_3.metadata_store_client_mut() = MetadataStoreClient::Embedded {
        address: cluster.nodes[0].node_address().clone(),
    };

    let mut worker_3_imported_snapshot = worker_3.lines(
        format!(
            "Importing partition store snapshot.*{}",
            snapshot_response.snapshot_id
        )
        .parse()?,
    );

    let ingress_url = format!(
        "http://{}/Counter/0/get",
        worker_3.config().ingress.bind_address
    );

    let _started_node = worker_3
        .start_clustered(cluster.base_dir(), cluster.cluster_name())
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(20), worker_3_imported_snapshot.next())
            .await
            .is_ok()
    );

    // todo(pavel): promote node 3 to be the leader for partition 0 and invoke the service again
    // for now, we just ensure that the new node is applying newly appended log records
    assert!(http_client
        .post(ingress_url)
        .send()
        .await?
        .status()
        .is_success());

    applied_lsn_converged(&mut client, Duration::from_secs(5), 3, 0).await?;

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
                State::Alive(s) => s
                    .partitions
                    .values()
                    .any(|p| p.effective_mode.cmp(&1).is_eq()),
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

async fn applied_lsn_converged(
    client: &mut ClusterCtrlSvcClient<Channel>,
    timeout: Duration,
    expected_processors: usize,
    partition_id: u32,
) -> googletest::Result<()> {
    assert!(expected_processors > 0);
    let deadline = tokio::time::Instant::now() + timeout;
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
                            .get(&partition_id)
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
            info!("All partition processors converged: {:?}", applied_lsn);
            break;
        }

        if tokio::time::Instant::now() > deadline {
            fail!(
                "Partition processors did not converge on the same applied LSN within {:?}",
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
) -> googletest::Result<()> {
    // todo(pavel): remove this hack which ensures we actually trim the log

    // Since we don't have a confirmed trimmed LSN in the response, and it takes a bit of time for
    // the log tail info to propagate to the admin node, we must wait long enough to cover the
    // heartbeat interval before we retry. The first attempt is usually a no-op as the admin does
    // not know the effective global tail.
    client
        .trim_log(TrimLogRequest {
            log_id: 0,
            trim_point,
        })
        .await?;

    tokio::time::sleep(Duration::from_secs(2)).await;
    client
        .trim_log(TrimLogRequest {
            log_id: 0,
            trim_point,
        })
        .await?;
    Ok(())
}

async fn grpc_connect(address: AdvertisedAddress) -> Result<Channel, tonic::transport::Error> {
    match address {
        AdvertisedAddress::Uds(uds_path) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("http://127.0.0.1")
                .expect("/ should be a valid Uri")
                .connect_with_connector(service_fn(move |_: Uri| {
                    let uds_path = uds_path.clone();
                    async move {
                        Ok::<_, io::Error>(TokioIo::new(UnixStream::connect(uds_path).await?))
                    }
                })).await
        }
        AdvertisedAddress::Http(uri) => {
            Channel::builder(uri)
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(2))
                .http2_adaptive_window(true)
                .connect()
                .await
        }
    }
}
