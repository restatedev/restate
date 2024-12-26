// Copyright (c) 2024 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use enumset::enum_set;
use futures_util::StreamExt;
use googletest::fail;
use hyper_util::rt::TokioIo;
use regex::Regex;
use tempfile::TempDir;
use test_log::test;
use tokio::io;
use tokio::net::UnixStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use url::Url;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::{
    ClusterStateRequest, CreatePartitionSnapshotRequest,
};
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
};
use restate_types::config::{LogFormat, MetadataStoreClient};
use restate_types::net::AdvertisedAddress;
use restate_types::protobuf::deprecated_cluster::node_state::State;
use restate_types::{config::Configuration, nodes_config::Role};

mod common;

#[test(restate_core::test)]
async fn create_and_restore_snapshot() -> googletest::Result<()> {
    let mut base_config = Configuration::default();
    base_config.common.bootstrap_num_partitions = 1.try_into()?;
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
        enum_set!(Role::Worker),
        1,
    );

    let mut partition_ready = nodes[1].lines(Regex::new("Won the leadership campaign")?);

    let cluster = Cluster::builder()
        .temp_base_dir()
        .nodes(nodes.clone())
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;
    assert!(partition_ready.next().await.is_some());

    let mut client =
        ClusterCtrlSvcClient::new(grpc_connect(cluster.nodes[0].node_address().clone()).await?)
            .accept_compressed(CompressionEncoding::Gzip);

    any_partition_active(&mut client, Duration::from_secs(5)).await?;

    let snapshot_response = client
        .create_partition_snapshot(CreatePartitionSnapshotRequest { partition_id: 0 })
        .await?
        .into_inner();

    let mut node_2 = Node::new_test_node(
        "node-2",
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::Worker),
    );
    *node_2.metadata_store_client_mut() = MetadataStoreClient::Embedded {
        address: cluster.nodes[0].node_address().clone(),
    };

    let mut snapshot_restored = node_2.lines(
        format!(
            "Importing partition store snapshot.*{}",
            snapshot_response.snapshot_id
        )
        .parse()?,
    );

    node_2
        .start_clustered(cluster.base_dir(), cluster.cluster_name())
        .await?;

    assert!(snapshot_restored.next().await.is_some());
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
            break; // partition is ready; we can request snapshot
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
