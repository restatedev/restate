// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]
use std::{sync::Arc, time::Duration};

use enumset::{enum_set, EnumSet};
use googletest::internal::test_outcome::TestAssertionFailure;
use googletest::IntoTestResult;

use restate_bifrost::{loglet::Loglet, Bifrost};
use restate_core::metadata_store::Precondition;
use restate_core::TaskCenter;
use restate_core::{metadata_store::MetadataStoreClient, MetadataWriter};
use restate_local_cluster_runner::{
    cluster::{Cluster, MaybeTempDir, StartedCluster},
    node::{BinarySource, Node},
};
use restate_rocksdb::RocksDbManager;
use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{Chain, LogletParams, SegmentIndex};
use restate_types::logs::LogletId;
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
use restate_types::{
    config::Configuration,
    live::Live,
    logs::{metadata::ProviderKind, LogId},
    net::{AdvertisedAddress, BindAddress},
    nodes_config::Role,
    replicated_loglet::{ReplicatedLogletParams, ReplicationProperty},
    GenerationalNodeId, PlainNodeId,
};

async fn replicated_loglet_client(
    mut config: Configuration,
    cluster: &StartedCluster,
    node_id: PlainNodeId,
) -> googletest::Result<(
    Bifrost,
    Arc<dyn Loglet>,
    MetadataWriter,
    MetadataStoreClient,
)> {
    let node_name = "replicated-loglet-client".to_owned();
    let node_dir = cluster.base_dir().join(&node_name);
    std::fs::create_dir_all(&node_dir)?;
    let node_socket = node_dir.join("node.sock");

    config.common.roles = EnumSet::empty();
    config.common.allow_bootstrap = false;
    config.common.force_node_id = Some(node_id);
    config.common.set_node_name(node_name);
    config.common.set_base_dir(cluster.base_dir());
    config
        .common
        .set_cluster_name(cluster.cluster_name().to_owned());
    config.common.advertised_address = AdvertisedAddress::Uds(node_socket.clone());
    config.common.bind_address = Some(BindAddress::Uds(node_socket.clone()));
    config.common.metadata_store_client = cluster.nodes[0]
        .config()
        .common
        .metadata_store_client
        .clone();

    restate_types::config::set_current_config(config.clone());

    let node = restate_node::Node::create(Live::from_value(config)).await?;

    let bifrost = node.bifrost();
    let metadata_writer = node.metadata_writer();
    let metadata_store_client = node.metadata_store_client();

    node.start().await.into_test_result()?;

    let loglet = bifrost.find_tail_loglet(LogId::MIN).await?;

    Ok((bifrost, loglet, metadata_writer, metadata_store_client))
}

pub struct TestEnv {
    pub bifrost: Bifrost,
    pub loglet: Arc<dyn Loglet>,
    pub metadata_writer: MetadataWriter,
    pub metadata_store_client: MetadataStoreClient,
}

pub async fn run_in_test_env<F, O>(
    mut base_config: Configuration,
    sequencer: GenerationalNodeId,
    replication: ReplicationProperty,
    log_server_count: u32,
    mut future: F,
) -> googletest::Result<()>
where
    F: FnMut(TestEnv) -> O,
    O: std::future::Future<Output = googletest::Result<()>> + Send,
{
    // disable the cluster controller to allow us to manually set the logs configuration
    base_config.admin.disable_cluster_controller = true;
    let nodes = Node::new_test_nodes_with_metadata(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::LogServer),
        log_server_count,
    );

    // ensure base dir lives longer than the node, otherwise it sees shutdown errors
    // this will still respect LOCAL_CLUSTER_RUNNER_RETAIN_TEMPDIR=true
    let base_dir: MaybeTempDir = tempfile::tempdir()?.into();

    RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));

    let mut cluster = Cluster::builder()
        .base_dir(base_dir.as_path().to_owned())
        .nodes(nodes)
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let loglet_params = ReplicatedLogletParams {
        loglet_id: LogletId::new(LogId::from(1u32), SegmentIndex::OLDEST),
        sequencer,
        replication,
        // node 1 is the metadata, 2..=count+1 are logservers
        nodeset: (2..=log_server_count + 1).collect(),
    };
    let loglet_params = loglet_params.serialize()?;

    let chain = Chain::new(ProviderKind::Replicated, LogletParams::from(loglet_params));
    let mut logs_builder = LogsBuilder::default();
    logs_builder.add_log(LogId::MIN, chain)?;

    let metadata_store_client = cluster.nodes[0]
        .metadata_client()
        .await
        .map_err(|err| TestAssertionFailure::create(err.to_string()))?;
    metadata_store_client
        .put(
            BIFROST_CONFIG_KEY.clone(),
            &logs_builder.build(),
            Precondition::None,
        )
        .await?;

    // join a new node to the cluster solely to act as a bifrost client
    // it will have node id log_server_count+2
    let (bifrost, loglet, metadata_writer, metadata_store_client) = replicated_loglet_client(
        base_config,
        &cluster,
        PlainNodeId::new(log_server_count + 2),
    )
    .await?;

    // global metadata should now be set, running in scope sets it in the task center context
    future(TestEnv {
        bifrost,
        loglet,
        metadata_writer,
        metadata_store_client,
    })
    .await?;

    cluster.graceful_shutdown(Duration::from_secs(1)).await?;
    TaskCenter::shutdown_node("test completed", 0).await;
    RocksDbManager::get().shutdown().await;
    Ok(())
}
