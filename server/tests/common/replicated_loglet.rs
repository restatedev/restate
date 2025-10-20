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
use std::num::NonZeroU32;
use std::{sync::Arc, time::Duration};

use enumset::{EnumSet, enum_set};
use googletest::IntoTestResult;
use googletest::internal::test_outcome::TestAssertionFailure;
use tracing::info;

use restate_bifrost::{Bifrost, loglet::Loglet};
use restate_core::TaskCenter;
use restate_core::{Metadata, MetadataWriter};
use restate_local_cluster_runner::{
    cluster::{Cluster, MaybeTempDir, StartedCluster},
    node::{BinarySource, NodeSpec},
};
use restate_metadata_store::{MetadataStoreClient, retry_on_retryable_error};
use restate_rocksdb::RocksDbManager;
use restate_tracing_instrumentation::prometheus_metrics::Prometheus;
use restate_types::logs::LogletId;
use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{Chain, LogletParams, SegmentIndex};
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
use restate_types::net::listener::AddressBook;
use restate_types::net::metadata::MetadataKind;
use restate_types::retries::RetryPolicy;
use restate_types::{
    GenerationalNodeId, PlainNodeId, Version, Versioned,
    config::Configuration,
    live::Live,
    logs::{LogId, metadata::ProviderKind},
    nodes_config::Role,
    replicated_loglet::ReplicatedLogletParams,
    replication::ReplicationProperty,
};

async fn replicated_loglet_client(
    mut config: Configuration,
    cluster: &StartedCluster,
    node_id: PlainNodeId,
    logs_version_to_await: Version,
) -> googletest::Result<(
    Bifrost,
    Arc<dyn Loglet>,
    MetadataWriter,
    MetadataStoreClient,
)> {
    let node_name = "n1".to_owned();
    let node_dir = cluster.base_dir().join(&node_name);
    std::fs::create_dir_all(&node_dir)?;
    let mut address_book = AddressBook::new(node_dir);

    config.common.roles = EnumSet::empty();
    config.common.auto_provision = false;
    config.common.force_node_id = Some(node_id);
    config.common.set_node_name(node_name);
    config.common.set_base_dir(cluster.base_dir());
    config
        .common
        .set_cluster_name(cluster.cluster_name().to_owned());
    config.common.metadata_client = cluster.nodes[0].config().common.metadata_client.clone();
    address_book.bind_from_config(&config).await?;

    restate_types::config::set_current_config(config.clone());

    let node = restate_node::Node::create(
        Live::from_value(config),
        Prometheus::default(),
        address_book,
    )
    .await?;

    let bifrost = node.bifrost();
    let metadata_writer = node.metadata_writer();
    let metadata_store_client = node.metadata_store_client();

    node.start().await.into_test_result()?;

    // before we can find the tail for the given log, we need to make sure that we have seen the
    // required logs configuration
    Metadata::current()
        .wait_for_version(MetadataKind::Logs, logs_version_to_await)
        .await?;

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
    dir_name: &str,
    mut future: F,
) -> googletest::Result<()>
where
    F: FnMut(TestEnv) -> O,
    O: std::future::Future<Output = googletest::Result<()>> + Send,
{
    // disable the cluster controller to allow us to manually set the logs configuration
    base_config.admin.disable_cluster_controller = true;
    let nodes = NodeSpec::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::MetadataServer | Role::LogServer),
        log_server_count,
        true,
    );
    // tests rely on manual control over the chain.
    base_config.bifrost.disable_auto_improvement = true;

    // ensure base dir lives longer than the node, otherwise it sees shutdown errors
    // this will still respect LOCAL_CLUSTER_RUNNER_RETAIN_TEMPDIR=true
    let base_dir = MaybeTempDir::new(&dir_name);

    RocksDbManager::init();

    let mut cluster = Cluster::builder()
        .base_dir(base_dir.as_path().to_owned())
        .nodes(nodes)
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    info!("Test cluster is healthy");

    let loglet_params = ReplicatedLogletParams {
        loglet_id: LogletId::new(LogId::MIN, SegmentIndex::OLDEST),
        sequencer,
        replication,
        // all nodes are log-servers
        nodeset: (1..=log_server_count).collect(),
    };
    let loglet_params = loglet_params.serialize()?;

    let chain = Chain::new(ProviderKind::Replicated, LogletParams::from(loglet_params));
    let mut logs_builder = LogsBuilder::default();
    logs_builder.add_log(LogId::MIN, chain)?;

    // some high version number to guarantee that there is no other logs configuration with the same
    // or higher version
    logs_builder.set_version(NonZeroU32::new(1336).expect("1336 > 0"));

    let metadata_store_client = cluster.nodes[0]
        .metadata_client()
        .await
        .map_err(|err| TestAssertionFailure::create(err.to_string()))?;
    let logs = logs_builder.build();
    retry_on_retryable_error(
        RetryPolicy::fixed_delay(Duration::from_millis(500), Some(20)),
        || metadata_store_client.put(BIFROST_CONFIG_KEY.clone(), &logs, Precondition::None),
    )
    .await?;

    info!("Written initial logs configuration: {logs:?}");

    // join a new node to the cluster solely to act as a bifrost client
    // it will have node id log_server_count+2
    let (bifrost, loglet, metadata_writer, metadata_store_client) = replicated_loglet_client(
        base_config,
        &cluster,
        PlainNodeId::new(log_server_count + 2),
        logs.version(),
    )
    .await?;

    info!("Starting test");

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
