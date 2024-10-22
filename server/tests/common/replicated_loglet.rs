#![allow(dead_code)]
use std::{sync::Arc, time::Duration};

use enumset::{enum_set, EnumSet};
use googletest::internal::test_outcome::TestAssertionFailure;
use googletest::IntoTestResult;

use restate_bifrost::{loglet::Loglet, Bifrost, BifrostAdmin, FindTailAttributes};
use restate_core::metadata_store::Precondition;
use restate_core::{metadata_store::MetadataStoreClient, MetadataWriter, TaskCenterBuilder};
use restate_local_cluster_runner::{
    cluster::{Cluster, MaybeTempDir, StartedCluster},
    node::{BinarySource, Node},
};
use restate_rocksdb::RocksDbManager;
use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{Chain, LogletParams};
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
use restate_types::{
    config::Configuration,
    live::Live,
    logs::{metadata::ProviderKind, LogId},
    net::{AdvertisedAddress, BindAddress},
    nodes_config::Role,
    replicated_loglet::{ReplicatedLogletId, ReplicatedLogletParams, ReplicationProperty},
    GenerationalNodeId, PlainNodeId,
};

async fn replicated_loglet_client(
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

    let mut config = Configuration::default();

    config.common.roles = EnumSet::empty();
    config.common.allow_bootstrap = false;
    config.common.force_node_id = Some(node_id);
    config.common.set_node_name(node_name);
    config.common.set_base_dir(cluster.base_dir());
    config
        .common
        .set_cluster_name(cluster.cluster_name().to_owned());
    config.common.advertised_address = AdvertisedAddress::Uds(node_socket.clone());
    config.common.bind_address = BindAddress::Uds(node_socket.clone());
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

    let loglet = bifrost
        .find_tail_loglet(LogId::MIN, FindTailAttributes::default())
        .await?;

    Ok((bifrost, loglet, metadata_writer, metadata_store_client))
}

pub struct TestEnv {
    bifrost: Bifrost,
    pub loglet: Arc<dyn Loglet>,
    metadata_writer: MetadataWriter,
    metadata_store_client: MetadataStoreClient,
    pub cluster: StartedCluster,
}

impl TestEnv {
    pub fn bifrost_admin(&self) -> BifrostAdmin<'_> {
        BifrostAdmin::new(
            &self.bifrost,
            &self.metadata_writer,
            &self.metadata_store_client,
        )
    }
}

pub async fn run_in_test_env<F, O>(
    base_config: Configuration,
    sequencer: GenerationalNodeId,
    replication: ReplicationProperty,
    log_server_count: u32,
    mut future: F,
) -> googletest::Result<()>
where
    F: FnMut(TestEnv) -> O,
    O: std::future::Future<Output = googletest::Result<()>> + Send,
{
    let nodes = Node::new_test_nodes_with_metadata(
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::LogServer),
        log_server_count,
    );

    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .ingress_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");

    // ensure base dir lives longer than the node, otherwise it sees shutdown errors
    // this will still respect LOCAL_CLUSTER_RUNNER_RETAIN_TEMPDIR=true
    let base_dir: MaybeTempDir = tempfile::tempdir()?.into();

    tc.run_in_scope("test", None, async {
        RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));

        let cluster = Cluster::builder()
            .base_dir(base_dir.as_path().to_owned())
            .nodes(nodes)
            .build()
            .start()
            .await?;

        cluster.wait_healthy(Duration::from_secs(30)).await?;

        let loglet_params = ReplicatedLogletParams {
            loglet_id: ReplicatedLogletId::new(1),
            sequencer,
            replication,
            // node 1 is the metadata, 2..=count+1 are logservers
            nodeset: (2..=log_server_count + 1).collect(),
            write_set: None,
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
        let (bifrost, loglet, metadata_writer, metadata_store_client) =
            replicated_loglet_client(&cluster, PlainNodeId::new(log_server_count + 2)).await?;

        // global metadata should now be set, running in scope sets it in the task center context
        tc.run_in_scope(
            "test-fn",
            None,
            future(TestEnv {
                bifrost,
                loglet,
                cluster,
                metadata_writer,
                metadata_store_client,
            }),
        )
        .await
    })
    .await?;

    tc.shutdown_node("test completed", 0).await;
    RocksDbManager::get().shutdown().await;
    Ok(())
}
