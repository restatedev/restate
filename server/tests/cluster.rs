use std::num::NonZeroU16;
use std::time::Duration;

use enumset::enum_set;
use futures_util::StreamExt;
use regex::Regex;
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
};
use restate_types::logs::metadata::ProviderKind;
use restate_types::{config::Configuration, nodes_config::Role, PlainNodeId};
use test_log::test;

mod common;

#[test(tokio::test)]
async fn node_id_mismatch() -> googletest::Result<()> {
    let base_config = Configuration::default();

    let nodes = Node::new_test_nodes_with_metadata(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Worker),
        1,
    );

    let mut cluster = Cluster::builder()
        .temp_base_dir()
        .nodes(nodes)
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    cluster.nodes[1]
        .graceful_shutdown(Duration::from_secs(2))
        .await?;

    let mismatch_node = Node::builder()
        .binary_source(BinarySource::CargoTest)
        .base_config(base_config)
        .with_node_name("node-1")
        .with_node_socket()
        .with_socket_metadata()
        .with_random_ports()
        .with_node_id(PlainNodeId::new(1234))
        .with_roles(enum_set!(Role::Admin | Role::Worker))
        .build();

    cluster.push_node(mismatch_node).await?;

    assert!(cluster.nodes[2]
        .lines("Node ID mismatch".parse()?)
        .next()
        .await
        .is_some());

    assert_eq!(Some(1), cluster.nodes[2].status().await?.code());

    Ok(())
}

#[test(tokio::test)]
async fn cluster_name_mismatch() -> googletest::Result<()> {
    let base_config = Configuration::default();

    let nodes = Node::new_test_nodes_with_metadata(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Worker),
        1,
    );

    let cluster = Cluster::builder()
        .cluster_name("cluster-1")
        .temp_base_dir()
        .nodes(nodes)
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let mismatch_node = Node::new_test_node(
        "mismatch",
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::Admin | Role::Worker),
    );

    let mut mismatch_node = mismatch_node
        .start_clustered(cluster.base_dir(), "cluster-2")
        .await?;

    assert!(mismatch_node
        .lines("Cluster name mismatch".parse()?)
        .next()
        .await
        .is_some());

    assert_eq!(Some(1), mismatch_node.status().await?.code());

    Ok(())
}

#[ignore = "is currently flaky"]
#[test(tokio::test)]
async fn replicated_loglet() -> googletest::Result<()> {
    let mut base_config = Configuration::default();
    base_config.bifrost.default_provider = ProviderKind::Replicated;
    base_config.common.bootstrap_num_partitions = NonZeroU16::new(1).expect("1 to be non-zero");

    let nodes = Node::new_test_nodes_with_metadata(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Worker | Role::LogServer),
        3,
    );

    let cluster = Cluster::builder()
        .cluster_name("cluster-1")
        .nodes(nodes)
        .temp_base_dir()
        .build()
        .start()
        .await?;

    // there is still a chance for a race condition because we cannot register the regex before we
    // create the cluster and we only start tracking the log lines once we register the regex.
    // If this should become an issue, then we need to add this feature.
    let regex: Regex = "PartitionProcessor starting up".parse()?;
    let mut partition_processors_starting_up: Vec<_> = (1..=3)
        .map(|idx| cluster.nodes[idx].lines(regex.clone()))
        .collect();

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    for partition_processor in &mut partition_processors_starting_up {
        assert!(partition_processor.next().await.is_some())
    }

    Ok(())
}
