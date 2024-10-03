use std::time::Duration;

use enumset::enum_set;
use futures_util::StreamExt;
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
};

use restate_types::{config::Configuration, nodes_config::Role, PlainNodeId};

#[tokio::test]
async fn node_id_mismatch() {
    let base_config = Configuration::default();

    let nodes = Node::new_test_nodes_with_metadata(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Admin | Role::Worker),
        1,
    );

    let mut cluster = Cluster::builder()
        .temp_base_dir()
        .nodes(nodes)
        .build()
        .start()
        .await
        .unwrap();

    assert!(cluster.wait_healthy(Duration::from_secs(30)).await);

    cluster.nodes[1]
        .graceful_shutdown(Duration::from_secs(2))
        .await
        .unwrap();

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

    cluster.push_node(mismatch_node).await.unwrap();

    assert!(cluster.nodes[2]
        .lines("Node ID mismatch".parse().unwrap())
        .next()
        .await
        .is_some());

    assert_eq!(Some(1), cluster.nodes[2].status().await.unwrap().code());
}

#[tokio::test]
async fn cluster_name_mismatch() {
    let base_config = Configuration::default();

    let nodes = Node::new_test_nodes_with_metadata(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Admin | Role::Worker),
        1,
    );

    let cluster = Cluster::builder()
        .cluster_name("cluster-1")
        .temp_base_dir()
        .nodes(nodes)
        .build()
        .start()
        .await
        .unwrap();

    assert!(cluster.wait_healthy(Duration::from_secs(30)).await);

    let mismatch_node = Node::new_test_node(
        "mismatch",
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::Admin | Role::Worker),
    );

    let mut mismatch_node = mismatch_node
        .start_clustered(cluster.base_dir(), "cluster-2")
        .await
        .unwrap();

    assert!(mismatch_node
        .lines("Cluster name mismatch".parse().unwrap())
        .next()
        .await
        .is_some());

    assert_eq!(Some(1), mismatch_node.status().await.unwrap().code())
}
