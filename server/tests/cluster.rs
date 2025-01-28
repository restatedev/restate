// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU16, NonZeroU8};
use std::time::Duration;

use enumset::enum_set;
use futures_util::StreamExt;
use googletest::IntoTestResult;
use regex::Regex;
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
};
use restate_types::config::{MetadataStoreClient, MetadataStoreKind};
use restate_types::logs::metadata::{ProviderConfiguration, ReplicatedLogletConfig};
use restate_types::partition_table::PartitionReplication;
use restate_types::replication::ReplicationProperty;
use restate_types::{config::Configuration, nodes_config::Role, PlainNodeId};
use test_log::test;

mod common;

#[test(restate_core::test)]
async fn node_id_mismatch() -> googletest::Result<()> {
    let mut base_config = Configuration::default();
    base_config.common.allow_bootstrap = false;
    base_config.metadata_store.kind = MetadataStoreKind::Raft;
    base_config.common.log_disable_ansi_codes = true;

    let nodes = Node::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Worker),
        3,
        true,
    );

    let mut cluster = Cluster::builder()
        .temp_base_dir()
        .nodes(nodes)
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    cluster
        .nodes
        .first_mut()
        .expect("to have at least one node")
        .graceful_shutdown(Duration::from_secs(2))
        .await?;

    let mut mismatch_node = Node::builder()
        .binary_source(BinarySource::CargoTest)
        .base_config(base_config)
        .with_node_name("node-1")
        .with_node_socket()
        .with_random_ports()
        .with_node_id(PlainNodeId::new(1234))
        .with_roles(enum_set!(Role::MetadataServer | Role::Worker))
        .build();

    *mismatch_node.metadata_store_client_mut() = MetadataStoreClient::Embedded {
        addresses: vec![cluster.nodes[1].node_address().clone()],
    };

    cluster.push_node(mismatch_node).await?;

    let mismatch_node = cluster.nodes.last_mut().expect("to have at least one node");
    assert!(mismatch_node
        .lines("node id mismatch".parse()?)
        .next()
        .await
        .is_some());

    assert_eq!(Some(1), mismatch_node.status().await?.code());

    Ok(())
}

#[test(restate_core::test)]
async fn cluster_name_mismatch() -> googletest::Result<()> {
    let base_config = Configuration::default();

    let nodes = Node::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Worker),
        1,
        true,
    );

    let cluster = Cluster::builder()
        .cluster_name("cluster-1")
        .temp_base_dir()
        .nodes(nodes)
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let mut mismatch_node = Node::new_test_node(
        "mismatch",
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::Admin | Role::Worker),
    );

    *mismatch_node.metadata_store_client_mut() = MetadataStoreClient::Embedded {
        addresses: vec![cluster.nodes[0].node_address().clone()],
    };

    let mut mismatch_node = mismatch_node
        .start_clustered(cluster.base_dir(), "cluster-2")
        .await?;

    assert!(mismatch_node
        .lines("trying to join wrong cluster".parse()?)
        .next()
        .await
        .is_some());

    assert_eq!(Some(1), mismatch_node.status().await?.code());

    Ok(())
}

#[test(restate_core::test)]
async fn replicated_loglet() -> googletest::Result<()> {
    let mut base_config = Configuration::default();
    // require an explicit provision step to configure the replication property to 2
    base_config.common.allow_bootstrap = false;
    base_config.common.bootstrap_num_partitions = NonZeroU16::new(1).expect("1 to be non-zero");

    let nodes = Node::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Worker | Role::LogServer),
        3,
        false,
    );

    let regex: Regex = "Starting the partition processor".parse()?;
    let mut partition_processors_starting_up: Vec<_> =
        nodes.iter().map(|node| node.lines(regex.clone())).collect();

    let cluster = Cluster::builder()
        .cluster_name("cluster-1")
        .nodes(nodes)
        .temp_base_dir()
        .build()
        .start()
        .await?;

    let replicated_loglet_config = ReplicatedLogletConfig {
        target_nodeset_size: 0,
        replication_property: ReplicationProperty::new(NonZeroU8::new(2).expect("to be non-zero")),
    };

    cluster.nodes[0]
        .provision_cluster(
            None,
            PartitionReplication::Everywhere,
            Some(ProviderConfiguration::Replicated(replicated_loglet_config)),
        )
        .await
        .into_test_result()?;

    cluster.wait_healthy(Duration::from_secs(30)).await?;

    for partition_processor in &mut partition_processors_starting_up {
        assert!(partition_processor.next().await.is_some())
    }

    Ok(())
}
