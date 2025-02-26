// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU8, NonZeroU16};
use std::time::Duration;

use enumset::enum_set;
use futures_util::StreamExt;
use googletest::IntoTestResult;
use regex::Regex;
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
};
use restate_types::logs::metadata::{NodeSetSize, ProviderConfiguration, ReplicatedLogletConfig};
use restate_types::partition_table::PartitionReplication;
use restate_types::replication::ReplicationProperty;
use restate_types::{config::Configuration, nodes_config::Role};
use test_log::test;

mod common;

#[test(restate_core::test)]
async fn replicated_loglet() -> googletest::Result<()> {
    let mut base_config = Configuration::default();
    // require an explicit provision step to configure the replication property to 2
    base_config.common.auto_provision = false;
    base_config.common.default_num_partitions = NonZeroU16::new(1).expect("1 to be non-zero");

    let nodes = Node::new_test_nodes(
        base_config.clone(),
        BinarySource::CargoTest,
        enum_set!(Role::Admin | Role::MetadataServer | Role::Worker | Role::LogServer),
        3,
        false,
    );

    let regex: Regex = "Partition [0-9]+ started".parse()?;
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
        target_nodeset_size: NodeSetSize::default(),
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
