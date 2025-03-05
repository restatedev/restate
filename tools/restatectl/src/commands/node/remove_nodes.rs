// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::commands::node::disable_node_checker::DisableNodeChecker;
use crate::connection::ConnectionInfo;
use anyhow::Context;
use clap::Parser;
use cling::{Collect, Run};
use itertools::Itertools;
use restate_cli_util::c_println;
use restate_core::metadata_store::MetadataStoreClient;
use restate_metadata_server::create_client;
use restate_types::PlainNodeId;
use restate_types::config::{MetadataClientKind, MetadataClientOptions};
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{NodesConfiguration, Role};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(alias = "rm")]
#[cling(run = "remove_nodes")]
pub struct RemoveNodesOpts {
    /// The node/s to remove from the cluster. Specify multiple nodes as a comma-separated list or
    /// specify the option multiple times.
    #[arg(long, required = true, visible_alias = "node", value_delimiter = ',')]
    nodes: Vec<PlainNodeId>,
}

pub async fn remove_nodes(
    connection: &ConnectionInfo,
    opts: &RemoveNodesOpts,
) -> anyhow::Result<()> {
    let nodes_configuration = connection.get_nodes_configuration().await?;
    let metadata_client = create_metadata_client(&nodes_configuration).await?;
    let logs = connection.get_logs().await?;

    let disable_node_checker = DisableNodeChecker::new(nodes_configuration, logs);

    for node_id in &opts.nodes {
        disable_node_checker
            .safe_to_disable_node(*node_id)
            .context("It is not safe to disable node {node_id}")?
    }

    let nodes_configuration = disable_node_checker.nodes_configuration();
    let mut updated_nodes_configuration = nodes_configuration.clone();

    for node_id in &opts.nodes {
        updated_nodes_configuration.remove_node_unchecked(*node_id);
    }

    updated_nodes_configuration.increment_version();

    metadata_client
        .put(
            NODES_CONFIG_KEY.clone(),
            &updated_nodes_configuration,
            Precondition::MatchesVersion(nodes_configuration.version()),
        )
        .await?;

    c_println!(
        "Successfully removed nodes [{}]",
        opts.nodes.iter().join(",")
    );

    Ok(())
}

async fn create_metadata_client(
    nodes_configuration: &NodesConfiguration,
) -> anyhow::Result<MetadataStoreClient> {
    // find metadata nodes
    let addresses: Vec<_> = nodes_configuration
        .iter_role(Role::MetadataServer)
        .map(|(_, config)| config.address.clone())
        .collect();
    if addresses.is_empty() {
        return Err(anyhow::anyhow!(
            "No nodes are configured to run metadata-server role, this command only \
             supports replicated metadata deployment"
        ));
    }

    // todo make this work with other metadata client kinds as well; maybe proxy through Restate server
    let metadata_store_client_options = MetadataClientOptions {
        kind: MetadataClientKind::Replicated { addresses },
        ..Default::default()
    };

    create_client(metadata_store_client_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create metadata store client: {}", e))
}
