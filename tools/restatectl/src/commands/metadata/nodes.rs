// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::app::ConnectionInfo;
use crate::commands::metadata::retrieve_nodes_configuration_from_node;
use anyhow::{anyhow, bail};
use clap::{Parser, Subcommand};
use cling::{Collect, Run};
use restate_cli_util::c_println;
use restate_metadata_store::local::create_client;
use restate_types::config::MetadataStoreClientOptions;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration, Role};
use restate_types::PlainNodeId;

#[derive(Run, Subcommand, Clone)]
pub enum Nodes {
    /// Add a node to the metadata store cluster
    Add(AddNodeOpts),
    /// Remove a node from the metadata store cluster
    Remove(RemoveNodeOpts),
}

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "add_node")]
pub struct AddNodeOpts {
    /// The plain node id of the node to add
    #[clap(long)]
    id: PlainNodeId,
}

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "remove_node")]
pub struct RemoveNodeOpts {
    /// The plain node id of the node to remove
    #[clap(long)]
    id: PlainNodeId,
}

async fn add_node(
    add_node_opts: &AddNodeOpts,
    connection_info: &ConnectionInfo,
) -> anyhow::Result<()> {
    let nodes_configuration =
        retrieve_nodes_configuration_from_node(connection_info.cluster_controller.clone()).await?;

    let metadata_servers: Vec<_> = nodes_configuration
        .iter()
        .filter_map(|(_, node_config)| {
            if node_config.has_role(Role::MetadataServer)
                && node_config.metadata_server_config.metadata_server_state
                    == MetadataServerState::Member
            {
                Some(node_config.address.clone())
            } else {
                None
            }
        })
        .collect();

    if metadata_servers.is_empty() {
        bail!("No metadata servers found in the cluster. Did you configure the embedded metadata store?");
    }

    let metadata_client_options = MetadataStoreClientOptions {
        metadata_store_client: restate_types::config::MetadataStoreClient::Embedded {
            addresses: metadata_servers,
        },
        ..Default::default()
    };

    let metadata_client = create_client(metadata_client_options)
        .await
        .map_err(|err| anyhow!("Failed creating metadata store client: {err}"))?;

    let nodes_configuration = metadata_client
        .read_modify_write(
            NODES_CONFIG_KEY.clone(),
            |nodes_configuration: Option<NodesConfiguration>| {
                let Some(mut nodes_configuration) = nodes_configuration else {
                    bail!("The Restate cluster seems to be not provisioned yet.");
                };

                let Ok(node_config) = nodes_configuration.find_node_by_id_mut(add_node_opts.id)
                else {
                    bail!(
                        "Node '{}' not found in the nodes configuration",
                        add_node_opts.id
                    );
                };

                if !node_config.has_role(Role::MetadataServer) {
                    bail!("Node '{}' is not a metadata server", add_node_opts.id);
                }

                if node_config.metadata_server_config.metadata_server_state
                    == MetadataServerState::Member
                {
                    bail!(
                        "Node '{}' is already a member of the metadata store cluster",
                        add_node_opts.id
                    );
                }

                node_config.metadata_server_config.metadata_server_state =
                    MetadataServerState::Member;
                nodes_configuration.increment_version();
                Ok(nodes_configuration)
            },
        )
        .await?;

    c_println!(
        "Added node '{}' to the metadata store cluster in nodes configuration '{}'",
        add_node_opts.id,
        nodes_configuration.version()
    );

    Ok(())
}

async fn remove_node() -> anyhow::Result<()> {
    unimplemented!("Removing a node is not yet implemented");
}
