// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use clap::Parser;
use cling::{Collect, Run};
use tracing::debug;

use restate_cli_util::{c_print, c_println};
use restate_metadata_server_grpc::grpc::RemoveNodeRequest;
use restate_metadata_server_grpc::grpc::metadata_server_svc_client::MetadataServerSvcClient;
use restate_types::PlainNodeId;
use restate_types::nodes_config::{MetadataServerState, Role};

use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "add")]
#[cling(run = "add_node")]
pub struct AddNodeOpts {
    /// A node-id or a list of node-ids (comma-separated) to add
    #[arg(required = true, value_delimiter = ',')]
    nodes: Vec<PlainNodeId>,
}

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "rm")]
#[cling(run = "remove_node")]
pub struct RemoveNodeOpts {
    /// A node-id or a list of node-ids (comma-separated) to remove
    #[arg(required = true, value_delimiter = ',')]
    nodes: Vec<PlainNodeId>,
}

async fn add_node(
    add_node_opts: &AddNodeOpts,
    connection_info: &ConnectionInfo,
) -> anyhow::Result<()> {
    let nodes_configuration = connection_info.get_nodes_configuration().await?;

    for node_to_add in &add_node_opts.nodes {
        let node_config = nodes_configuration
            .find_node_by_id(*node_to_add)
            .context(format!(
                "failed to add unknown node {node_to_add} to the metadata cluster"
            ))?;

        // todo proxy request through an arbitrary Restate node to avoid requiring access to all metadata server nodes
        let channel = connection_info
            .connect(&node_config.address)
            .await
            .context(format!("failed connecting to node {node_to_add}"))?;
        let mut client = MetadataServerSvcClient::new(channel);
        // todo think about whether to run these calls in parallel
        client.add_node(()).await.context(format!(
            "failed adding node {node_to_add} to metadata cluster"
        ))?;

        c_println!("Added node '{node_to_add}' to the metadata cluster",);
    }

    Ok(())
}

async fn remove_node(
    remove_node_opts: &RemoveNodeOpts,
    connection_info: &ConnectionInfo,
) -> anyhow::Result<()> {
    let nodes_configuration = connection_info.get_nodes_configuration().await?;

    for node_to_remove in &remove_node_opts.nodes {
        let mut success = false;
        let mut errors = vec![];

        // check that we know the specified node
        let _ = nodes_configuration
            .find_node_by_id(*node_to_remove)
            .context(format!(
                "failed to remove unknown node {node_to_remove} from the metadata cluster"
            ))?;

        // todo try to figure out who's the current leader and directly reach out to the leader
        //  based on the metadata server status call
        for (server_id, node_config) in nodes_configuration.iter_role(Role::MetadataServer) {
            if node_config.metadata_server_config.metadata_server_state
                == MetadataServerState::Member
            {
                let channel = connection_info.connect(&node_config.address).await?;

                let mut client = MetadataServerSvcClient::new(channel);
                // todo think about whether to run these calls in parallel

                match client
                    .remove_node(RemoveNodeRequest {
                        plain_node_id: u32::from(*node_to_remove),
                        created_at_millis: None,
                    })
                    .await
                {
                    Ok(_response) => {
                        success = true;
                        break;
                    }
                    Err(err) => {
                        debug!(%err, "Failed removing node from the metadata cluster. Trying different metadata server.");
                        errors.push((server_id, err));
                    }
                }
            }
        }

        if success {
            c_println!("Removed node '{node_to_remove}' from the metadata cluster");
        } else {
            c_println!("Failed removing node '{node_to_remove}' from the metadata cluster",);
            for (server_id, error) in errors {
                c_print!("  ├─ {}: {}", server_id, error.code());

                let message = error.message();
                if !message.is_empty() {
                    c_print!(". {}", message);
                }

                c_println!("");
            }
        }
    }

    Ok(())
}
