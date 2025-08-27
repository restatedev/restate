// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use cling::{Collect, Run};
use tonic::Status;
use tracing::debug;

use restate_cli_util::{c_print, c_println};
use restate_metadata_server_grpc::grpc::RemoveNodeRequest;
use restate_metadata_server_grpc::grpc::metadata_server_svc_client::MetadataServerSvcClient;
use restate_types::PlainNodeId;
use restate_types::nodes_config::{MetadataServerState, NodeConfig, Role};
use restate_types::retries::RetryPolicy;

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
        match client.add_node(()).await {
            Ok(_) => {
                c_println!("Added node '{node_to_add}' to the metadata cluster",);
            }
            Err(status)
                if status.code() == tonic::Code::AlreadyExists
                    || status.message().contains(
                        "cannot add node because it is still a member of the metadata cluster",
                    ) =>
            {
                // the check (status.message().contains()) is a for backward compatibility in case
                // running against an older version that did not return proper error codes.

                c_println!("Node '{node_to_add}' is already a member of the metadata cluster");
            }
            Err(status) => {
                anyhow::bail!("failed adding node {node_to_add} to metadata cluster: {status}");
            }
        }
    }

    Ok(())
}

async fn remove_node(
    remove_node_opts: &RemoveNodeOpts,
    connection_info: &ConnectionInfo,
) -> anyhow::Result<()> {
    let nodes_configuration = connection_info.get_nodes_configuration().await?;

    let mut metadata_node_set = nodes_configuration
        .iter_role(Role::MetadataServer)
        .filter(|(_, config)| {
            config.metadata_server_config.metadata_server_state == MetadataServerState::Member
        })
        .collect::<HashMap<_, _>>();

    let retry_policy = RetryPolicy::exponential(
        Duration::from_millis(200),
        2.0,
        Some(10),
        Some(Duration::from_millis(1500)),
    );

    // one_node_failed is set to true if any node removal fails
    let mut one_node_failed = false;
    for node_to_remove in &remove_node_opts.nodes {
        let mut errors = None;

        if metadata_node_set.is_empty() {
            anyhow::bail!(
                "No metadata servers available to remove node '{node_to_remove}' from the metadata cluster"
            );
        }

        for sleep in std::iter::once(Duration::ZERO).chain(retry_policy.iter()) {
            if sleep > Duration::ZERO {
                tokio::time::sleep(sleep).await;
            }

            match try_remove_node(*node_to_remove, &metadata_node_set, connection_info).await {
                Ok(_) => {
                    errors = None;
                    metadata_node_set.remove(node_to_remove);
                    break;
                }
                Err(err) => {
                    errors = Some(err);
                }
            }
        }

        if let Some(errors) = errors {
            one_node_failed = true;
            c_println!("Failed removing node '{node_to_remove}' from the metadata cluster",);

            for (server_id, error) in errors {
                c_print!("  ├─ {}: {}", server_id, error.code());

                let message = error.message();
                if !message.is_empty() {
                    c_print!(". {}", message);
                }

                c_println!("");
            }
        } else {
            c_println!("Removed node '{node_to_remove}' from the metadata cluster");
        }
    }

    if one_node_failed {
        return Err(anyhow::anyhow!(
            "Failed removing some nodes from the metadata cluster"
        ));
    }

    Ok(())
}

async fn try_remove_node(
    node_to_remove: PlainNodeId,
    metadata_node_set: &HashMap<PlainNodeId, &NodeConfig>,
    connection_info: &ConnectionInfo,
) -> Result<(), HashMap<PlainNodeId, Status>> {
    assert!(
        !metadata_node_set.is_empty(),
        "metadata cluster is not empty"
    );

    let mut errors = HashMap::new();
    // todo try to figure out who's the current leader and directly reach out to the leader
    //  based on the metadata server status call
    for (server_id, node_config) in metadata_node_set.iter() {
        let channel = match connection_info.connect(&node_config.address).await {
            Ok(channel) => channel,
            Err(err) => {
                errors.insert(*server_id, Status::unavailable(err.to_string()));
                continue;
            }
        };
        let mut client = MetadataServerSvcClient::new(channel);

        match client
            .remove_node(RemoveNodeRequest {
                plain_node_id: u32::from(node_to_remove),
                created_at_millis: None,
            })
            .await
        {
            Ok(_response) => {
                return Ok(());
            }
            Err(err) if err.message().contains("is not a member") => {
                // todo(azmy): grpc server should return proper error codes.
                c_println!("Node '{node_to_remove}' is not a member of the metadata cluster");
                return Ok(());
            }
            Err(err) => {
                debug!(%err, "Failed removing node from the metadata cluster. Trying different metadata server.");
                errors.insert(*server_id, err);
            }
        }
    }

    Err(errors)
}
