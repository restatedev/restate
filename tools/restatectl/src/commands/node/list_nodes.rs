// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::Context;
use chrono::TimeDelta;
use cling::prelude::*;
use itertools::Itertools;
use tokio::task::JoinSet;
use tracing::{info, warn};

use restate_cli_util::_comfy_table::{Cell, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::{Tense, duration_to_human_rough};
use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::node_ctl_svc::{IdentResponse, new_node_ctl_client};
use restate_types::health::MetadataServerStatus;
use restate_types::net::address::{AdvertisedAddress, FabricPort, ListenerPort};
use restate_types::nodes_config::NodesConfiguration;

use crate::connection::{ConnectionInfo, ConnectionInfoError};
use crate::util::grpc_channel;

// Default timeout for the [optional] GetIdent call made to all nodes
const GET_IDENT_TIMEOUT_1S: Duration = Duration::from_secs(1);

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(alias = "ls")]
#[cling(run = "list_nodes")]
pub struct ListNodesOpts {
    /// Gather and display additional information such as uptime, role-specific status, and metadata versions for
    /// various aspects of the cluster configuration, as seen by the nodes themselves
    #[arg(long)]
    pub(crate) extra: bool,
}

pub async fn list_nodes(connection: &ConnectionInfo, opts: &ListNodesOpts) -> anyhow::Result<()> {
    match connection.get_nodes_configuration().await {
        Ok(nodes_configuration) => list_nodes_configuration(&nodes_configuration, opts).await,
        Err(ConnectionInfoError::MetadataValueNotAvailable { contacted_nodes })
            if !contacted_nodes.is_empty() =>
        {
            warn!(
                "Could not read nodes configuration from cluster, using GetIdent responses to render basic list"
            );
            list_nodes_lite(&contacted_nodes, opts);

            // short-circuit if called as part of `restatectl status`
            if contacted_nodes.iter().any(|(_, ident)| {
                ident.metadata_server_status() == MetadataServerStatus::AwaitingProvisioning
            }) {
                Err(ConnectionInfoError::ClusterNotProvisioned.into())
            } else {
                Err(ConnectionInfoError::MetadataValueNotAvailable { contacted_nodes }.into())
            }
        }
        Err(err) => Err(err.into()),
    }
}

async fn list_nodes_configuration(
    nodes_configuration: &NodesConfiguration,
    opts: &ListNodesOpts,
) -> anyhow::Result<()> {
    let nodes = nodes_configuration.iter().collect::<BTreeMap<_, _>>();

    let nodes_extra_info = if opts.extra {
        fetch_extra_info(nodes_configuration).await?
    } else {
        HashMap::new()
    };

    c_println!("Node Configuration ({})", nodes_configuration.version());

    let mut nodes_table = Table::new_styled();
    let mut header = vec!["NODE", "GEN", "NAME", "ADDRESS", "ROLES"];
    if opts.extra {
        header.extend(vec![
            "LOCATION",
            "STORAGE-STATE",
            "WORKER-STATE",
            "UPTIME",
            "STATUS",
            "ADMIN",
            "WORKER",
            "LOG-SERVER",
            "META",
            "NODES",
            "LOGS",
            "SCHEMA",
            "P-TABLE",
        ]);
    }
    nodes_table.set_styled_header(header);

    for (node_id, node_config) in nodes {
        let mut node_row = vec![
            Cell::new(node_id.to_string()),
            Cell::new(node_config.current_generation.generation().to_string()),
            Cell::new(node_config.name.clone()),
            Cell::new(node_config.address.to_string()),
            Cell::new(
                node_config
                    .roles
                    .iter()
                    .map(|r| r.to_string())
                    .sorted()
                    .collect::<Vec<_>>()
                    .join(" | "),
            ),
        ];

        if opts.extra {
            node_row.extend(vec![
                Cell::new(node_config.location.clone()),
                Cell::new(node_config.log_server_config.storage_state),
                Cell::new(node_config.worker_config.worker_state),
            ]);
            node_row.extend(render_optional_columns(
                nodes_extra_info.get(&node_config.address),
                render_ident_extras,
            ));
        }
        nodes_table.add_row(node_row);
    }
    c_println!("{}", nodes_table);

    Ok(())
}

pub fn list_nodes_lite<P: ListenerPort>(
    ident_responses: &HashMap<AdvertisedAddress<P>, IdentResponse>,
    opts: &ListNodesOpts,
) {
    let mut nodes_table = Table::new_styled();
    let mut header = vec!["NODE", "GEN", "NAME", "ADDRESS", "ROLES"];
    if opts.extra {
        header.extend(vec![
            "UPTIME",
            "STATUS",
            "ADMIN",
            "WORKER",
            "LOG-SERVER",
            "META",
            "NODES",
            "LOGS",
            "SCHEMA",
            "P-TABLE",
        ]);
    }
    nodes_table.set_styled_header(header);

    for (address, ident) in ident_responses
        .iter()
        .sorted_by_key(|&(addr, _)| addr.to_string())
    {
        let mut node_row = vec![
            Cell::new(
                ident
                    .node_id
                    .map(|id| format!("{}", id.id))
                    .unwrap_or("n/a".to_owned()),
            ),
            Cell::new(
                ident
                    .node_id
                    .and_then(|id| id.generation)
                    .map(|generation| format!("{generation}"))
                    .unwrap_or("".to_owned()),
            ),
            Cell::new("-"),
            Cell::new(address.to_string()),
            Cell::new(
                ident
                    .roles
                    .iter()
                    .map(|r| r.to_string())
                    .sorted()
                    .collect::<Vec<_>>()
                    .join(" | "),
            ),
        ];
        if opts.extra {
            node_row.extend(render_ident_extras(ident));
        }
        nodes_table.add_row(node_row);
    }

    c_println!(
        "The cluster metadata service was unavailable but the following node(s) from the address list responded directly"
    );
    c_println!("{}", nodes_table);
}

fn render_optional_columns<F>(ident_response: Option<&IdentResponse>, f: F) -> Vec<Cell>
where
    F: FnOnce(&IdentResponse) -> Vec<Cell>,
{
    ident_response
        .map(f)
        .unwrap_or(vec![Cell::new("N/A"), Cell::new("Unknown")])
}

fn render_ident_extras(ident_response: &IdentResponse) -> Vec<Cell> {
    vec![
        duration_to_human_rough(
            TimeDelta::seconds(ident_response.age_s as i64),
            Tense::Present,
        ),
        // We reuse the prost-generated Debug formatting but cut out the "Unknown"s
        format!("{:?}", ident_response.status()).replace("Unknown", "-"),
        format!("{:?}", ident_response.admin_status()).replace("Unknown", "-"),
        format!("{:?}", ident_response.worker_status()).replace("Unknown", "-"),
        format!("{:?}", ident_response.log_server_status()).replace("Unknown", "-"),
        format!("{:?}", ident_response.metadata_server_status()).replace("Unknown", "-"),
        format!("v{}", ident_response.nodes_config_version),
        format!("v{}", ident_response.logs_version),
        format!("v{}", ident_response.schema_version),
        format!("v{}", ident_response.partition_table_version),
    ]
    .into_iter()
    .map(Cell::new)
    .collect()
}

async fn fetch_extra_info(
    nodes_configuration: &NodesConfiguration,
) -> anyhow::Result<HashMap<AdvertisedAddress<FabricPort>, IdentResponse>> {
    let mut get_ident_tasks =
        JoinSet::<anyhow::Result<(AdvertisedAddress<FabricPort>, IdentResponse)>>::new();

    for (node_id, node_config) in nodes_configuration.iter() {
        let address = node_config.address.clone();
        let get_ident = async move {
            let channel = grpc_channel(address.clone());
            let mut node_ctl_svc_client = new_node_ctl_client(channel, &CliContext::get().network);

            let ident_response = node_ctl_svc_client.get_ident(()).await?.into_inner();
            Ok((address, ident_response))
        };

        get_ident_tasks.spawn(async move {
            tokio::time::timeout(GET_IDENT_TIMEOUT_1S, get_ident)
                .await
                .context(format!("timeout calling {node_id}"))?
        });
    }

    let mut ident_responses = HashMap::new();
    while let Some(result) = get_ident_tasks.join_next().await {
        match result {
            Ok(Ok((address, ident_response))) => {
                ident_responses.insert(address, ident_response);
            }
            Ok(Err(err)) => {
                info!("get_ident error: {}", err)
            }
            Err(err) => {
                info!("get_ident failed: {}", err)
            }
        }
    }

    Ok(ident_responses)
}
