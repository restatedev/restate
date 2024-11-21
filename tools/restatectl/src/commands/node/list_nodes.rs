// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::ListNodesRequest;
use restate_cli_util::_comfy_table::{Cell, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::{duration_to_human_rough, Tense};
use restate_core::network::protobuf::node_svc::node_svc_client::NodeSvcClient;
use restate_core::network::protobuf::node_svc::IdentResponse;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::storage::StorageCodec;
use restate_types::PlainNodeId;

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

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
    let channel = grpc_connect(connection.cluster_controller.clone())
        .await
        .with_context(|| {
            format!(
                "cannot connect to cluster controller at {}",
                connection.cluster_controller
            )
        })?;
    let mut client =
        ClusterCtrlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let mut response = client
        .list_nodes(ListNodesRequest::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list nodes: {:?}", e))?
        .into_inner();

    let nodes_configuration =
        StorageCodec::decode::<NodesConfiguration, _>(&mut response.nodes_configuration)?;
    let nodes = nodes_configuration.iter().collect::<BTreeMap<_, _>>();

    let nodes_extra_info = if opts.extra {
        fetch_extra_info(&nodes_configuration).await?
    } else {
        HashMap::new()
    };

    c_println!("Node Configuration ({})", nodes_configuration.version());

    let mut nodes_table = Table::new_styled();
    let mut header = vec!["NODE", "GEN", "NAME", "ADDRESS", "ROLES"];
    if opts.extra {
        header.extend(vec![
            "UPTIME", "STATUS", "ADMIN", "WORKER", "LOG-SVR", "META", "NODES", "LOGS", "SCHEMA",
            "PRTNS",
        ]);
    }
    nodes_table.set_styled_header(header);

    for (node_id, node_config) in nodes.clone() {
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
            node_row.extend(render_extra_columns(
                nodes_extra_info.get(&node_id),
                |ident_response| {
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
                        format!("{:?}", ident_response.metadata_server_status())
                            .replace("Unknown", "-"),
                        format!("v{}", ident_response.nodes_config_version),
                        format!("v{}", ident_response.logs_version),
                        format!("v{}", ident_response.schema_version),
                        format!("v{}", ident_response.partition_table_version),
                    ]
                    .iter()
                    .map(Cell::new)
                    .collect()
                },
            ));
        }
        nodes_table.add_row(node_row);
    }
    c_println!("{}", nodes_table);

    Ok(())
}

fn render_extra_columns<F>(ident_response: Option<&IdentResponse>, f: F) -> Vec<Cell>
where
    F: FnOnce(&IdentResponse) -> Vec<Cell>,
{
    ident_response
        .map(f)
        .unwrap_or(vec![Cell::new("N/A"), Cell::new("Unknown")])
}

async fn fetch_extra_info(
    nodes_configuration: &NodesConfiguration,
) -> anyhow::Result<HashMap<PlainNodeId, IdentResponse>> {
    let mut get_ident_tasks = JoinSet::<anyhow::Result<IdentResponse>>::new();

    for (node_id, node_config) in nodes_configuration.iter() {
        let address = node_config.address.clone();
        let get_ident = async move {
            let node_channel = grpc_connect(address).await?;
            let mut node_svc_client =
                NodeSvcClient::new(node_channel).accept_compressed(CompressionEncoding::Gzip);

            Ok(node_svc_client
                .get_ident(())
                .await
                .map_err(|e| {
                    // we ignore individual errors in table rendering so this is the only place to log them
                    c_println!("failed to call GetIdent for node {}: {:?}", node_id, e);
                    anyhow::anyhow!(e)
                })?
                .into_inner())
        };
        get_ident_tasks.spawn(async move {
            match tokio::time::timeout(GET_IDENT_TIMEOUT_1S, get_ident).await {
                Ok(res) => res,
                Err(e) => {
                    c_println!("timeout calling GetIdent for node {}", node_id);
                    Err(e.into())
                }
            }
        });
    }

    let mut ident_responses = HashMap::new();
    while let Some(Ok(Ok(ident_response))) = get_ident_tasks.join_next().await {
        ident_responses.insert(
            PlainNodeId::from(ident_response.node_id.expect("has node_id").id),
            ident_response,
        );
    }

    Ok(ident_responses)
}
