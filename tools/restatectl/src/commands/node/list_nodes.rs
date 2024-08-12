// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use anyhow::Context;
use cling::prelude::*;
use itertools::Itertools;
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::ListNodesRequest;
use restate_cli_util::_comfy_table::{Cell, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::storage::StorageCodec;

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "nodes")]
#[cling(run = "list_nodes")]
pub struct ListNodesOpts {}

async fn list_nodes(connection: &ConnectionInfo, _opts: &ListNodesOpts) -> anyhow::Result<()> {
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

    c_title!(
        "📋️",
        format!("Configuration {}", nodes_configuration.version())
    );

    let mut nodes_table = Table::new_styled();
    nodes_table.set_styled_header(vec!["NODE", "GEN", "NAME", "ADDRESS", "ROLES"]);
    for (node_id, node_config) in nodes {
        let node_row = vec![
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
        nodes_table.add_row(node_row);
    }
    c_println!("{}", nodes_table);

    Ok(())
}
