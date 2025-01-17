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
use crate::util::grpc_channel;
use clap::Parser;
use cling::{Collect, Run};
use itertools::Itertools;
use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_core::protobuf::node_ctl_svc::GetMetadataRequest;
use restate_metadata_store::grpc_svc::metadata_store_svc_client::MetadataStoreSvcClient;
use restate_metadata_store::MemberId;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{NodesConfiguration, Role};
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::storage::StorageCodec;
use std::collections::BTreeMap;
use tonic::codec::CompressionEncoding;
use tonic::IntoRequest;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "status")]
pub struct StatusOpts {}

async fn status(connection: &ConnectionInfo) -> anyhow::Result<()> {
    let channel = grpc_channel(connection.cluster_controller.clone());
    let mut client = NodeCtlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);
    let req = GetMetadataRequest {
        kind: MetadataKind::NodesConfiguration.into(),
        sync: false,
    };
    let mut response = client.get_metadata(req).await?.into_inner();
    let nodes_configuration = StorageCodec::decode::<NodesConfiguration, _>(&mut response.encoded)?;

    let mut metadata_nodes_table = Table::new_styled();
    let header = vec!["NODE", "STATUS", "CONFIG-ID", "LEADER", "MEMBERS"];
    metadata_nodes_table.set_styled_header(header);

    let mut unreachable_nodes = BTreeMap::default();

    for (node_id, node_config) in nodes_configuration.iter() {
        if node_config.roles.contains(Role::MetadataServer) {
            let metadata_channel = grpc_channel(node_config.address.clone());
            let mut metadata_client = MetadataStoreSvcClient::new(metadata_channel)
                .accept_compressed(CompressionEncoding::Gzip);

            let metadata_store_status = metadata_client.status(().into_request()).await;

            let status = match metadata_store_status {
                Ok(response) => response.into_inner(),
                Err(err) => {
                    unreachable_nodes.insert(node_id, err.to_string());
                    continue;
                }
            };

            metadata_nodes_table.add_row(vec![
                Cell::new(node_id),
                render_metadata_server_status(status.status()),
                Cell::new(
                    status
                        .configuration
                        .as_ref()
                        .map(|config| config.id.to_string())
                        .unwrap_or("-".to_owned()),
                ),
                Cell::new(
                    status
                        .leader
                        .map(|member_id| MemberId::from(member_id).to_string())
                        .unwrap_or("-".to_owned()),
                ),
                Cell::new(
                    status
                        .configuration
                        .map(|config| {
                            format!(
                                "[{}]",
                                config
                                    .members
                                    .into_iter()
                                    .map(MemberId::from)
                                    .map(|member_id| member_id.to_string())
                                    .join(",")
                            )
                        })
                        .unwrap_or("[]".to_owned()),
                ),
            ]);
        }
    }

    c_println!("{}", metadata_nodes_table);

    if !unreachable_nodes.is_empty() {
        c_println!();
        c_println!("🔌 Unreachable nodes");
        let mut unreachable_nodes_table = Table::new_styled();
        unreachable_nodes_table.set_styled_header(vec!["NODE", "REASON"]);

        for (node_id, reason) in unreachable_nodes {
            unreachable_nodes_table.add_row(vec![Cell::new(node_id), Cell::new(reason)]);
        }

        c_println!("{}", unreachable_nodes_table);
    }

    Ok(())
}

fn render_metadata_server_status(metadata_server_status: MetadataServerStatus) -> Cell {
    match metadata_server_status {
        MetadataServerStatus::Unknown => Cell::new("UNKNOWN").fg(Color::Red),
        MetadataServerStatus::StartingUp => Cell::new("Starting").fg(Color::Yellow),
        MetadataServerStatus::AwaitingProvisioning => Cell::new("Provisioning"),
        MetadataServerStatus::Active => Cell::new("Active").fg(Color::Green),
        MetadataServerStatus::Passive => Cell::new("Passive"),
    }
}
