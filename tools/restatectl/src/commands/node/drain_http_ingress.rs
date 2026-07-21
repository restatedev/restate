// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use cling::prelude::*;

use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::node_ctl_svc::{
    DrainHttpIngressRequest, HttpIngressDrainStatus, new_node_ctl_client,
};
use restate_types::GenerationalNodeId;
use restate_types::net::address::{AdvertisedAddress, FabricPort};

use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "drain_http_ingress")]
/// Starts or polls the lifetime-scoped HTTP ingress drain on one node.
///
/// Use `--single-address http://127.0.0.1:5122` to address the local node directly without
/// reading the nodes configuration. This is the recommended mode for a preStop hook.
pub struct DrainHttpIngressOpts {
    /// The exact node generation to drain (for example N1:9)
    #[arg(long)]
    node: GenerationalNodeId,

    /// Print the response as JSON
    #[arg(long)]
    json: bool,
}

async fn drain_http_ingress(
    connection: &ConnectionInfo,
    opts: &DrainHttpIngressOpts,
) -> anyhow::Result<()> {
    let address = target_address(connection, opts.node).await?;
    let channel = grpc_channel(address);
    let mut client = new_node_ctl_client(channel, &CliContext::get().network);
    let request = DrainHttpIngressRequest {
        expected_node_id: Some(opts.node.into()),
    };

    let timeout = CliContext::get().request_timeout();
    let response = tokio::time::timeout(timeout, client.drain_http_ingress(request))
        .await
        .map_err(|_| anyhow::anyhow!("HTTP ingress drain request timed out after {timeout:?}"))??
        .into_inner();
    let status = HttpIngressDrainStatus::try_from(response.status)
        .unwrap_or(HttpIngressDrainStatus::Unspecified);
    let status = status_name(status);

    if opts.json {
        c_println!(
            "{}",
            serde_json::json!({
                "node_id": response.node_id.map(|node_id| node_id.to_string()),
                "status": status,
                "in_flight_requests": response.in_flight_requests,
                "in_flight_connections": response.in_flight_connections,
            })
        );
    } else {
        c_println!(
            "{} HTTP ingress: {}; in-flight requests: {}; in-flight connections: {}",
            opts.node,
            status,
            response.in_flight_requests,
            response.in_flight_connections,
        );
    }

    Ok(())
}

async fn target_address(
    connection: &ConnectionInfo,
    node_id: GenerationalNodeId,
) -> anyhow::Result<AdvertisedAddress<FabricPort>> {
    if let Some(address) = &connection.single_address {
        return Ok(address.clone());
    }

    let nodes_config = connection
        .get_nodes_configuration()
        .await
        .context("failed to discover the target node; use --single-address to connect directly")?;
    Ok(nodes_config.find_node_by_id(node_id)?.address.clone())
}

fn status_name(status: HttpIngressDrainStatus) -> &'static str {
    match status {
        HttpIngressDrainStatus::NotPresent => "not-present",
        HttpIngressDrainStatus::Active => "active",
        HttpIngressDrainStatus::Draining => "draining",
        HttpIngressDrainStatus::Drained => "drained",
        HttpIngressDrainStatus::Unspecified => "unspecified",
    }
}
