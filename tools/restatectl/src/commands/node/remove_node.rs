// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use chrono::TimeDelta;
use cling::prelude::*;
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::ClusterStateRequest;
use restate_cli_util::ui::console::confirm_or_exit;
use restate_cli_util::ui::{duration_to_human_rough, Tense};
use restate_cli_util::{c_error, c_println};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_core::protobuf::node_ctl_svc::RemoveNodeRequest;
use restate_types::nodes_config::Role;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::protobuf::cluster::DeadNode;
use restate_types::PlainNodeId;

use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "remove_node")]
pub struct RemoveNodeOpts {
    /// The node id to remove from the cluster. Remove permanently decommissioned nodes from the
    /// cluster so that they are no longer reported as dead.
    #[arg(long)]
    pub(crate) node: PlainNodeId,
}

pub async fn remove_node(connection: &ConnectionInfo, opts: &RemoveNodeOpts) -> anyhow::Result<()> {
    let cluster_state = connection
        .try_each(Some(Role::Admin), |channel| async {
            let mut client = ClusterCtrlSvcClient::new(channel)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);

            client
                .get_cluster_state(ClusterStateRequest::default())
                .await
        })
        .await?
        .into_inner()
        .cluster_state
        .ok_or_else(|| anyhow::anyhow!("no cluster state returned"))?;

    let nodes_configuration = connection.get_nodes_configuration().await?;

    let nodes = nodes_configuration.iter().collect::<BTreeMap<_, _>>();
    let Some(node) = nodes.get(&opts.node) else {
        c_error!("Node {} not found in cluster", opts.node);
        return Ok(());
    };

    let last_seen = match cluster_state
        .nodes
        .get(&opts.node.into())
        .and_then(|n| n.state.as_ref())
    {
        Some(State::Dead(DeadNode { last_seen_alive })) => last_seen_alive,
        None => {
            c_error!(
                "The was found in the cluster nodes list but not reported in the cluster status. \
                Please try again.",
            );
            anyhow::bail!("Refusing to operate on node {}", opts.node)
        }
        Some(_) => {
            c_error!(
                "Node {} ({}) is not reported as dead and cannot be removed. Use this tool to permanently \
                remove decommissioned nodes from cluster configuration.",
                opts.node,
                node.name,
            );
            anyhow::bail!("Node {} is not dead and cannot be removed", opts.node)
        }
    };

    confirm_or_exit(&format!(
        "Are you sure you want to remove node {} ({}) from the cluster?{}",
        opts.node,
        node.name,
        last_seen
            .and_then(|ts| TimeDelta::new(
                ts.seconds,
                u32::try_from(ts.nanos).expect("i32 fits into u32")
            ))
            .map(|delta| format!(
                " The node was last seen by the cluster {}.",
                duration_to_human_rough(delta, Tense::Past),
            ))
            .unwrap_or_else(|| "".to_string())
    ))?;

    let address = connection.addresses.first().expect("address");
    let channel = grpc_channel(address.clone());

    let mut node_ctl_svc_client = NodeCtlSvcClient::new(channel)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

    node_ctl_svc_client
        .remove_node(RemoveNodeRequest {
            node_id: opts.node.into(),
            nodes_config_version: cluster_state
                .nodes_config_version
                .expect("nodes_config_version")
                .value,
        })
        .await
        .map_err(|e| {
            // we ignore individual errors in table rendering so this is the only place to log them
            c_println!("failed to remove node {}: {:?}", opts.node, e);
            anyhow::anyhow!(e)
        })?
        .into_inner();
    Ok(())
}
