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
use itertools::Itertools;

use restate_cli_util::{CliContext, c_println};
use restate_metadata_store::protobuf::metadata_proxy_svc::{
    PutRequest, client::new_metadata_proxy_client,
};
use restate_metadata_store::serialize_value;
use restate_types::PlainNodeId;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;

use crate::commands::node::disable_node_checker::DisableNodeChecker;
use crate::connection::ConnectionInfo;

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
    let mut nodes_configuration = connection.get_nodes_configuration().await?;
    let logs = connection.get_logs().await?;

    let disable_node_checker = DisableNodeChecker::new(&nodes_configuration, &logs);

    for node_id in &opts.nodes {
        disable_node_checker
            .safe_to_disable_node(*node_id)
            .context("It is not safe to disable node {node_id}")?
    }

    let precondition = Precondition::MatchesVersion(nodes_configuration.version());

    for node_id in &opts.nodes {
        nodes_configuration.remove_node_unchecked(*node_id);
    }

    nodes_configuration.increment_version();

    let request = PutRequest {
        key: NODES_CONFIG_KEY.to_string(),
        precondition: Some(precondition.into()),
        value: Some(serialize_value(&nodes_configuration)?.into()),
    };

    connection
        .try_each(None, |channel| async {
            new_metadata_proxy_client(channel, &CliContext::get().network)
                .put(request.clone())
                .await
        })
        .await?;

    c_println!(
        "Successfully removed nodes [{}]",
        opts.nodes.iter().join(",")
    );

    Ok(())
}
