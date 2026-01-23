// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use anyhow::bail;
use assert2::let_assert;
use cling::prelude::*;

use restate_cli_util::{CliContext, c_println};
use restate_metadata_store::MetadataStoreClient;
use restate_metadata_store::protobuf::metadata_proxy_svc::client::MetadataStoreProxy;
use restate_types::PlainNodeId;
use restate_types::config::MetadataClientOptions;
use restate_types::nodes_config::{Role, WorkerState};
use restate_types::partitions::{PartitionReplication, worker_candidate_filter};
use restate_types::replication::balanced_spread_selector::{
    BalancedSpreadSelector, SelectorOptions,
};

use crate::commands::node::update_state;
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "set_worker_state")]
pub struct SetOpts {
    /// A node-id or a list of node-ids (comma-separated) to update
    #[arg(long, required = true, visible_alias = "node", value_delimiter = ',')]
    nodes: Vec<PlainNodeId>,
    #[arg(long)]
    worker_state: WorkerState,
    /// [dangerous] ignore safety checks and force the update.
    #[arg(long)]
    force: bool,
}

async fn set_worker_state(connection: &ConnectionInfo, opts: &SetOpts) -> anyhow::Result<()> {
    let nodes_config = connection.get_nodes_configuration().await?;

    let mut current_states = HashMap::new();

    for node_id in &opts.nodes {
        let node = nodes_config.find_node_by_id(node_id)?;
        let current_state = node.worker_config.worker_state;
        let current_generation = node.current_generation;
        current_states.insert(current_generation, current_state);

        if !opts.force {
            if !node.has_role(Role::Worker) {
                bail!(
                    "Node {} doesn't have `worker` role. Its last-observed generation {} has roles: [{}]",
                    node_id,
                    current_generation,
                    node.roles
                );
            }

            let is_safe = match (current_state, opts.worker_state) {
                (current, target) if current == target => true,
                (WorkerState::Provisioning, _) => true,
                // if we are active, then we first need to drain partitions from this node
                (WorkerState::Active, WorkerState::Provisioning | WorkerState::Disabled) => false,
                (WorkerState::Active, WorkerState::Draining) => true,
                // it's ok to start using a draining node again
                (WorkerState::Draining, WorkerState::Active) => true,
                // to safely disable a draining node, we need to make sure that all partitions are drained
                (WorkerState::Draining, WorkerState::Provisioning | WorkerState::Disabled) => false,
                // A drained worker can be activated again
                (WorkerState::Disabled, WorkerState::Active) => true,
                (_, _) => false,
            };

            if !is_safe {
                bail!(
                    "Node {} is in state {}. It's not safe to change its worker-state to {}",
                    node_id,
                    current_state,
                    opts.worker_state
                );
            }
        }
    }

    if !opts.force && opts.worker_state != WorkerState::Active {
        // check whether we can still create new replica sets
        let partition_table = connection.get_partition_table().await?;
        let_assert!(
            PartitionReplication::Limit(partition_replication) = partition_table.replication(),
            "Partition table replication is not a limit, but {:?}. This is not supported for worker-state changes",
            partition_table.replication()
        );

        if BalancedSpreadSelector::select(
            &nodes_config,
            partition_replication,
            |node_id, node_config| {
                if opts.nodes.contains(&node_id) {
                    false
                } else {
                    worker_candidate_filter(node_id, node_config)
                }
            },
            &SelectorOptions::default(),
        )
        .is_err()
        {
            bail!(
                "Safety-check failed: Can no longer create new replica sets with worker-state changes."
            );
        }
    }

    let backoff_policy = &MetadataClientOptions::default().backoff_policy;

    connection
        .try_each(None, |channel| async {
            let metadata_store_proxy = MetadataStoreProxy::new(channel, &CliContext::get().network);
            let metadata_store_client =
                MetadataStoreClient::new(metadata_store_proxy, Some(backoff_policy.clone()));

            update_state(
                &metadata_store_client,
                &current_states,
                opts.worker_state,
                |node_config| &mut node_config.worker_config.worker_state,
            )
            .await
        })
        .await?;

    for (node, old_state) in current_states {
        c_println!(
            "{} worker-state has been updated from {} to {}",
            node,
            old_state,
            opts.worker_state
        );
    }

    Ok(())
}
