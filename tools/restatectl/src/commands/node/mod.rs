// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod disable_node_checker;
pub mod list_nodes;
mod remove_nodes;
mod storage_state;
mod worker_state;

use std::collections::HashMap;
use std::fmt;

use anyhow::Context;
use cling::prelude::*;

use restate_metadata_store::MetadataStoreClient;
use restate_types::GenerationalNodeId;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{NodeConfig, NodesConfiguration};

use crate::connection::NodeOperationError;

#[derive(Run, Subcommand, Clone)]
pub enum Nodes {
    /// Print a summary of the active nodes registered in a cluster
    List(list_nodes::ListNodesOpts),
    /// Removes the given node/s from the cluster. You should only use this command if you are
    /// certain that the specified nodes are no longer part of any node sets, not members of the
    /// metadata cluster nor required to run partition processors.
    Remove(remove_nodes::RemoveNodesOpts),
    /// [dangerous] low-level unprotected log-server's storage-state manipulation
    SetStorageState(storage_state::SetOpts),
    /// Sets the state of a worker
    SetWorkerState(worker_state::SetOpts),
}

/// Updates the state in the [`NodeConfig`] specified by `state_mut` to the target state
/// `target_state` if it meets the expected state `expected_states`.
pub(crate) async fn update_state<T>(
    metadata_client: &MetadataStoreClient,
    expected_states: &HashMap<GenerationalNodeId, T>,
    target_state: T,
    state_mut: impl Fn(&mut NodeConfig) -> &mut T,
) -> Result<(), NodeOperationError>
where
    T: Copy + PartialEq + fmt::Display,
{
    metadata_client
        .read_modify_write(
            NODES_CONFIG_KEY.clone(),
            move |nodes_config: Option<NodesConfiguration>| {
                let mut nodes_config = nodes_config.context("Missing nodes configuration")?;

                for (my_node_id, expected_state) in expected_states {
                    // If this fails, it means that a newer node has started somewhere else, and we
                    // should not attempt to update the state. Instead, we fail.
                    let mut node = nodes_config
                        // note that we find by the generational node id.
                        .find_node_by_id(*my_node_id)?
                        .clone();

                    let actual_state = state_mut(&mut node);

                    if *actual_state != *expected_state {
                        // Something might have caused this state to change.
                        return Err(anyhow::anyhow!(
                            "Node {} has changed it state during the update, expected={}, found={}",
                            my_node_id,
                            expected_state,
                            actual_state,
                        ));
                    }

                    *actual_state = target_state;
                    nodes_config.upsert_node(node);
                }
                nodes_config.increment_version();
                Ok(nodes_config)
            },
        )
        .await
        .map(|_| ())
        .map_err(Into::into)
}
