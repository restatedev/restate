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

use anyhow::Context;
use cling::prelude::*;
use restate_core::protobuf::metadata_proxy_svc::MetadataStoreProxy;
use restate_metadata_server::MetadataStoreClient;

use restate_bifrost::providers::replicated_loglet::logserver_candidate_filter;
use restate_bifrost::providers::replicated_loglet::replication::NodeSetChecker;
use restate_cli_util::c_println;
use restate_types::config::MetadataClientOptions;
use restate_types::logs::metadata::ProviderKind;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{NodesConfiguration, Role, StorageState};
use restate_types::replication::{NodeSetSelector, NodeSetSelectorOptions};
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::connection::{ConnectionInfo, NodeOperationError};

// note 1: this is until we have a way to proxy metadata requests through nodes
// note 2: currently supports replicated metadata only, node must be one of metadata nodes
#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "set_storage_state")]
pub struct SetOpts {
    /// A node-id or a list of node-ids (comma-separated) to update
    #[arg(long, required = true, visible_alias = "node", value_delimiter = ',')]
    nodes: Vec<PlainNodeId>,
    #[arg(long)]
    storage_state: StorageState,
    /// [dangerous] ignore safety checks and force the update. Note that this might cause cluster unavailability or data loss.
    #[arg(long)]
    force: bool,
}

async fn set_storage_state(connection: &ConnectionInfo, opts: &SetOpts) -> anyhow::Result<()> {
    if opts.nodes.is_empty() {
        return Err(anyhow::anyhow!("--node/--nodes is required"));
    }

    let nodes_config = connection.get_nodes_configuration().await?;

    let mut current_states: HashMap<GenerationalNodeId, StorageState> =
        HashMap::with_capacity(opts.nodes.len());
    for node_id in opts.nodes.iter().cloned() {
        let node = nodes_config.find_node_by_id(node_id)?;
        let current_state = node.log_server_config.storage_state;
        let current_generation = node.current_generation;
        current_states.insert(current_generation, current_state);

        if !opts.force {
            if !node.has_role(Role::LogServer) {
                return Err(anyhow::anyhow!(
                    "Node {} doesn't have `log-server` role. Its last-observed generation {} has roles: [{}]",
                    node_id,
                    current_generation,
                    node.roles,
                ));
            }

            let safe = match (current_state, opts.storage_state) {
                (from, to) if from == to => true,
                (StorageState::Provisioning, StorageState::Disabled) => {
                    // one can do that, but the node will never be a log-server in the future
                    true
                }
                (StorageState::Provisioning, _) => false,
                (StorageState::Disabled, StorageState::Provisioning) => {
                    // technically, this can be possible if the node is not in any historical nodeset,
                    // todo: perform this check to allow those operations to take place.
                    false
                }
                // all transitions from disable -> anything other than provisioning are not allowed.
                (StorageState::Disabled, _) => false,
                (StorageState::ReadOnly, StorageState::ReadWrite) => true,
                (StorageState::ReadWrite, StorageState::ReadOnly) => {
                    // drain.
                    // We should allow this only if we won't lose ability to generate nodesets with the
                    // remaining set of writeable nodes
                    true
                }
                (StorageState::ReadOnly, StorageState::Disabled) => {
                    // only possible if this node:
                    // - removed from all nodesets (chains trimmed)
                    false
                }
                (StorageState::ReadOnly, _) => false,
                (StorageState::ReadWrite, StorageState::Provisioning) => false,
                (StorageState::ReadWrite, _) => false,
                (StorageState::DataLoss, StorageState::ReadWrite) => {
                    // that's only possible if we are okay with either:
                    // - declaring permanent data-loss of data on this node
                    // - the node transitioned into data-loss, but we didn't actually lose data.
                    false
                }
                (StorageState::DataLoss, StorageState::Provisioning) => false,
                (StorageState::DataLoss, StorageState::ReadOnly) => {
                    // silent under-replication
                    false
                }
                (StorageState::DataLoss, StorageState::Disabled) => {
                    // only if data were trimmed
                    false
                }
                (_, _) => false,
            };

            if !safe {
                return Err(anyhow::anyhow!(
                    "This node is currently in `{current_state}` storage-state. Transitioning into `{}` is unsafe.",
                    opts.storage_state
                ));
            }
        }
    }

    // run safety checks
    if !opts.force && !opts.storage_state.can_write_to() {
        let logs = connection.get_logs().await?;
        let provider = &logs.configuration().default_provider;
        if let ProviderKind::Replicated = provider.kind() {
            // any log-id would do.
            let options = NodeSetSelectorOptions::new(0)
                .with_target_size(provider.target_nodeset_size().unwrap());
            let replication_property = provider.replication().unwrap();
            let nodeset = NodeSetSelector::select(
                &nodes_config,
                replication_property,
                |node_id, config| {
                    // skip our selection of nodes from being candidate
                    if opts.nodes.contains(&node_id) {
                        false
                    } else {
                        logserver_candidate_filter(node_id, config)
                    }
                },
                |_, config| {
                    matches!(
                        config.log_server_config.storage_state,
                        StorageState::ReadWrite
                    )
                },
                options,
            )
            .context("Safety-check failed: lost the ability to generate nodesets")?;

            let mut checker = NodeSetChecker::new(&nodeset, &nodes_config, replication_property);
            checker.fill_with(true);
            if !checker.check_write_quorum(|attr| *attr) {
                anyhow::bail!(
                    "Safety-check failed: nodesets like {checker} will not be able to satisfy replication property"
                );
            }
        }
    }

    let backoff_policy = &MetadataClientOptions::default().backoff_policy;

    connection
        .try_each(None, |channel| async {
            let metadata_store_proxy = MetadataStoreProxy::new(channel);
            let metadata_store_client =
                MetadataStoreClient::new(metadata_store_proxy, Some(backoff_policy.clone()));

            update_storage_state(&metadata_store_client, &current_states, opts.storage_state).await
        })
        .await?;

    for (node, old_state) in current_states {
        c_println!(
            "{} storage-state has been updated from {} to {}",
            node.as_plain(),
            old_state,
            opts.storage_state,
        );
    }
    Ok(())
}

async fn update_storage_state(
    metadata_client: &MetadataStoreClient,
    nodes: &HashMap<GenerationalNodeId, StorageState>,
    target_state: StorageState,
) -> Result<(), NodeOperationError> {
    metadata_client
        .read_modify_write(
            NODES_CONFIG_KEY.clone(),
            move |nodes_config: Option<NodesConfiguration>| {
                let mut nodes_config = nodes_config.context("Missing nodes configuration")?;

                for (my_node_id, expected_state) in nodes {
                    // If this fails, it means that a newer node has started somewhere else, and we
                    // should not attempt to update the storage-state. Instead, we fail.
                    let mut node = nodes_config
                        // note that we find by the generational node id.
                        .find_node_by_id(*my_node_id)?
                        .clone();

                    if node.log_server_config.storage_state != *expected_state {
                        // Something might have caused this state to change.
                        return Err(anyhow::anyhow!(
                            "Node {} has changed it state during the update, expected={}, found={}",
                            my_node_id,
                            expected_state,
                            node.log_server_config.storage_state,
                        ));
                    }

                    node.log_server_config.storage_state = target_state;
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
