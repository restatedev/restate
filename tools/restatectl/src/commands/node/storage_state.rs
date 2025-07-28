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

use crate::commands::node::update_state;
use crate::connection::ConnectionInfo;
use restate_cli_util::c_println;
use restate_metadata_store::MetadataStoreClient;
use restate_metadata_store::protobuf::metadata_proxy_svc::client::MetadataStoreProxy;
use restate_types::config::MetadataClientOptions;
use restate_types::logs::metadata::ProviderKind;
use restate_types::nodes_config::{Role, StorageState};
use restate_types::replicated_loglet::logserver_candidate_filter;
use restate_types::replication::{NodeSetChecker, NodeSetSelector, NodeSetSelectorOptions};
use restate_types::{GenerationalNodeId, PlainNodeId};

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
    if !opts.force && !opts.storage_state.should_write_to() {
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

            update_state(
                &metadata_store_client,
                &current_states,
                opts.storage_state,
                |node_config| &mut node_config.log_server_config.storage_state,
            )
            .await
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
