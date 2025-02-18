// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;

use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_core::metadata_store::{retry_on_retryable_error, ReadWriteError};
use restate_metadata_server::{create_client, MetadataStoreClient};
use restate_types::config::{CommonOptions, MetadataClientKind, MetadataClientOptions};
use restate_types::logs::metadata::ProviderKind;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{NodesConfiguration, Role, StorageState};
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::{GenerationalNodeId, PlainNodeId, Versioned};

use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "list_servers")]
#[clap(visible_alias = "servers")]
pub struct ListServersOpts;

// note 1: this is until we have a way to proxy metadata requests through nodes
// note 2: currently supports replicated metadata only, node must be one of metadata nodes
#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "set_storage_state")]
pub struct SetOpts {
    /// The node Id of the log-server
    #[arg(long)]
    node_id: PlainNodeId,
    #[arg(long)]
    storage_state: StorageState,
    /// [dangerous] ignore safety checks and force the update. Note that this might cause cluster unavailability or data loss.
    #[arg(long)]
    force: bool,
}

fn render_storage_state(state: StorageState) -> Cell {
    let cell = Cell::new(state);
    match state {
        StorageState::ReadWrite => cell.fg(Color::Green),
        StorageState::ReadOnly => cell.fg(Color::Yellow),
        StorageState::DataLoss => cell.fg(Color::Red),
        StorageState::Provisioning => cell.fg(Color::Reset),
        StorageState::Disabled => cell.fg(Color::Grey),
    }
}

async fn list_servers(connection: &ConnectionInfo) -> anyhow::Result<()> {
    let nodes_config = connection.get_nodes_configuration().await?;
    let logs = connection.get_logs().await?;

    let mut servers_table = Table::new_styled();
    let header = vec![
        "NODE",
        "GEN",
        "STORAGE-STATE",
        "HISTORICAL LOGLETS",
        "ACTIVE LOGLETS",
    ];
    servers_table.set_styled_header(header);
    for (node_id, config) in nodes_config.iter_role(Role::LogServer) {
        let count_loglets = logs
            .iter_replicated_loglets()
            .filter(|(_, loglet)| loglet.params.nodeset.contains(node_id))
            .count();
        let count_active = logs
            .iter_writeable()
            .filter(|(_, segment)| {
                if segment.config.kind != ProviderKind::Replicated {
                    return false;
                }
                let params =
                    ReplicatedLogletParams::deserialize_from(segment.config.params.as_bytes())
                        .expect("loglet config is deserializable");
                params.nodeset.contains(node_id)
            })
            .count();
        servers_table.add_row(vec![
            Cell::new(node_id.to_string()),
            Cell::new(config.current_generation.to_string()),
            render_storage_state(config.log_server_config.storage_state),
            Cell::new(count_loglets),
            Cell::new(count_active),
        ]);
    }
    c_println!("Node configuration {}", nodes_config.version());
    c_println!("Log chain {}", logs.version());
    c_println!("{}", servers_table);

    Ok(())
}

async fn set_storage_state(connection: &ConnectionInfo, opts: &SetOpts) -> anyhow::Result<()> {
    let nodes_config = connection.get_nodes_configuration().await?;

    // find metadata nodes
    let addresses: Vec<_> = nodes_config
        .iter_role(Role::MetadataServer)
        .map(|(_, config)| config.address.clone())
        .collect();
    if addresses.is_empty() {
        return Err(anyhow::anyhow!(
            "No nodes are configured to run metadata-server role, this command only \
             supports replicated metadata deployment"
        ));
    }

    let metadata_store_client_options = MetadataClientOptions {
        kind: MetadataClientKind::Replicated { addresses },
        ..Default::default()
    };

    let metadata_client = create_client(metadata_store_client_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create metadata store client: {}", e))?;

    let node = nodes_config.find_node_by_id(opts.node_id)?;
    let current_state = node.log_server_config.storage_state;
    let current_generation = node.current_generation;

    if !opts.force {
        if !node.has_role(Role::LogServer) {
            return Err(anyhow::anyhow!(
                "Node {} doesn't have `log-server` role. Its last-observed generation {} has roles: [{}]",
                opts.node_id,
                current_generation,
                node.roles,
            ));
        }

        let safe = match (current_state, opts.storage_state) {
            (from, to) if from == to => {
                c_println!(
                    "Node {} storage-state was already {}",
                    opts.node_id,
                    current_state
                );
                return Ok(());
            }
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
                // todo (now): check if we can generate nodesets without this node.
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

    let new_state = update_storage_state(
        &metadata_client,
        current_generation,
        current_state,
        opts.storage_state,
    )
    .await?;
    c_println!(
        "Node {} storage-state updated from {} to {}",
        opts.node_id,
        current_state,
        new_state
    );

    Ok(())
}

async fn update_storage_state(
    metadata_client: &MetadataStoreClient,
    my_node_id: GenerationalNodeId,
    expected_state: StorageState,
    target_state: StorageState,
) -> anyhow::Result<StorageState> {
    let retry_policy = CommonOptions::default().network_error_retry_policy;
    let mut first_attempt = true;

    retry_on_retryable_error(retry_policy, || {
        metadata_client.read_modify_write(
            NODES_CONFIG_KEY.clone(),
            move |nodes_config: Option<NodesConfiguration>| {
                let mut nodes_config =
                    nodes_config.ok_or(StorageStateUpdateError::MissingNodesConfiguration)?;
                // If this fails, it means that a newer node has started somewhere else, and we
                // should not attempt to update the storage-state. Instead, we fail.
                let mut node = nodes_config
                    // note that we find by the generational node id.
                    .find_node_by_id(my_node_id)?
                    .clone();

                if node.log_server_config.storage_state != expected_state {
                    return if first_attempt {
                        // Something might have caused this state to change.
                        Err(StorageStateUpdateError::NotInExpectedState(
                            node.log_server_config.storage_state,
                        ))
                    } else {
                        // If we end up here, then we must have changed the StorageState in a previous attempt.
                        // It cannot happen that there is a newer generation of me that changed the StorageState,
                        // because then I would have failed before when retrieving my NodeConfig with my generational
                        // node id.
                        Err(StorageStateUpdateError::PreviousAttemptSucceeded(
                            nodes_config,
                        ))
                    };
                }

                node.log_server_config.storage_state = target_state;

                first_attempt = false;
                nodes_config.upsert_node(node);
                nodes_config.increment_version();
                Ok(nodes_config)
            },
        )
    })
    .await?;

    Ok(target_state)
}

#[derive(Debug, thiserror::Error)]
enum StorageStateUpdateError {
    #[error("cluster must be provisioned before log-server is started")]
    MissingNodesConfiguration,
    #[error(transparent)]
    NodesConfigError(#[from] restate_types::nodes_config::NodesConfigError),
    #[error("log-server found an unexpected storage-state '{0}' in metadata store, this could mean that another node has updated it")]
    NotInExpectedState(StorageState),
    #[error("succeeded updating NodesConfiguration in a previous attempt")]
    PreviousAttemptSucceeded(NodesConfiguration),
    #[error(transparent)]
    MetadataStore(#[from] ReadWriteError),
}
