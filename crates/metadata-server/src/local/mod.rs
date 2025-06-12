// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::info;

use restate_metadata_store::serialize_value;
use restate_types::PlainNodeId;
use restate_types::config::Configuration;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration};
use restate_types::storage::{StorageCodec, StorageDecodeError, StorageEncodeError};

use crate::RequestError;
use crate::local::storage::RocksDbStorage;

pub mod storage;

const DATA_DIR: &str = "local-metadata-store";
const DB_NAME: &str = "local-metadata-store";
const KV_PAIRS: &str = "kv_pairs";
const SEALED_KEY: &str = "##restate_internal_sealed##";

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("failed reading/writing: {0}")]
    ReadWrite(#[from] RequestError),
    #[error("encode error: {0}")]
    Encode(#[from] StorageEncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] StorageDecodeError),
    #[error("cannot auto migrate a multi node cluster which uses the local metadata server")]
    MultiNodeCluster,
}

pub async fn migrate_nodes_configuration(
    storage: &mut RocksDbStorage,
) -> Result<(), MigrationError> {
    let value = storage.get(&NODES_CONFIG_KEY)?;

    if value.is_none() {
        // nothing to migrate
        return Ok(());
    };

    let value = value.unwrap();
    let mut bytes = value.value.as_ref();

    let mut nodes_configuration = StorageCodec::decode::<NodesConfiguration, _>(&mut bytes)?;

    let mut modified = false;

    if let Some(node_config) =
        nodes_configuration.find_node_by_name(Configuration::pinned().common.node_name())
    {
        // Only needed if we resume from a Restate version that has not properly set the
        // MetadataServerState to Member in the NodesConfiguration.
        if !matches!(
            node_config.metadata_server_config.metadata_server_state,
            MetadataServerState::Member
        ) {
            info!(
                "Setting MetadataServerState to Member in NodesConfiguration for node {}",
                node_config.name
            );
            let mut new_node_config = node_config.clone();
            new_node_config.metadata_server_config.metadata_server_state =
                MetadataServerState::Member;

            nodes_configuration.upsert_node(new_node_config);
            modified = true;
        }
    }

    // If we have a node-id 0 in our NodesConfiguration, then we need to migrate it to another
    // value since node-id 0 is a reserved value now.
    let zero = PlainNodeId::new(0);
    if let Ok(node_config) = nodes_configuration.find_node_by_id(zero) {
        if nodes_configuration.len() > 1 {
            return Err(MigrationError::MultiNodeCluster);
        }

        assert_eq!(
            node_config.name,
            Configuration::pinned().node_name(),
            "The only known node of this cluster is {} but my node name is {}. This indicates that my node name was changed after an initial provisioning of the node",
            node_config.name,
            Configuration::pinned().node_name()
        );

        let plain_node_id_to_migrate_to =
            if let Some(force_node_id) = Configuration::pinned().common.force_node_id {
                assert_ne!(
                    force_node_id, zero,
                    "It should no longer be allowed to force the node id to 0"
                );
                force_node_id
            } else {
                PlainNodeId::MIN
            };

        let mut new_node_config = node_config.clone();
        new_node_config.current_generation = plain_node_id_to_migrate_to
            .with_generation(node_config.current_generation.generation());
        info!(
            "Migrating node id of node '{}' from '{}' to '{}'",
            node_config.name, node_config.current_generation, new_node_config.current_generation
        );

        nodes_configuration.remove_node_unchecked(node_config.current_generation);
        nodes_configuration.upsert_node(new_node_config);
        modified = true;
    }

    if modified {
        nodes_configuration.increment_version();

        let new_nodes_configuration = serialize_value(&nodes_configuration)?;

        storage
            .put(
                &NODES_CONFIG_KEY,
                &new_nodes_configuration,
                Precondition::MatchesVersion(value.version),
            )
            .await?;

        info!("Successfully completed NodesConfiguration migration");
    }

    Ok(())
}
