// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use tracing::{debug, info, instrument};

use restate_core::network::{MessageRouterBuilder, Networking};
use restate_core::{Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{NodesConfiguration, StorageState};
use restate_types::GenerationalNodeId;

use crate::error::LogServerBuildError;
use crate::logstore::LogStore;
use crate::metadata::LogStoreMarker;
use crate::metric_definitions::describe_metrics;
use crate::network::RequestPump;
use crate::rocksdb_logstore::RocksDbLogStoreBuilder;

pub struct LogServerService {
    updateable_config: Live<Configuration>,
    task_center: TaskCenter,
    metadata: Metadata,
    networking: Networking,
    request_processor: RequestPump,
    metadata_store_client: MetadataStoreClient,
}

impl LogServerService {
    pub async fn create(
        updateable_config: Live<Configuration>,
        task_center: TaskCenter,
        metadata: Metadata,
        networking: Networking,
        metadata_store_client: MetadataStoreClient,
        router_builder: &mut MessageRouterBuilder,
    ) -> Result<Self, LogServerBuildError> {
        describe_metrics();

        let request_processor = RequestPump::new(
            task_center.clone(),
            metadata.clone(),
            updateable_config.clone(),
            router_builder,
        );

        Ok(Self {
            updateable_config,
            task_center,
            metadata,
            networking,
            request_processor,
            metadata_store_client,
        })
    }

    pub async fn start(self, metadata_writer: MetadataWriter) -> anyhow::Result<()> {
        let tc = self.task_center.clone();
        tc.spawn(TaskKind::SystemService, "log-server", None, async {
            self.run(metadata_writer).await
        })?;

        Ok(())
    }

    async fn run(self, mut metadata_writer: MetadataWriter) -> anyhow::Result<()> {
        let LogServerService {
            updateable_config,
            task_center,
            metadata,
            request_processor: request_pump,
            networking,
            mut metadata_store_client,
        } = self;
        // What do we need to start the log-server?
        //
        // 1. A log-store
        let log_store_builder = RocksDbLogStoreBuilder::create(
            updateable_config.clone().map(|c| &c.log_server).boxed(),
            updateable_config.map(|c| &c.log_server.rocksdb).boxed(),
        )
        .await?;

        // 2. Fire up the log store.
        let mut log_store = log_store_builder
            .start(&task_center)
            .await
            .context("Couldn't start log-server's log store")?;

        // 3. Run log-store checks and self-provision if needed.
        let storage_state = Self::provision_node(
            &metadata,
            &mut log_store,
            &mut metadata_store_client,
            &mut metadata_writer,
        )
        .await?;

        task_center.spawn_child(
            TaskKind::NetworkMessageHandler,
            "log-server-req-pump",
            None,
            request_pump.run(networking, log_store, storage_state),
        )?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn provision_node(
        metadata: &Metadata,
        log_store: &mut impl LogStore,
        metadata_store_client: &mut MetadataStoreClient,
        metadata_writer: &mut MetadataWriter,
    ) -> anyhow::Result<StorageState> {
        let my_node_id = metadata.my_node_id();
        let mut my_storage_state = metadata
            .nodes_config_ref()
            .find_node_by_id(my_node_id)
            .expect("to find this node in NodesConfig")
            .log_server_config
            .storage_state;

        // Failure to load the marker indicates a lower-level store problem, we should stop
        // loading and the server should shutdown.
        let maybe_marker = log_store
            .load_marker()
            .await
            .context("Cannot load log-server marker from log-store")?;
        // If we have a marker, it must match our own node id.
        if let Some(marker) = maybe_marker {
            if marker.node_id() != my_node_id.as_plain() {
                return Err(anyhow::anyhow!(
                    "LogStoreMarker doesn't match our own node-id. Found NodeId {} while our node is {}",
                    marker.node_id(),
                    my_node_id.as_plain()
                ));
            } else {
                debug!(
                    "Found matching LogStoreMarker in log-store, written at '{:?}'",
                    marker.created_at()
                );
            }
        }

        // Only store the marker if we are in provisioning state.
        if my_storage_state == StorageState::Provisioning {
            // If the node is in provisioning and we already have a marker, that's okay as long as
            // it matches the stored node-id (checked earlier). We override the stored value if
            // exists and attempt to update the nodes config.
            log_store
                .store_marker(LogStoreMarker::new(my_node_id.as_plain()))
                .await
                .context("Couldn't store LogStoreMarker on log-store")?;
            debug!(
                "Stored LogStoreMarker in log-store for node '{}'",
                my_node_id.as_plain()
            );
            // Self transition our storage state into read-write.
            my_storage_state = Self::mark_the_node_as_writeable(
                my_node_id,
                metadata_store_client,
                metadata_writer,
            )
            .await?;
        }

        debug!("My storage state: {:?}", my_storage_state);
        Ok(my_storage_state)
    }

    async fn mark_the_node_as_writeable(
        my_node_id: GenerationalNodeId,
        metadata_store_client: &mut MetadataStoreClient,
        metadata_writer: &mut MetadataWriter,
    ) -> anyhow::Result<StorageState> {
        let target_storage_state = StorageState::ReadWrite;

        let nodes_config = metadata_store_client
            .read_modify_write(
                NODES_CONFIG_KEY.clone(),
                move |nodes_config: Option<NodesConfiguration>| {
                    let mut nodes_config = nodes_config.ok_or(anyhow::anyhow!(
                        "NodesConfiguration must be provisioned before enabling log-store"
                    ))?;
                    // If this fails, it means that a newer node has started somewhere else and we
                    // should not attempt to update the storage-state. Instead, we fail.
                    let mut node = nodes_config
                        // note that we find by the generational node id.
                        .find_node_by_id(my_node_id)
                        .context("Another instance of the same node might have started, this node cannot proceed with log-store provisioning")?.clone();

                    if node.log_server_config.storage_state != StorageState::Provisioning {
                        // Something might have cause this state to change. This should not happen,
                        // bail!
                        return Err(anyhow::anyhow!(
                            "Node is not in provisioning state, cannot proceed with log-store provisioning"
                        ));
                    }
                    node.log_server_config.storage_state = target_storage_state;

                    nodes_config.upsert_node(node);
                    nodes_config.increment_version();
                    anyhow::Ok(nodes_config)
                },
            )
            .await
            .map_err(|e| e.transpose())?;

        metadata_writer.update(nodes_config).await?;
        info!("Log-store self-provisioning is complete, the node's log-store is now in read-write state");
        Ok(target_storage_state)
    }
}
