// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use anyhow::Context;
use tonic::codec::CompressionEncoding;
use tracing::{debug, info, instrument};

use restate_core::network::tonic_service_filter::{TonicServiceFilter, WaitForReady};
use restate_core::network::{MessageRouterBuilder, NetworkServerBuilder};
use restate_core::{Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::Configuration;
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::logs::RecordCache;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{NodesConfiguration, StorageState};
use restate_types::protobuf::common::LogServerStatus;
use restate_types::GenerationalNodeId;

use crate::error::LogServerBuildError;
use crate::grpc_svc_handler::LogServerSvcHandler;
use crate::logstore::LogStore;
use crate::metadata::{LogStoreMarker, LogletStateMap};
use crate::metric_definitions::describe_metrics;
use crate::network::RequestPump;
use crate::protobuf::log_server_svc_server::LogServerSvcServer;
use crate::rocksdb_logstore::{RocksDbLogStore, RocksDbLogStoreBuilder};

pub struct LogServerService {
    health_status: HealthStatus<LogServerStatus>,
    metadata: Metadata,
    request_processor: RequestPump,
    metadata_store_client: MetadataStoreClient,
    state_map: LogletStateMap,
    log_store: RocksDbLogStore,
}

impl LogServerService {
    pub async fn create(
        health_status: HealthStatus<LogServerStatus>,
        updateable_config: Live<Configuration>,
        metadata: Metadata,
        metadata_store_client: MetadataStoreClient,
        record_cache: RecordCache,
        router_builder: &mut MessageRouterBuilder,
        server_builder: &mut NetworkServerBuilder,
    ) -> Result<Self, LogServerBuildError> {
        describe_metrics();
        health_status.update(LogServerStatus::StartingUp);

        // What do we need to create the log-server?
        //
        // 1. A log-store
        let log_store_builder = RocksDbLogStoreBuilder::create(
            updateable_config.clone().map(|c| &c.log_server).boxed(),
            updateable_config
                .clone()
                .map(|c| &c.log_server.rocksdb)
                .boxed(),
            record_cache.clone(),
        )
        .await
        .map_err(LogServerBuildError::other)?;

        // 2. Fire up the log store.
        let log_store = log_store_builder
            .start(health_status.clone())
            .await
            .map_err(LogServerBuildError::other)?;

        // Might fetch all known loglets from disk
        let state_map = LogletStateMap::load_all(&log_store)
            .await
            .map_err(LogServerBuildError::other)?;

        // 3. Register the log-server grpc service
        server_builder.register_grpc_service(
            TonicServiceFilter::new(
                LogServerSvcServer::new(LogServerSvcHandler::new(
                    log_store.clone(),
                    state_map.clone(),
                    record_cache,
                ))
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
                WaitForReady::new(health_status.clone(), LogServerStatus::Ready),
            ),
            crate::protobuf::FILE_DESCRIPTOR_SET,
        );

        let request_processor = RequestPump::new(updateable_config, router_builder);

        Ok(Self {
            health_status,
            metadata,
            request_processor,
            metadata_store_client,
            state_map,
            log_store,
        })
    }

    pub async fn start(self, mut metadata_writer: MetadataWriter) -> anyhow::Result<()> {
        let LogServerService {
            health_status,
            metadata,
            request_processor: request_pump,
            mut metadata_store_client,
            state_map,
            mut log_store,
        } = self;

        // Run log-store checks and self-provision if needed.
        let storage_state = Self::provision_node(
            &metadata,
            &mut log_store,
            &mut metadata_store_client,
            &mut metadata_writer,
        )
        .await?;

        let _ = TaskCenter::spawn_child(
            TaskKind::SystemService,
            "log-server",
            request_pump.run(health_status, log_store, state_map, storage_state),
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

        metadata_writer.update(Arc::new(nodes_config)).await?;
        info!("Log-store self-provisioning is complete, the node's log-store is now in read-write state");
        Ok(target_storage_state)
    }
}
