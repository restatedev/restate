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
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use tracing::{debug, instrument, warn};

use restate_core::network::tonic_service_filter::{TonicServiceFilter, WaitForReady};
use restate_core::network::{MessageRouterBuilder, NetworkServerBuilder};
use restate_core::{Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_metadata_store::{ReadWriteError, RetryError, retry_on_retryable_error};
use restate_types::GenerationalNodeId;
use restate_types::config::Configuration;
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::live::LiveLoadExt;
use restate_types::logs::RecordCache;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{NodesConfiguration, StorageState};
use restate_types::protobuf::common::LogServerStatus;

use crate::error::LogServerBuildError;
use crate::grpc_svc_handler::LogServerSvcHandler;
use crate::logstore::LogStore;
use crate::metadata::{LogStoreMarker, LogletStateMap};
use crate::metric_definitions::describe_metrics;
use crate::network::RequestPump;
use crate::rocksdb_logstore::{RocksDbLogStore, RocksDbLogStoreBuilder};

pub struct LogServerService {
    health_status: HealthStatus<LogServerStatus>,
    metadata: Metadata,
    request_processor: RequestPump,
    state_map: LogletStateMap,
    log_store: RocksDbLogStore,
}

impl LogServerService {
    pub async fn create(
        health_status: HealthStatus<LogServerStatus>,
        mut updateable_config: Live<Configuration>,
        metadata: Metadata,
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
            updateable_config.clone().map(|c| &c.log_server),
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
                LogServerSvcHandler::new(log_store.clone(), state_map.clone(), record_cache)
                    .into_server(&updateable_config.live_load().networking),
                WaitForReady::new(health_status.clone(), LogServerStatus::Ready),
            ),
            crate::protobuf::FILE_DESCRIPTOR_SET,
        );

        let request_processor = RequestPump::new(updateable_config, router_builder);

        Ok(Self {
            health_status,
            metadata,
            request_processor,
            state_map,
            log_store,
        })
    }

    pub async fn start(self, mut metadata_writer: MetadataWriter) -> anyhow::Result<()> {
        let LogServerService {
            health_status,
            metadata,
            request_processor: request_pump,
            state_map,
            mut log_store,
        } = self;

        // Run log-store checks and self-provision if needed.
        let storage_state =
            Self::provision_node(&metadata, &mut log_store, &mut metadata_writer).await?;

        let _ = TaskCenter::spawn(
            TaskKind::LogServerRole,
            "log-server",
            request_pump.run(health_status, log_store, state_map, storage_state),
        )?;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn provision_node(
        metadata: &Metadata,
        log_store: &mut impl LogStore,
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
        if let Some(marker) = &maybe_marker {
            if *marker.id() != my_node_id.as_plain() {
                return Err(anyhow::anyhow!(
                    "log-server storage marker was found but it was created by another node. Found {} while this node is {}",
                    marker.id(),
                    my_node_id.as_plain()
                ));
            } else {
                debug!(
                    "Found log-server storage marker, written at '{}'",
                    marker.created_at().to_rfc2822()
                );
            }
        }

        match my_storage_state {
            StorageState::Provisioning => {
                // Only provision the marker if we are in provisioning state.
                // If the node is in provisioning and we already have a marker, that's okay as long as
                // it matches the stored node-id (checked earlier). We override the stored value if
                // exists and attempt to update the nodes config.
                let new_marker = LogStoreMarker::new(my_node_id.as_plain());
                log_store
                    .store_marker(new_marker.clone())
                    .await
                    .context("Couldn't store log-server's storage marker on log-store")?;
                debug!(
                    "Written log-server storage marker for my node {} at {}",
                    new_marker.id(),
                    new_marker.created_at().to_rfc2822()
                );
                // Self transition our storage state into read-write.
                my_storage_state = Self::update_storage_state(
                    my_node_id,
                    metadata_writer,
                    StorageState::Provisioning,
                    StorageState::ReadWrite,
                )
                .await?;
                debug!(
                    "Log-store self-provisioning is complete, the node's log-store is now in read-write state"
                );
            }
            StorageState::DataLoss => {
                // Reject start. In the future we should be able to start with log-store being in
                // fail-safe mode but that's not thoroughly tested yet, we choose to stop until
                // this is fully tested.
                return Err(anyhow::anyhow!(
                    "A previous generation of my node {} has marked this log-server's storage-state as `data-loss`. \
                        If this is a new node, please choose a different node-name to provision it as a new node",
                    my_node_id.as_plain(),
                ));
            }
            StorageState::Disabled => {
                // NOTE: This behaviour is subject to change in the future
                return Err(anyhow::anyhow!(
                    "This node's storage-state is marked as `disabled`, log-server role cannot run on disabled nodes"
                ));
            }
            StorageState::ReadWrite | StorageState::ReadOnly if maybe_marker.is_none() => {
                // Mark as data loss
                warn!(
                    "Detected data loss for log-server of my node {}. The log-server marker is missing, storage-state was {}, will transition into `data-loss`",
                    my_node_id.as_plain(),
                    my_storage_state
                );
                // Self transition our storage state into data-loss
                Self::update_storage_state(
                    my_node_id,
                    metadata_writer,
                    my_storage_state,
                    StorageState::DataLoss,
                )
                .await?;
                return Err(anyhow::anyhow!(
                    "Node cannot start a log-server on {}, it has detected that it has lost its data. storage-state is `data-loss`",
                    my_node_id.as_plain(),
                ));
            }
            StorageState::ReadWrite | StorageState::ReadOnly | StorageState::Gone => {}
        }

        debug!("My storage state: {:?}", my_storage_state);
        Ok(my_storage_state)
    }

    async fn update_storage_state(
        my_node_id: GenerationalNodeId,
        metadata_writer: &mut MetadataWriter,
        expected_state: StorageState,
        target_state: StorageState,
    ) -> anyhow::Result<StorageState> {
        let retry_policy = Configuration::pinned()
            .common
            .network_error_retry_policy
            .clone();
        // We need to use an atomic bool here only because the retry_on_retryable_error closure
        // returns an async block which captures a reference to this variable, and therefore it would
        // escape the closure body. With an atomic bool, we can pass in a simple borrow which can
        // escape the closure body. This can be changed once AsyncFnMut allows us to define Send bounds.
        let first_attempt = AtomicBool::new(true);

        let nodes_config = match retry_on_retryable_error(retry_policy, || {
            metadata_writer
                .raw_metadata_store_client()
                .read_modify_write(
                    NODES_CONFIG_KEY.clone(),
                    |nodes_config: Option<NodesConfiguration>| {
                        let mut nodes_config = nodes_config
                            .ok_or(StorageStateUpdateError::MissingNodesConfiguration)?;
                        // If this fails, it means that a newer node has started somewhere else, and we
                        // should not attempt to update the storage-state. Instead, we fail.
                        let mut node = nodes_config
                            // note that we find by the generational node id.
                            .find_node_by_id(my_node_id)
                            .map_err(|_| StorageStateUpdateError::NewerGenerationDetected)?
                            .clone();

                        if node.log_server_config.storage_state != expected_state {
                            return if first_attempt.load(Ordering::Relaxed) {
                                // Something might have caused this state to change. This should not happen,
                                // bail!
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

                        first_attempt.store(false, Ordering::Relaxed);
                        nodes_config.upsert_node(node);
                        nodes_config.increment_version();
                        Ok(nodes_config)
                    },
                )
        })
        .await
        .map_err(|e| e.map(|err| err.transpose()))
        {
            Ok(nodes_config) => nodes_config,
            Err(RetryError::NotRetryable(StorageStateUpdateError::PreviousAttemptSucceeded(
                nodes_config,
            )))
            | Err(RetryError::RetriesExhausted(
                StorageStateUpdateError::PreviousAttemptSucceeded(nodes_config),
            )) => nodes_config,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to update this log-server's storage-state to {target_state}")
                });
            }
        };

        debug_assert!(
            !first_attempt.load(Ordering::Relaxed),
            "Should have tried to set the storage-state at least once"
        );
        metadata_writer.update(Arc::new(nodes_config)).await?;
        Ok(target_state)
    }
}

#[derive(Debug, thiserror::Error)]
enum StorageStateUpdateError {
    #[error("cluster must be provisioned before log-server is started")]
    MissingNodesConfiguration,
    #[error(
        "another instance of the same node might have started, updating storage-state will fail"
    )]
    NewerGenerationDetected,
    #[error(
        "log-server found an unexpected storage-state '{0}' in metadata store, this could mean that another node has updated it"
    )]
    NotInExpectedState(StorageState),
    #[error("succeeded updating NodesConfiguration in a previous attempt")]
    PreviousAttemptSucceeded(NodesConfiguration),
    #[error(transparent)]
    MetadataStore(#[from] ReadWriteError),
}
