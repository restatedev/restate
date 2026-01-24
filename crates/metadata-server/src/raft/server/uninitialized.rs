// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::raft::kv_memory_storage::KvMemoryStorage;
use crate::raft::network::ConnectionManager;
use crate::raft::server::{
    Error, RAFT_INITIAL_LOG_INDEX, RAFT_INITIAL_LOG_TERM, RaftServerComponents,
};
use crate::raft::storage::RocksDbStorage;
use crate::raft::{RaftServerState, StorageMarker, to_raft_id};
use crate::{
    JoinClusterError, JoinClusterReceiver, MemberId, MetadataCommandError, MetadataCommandReceiver,
    MetadataServerSummary, ProvisionError, ProvisionReceiver, RequestError, RequestReceiver,
    StatusSender, nodes_configuration_for_metadata_cluster_seed,
};
use arc_swap::ArcSwapOption;
use prost::Message as ProstMessag;
use raft_proto::eraftpb::{ConfState, Message, Snapshot};
use restate_core::{Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_metadata_server_grpc::MetadataServerConfiguration;
use restate_metadata_server_grpc::grpc::MetadataServerSnapshot;
use restate_metadata_store::serialize_value;
use restate_types::config::{Configuration, data_dir};
use restate_types::health::{HealthStatus, MetadataServerStatus};
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::{PlainNodeId, Version};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::__private17::AsDisplay;
use tracing::{debug, info, warn};

pub struct Uninitialized {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    storage: RocksDbStorage,
    request_rx: RequestReceiver,
    status_tx: StatusSender,
    command_rx: MetadataCommandReceiver,
    join_cluster_rx: JoinClusterReceiver,
    provision_rx: Option<ProvisionReceiver>,
    metadata_writer: Option<MetadataWriter>,
}

impl Uninitialized {
    pub fn new(
        connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
        storage: RocksDbStorage,
        request_rx: RequestReceiver,
        status_tx: StatusSender,
        command_rx: MetadataCommandReceiver,
        join_cluster_rx: JoinClusterReceiver,
        provision_rx: Option<ProvisionReceiver>,
    ) -> Self {
        Self {
            connection_manager,
            storage,
            request_rx,
            status_tx,
            command_rx,
            join_cluster_rx,
            provision_rx,
            metadata_writer: None,
        }
    }

    pub fn into_inner(self) -> RaftServerComponents {
        RaftServerComponents {
            storage: self.storage,
            connection_manager: self.connection_manager,
            request_rx: self.request_rx,
            status_tx: self.status_tx,
            command_rx: self.command_rx,
            join_cluster_rx: self.join_cluster_rx,
            metadata_writer: self.metadata_writer,
        }
    }

    pub async fn initialize(
        &mut self,
        health_status: &HealthStatus<MetadataServerStatus>,
        metadata_writer: MetadataWriter,
    ) -> Result<RaftServerState, Error> {
        // Try to read a persisted nodes configuration in order to learn about the addresses of our
        // potential peers and the metadata store states.
        if let Some(nodes_configuration) = self.storage.get_nodes_configuration()? {
            metadata_writer
                .update(Arc::new(nodes_configuration))
                .await?;
        }

        self.metadata_writer = Some(metadata_writer);

        // Check whether we are already provisioned based on the StorageMarker
        if self.storage.get_marker()?.is_none() {
            debug!("Provisioning replicated metadata store");
            health_status.update(MetadataServerStatus::AwaitingProvisioning);
            self.provision().await?;
        } else {
            debug!("Replicated metadata store is already provisioned");
        }

        let mut provision_rx = self.provision_rx.take().expect("must be present");
        TaskCenter::spawn_unmanaged(TaskKind::Background, "provision-responder", async move {
            while let Some(request) = provision_rx.recv().await {
                let _ = request.result_tx.send(Ok(false));
            }
        })?;

        Ok(self.storage.get_raft_server_state()?)
    }

    async fn provision(&mut self) -> Result<(), Error> {
        let _ = self.status_tx.send(MetadataServerSummary::Provisioning);

        // This check has been introduced with v1.6.0 when the local metadata store was completely
        // removed. See https://github.com/restatedev/restate/issues/4040.
        const LOCAL_METADATA_STORE_DATA_DIR: &str = "local-metadata-store";
        let local_metadata_store_data_dir = data_dir(LOCAL_METADATA_STORE_DATA_DIR);
        if local_metadata_store_data_dir.exists() {
            // safety check which should only trigger if users didn't migrate properly from v1.3
            // through v1.4 to the current version. Migrating from v1.3 to v1.4 should have
            // automatically migrated the local metadata store to the replicated metadata store.
            panic!(
                "Detected a non empty local metadata store data directory at {}. This indicates \
            that you might have missed running migration from the local to the replicated metadata \
            store which happens in Restate v1.4 and v1.5. When upgrading Restate make sure to not \
            jump minor versions but instead go through all minor versions to run all required \
            migrations. If you are sure that the local metadata store data directory does not contain \
            relevant data, then you can also delete it to allow this node to provision on restart.",
                local_metadata_store_data_dir.as_display()
            );
        }

        self.await_provisioning_signal().await?;
        Ok(())
    }

    async fn await_provisioning_signal(&mut self) -> Result<(), Error> {
        let mut nodes_config_watcher =
            Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));
        if !Configuration::pinned().common.auto_provision {
            info!(
                "Cluster has not been provisioned, yet. Awaiting provisioning via `restatectl provision`"
            );
        }
        loop {
            tokio::select! {
                Some(request) = self.request_rx.recv() => {
                    // Fail incoming requests while we are waiting for the provision signal. We
                    // don't validate the cluster fingerprint because we aren't initialized and
                    // might not have a valid NodesConfiguration yet.
                    let (request, _) = request.into_request();
                    request.fail(RequestError::Unavailable("Metadata store has not been provisioned yet".into(), None))
                },
                Some(request) = self.command_rx.recv() => {
                    request.fail(MetadataCommandError::Unavailable("Metadata store has not been been provisioned yet".to_owned()))
                },
                Some(request) = self.join_cluster_rx.recv() => {
                    let _ = request.response_tx.send(Err(JoinClusterError::NotMember(None)));
                },
                Some(request) = self.provision_rx.as_mut().expect("must be present").recv() => {
                    match self.initialize_storage_from_nodes_configuration(request.nodes_configuration).await {
                        Ok(my_member_id) => {
                            let _ = request.result_tx.send(Ok(true));
                            debug!(member_id = %my_member_id, "Successfully provisioned the metadata store");
                            return Ok(());
                        },
                        Err(err) => {
                            warn!("Failed to provision the metadata store: {err}");
                            let _ = request.result_tx.send(Err(ProvisionError::Internal(err.into())));
                        }
                    }
                },
                Ok(()) = nodes_config_watcher.changed() => {
                    if *nodes_config_watcher.borrow_and_update() > Version::INVALID {
                        // The metadata store must have been provisioned if there exists a
                        // NodesConfiguration. So let's move on.
                        debug!("Detected a valid nodes configuration. This indicates that the metadata store cluster has been provisioned");

                        // mark the storage as provisioned
                        let storage_marker = Self::create_storage_marker();
                        let mut txn = self.storage.txn();
                        txn.store_marker(&storage_marker);
                        txn.commit().await?;

                        return Ok(());
                    }
                }
            }
        }
    }

    async fn initialize_storage_from_nodes_configuration(
        &mut self,
        mut nodes_configuration: NodesConfiguration,
    ) -> anyhow::Result<MemberId> {
        debug!("Initialize storage from nodes configuration");

        let my_plain_node_id = nodes_configuration_for_metadata_cluster_seed(
            &Configuration::pinned(),
            &mut nodes_configuration,
        )?;

        let mut initial_state = KvMemoryStorage::new(None);
        let versioned_value = serialize_value(&nodes_configuration)?;
        initial_state.put(
            NODES_CONFIG_KEY.clone(),
            versioned_value,
            Precondition::DoesNotExist,
        )?;

        self.initialize_storage(my_plain_node_id, initial_state)
            .await
    }

    async fn initialize_storage(
        &mut self,
        my_plain_node_id: PlainNodeId,
        initial_state: KvMemoryStorage,
    ) -> anyhow::Result<MemberId> {
        assert!(
            self.storage.is_empty()?,
            "storage must be empty to get initialized"
        );

        let storage_marker = Self::create_storage_marker();

        let my_member_id = MemberId::new(
            my_plain_node_id,
            storage_marker.created_at().timestamp_millis(),
        );

        let initial_conf_state = ConfState::from((vec![to_raft_id(my_member_id.node_id)], vec![]));

        // initialize storage with an initial snapshot so that newly started nodes will fetch it
        // first to start with the same initial conf state.
        let mut members = HashMap::default();
        members.insert(my_member_id.node_id, my_member_id.created_at_millis);
        let mut metadata_store_snapshot = MetadataServerSnapshot {
            configuration: Some(
                restate_metadata_server_grpc::grpc::MetadataServerConfiguration::from(
                    MetadataServerConfiguration {
                        version: Version::MIN,
                        members,
                    },
                ),
            ),
            ..MetadataServerSnapshot::default()
        };

        initial_state.snapshot(&mut metadata_store_snapshot);

        let mut snapshot = Snapshot::new();
        snapshot.mut_metadata().term = RAFT_INITIAL_LOG_TERM;
        snapshot.mut_metadata().index = RAFT_INITIAL_LOG_INDEX;
        snapshot.mut_metadata().set_conf_state(initial_conf_state);
        snapshot.data = metadata_store_snapshot.encode_to_vec().into();

        let mut txn = self.storage.txn();
        // it's important to first apply the snapshot so that the initial entry has the right index
        txn.apply_snapshot(&snapshot)?;
        txn.store_raft_server_state(&RaftServerState::Member {
            my_member_id,
            min_expected_nodes_config_version: Some(
                initial_state.last_seen_nodes_configuration().version(),
            ),
        })?;
        txn.store_marker(&storage_marker);
        txn.commit().await?;

        Ok(my_member_id)
    }

    fn create_storage_marker() -> StorageMarker {
        StorageMarker::new(Configuration::pinned().common.node_name().to_owned())
    }
}
