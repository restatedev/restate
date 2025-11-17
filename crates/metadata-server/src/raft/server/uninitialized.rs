// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::local::migrate_nodes_configuration;
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
    StatusSender, local, nodes_configuration_for_metadata_cluster_seed,
};
use arc_swap::ArcSwapOption;
use prost::Message as ProstMessag;
use raft_proto::eraftpb::{ConfState, Message, Snapshot};
use restate_core::{Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_metadata_server_grpc::MetadataServerConfiguration;
use restate_metadata_server_grpc::grpc::MetadataServerSnapshot;
use restate_metadata_store::serialize_value;
use restate_rocksdb::RocksError;
use restate_types::config::Configuration;
use restate_types::health::{HealthStatus, MetadataServerStatus};
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::{PlainNodeId, Version};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

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
        mut metadata_writer: Option<MetadataWriter>,
    ) -> Result<RaftServerState, Error> {
        if let Some(metadata_writer) = metadata_writer.as_mut() {
            // Try to read a persisted nodes configuration in order to learn about the addresses of our
            // potential peers and the metadata store states.
            if let Some(nodes_configuration) = self.storage.get_nodes_configuration()? {
                metadata_writer
                    .update(Arc::new(nodes_configuration))
                    .await?;
            }
        }

        self.metadata_writer = metadata_writer;

        // Check whether we are already provisioned based on the StorageMarker
        if self.storage.get_marker()?.is_none() {
            debug!("Provisioning replicated metadata store");
            health_status.update(MetadataServerStatus::AwaitingProvisioning);
            self.provision().await?;
        } else {
            debug!("Replicated metadata store is already provisioned");
        }

        // todo: drop the local metadata store db in v1.5.0

        // By always sealing the local metadata store, we enable a safe downgrade to v1.3.x. This
        // prevents creating an empty local metadata store and ending up with split-brain.
        if let Err(err) = Self::try_sealing_local_metadata_server().await {
            // If we are in this branch, then we assume that users have explicitly configured the
            // replicated metadata server. Hence, if sealing fails, it is not a catastrophe. With
            // Restate 1.4.0, the minimum version is 1.3.0 which means that this installation can
            // not be rolled back to 1.2.x or earlier, which are not aware of the seal marker.
            warn!(%err, "Failed sealing local metadata store. This can be problematic if you \
            ever switch back to the local metadata server explicitly and downgrade to Restate \
            version <= v1.4.0");
        } else {
            debug!("Sealed local metadata store to prevent Restate v1.3.x from using it");
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

        if local::storage::RocksDbStorage::data_dir_exists() {
            info!("Trying to migrate local to replicated metadata");

            let my_member_id = self
                .initialize_storage_from_local_metadata_server()
                .await
                .map_err(|err| {
                    error!(%err, "Failed to migrate local to replicated metadata. Please make sure \
                    that {} exists and has not been corrupted. If the directory does not contain \
                    local metadata you want to migrate from, then please remove it", local::storage::RocksDbStorage::data_dir().display());
                    Error::ProvisionFromLocal(err.into())
                })?;

            info!(member_id = %my_member_id, "Successfully migrated local to replicated metadata");
        } else {
            self.await_provisioning_signal().await?
        }

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
                    // fail incoming requests while we are waiting for the provision signal
                    let request = request.into_request();
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

    async fn initialize_storage_from_local_metadata_server(&mut self) -> anyhow::Result<MemberId> {
        let mut initial_state = self.load_initial_state_from_local_metadata_server().await?;
        let mut nodes_configuration = initial_state.last_seen_nodes_configuration().clone();

        let previous_version = nodes_configuration.version();
        let my_plain_node_id = nodes_configuration_for_metadata_cluster_seed(
            &Configuration::pinned(),
            &mut nodes_configuration,
        )?;
        nodes_configuration.increment_version();

        let versioned_value = serialize_value(&nodes_configuration)?;
        initial_state
            .put(
                NODES_CONFIG_KEY.clone(),
                versioned_value,
                Precondition::MatchesVersion(previous_version),
            )
            .expect("no precondition violation");

        self.initialize_storage(my_plain_node_id, initial_state)
            .await
    }

    async fn load_initial_state_from_local_metadata_server(
        &mut self,
    ) -> anyhow::Result<KvMemoryStorage> {
        let mut local_storage = Self::open_local_metadata_storage().await?;

        // if the local storage is sealed, then someone has run the if block before
        if !local_storage.is_sealed() {
            // Try to migrate older nodes configuration versions
            migrate_nodes_configuration(&mut local_storage).await?;
        }

        // make sure that no more changes can be made to the local metadata server when rolling back
        local_storage.seal().await?;

        let iter = local_storage.iter();
        let mut kv_memory_storage = KvMemoryStorage::new(None);

        for kv_pair in iter {
            let (key, value) = kv_pair?;
            debug!(
                "Migrate key-value pair '{key}' with version '{}' from local to replicated metadata server",
                value.version
            );
            kv_memory_storage
                .put(key, value, Precondition::DoesNotExist)
                .expect("initial values should not exist");
        }

        // todo close underlying RocksDb instance of local_storage

        Ok(kv_memory_storage)
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

    async fn try_sealing_local_metadata_server() -> anyhow::Result<()> {
        let mut local_storage = Self::open_local_metadata_storage().await?;

        if !local_storage.is_sealed() {
            local_storage.seal().await?;
        }
        Ok(())
    }

    async fn open_local_metadata_storage() -> Result<local::storage::RocksDbStorage, RocksError> {
        local::storage::RocksDbStorage::open_or_create().await
    }

    fn create_storage_marker() -> StorageMarker {
        StorageMarker::new(Configuration::pinned().common.node_name().to_owned())
    }
}
