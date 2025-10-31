// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod member;
mod standby;

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use futures::never::Never;
use prost::{DecodeError, EncodeError, Message as ProstMessage};
use protobuf::ProtobufError;
use raft::prelude::{ConfState, Message};
use raft_proto::eraftpb::Snapshot;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

use restate_core::network::NetworkServerBuilder;
use restate_core::{
    Metadata, MetadataWriter, ShutdownError, TaskCenter, TaskKind, cancellation_watcher,
};
use restate_metadata_server_grpc::grpc::MetadataServerSnapshot;
use restate_metadata_store::serialize_value;
use restate_rocksdb::RocksError;
use restate_ty::metadata::MetadataKind;
use restate_ty::protobuf::MetadataServerStatus;
use restate_types::config::Configuration;
use restate_types::errors::{ConversionError, GenericError};
use restate_types::health::HealthStatus;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::{PlainNodeId, Version};

use crate::grpc::handler::MetadataServerHandler;
use crate::local::migrate_nodes_configuration;
use crate::raft::kv_memory_storage::KvMemoryStorage;
use crate::raft::network::{ConnectionManager, MetadataServerNetworkHandler};
use crate::raft::server::member::Member;
use crate::raft::server::standby::Standby;
use crate::raft::storage::RocksDbStorage;
use crate::raft::{RaftServerState, StorageMarker, network, storage, to_raft_id};
use crate::{
    JoinClusterError, JoinClusterHandle, JoinClusterReceiver, MemberId, MetadataCommandError,
    MetadataCommandReceiver, MetadataServer, MetadataServerConfiguration, MetadataServerSummary,
    ProvisionError, ProvisionReceiver, RequestError, RequestReceiver, StatusSender, local,
    nodes_configuration_for_metadata_cluster_seed,
};

const RAFT_INITIAL_LOG_TERM: u64 = 1;
const RAFT_INITIAL_LOG_INDEX: u64 = 1;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("failed creating raft node: {0}")]
    Raft(#[from] raft::Error),
    #[error("failed creating raft storage: {0}")]
    Storage(#[from] storage::BuildError),
    #[error("failed initializing the storage: {0}")]
    InitStorage(String),
    #[error("failed bootstrapping conf state: {0}")]
    BootstrapConfState(#[from] storage::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed running raft: {0}")]
    Raft(#[from] raft::Error),
    #[error("failed deserializing raft serialized requests: {0}")]
    DecodeRequest(GenericError),
    #[error("failed changing conf: {0}")]
    ConfChange(#[from] ConfChangeError),
    #[error("failed reading/writing from/to storage: {0}")]
    Storage(#[from] storage::Error),
    #[error("failed restoring snapshot: {0}")]
    RestoreSnapshot(#[from] RestoreSnapshotError),
    #[error("failed creating snapshot: {0}")]
    CreateSnapshot(#[from] CreateSnapshotError),
    #[error("failed provisioning from local metadata: {0}")]
    ProvisionFromLocal(GenericError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

#[derive(Debug, thiserror::Error)]
pub enum ConfChangeError {
    #[error("failed applying conf change: {0}")]
    Apply(#[from] raft::Error),
    #[error("failed decoding conf change: {0}")]
    DecodeConfChange(#[from] ProtobufError),
    #[error("failed decoding conf context: {0}")]
    DecodeContext(#[from] DecodeError),
    #[error("failed creating snapshot after conf change: {0}")]
    Snapshot(#[from] CreateSnapshotError),
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreSnapshotError {
    #[error(transparent)]
    Protobuf(#[from] DecodeError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

#[derive(Debug, thiserror::Error)]
pub enum CreateSnapshotError {
    #[error("failed applying snapshot: {0}")]
    ApplySnapshot(#[from] storage::Error),
    #[error("failed encoding snapshot: {0}")]
    Encode(#[from] EncodeError),
}

pub struct RaftMetadataServer {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    storage: RocksDbStorage,

    health_status: Option<HealthStatus<MetadataServerStatus>>,

    request_rx: RequestReceiver,

    provision_rx: Option<ProvisionReceiver>,

    join_cluster_rx: JoinClusterReceiver,

    status_tx: StatusSender,

    command_rx: MetadataCommandReceiver,
}

impl RaftMetadataServer {
    pub async fn create(
        health_status: HealthStatus<MetadataServerStatus>,
        server_builder: &mut NetworkServerBuilder,
    ) -> Result<Self, BuildError> {
        health_status.update(MetadataServerStatus::StartingUp);

        let (request_tx, request_rx) = mpsc::channel(2);
        let (command_tx, command_rx) = mpsc::channel(2);
        let (provision_tx, provision_rx) = mpsc::channel(1);
        let (join_cluster_tx, join_cluster_rx) = mpsc::channel(1);
        let (status_tx, status_rx) = watch::channel(MetadataServerSummary::default());

        let storage = RocksDbStorage::create().await?;

        // make sure that the storage is initialized with a storage id to be able to detect disk losses
        if let Some(storage_marker) = storage
            .get_marker()
            .map_err(|err| BuildError::InitStorage(err.to_string()))?
        {
            if storage_marker.id() != Configuration::pinned().common.node_name() {
                return Err(BuildError::InitStorage(format!(
                    "metadata-server storage marker was found but it was created by another node. Found node name '{}' while this node name is '{}'",
                    storage_marker.id(),
                    Configuration::pinned().common.node_name()
                )));
            } else {
                debug!(
                    "Found matching metadata-server storage marker in raft-storage, written at '{}'",
                    storage_marker.created_at().to_rfc2822()
                );
            }
        }

        let connection_manager = Arc::default();

        server_builder.register_grpc_service(
            MetadataServerNetworkHandler::new(
                Arc::clone(&connection_manager),
                Some(JoinClusterHandle::new(join_cluster_tx)),
            )
            .into_server(&Configuration::pinned().networking),
            network::FILE_DESCRIPTOR_SET,
        );
        server_builder.register_grpc_service(
            MetadataServerHandler::new(request_tx, Some(provision_tx), Some(status_rx), command_tx)
                .into_server(&Configuration::pinned().networking),
            restate_metadata_server_grpc::grpc::FILE_DESCRIPTOR_SET,
        );

        Ok(Self {
            connection_manager,
            storage,
            health_status: Some(health_status),
            request_rx,
            provision_rx: Some(provision_rx),
            join_cluster_rx,
            status_tx,
            command_rx,
        })
    }

    pub async fn run(mut self, metadata_writer: Option<MetadataWriter>) -> Result<(), Error> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let health_status = self.health_status.take().expect("to be present");

        let result = tokio::select! {
            _ = &mut shutdown => {
                debug!("Shutting down RaftMetadataServer");
                Ok(())
            },
            result = self.run_inner(&health_status, metadata_writer) => {
                result.map(|_| ())
            },
        };

        health_status.update(MetadataServerStatus::Unknown);

        result
    }

    async fn run_inner(
        mut self,
        health_status: &HealthStatus<MetadataServerStatus>,
        mut metadata_writer: Option<MetadataWriter>,
    ) -> Result<Never, Error> {
        if let Some(metadata_writer) = metadata_writer.as_mut() {
            // Try to read a persisted nodes configuration in order to learn about the addresses of our
            // potential peers and the metadata store states.
            if let Some(nodes_configuration) = self.storage.get_nodes_configuration()? {
                metadata_writer
                    .update(Arc::new(nodes_configuration))
                    .await?;
            }
        }

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

        let mut provisioned = if let RaftServerState::Member {
            my_member_id,
            min_expected_nodes_config_version,
        } = self.storage.get_raft_server_state()?
        {
            Provisioned::Member(
                self.become_member(
                    my_member_id,
                    min_expected_nodes_config_version
                        .unwrap_or(Version::MIN)
                        .max(Metadata::with_current(|m| m.nodes_config_version())),
                    metadata_writer,
                )?,
            )
        } else {
            Provisioned::Standby(self.become_standby(metadata_writer))
        };

        loop {
            match provisioned {
                Provisioned::Member(member) => {
                    health_status.update(MetadataServerStatus::Member);
                    provisioned = Provisioned::Standby(member.run().await?);
                }
                Provisioned::Standby(standby) => {
                    health_status.update(MetadataServerStatus::Standby);
                    provisioned = Provisioned::Member(standby.run().await?);
                }
            }
        }
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

    fn become_standby(self, metadata_writer: Option<MetadataWriter>) -> Standby {
        let Self {
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            status_tx,
            command_rx,
            ..
        } = self;

        Standby::new(
            storage,
            connection_manager,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            command_rx,
        )
    }

    fn become_member(
        self,
        my_member_id: MemberId,
        min_expected_nodes_config_version: Version,
        metadata_writer: Option<MetadataWriter>,
    ) -> Result<Member, Error> {
        let Self {
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            status_tx,
            command_rx,
            ..
        } = self;

        Member::create(
            my_member_id,
            min_expected_nodes_config_version,
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            command_rx,
        )
    }
}

#[async_trait::async_trait]
impl MetadataServer for RaftMetadataServer {
    async fn run(self, metadata_writer: Option<MetadataWriter>) -> anyhow::Result<()> {
        self.run(metadata_writer).await.map_err(Into::into)
    }
}

/// States of a provisioned metadata store. The metadata store can be either a member or a stand by.
#[allow(clippy::large_enum_variant)]
enum Provisioned {
    /// Being an active member of the metadata store cluster
    Member(Member),
    /// Not being a member of the metadata store cluster
    Standby(Standby),
}

impl From<raft::Error> for RequestError {
    fn from(value: raft::Error) -> Self {
        match value {
            err @ raft::Error::ProposalDropped => RequestError::Unavailable(err.into(), None),
            err => RequestError::Internal(err.into()),
        }
    }
}
