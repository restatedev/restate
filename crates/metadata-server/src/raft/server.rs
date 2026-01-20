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
mod uninitialized;

use arc_swap::ArcSwapOption;
use futures::never::Never;
use prost::{DecodeError, EncodeError};
use protobuf::ProtobufError;
use raft_proto::eraftpb::Message;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::debug;

use restate_core::network::NetworkServerBuilder;
use restate_core::{MetadataWriter, ShutdownError, cancellation_watcher};
use restate_types::Version;
use restate_types::config::Configuration;
use restate_types::errors::{ConversionError, GenericError};
use restate_types::health::HealthStatus;
use restate_types::protobuf::common::MetadataServerStatus;

use crate::grpc::handler::MetadataServerHandler;
use crate::raft::network::{ConnectionManager, MetadataServerNetworkHandler};
use crate::raft::server::member::Member;
use crate::raft::server::standby::Standby;
use crate::raft::server::uninitialized::Uninitialized;
use crate::raft::storage::RocksDbStorage;
use crate::raft::{network, storage};
use crate::{
    JoinClusterHandle, JoinClusterReceiver, MemberId, MetadataCommandReceiver, MetadataServer,
    MetadataServerSummary, RequestError, RequestReceiver, StatusSender,
};

const RAFT_INITIAL_LOG_TERM: u64 = 1;
const RAFT_INITIAL_LOG_INDEX: u64 = 1;

struct RaftServerComponents {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    storage: RocksDbStorage,
    request_rx: RequestReceiver,
    status_tx: StatusSender,
    command_rx: MetadataCommandReceiver,
    join_cluster_rx: JoinClusterReceiver,
    metadata_writer: MetadataWriter,
}

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
    health_status: Option<HealthStatus<MetadataServerStatus>>,
    inner: Option<RaftMetadataServerState>,
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
            MetadataServerHandler::new(request_tx, provision_tx, status_rx, command_tx)
                .into_server(&Configuration::pinned().networking),
            restate_metadata_server_grpc::grpc::FILE_DESCRIPTOR_SET,
        );

        Ok(Self {
            health_status: Some(health_status),
            inner: Some(RaftMetadataServerState::Uninitialized(Uninitialized::new(
                connection_manager,
                storage,
                request_rx,
                status_tx,
                command_rx,
                join_cluster_rx,
                provision_rx,
            ))),
        })
    }

    pub async fn run(mut self, metadata_writer: MetadataWriter) -> Result<(), Error> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let health_status = self.health_status.take().expect("to be present");

        let result = tokio::select! {
            _ = &mut shutdown => {
                debug!("Shutting down RaftMetadataServer");
                self.shutdown().await?;
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
        &mut self,
        health_status: &HealthStatus<MetadataServerStatus>,
        metadata_writer: MetadataWriter,
    ) -> Result<Never, Error> {
        self.initialize(health_status, metadata_writer).await?;

        loop {
            match self.inner.as_mut().expect("inner state must be set") {
                RaftMetadataServerState::Member(member) => {
                    health_status.update(MetadataServerStatus::Member);
                    member.run().await?;
                    self.become_standby();
                }
                RaftMetadataServerState::Standby(standby) => {
                    health_status.update(MetadataServerStatus::Standby);
                    let (my_member_id, min_expected_nodes_config_version) = standby.run().await?;
                    self.become_member(my_member_id, min_expected_nodes_config_version)?;
                }
                RaftMetadataServerState::Uninitialized(_uninitialized) => {
                    panic!("Raft metadata server is uninitialized");
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        self.inner
            .as_mut()
            .expect("inner state must be set")
            .shutdown()
            .await
    }

    async fn initialize(
        &mut self,
        health_status: &HealthStatus<MetadataServerStatus>,
        metadata_writer: MetadataWriter,
    ) -> Result<(), Error> {
        let RaftMetadataServerState::Uninitialized(uninitialized) =
            self.inner.take().expect("inner state should be set")
        else {
            panic!("Raft metadata server should have been uninitialized");
        };

        self.inner = Some(
            uninitialized
                .initialize(health_status, metadata_writer)
                .await?,
        );
        Ok(())
    }

    fn become_standby(&mut self) {
        self.inner = Some(RaftMetadataServerState::Standby(
            match self.inner.take().expect("inner state must be set") {
                RaftMetadataServerState::Uninitialized(_) => {
                    panic!(
                        "Raft metadata server should have been initialized via RaftMetadataServer::initialize first"
                    )
                }
                RaftMetadataServerState::Member(member) => Standby::from(member),
                RaftMetadataServerState::Standby(standby) => standby,
            },
        ));
    }

    fn become_member(
        &mut self,
        my_member_id: MemberId,
        min_expected_nodes_config_version: Version,
    ) -> Result<(), Error> {
        self.inner = Some(RaftMetadataServerState::Member(
            match self.inner.take().expect("inner state must be set") {
                RaftMetadataServerState::Uninitialized(_) => {
                    panic!(
                        "Raft metadata server should have been initialized via RaftMetadataServer::initialize first"
                    )
                }
                RaftMetadataServerState::Member(member) => member,
                RaftMetadataServerState::Standby(standby) => Member::try_from_standby(
                    my_member_id,
                    min_expected_nodes_config_version,
                    standby,
                )?,
            },
        ));

        Ok(())
    }
}

#[allow(clippy::large_enum_variant)]
enum RaftMetadataServerState {
    Uninitialized(Uninitialized),
    Member(Member),
    Standby(Standby),
}

impl RaftMetadataServerState {
    async fn shutdown(&mut self) -> Result<(), Error> {
        match self {
            RaftMetadataServerState::Uninitialized(_) => {
                // nothing to do since we are uninitialized
            }
            RaftMetadataServerState::Member(member) => member.shutdown().await?,
            RaftMetadataServerState::Standby(_) => {
                // nothing to do since we are in standby mode
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl MetadataServer for RaftMetadataServer {
    async fn run(self, metadata_writer: MetadataWriter) -> anyhow::Result<()> {
        self.run(metadata_writer).await.map_err(Into::into)
    }
}

impl From<raft::Error> for RequestError {
    fn from(value: raft::Error) -> Self {
        match value {
            err @ raft::Error::ProposalDropped => RequestError::Unavailable(err.into(), None),
            err => RequestError::Internal(err.into()),
        }
    }
}
