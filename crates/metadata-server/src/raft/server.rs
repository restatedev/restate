// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::grpc::MetadataServerSnapshot;
use crate::grpc::handler::MetadataServerHandler;
use crate::local::migrate_nodes_configuration;
use crate::metric_definitions::{
    METADATA_SERVER_REPLICATED_APPLIED_LSN, METADATA_SERVER_REPLICATED_COMMITTED_LSN,
    METADATA_SERVER_REPLICATED_FIRST_INDEX, METADATA_SERVER_REPLICATED_LAST_INDEX,
    METADATA_SERVER_REPLICATED_LEADER_ID, METADATA_SERVER_REPLICATED_SNAPSHOT_SIZE_BYTES,
    METADATA_SERVER_REPLICATED_TERM,
};
use crate::raft::kv_memory_storage::KvMemoryStorage;
use crate::raft::network::{ConnectionManager, MetadataServerNetworkHandler, Networking};
use crate::raft::storage::RocksDbStorage;
use crate::raft::{RaftServerState, StorageMarker, network, storage, to_plain_node_id, to_raft_id};
use crate::{
    JoinClusterError, JoinClusterHandle, JoinClusterReceiver, JoinClusterRequest, JoinError,
    KnownLeader, MemberId, MetadataServer, MetadataServerConfiguration, MetadataServerSummary,
    MetadataStoreRequest, ProvisionError, ProvisionReceiver, RaftSummary, Request, RequestError,
    RequestReceiver, SnapshotSummary, StatusSender, WriteRequest, grpc, local,
    prepare_initial_nodes_configuration,
};
use arc_swap::ArcSwapOption;
use assert2::let_assert;
use bytes::BytesMut;
use futures::FutureExt;
use futures::future::{FusedFuture, OptionFuture};
use futures::never::Never;
use metrics::gauge;
use prost::{DecodeError, EncodeError, Message as ProstMessage};
use protobuf::{Message as ProtobufMessage, ProtobufError};
use raft::prelude::{ConfChange, ConfChangeV2, ConfState, Entry, EntryType, Message};
use raft::{
    Config, Error as RaftError, INVALID_ID, RawNode, ReadOnlyOption, SnapshotStatus, Storage,
};
use raft_proto::ConfChangeI;
use raft_proto::eraftpb::{ConfChangeSingle, ConfChangeType, Snapshot, SnapshotMetadata};
use rand::prelude::IteratorRandom;
use rand::rng;
use restate_core::metadata_store::serialize_value;
use restate_core::network::NetworkServerBuilder;
use restate_core::network::net_util::create_tonic_channel;
use restate_core::{
    Metadata, MetadataWriter, ShutdownError, TaskCenter, TaskKind, cancellation_watcher,
};
use restate_types::config::{
    Configuration, MetadataServerKind, MetadataServerOptions, RocksDbOptions,
};
use restate_types::errors::{ConversionError, GenericError};
use restate_types::health::HealthStatus;
use restate_types::live::{BoxedLiveLoad, Constant};
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration, Role};
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::retries::RetryPolicy;
use restate_types::{PlainNodeId, Version};
use slog::o;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use thiserror::__private::AsDisplay;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{Span, debug, error, info, instrument, trace, warn};
use tracing_slog::TracingSlogDrain;
use ulid::Ulid;

use super::network::grpc_svc::new_metadata_server_network_client;

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
}

impl RaftMetadataServer {
    pub async fn create(
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
        health_status: HealthStatus<MetadataServerStatus>,
        server_builder: &mut NetworkServerBuilder,
    ) -> Result<Self, BuildError> {
        health_status.update(MetadataServerStatus::StartingUp);

        let (request_tx, request_rx) = mpsc::channel(2);
        let (provision_tx, provision_rx) = mpsc::channel(1);
        let (join_cluster_tx, join_cluster_rx) = mpsc::channel(1);
        let (status_tx, status_rx) = watch::channel(MetadataServerSummary::default());

        let mut metadata_server_options =
            Configuration::updateable().map(|configuration| &configuration.metadata_server);
        let storage =
            RocksDbStorage::create(metadata_server_options.live_load(), rocksdb_options).await?;

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
            .into_server(),
            network::FILE_DESCRIPTOR_SET,
        );
        server_builder.register_grpc_service(
            MetadataServerHandler::new(request_tx, Some(provision_tx), Some(status_rx))
                .into_server(),
            grpc::FILE_DESCRIPTOR_SET,
        );

        Ok(Self {
            connection_manager,
            storage,
            health_status: Some(health_status),
            request_rx,
            provision_rx: Some(provision_rx),
            join_cluster_rx,
            status_tx,
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
                    .await?
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

        let mut provision_rx = self.provision_rx.take().expect("must be present");
        TaskCenter::spawn_unmanaged(TaskKind::Background, "provision-responder", async move {
            while let Some(request) = provision_rx.recv().await {
                let _ = request.result_tx.send(Ok(false));
            }
        })?;

        let mut provisioned = if let RaftServerState::Member { my_member_id } =
            self.storage.get_raft_server_state()?
        {
            Provisioned::Member(self.become_member(my_member_id, metadata_writer)?)
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
                    local metadata you want to migrate from, then please remove it", local::storage::RocksDbStorage::data_dir().as_display());
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

        let my_plain_node_id = prepare_initial_nodes_configuration(
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
        let my_plain_node_id = prepare_initial_nodes_configuration(
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
        let local_metadata_server_options = MetadataServerOptions::default();

        let mut local_storage = local::storage::RocksDbStorage::create(
            &local_metadata_server_options,
            Constant::new(RocksDbOptions::default()).boxed(),
        )
        .await?;

        // Try to migrate older nodes configuration versions
        migrate_nodes_configuration(&mut local_storage).await?;

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
            configuration: Some(grpc::MetadataServerConfiguration::from(
                MetadataServerConfiguration {
                    version: Version::MIN,
                    members,
                },
            )),
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
        txn.store_raft_server_state(&RaftServerState::Member { my_member_id })?;
        txn.store_marker(&storage_marker);
        txn.commit().await?;

        Ok(my_member_id)
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
            ..
        } = self;

        Standby::new(
            storage,
            connection_manager,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
        )
    }

    fn become_member(
        self,
        my_member_id: MemberId,
        metadata_writer: Option<MetadataWriter>,
    ) -> Result<Member, Error> {
        let Self {
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            status_tx,
            ..
        } = self;

        Member::create(
            my_member_id,
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
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
enum Provisioned {
    /// Being an active member of the metadata store cluster
    Member(Member),
    /// Not being a member of the metadata store cluster
    Standby(Standby),
}

#[allow(dead_code)]
struct Member {
    _logger: slog::Logger,

    raw_node: RawNode<RocksDbStorage>,
    networking: Networking<Message>,
    raft_rx: mpsc::Receiver<Message>,

    tick_interval: Interval,
    status_update_interval: Interval,

    my_member_id: MemberId,
    configuration: MetadataServerConfiguration,
    kv_storage: KvMemoryStorage,
    is_leader: bool,
    pending_join_requests: HashMap<MemberId, oneshot::Sender<Result<(), JoinClusterError>>>,
    read_index_to_request_id: VecDeque<(u64, Ulid)>,
    snapshot_summary: Option<SnapshotSummary>,

    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    metadata_writer: Option<MetadataWriter>,

    request_rx: RequestReceiver,
    join_cluster_rx: JoinClusterReceiver,
    status_tx: StatusSender,
}

impl Member {
    fn create(
        my_member_id: MemberId,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
        storage: RocksDbStorage,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
        status_tx: StatusSender,
    ) -> Result<Self, Error> {
        let (raft_tx, raft_rx) = mpsc::channel(128);
        let new_connection_manager = ConnectionManager::new(my_member_id.node_id, raft_tx);
        let mut networking = Networking::new(new_connection_manager.clone());

        networking.register_address(
            my_member_id.node_id,
            Configuration::pinned().common.advertised_address.clone(),
        );

        // todo remove additional indirection from Arc
        connection_manager.store(Some(Arc::new(new_connection_manager)));

        let_assert!(
            MetadataServerKind::Raft(raft_options) =
                &Configuration::pinned().metadata_server.kind(),
            "Expecting that the replicated/raft metadata server has been configured"
        );

        let mut config = Config {
            id: to_raft_id(my_member_id.node_id),
            election_tick: raft_options.raft_election_tick.get(),
            heartbeat_tick: raft_options.raft_heartbeat_tick.get(),
            read_only_option: ReadOnlyOption::Safe,
            check_quorum: true,
            pre_vote: true,
            // 64 KiB
            max_size_per_msg: 64 * 1024,
            max_inflight_msgs: 256,
            // 4 MiB
            max_uncommitted_size: 4 * 1024 * 1024,
            // 4 MiB
            max_committed_size_per_ready: 4 * 1024 * 1024,
            // only the leader should be allowed to accept proposals
            disable_proposal_forwarding: true,
            ..Config::default()
        };

        let drain = TracingSlogDrain;
        let logger = slog::Logger::root(drain, o!());

        let mut kv_storage = KvMemoryStorage::new(metadata_writer.clone());
        let mut snapshot_summary = None;
        let mut configuration = MetadataServerConfiguration::default();

        if let Ok(snapshot) = storage.snapshot(0, to_raft_id(my_member_id.node_id)) {
            config.applied = snapshot.get_metadata().get_index();
            Self::restore_fsm_snapshot(snapshot.get_data(), &mut configuration, &mut kv_storage)?;
            snapshot_summary = Some(SnapshotSummary::from_snapshot(&snapshot));
        }

        config.validate()?;
        let mut raw_node = RawNode::new(&config, storage, &logger)?;

        if raw_node
            .raft
            .prs()
            .conf()
            .voters()
            .contains(to_raft_id(my_member_id.node_id))
        {
            // Campaign if we are part of the voters to quickly become leader if there is none. This
            // won't cause the current leader to step down since pre-vote is enabled.
            raw_node.campaign()?;
        }

        let mut tick_interval = time::interval(raft_options.raft_tick_interval.into());
        tick_interval.set_missed_tick_behavior(MissedTickBehavior::Burst);
        let status_update_interval = time::interval(raft_options.status_update_interval.into());

        let member = Member {
            _logger: logger,
            is_leader: false,
            my_member_id,
            configuration,
            raw_node,
            connection_manager,
            networking,
            raft_rx,
            kv_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            tick_interval,
            status_update_interval,
            status_tx,
            pending_join_requests: HashMap::default(),
            read_index_to_request_id: VecDeque::default(),
            snapshot_summary,
        };

        member.validate_metadata_server_configuration();

        Ok(member)
    }

    #[instrument(level = "info", skip_all, fields(member_id = %self.my_member_id))]
    pub async fn run(mut self) -> Result<Standby, Error> {
        info!(configuration = %self.configuration, "Run as member of the metadata cluster");
        self.update_status();

        let mut nodes_config_watch =
            Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));
        nodes_config_watch.mark_changed();

        loop {
            tokio::select! {
                Some(request) = self.request_rx.recv() => {
                    self.handle_request(request);
                },
                Some(request) = self.join_cluster_rx.recv() => {
                    self.handle_join_request(request);
                }
                Some(raft) = self.raft_rx.recv() => {
                    self.raw_node.step(raft)?;
                },
                Ok(()) = nodes_config_watch.changed() => {
                    self.update_node_addresses();
                },
                _ = self.tick_interval.tick() => {
                    self.raw_node.tick();
                },
                _ = self.status_update_interval.tick() => {
                    self.update_status();
                }
            }

            self.on_ready().await?;
            self.update_leadership();
        }
    }

    fn update_leadership(&mut self) {
        let previous_is_leader = self.is_leader;
        self.is_leader = self.raw_node.raft.leader_id == self.raw_node.raft.id;

        if previous_is_leader && !self.is_leader {
            let known_leader = self.known_leader();
            info!(
                possible_leader = ?known_leader,
                "Lost metadata cluster leadership"
            );

            // todo we might fail some of the request too eagerly here because the answer might be
            //  stored in the unapplied log entries. Better to fail the callbacks based on
            //  (term, index).
            // we lost leadership :-( notify callers that their requests might not get committed
            // because we don't know whether the leader will start with the same log as we have.
            self.kv_storage.fail_pending_requests(|| {
                RequestError::Unavailable("lost leadership".into(), known_leader.clone())
            });
            self.fail_join_callbacks(|| JoinClusterError::NotLeader(known_leader.clone()));
            self.read_index_to_request_id.clear();
        } else if !previous_is_leader && self.is_leader {
            info!("Won metadata cluster leadership");
        }
    }

    fn handle_request(&mut self, request: MetadataStoreRequest) {
        let request = request.into_request();
        trace!("Handle metadata store request: {request:?}");

        if !self.is_leader {
            request.fail(RequestError::Unavailable(
                "not leader".into(),
                self.known_leader(),
            ));
            return;
        }

        match request {
            Request::ReadOnly(read_only_request) => {
                let read_ctx = read_only_request.request_id.to_bytes().to_vec();

                let previous_ready_read_count = self.raw_node.raft.ready_read_count();
                let previous_pending_read_count = self.raw_node.raft.pending_read_count();

                self.raw_node.read_index(read_ctx);

                // check whether the read request was silently dropped
                if previous_ready_read_count == self.raw_node.raft.ready_read_count()
                    && previous_pending_read_count == self.raw_node.raft.pending_read_count()
                {
                    // fail the request if we cannot serve read-only requests yet
                    read_only_request.fail(RequestError::Unavailable("Cannot serve read-only queries yet because the latest commit index has not been retrieved. Try again in a bit".into(), self.known_leader()));
                } else {
                    self.kv_storage
                        .register_read_only_request(read_only_request);
                }
            }
            Request::Write { request, callback } => {
                if let Err(err) = request
                    .encode_to_vec()
                    .map_err(Into::into)
                    .and_then(|request| {
                        self.raw_node
                            .propose(vec![], request)
                            .map_err(RequestError::from)
                    })
                {
                    info!(%err, "Failed handling request");
                    callback.fail(err)
                } else {
                    self.kv_storage.register_callback(callback);
                }
            }
        }
    }

    fn handle_join_request(&mut self, join_cluster_request: JoinClusterRequest) {
        let (response_tx, joining_member_id) = join_cluster_request.into_inner();

        trace!("Handle join request from node '{}'", joining_member_id);

        // sanity checks

        if !self.is_leader {
            let _ = response_tx.send(Err(JoinClusterError::NotLeader(self.known_leader())));
            return;
        }

        if self.raw_node.raft.has_pending_conf() {
            let _ = response_tx.send(Err(JoinClusterError::PendingReconfiguration));
            return;
        }

        if self.is_member(joining_member_id) {
            let _ = response_tx.send(Ok(()));
            return;
        }

        if self.is_member_plain_node_id(joining_member_id.node_id) {
            let warning = format!(
                "Node '{}' has registered before with a different storage id. This indicates that this node has lost its disk. Rejecting the join attempt.",
                joining_member_id
            );
            warn!(warning);
            let _ = response_tx.send(Err(JoinClusterError::Internal(warning)));
            return;
        }

        let metadata_nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        let nodes_config =
            Self::latest_nodes_configuration(&self.kv_storage, &metadata_nodes_config);

        let Ok(joining_node_config) = nodes_config.find_node_by_id(joining_member_id.node_id)
        else {
            let _ = response_tx.send(Err(JoinClusterError::UnknownNode(
                joining_member_id.node_id,
            )));
            return;
        };

        if !joining_node_config.has_role(Role::MetadataServer) {
            let _ = response_tx.send(Err(JoinClusterError::InvalidRole(
                joining_member_id.node_id,
            )));
            return;
        }

        if joining_node_config
            .metadata_server_config
            .metadata_server_state
            == MetadataServerState::Standby
        {
            let _ = response_tx.send(Err(JoinClusterError::Standby(joining_member_id.node_id)));
            return;
        }

        // It's possible to batch multiple new joining nodes into a single conf change if we want.
        // This will, however, require joint consensus.
        let mut conf_change_single = ConfChangeSingle::new();
        conf_change_single.change_type = ConfChangeType::AddNode;
        conf_change_single.node_id = to_raft_id(joining_member_id.node_id);

        let mut conf_change = ConfChangeV2::new();
        conf_change.set_changes(vec![conf_change_single].into());

        let mut next_configuration = self.configuration.clone();
        next_configuration.version = next_configuration.version.next();
        next_configuration.members.insert(
            joining_member_id.node_id,
            joining_member_id.created_at_millis,
        );

        let next_configuration_bytes =
            grpc::MetadataServerConfiguration::from(next_configuration).encode_to_vec();

        if let Err(err) = self
            .raw_node
            .propose_conf_change(next_configuration_bytes, conf_change)
        {
            let response = match err {
                RaftError::ProposalDropped => JoinClusterError::ProposalDropped,
                err => JoinClusterError::Internal(err.to_string()),
            };

            let _ = response_tx.send(Err(response));
        } else {
            info!(
                "Adding node '{}' to metadata cluster",
                joining_member_id.node_id
            );
            self.register_join_callback(joining_member_id, response_tx);
        }
    }

    async fn on_ready(&mut self) -> Result<(), Error> {
        if !self.raw_node.has_ready() {
            return Ok(());
        }

        let mut ready = self.raw_node.ready();

        // first need to send outgoing messages
        if !ready.messages().is_empty() {
            self.send_messages(ready.take_messages());
        }

        // apply snapshot if one was sent
        if !ready.snapshot().is_empty() {
            self.apply_snapshot(ready.snapshot()).await?;
        }

        // handle read states
        self.handle_read_states(ready.take_read_states()).await?;

        // then handle committed entries
        self.handle_committed_entries(ready.take_committed_entries())
            .await?;

        // append new Raft entries to storage
        self.raw_node.mut_store().append(ready.entries()).await?;

        // update the hard state if an update was produced (e.g. vote has happened)
        if let Some(hs) = ready.hs() {
            self.raw_node
                .mut_store()
                .store_hard_state(hs.clone())
                .await?;
        }

        // send persisted messages (after entries were appended and hard state was updated)
        if !ready.persisted_messages().is_empty() {
            self.send_messages(ready.take_persisted_messages());
        }

        // advance the raft node
        let mut light_ready = self.raw_node.advance(ready);

        // update the commit index if it changed
        if let Some(_commit) = light_ready.commit_index() {
            // todo update commit index in cached hard_state once we cache it; no need to persist it though
        }

        // send outgoing messages
        if !light_ready.messages().is_empty() {
            self.send_messages(light_ready.take_messages());
        }

        // handle committed entries
        if !light_ready.committed_entries().is_empty() {
            self.handle_committed_entries(light_ready.take_committed_entries())
                .await?;
        }

        self.raw_node.advance_apply();

        // after we have applied new entries, check whether we can fulfill some read-only requests
        self.handle_read_only_requests();

        self.try_trim_log().await?;
        self.check_requested_snapshot().await?;

        Ok(())
    }

    async fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        Self::restore_fsm_snapshot(
            snapshot.get_data(),
            &mut self.configuration,
            &mut self.kv_storage,
        )?;
        info!(configuration = %self.configuration, "Restored configuration from snapshot");

        self.validate_metadata_server_configuration();

        self.raw_node.mut_store().apply_snapshot(snapshot).await?;

        self.snapshot_summary = Some(SnapshotSummary::from_snapshot(snapshot));

        Ok(())
    }

    fn restore_fsm_snapshot(
        mut data: &[u8],
        configuration: &mut MetadataServerConfiguration,
        kv_storage: &mut KvMemoryStorage,
    ) -> Result<(), RestoreSnapshotError> {
        if data.is_empty() {
            return Ok(());
        }

        let mut snapshot = MetadataServerSnapshot::decode(&mut data)?;
        *configuration = snapshot
            .configuration
            .take()
            .map(MetadataServerConfiguration::from)
            .expect("configuration metadata expected");

        kv_storage.restore(snapshot)?;
        Ok(())
    }

    fn send_messages(&mut self, messages: Vec<Message>) {
        for message in messages {
            let snapshot_target = if message.has_snapshot() {
                Some(message.to)
            } else {
                None
            };

            if let Err(err) = self.networking.try_send(message) {
                trace!("failed sending message: {err}");

                if let Some(message) = err.into_message() {
                    if message.has_snapshot() {
                        self.raw_node
                            .report_snapshot(message.to, SnapshotStatus::Failure);
                    } else {
                        self.raw_node.report_unreachable(message.to);
                    }
                }
            } else if let Some(snapshot_target) = snapshot_target {
                self.raw_node
                    .report_snapshot(snapshot_target, SnapshotStatus::Finish);
            }
        }
    }

    async fn handle_read_states(&mut self, read_states: Vec<raft::ReadState>) -> Result<(), Error> {
        for read_state in read_states {
            let request_id =
                Ulid::from_bytes(read_state.request_ctx.try_into().map_err(|_err| {
                    Error::DecodeRequest("could not deserialize Ulid from read request ctx".into())
                })?);

            if read_state.index <= self.raw_node.raft.raft_log.applied {
                self.kv_storage.handle_read_only_request(request_id);
            } else {
                self.read_index_to_request_id
                    .push_back((read_state.index, request_id));
            }
        }

        Ok(())
    }

    fn handle_read_only_requests(&mut self) {
        let applied_index = self.raw_node.raft.raft_log.applied;
        while self
            .read_index_to_request_id
            .front()
            .is_some_and(|(index, _)| *index <= applied_index)
        {
            let (_, request_id) = self
                .read_index_to_request_id
                .pop_front()
                .expect("to be present");
            self.kv_storage.handle_read_only_request(request_id);
        }
    }

    async fn handle_committed_entries(
        &mut self,
        committed_entries: Vec<Entry>,
    ) -> Result<(), Error> {
        for entry in committed_entries {
            if entry.data.is_empty() {
                // new leader was elected
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => self.handle_normal_entry(entry)?,
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_conf_change(entry).await?
                }
            }
        }

        Ok(())
    }

    fn handle_normal_entry(&mut self, entry: Entry) -> Result<(), Error> {
        let request = WriteRequest::decode_from_bytes(entry.data)
            .map_err(|err| Error::DecodeRequest(err.into()))?;
        self.kv_storage.handle_request(request);

        Ok(())
    }

    async fn handle_conf_change(&mut self, entry: Entry) -> Result<(), ConfChangeError> {
        let cc_v2 = match entry.entry_type {
            EntryType::EntryNormal => {
                panic!("normal entries should be handled by handle_normal_entry")
            }
            EntryType::EntryConfChange => {
                let mut cc = ConfChange::default();
                cc.merge_from_bytes(&entry.data)?;
                cc.into_v2()
            }
            EntryType::EntryConfChangeV2 => {
                let mut cc = ConfChangeV2::default();
                cc.merge_from_bytes(&entry.data)?;
                cc
            }
        };

        self.raw_node.apply_conf_change(&cc_v2)?;

        let new_configuration = MetadataServerConfiguration::from(
            grpc::MetadataServerConfiguration::decode(entry.context)?,
        );

        // sanity checks
        assert_eq!(
            self.configuration.version.next(),
            new_configuration.version,
            "new configuration version must be '{}' but was '{}'",
            self.configuration.version.next(),
            new_configuration.version
        );
        self.configuration = new_configuration;

        info!(configuration = %self.configuration, "Applied new configuration");

        self.validate_metadata_server_configuration();

        self.update_membership_in_nodes_configuration();

        self.create_snapshot(entry.index, entry.term).await?;

        self.answer_join_callbacks();
        self.update_leadership();
        self.update_node_addresses();
        self.update_status();

        Ok(())
    }

    fn validate_metadata_server_configuration(&self) {
        assert_eq!(
            self.configuration.members.len(),
            self.raw_node.raft.prs().conf().voters().ids().len(),
            "number of members in configuration doesn't match number of voters in Raft"
        );
        for voter in self.raw_node.raft.prs().conf().voters().ids().iter() {
            assert!(
                self.configuration
                    .members
                    .contains_key(&to_plain_node_id(voter)),
                "voter '{}' in Raft configuration not found in MetadataServerConfiguration",
                voter
            );
        }
    }

    /// Checks whether it's time to snapshot the state machine and trim the Raft log.
    async fn try_trim_log(&mut self) -> Result<(), Error> {
        // todo make configurable
        const TRIM_THRESHOLD: u64 = 1000;
        let applied_index = self.raw_node.raft.raft_log.applied();
        if applied_index.saturating_sub(self.raw_node.store().get_first_index()) >= TRIM_THRESHOLD {
            debug!(
                "Trimming Raft log: [{}, {applied_index}]",
                self.raw_node.store().get_first_index()
            );
            self.create_snapshot(
                applied_index,
                self.raw_node.raft.raft_log.term(applied_index)?,
            )
            .await?;
        }

        Ok(())
    }

    /// Checks whether Raft requested a newer snapshot.
    async fn check_requested_snapshot(&mut self) -> Result<(), Error> {
        if let Some(index) = self.raw_node.mut_store().requested_snapshot() {
            let applied_index = self.raw_node.raft.raft_log.applied;
            if index <= applied_index {
                debug!("Creating requested snapshot for index '{index}'.");
                self.create_snapshot(
                    applied_index,
                    self.raw_node.raft.raft_log.term(applied_index)?,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn create_snapshot(&mut self, index: u64, term: u64) -> Result<(), CreateSnapshotError> {
        let mut snapshot = MetadataServerSnapshot {
            configuration: Some(grpc::MetadataServerConfiguration::from(
                self.configuration.clone(),
            )),
            ..MetadataServerSnapshot::default()
        };
        self.kv_storage.snapshot(&mut snapshot);
        let mut data = BytesMut::new();
        snapshot.encode(&mut data)?;

        let mut snapshot = Snapshot::new();
        let mut metadata = SnapshotMetadata::new();
        metadata.set_index(index);
        metadata.set_term(term);
        metadata.set_conf_state(self.raw_node.raft.prs().conf().to_conf_state());
        snapshot.set_data(data.freeze());
        snapshot.set_metadata(metadata);

        debug!(%index, %term, "Created snapshot: '{}' bytes", snapshot.get_data().len());

        self.raw_node.mut_store().apply_snapshot(&snapshot).await?;
        self.snapshot_summary = Some(SnapshotSummary::from_snapshot(&snapshot));

        Ok(())
    }

    fn update_membership_in_nodes_configuration(&mut self) {
        let mut new_nodes_configuration = self.kv_storage.last_seen_nodes_configuration().clone();
        let previous_version = new_nodes_configuration.version();

        for (node_id, node_config) in new_nodes_configuration.iter_mut() {
            if self.is_member_plain_node_id(node_id) {
                // Should be a no-op since we currently use Member also to tell nodes to try join
                // the cluster. This needs to change once we support removing nodes from the
                // metadata store cluster.
                node_config.metadata_server_config.metadata_server_state =
                    MetadataServerState::Member;
            }
        }

        new_nodes_configuration.increment_version();

        debug!(
            "Update membership after reconfiguration in NodesConfiguration '{}'",
            new_nodes_configuration.version()
        );

        let versioned_value = serialize_value(&new_nodes_configuration)
            .expect("should be able to serialize NodesConfiguration");
        self.kv_storage
            .put(
                NODES_CONFIG_KEY.clone(),
                versioned_value,
                Precondition::MatchesVersion(previous_version),
            )
            .expect("should be able to update NodesConfiguration with new members");
    }

    fn register_join_callback(
        &mut self,
        member_id: MemberId,
        reconfiguration_callback: oneshot::Sender<Result<(), JoinClusterError>>,
    ) {
        if let Some(previous_callback) = self
            .pending_join_requests
            .insert(member_id, reconfiguration_callback)
        {
            let _ =
                previous_callback.send(Err(JoinClusterError::ConcurrentRequest(member_id.node_id)));
        }
    }

    fn fail_join_callbacks(&mut self, cause: impl Fn() -> JoinClusterError) {
        for (_, response_tx) in self.pending_join_requests.drain() {
            let _ = response_tx.send(Err(cause()));
        }
    }

    fn answer_join_callbacks(&mut self) {
        let pending_join_request: Vec<_> = self.pending_join_requests.drain().collect();
        for (member_id, response_tx) in pending_join_request {
            if self.is_member(member_id) {
                let _ = response_tx.send(Ok(()));
            } else {
                // latest reconfiguration didn't include this node, fail it so that caller can retry
                let _ = response_tx.send(Err(JoinClusterError::Internal(format!(
                    "failed to include node '{}' in new configuration",
                    member_id
                ))));
            }
        }
    }

    fn update_node_addresses(&mut self) {
        let metadata_nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        let nodes_config =
            Self::latest_nodes_configuration(&self.kv_storage, &metadata_nodes_config);

        trace!(
            "Update node addresses in networking based on NodesConfiguration '{}'",
            nodes_config.version()
        );

        for node_id in self.raw_node.raft.prs().conf().voters().ids().iter() {
            let plain_node_id = to_plain_node_id(node_id);
            if let Ok(node_config) = nodes_config.find_node_by_id(plain_node_id) {
                self.networking
                    .register_address(plain_node_id, node_config.address.clone());
            }
        }
    }

    fn update_status(&self) {
        self.status_tx.send_modify(|current_status| {
            // todo fix member id to contain correct storage id
            let current_leader = if self.raw_node.raft.leader_id == INVALID_ID {
                None
            } else {
                Some(to_plain_node_id(self.raw_node.raft.leader_id))
            };

            if let MetadataServerSummary::Member {
                leader,
                configuration,
                raft,
                snapshot,
            } = current_status
            {
                *leader = current_leader;
                if configuration.version != self.configuration.version {
                    *configuration = self.configuration.clone();
                }
                *raft = self.raft_summary();
                *snapshot = self.snapshot_summary.clone();
            } else {
                let raft = self.raft_summary();

                *current_status = MetadataServerSummary::Member {
                    leader: current_leader,
                    configuration: self.configuration.clone(),
                    raft,
                    snapshot: self.snapshot_summary.clone(),
                };
            }
        });

        self.record_summary_metrics(&self.status_tx.borrow());
    }

    fn record_summary_metrics(&self, summary: &MetadataServerSummary) {
        let MetadataServerSummary::Member {
            leader,
            raft,
            snapshot,
            ..
        } = summary
        else {
            return;
        };

        if let Some(id) = leader {
            gauge!(METADATA_SERVER_REPLICATED_LEADER_ID).set(u32::from(*id) as f64);
        } else {
            gauge!(METADATA_SERVER_REPLICATED_LEADER_ID).set(INVALID_ID as f64);
        }

        if let Some(snapshot) = snapshot {
            gauge!(METADATA_SERVER_REPLICATED_SNAPSHOT_SIZE_BYTES).set(snapshot.size as f64);
        } else {
            gauge!(METADATA_SERVER_REPLICATED_SNAPSHOT_SIZE_BYTES).set(0);
        }

        gauge!(METADATA_SERVER_REPLICATED_TERM).set(raft.term as f64);
        gauge!(METADATA_SERVER_REPLICATED_APPLIED_LSN).set(raft.applied as f64);
        gauge!(METADATA_SERVER_REPLICATED_COMMITTED_LSN).set(raft.committed as f64);
        gauge!(METADATA_SERVER_REPLICATED_FIRST_INDEX).set(raft.first_index as f64);
        gauge!(METADATA_SERVER_REPLICATED_LAST_INDEX).set(raft.last_index as f64);
    }

    fn raft_summary(&self) -> RaftSummary {
        RaftSummary {
            term: self.raw_node.raft.term,
            committed: self.raw_node.raft.raft_log.committed,
            applied: self.raw_node.raft.raft_log.applied,
            last_index: self.raw_node.store().get_last_index(),
            first_index: self.raw_node.store().get_first_index(),
        }
    }

    fn is_member(&self, member_id: MemberId) -> bool {
        self.configuration.members.get(&member_id.node_id) == Some(&member_id.created_at_millis)
    }

    fn is_member_plain_node_id(&self, node_id: PlainNodeId) -> bool {
        self.configuration.members.contains_key(&node_id)
    }

    fn latest_nodes_configuration<'a>(
        kv_storage: &'a KvMemoryStorage,
        metadata_nodes_config: &'a NodesConfiguration,
    ) -> &'a NodesConfiguration {
        let kv_storage_nodes_config = kv_storage.last_seen_nodes_configuration();

        if metadata_nodes_config.version() > kv_storage_nodes_config.version() {
            metadata_nodes_config
        } else {
            kv_storage_nodes_config
        }
    }

    /// Returns the known leader from the Raft instance or a random known leader from the
    /// current nodes configuration.
    fn known_leader(&self) -> Option<KnownLeader> {
        if self.raw_node.raft.leader_id == INVALID_ID {
            return None;
        }

        let leader = to_plain_node_id(self.raw_node.raft.leader_id);

        let metadata_nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        let nodes_config =
            Self::latest_nodes_configuration(&self.kv_storage, &metadata_nodes_config);
        nodes_config
            .find_node_by_id(leader)
            .ok()
            .map(|node_config| KnownLeader {
                node_id: leader,
                address: node_config.address.clone(),
            })
    }
}

#[allow(dead_code)]
struct Standby {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    storage: RocksDbStorage,
    request_rx: RequestReceiver,
    join_cluster_rx: JoinClusterReceiver,
    metadata_writer: Option<MetadataWriter>,
    status_tx: StatusSender,
}

impl Standby {
    fn new(
        storage: RocksDbStorage,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
        status_tx: StatusSender,
    ) -> Self {
        connection_manager.store(None);

        Standby {
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
        }
    }

    #[instrument(level = "info", skip_all, fields(member_id = tracing::field::Empty))]
    async fn run(self) -> Result<Member, Error> {
        debug!("Run as standby metadata server.");

        let Standby {
            connection_manager,
            mut storage,
            mut request_rx,
            mut join_cluster_rx,
            metadata_writer,
            status_tx,
        } = self;

        let _ = status_tx.send(MetadataServerSummary::Standby);

        // todo make configurable
        let mut join_retry_policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            None,
            Some(Duration::from_secs(5)),
        )
        .into_iter();

        let created_at_millis = storage
            .get_marker()?
            .expect("StorageMarker must be present")
            .created_at()
            .timestamp_millis();

        let mut join_cluster: std::pin::Pin<&mut OptionFuture<_>> = std::pin::pin!(None.into());

        let mut nodes_config_watcher =
            Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));
        nodes_config_watcher.mark_changed();
        let my_node_name = Configuration::pinned().common.node_name().to_owned();
        let mut my_member_id = None;

        loop {
            tokio::select! {
                Some(request) = request_rx.recv() => {
                    let request = request.into_request();
                    request.fail(RequestError::Unavailable(
                        "Not being part of the metadata store cluster.".into(),
                        Standby::random_member(),
                    ))
                },
                Some(request) = join_cluster_rx.recv() => {
                    let _ = request.response_tx.send(Err(JoinClusterError::NotMember(Standby::random_member())));
                }
                Some(join_result) = &mut join_cluster => {
                    match join_result {
                        Ok(my_member_id) => {
                            storage.store_raft_server_state(&RaftServerState::Member{ my_member_id }).await?;

                            return Member::create(
                                my_member_id,
                                connection_manager,
                                storage,
                                request_rx,
                                join_cluster_rx,
                                metadata_writer,
                                status_tx);
                        },
                        Err(err) => {
                            debug!("Failed joining raft cluster. Retrying. {err}");

                            match err {
                               JoinError::Rpc(_, known_leader) => {
                                    // if we have learned about a new leader, then try immediately rejoining
                                    join_cluster.set(Some(Self::join_cluster(known_leader, None, my_member_id.expect("to be known")).fuse()).into());
                                }
                                _ => {
                                    join_cluster.set(Some(Self::join_cluster(None, join_retry_policy.next(), my_member_id.expect("to be known")).fuse()).into());
                                }
                            }

                        }
                    }
                }
                _ = nodes_config_watcher.changed() => {
                    let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
                    if let Some(node_config) = nodes_config.find_node_by_name(&my_node_name) {
                        if my_member_id.is_none() {
                            let member_id = MemberId::new(node_config.current_generation.as_plain(), created_at_millis);
                            Span::current().record("member_id", member_id.to_string());
                            my_member_id = Some(member_id);
                        }

                        if matches!(node_config.metadata_server_config.metadata_server_state, MetadataServerState::Member) {
                            if join_cluster.is_terminated() {
                                debug!("Node is part of the metadata store cluster. Trying to join the raft cluster.");

                                // Persist latest NodesConfiguration so that we know about the MetadataServerState at least
                                // as of now when restarting.
                                storage
                                    .store_nodes_configuration(&nodes_config)
                                    .await?;
                                join_cluster.set(Some(Self::join_cluster(None, None, my_member_id.expect("MemberId to be known")).fuse()).into());
                            }
                        } else {
                            debug!("Node is not part of the metadata store cluster. Waiting to become a candidate.");
                            join_cluster.set(None.into());
                        }
                    } else {
                        trace!("Node '{}' has not joined the cluster yet :-(", my_node_name);
                    }
                }
            }
        }
    }

    async fn join_cluster(
        known_leader: Option<KnownLeader>,
        join_delay: Option<Duration>,
        member_id: MemberId,
    ) -> Result<MemberId, JoinError> {
        if let Some(delay) = join_delay {
            time::sleep(delay).await
        }
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

        let address = if let Some(known_leader) = known_leader {
            debug!(
                "Trying to join metadata store at node '{}'",
                known_leader.node_id
            );
            known_leader.address
        } else {
            // pick random metadata store member node
            let member_node = nodes_config.iter().filter_map(|(node, config)| {
                if config.has_role(Role::MetadataServer) && node != member_id.node_id && matches!(config.metadata_server_config.metadata_server_state, MetadataServerState::Member) {
                    Some(node)
                } else {
                    None
                }
            }).choose(&mut rng()).ok_or(JoinError::Other("No other metadata store member present in the cluster. This indicates a misconfiguration.".into()))?;

            debug!(
                "Trying to join metadata store cluster at randomly chosen node '{}'",
                member_node
            );

            nodes_config
                .find_node_by_id(member_node)
                .expect("must be present")
                .address
                .clone()
        };

        let channel = create_tonic_channel(address, &Configuration::pinned().networking);

        if let Err(status) = new_metadata_server_network_client(channel)
            .join_cluster(crate::raft::network::grpc_svc::JoinClusterRequest {
                node_id: u32::from(member_id.node_id),
                created_at_millis: member_id.created_at_millis,
            })
            .await
        {
            let known_leader = KnownLeader::from_status(&status);
            Err(JoinError::Rpc(status, known_leader))?
        };

        Ok(member_id)
    }

    /// Returns a random metadata store member from the current nodes configuration.
    fn random_member() -> Option<KnownLeader> {
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

        nodes_config
            .iter()
            .filter_map(|(node_id, node_config)| {
                if node_config.metadata_server_config.metadata_server_state
                    == MetadataServerState::Member
                {
                    Some((node_id, node_config))
                } else {
                    None
                }
            })
            .choose(&mut rng())
            .map(|(node_id, node_config)| KnownLeader {
                node_id,
                address: node_config.address.clone(),
            })
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
