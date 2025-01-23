// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::network::grpc_svc::metadata_store_network_svc_client::MetadataStoreNetworkSvcClient;
use crate::network::{ConnectionManager, Networking};
use crate::raft::kv_memory_storage::KvMemoryStorage;
use crate::raft::storage::RocksDbStorage;
use crate::raft::{storage, RaftConfiguration};
use crate::{
    prepare_initial_nodes_configuration, InvalidConfiguration, JoinClusterError, JoinClusterHandle,
    JoinClusterReceiver, JoinClusterRequest, JoinClusterSender, JoinError, KnownLeader, MemberId,
    MetadataStoreBackend, MetadataStoreConfiguration, MetadataStoreRequest, MetadataStoreSummary,
    ProvisionError, ProvisionReceiver, ProvisionSender, Request, RequestError, RequestKind,
    RequestReceiver, RequestSender, StatusSender, StatusWatch, StorageId, WriteRequest,
};
use arc_swap::ArcSwapOption;
use bytes::BytesMut;
use flexbuffers::{DeserializationError, SerializationError};
use futures::future::{FusedFuture, OptionFuture};
use futures::never::Never;
use futures::FutureExt;
use futures::TryFutureExt;
use protobuf::{Message as ProtobufMessage, ProtobufError};
use raft::prelude::{ConfChange, ConfChangeV2, ConfState, Entry, EntryType, Message};
use raft::{
    Config, Error as RaftError, RawNode, ReadOnlyOption, SnapshotStatus, Storage, INVALID_ID,
};
use raft_proto::eraftpb::{ConfChangeSingle, ConfChangeType, Snapshot, SnapshotMetadata};
use raft_proto::ConfChangeI;
use rand::prelude::IteratorRandom;
use rand::{random, thread_rng};
use restate_core::metadata_store::{serialize_value, Precondition};
use restate_core::network::net_util::create_tonic_channel;
use restate_core::{
    cancellation_watcher, Metadata, MetadataWriter, ShutdownError, TaskCenter, TaskKind,
};
use restate_types::config::{Configuration, RocksDbOptions};
use restate_types::errors::GenericError;
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration, Role};
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::retries::RetryPolicy;
use restate_types::{PlainNodeId, Version};
use slog::o;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, instrument, trace, warn, Span};
use tracing_slog::TracingSlogDrain;
use ulid::Ulid;

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
    #[error("failed deserializing conf change: {0}")]
    DecodeConfChange(ProtobufError),
    #[error("failed applying conf change: {0}")]
    ApplyConfChange(raft::Error),
    #[error("failed reading/writing from/to storage: {0}")]
    Storage(#[from] storage::Error),
    #[error("failed restoring the snapshot: {0}")]
    RestoreSnapshot(#[from] DeserializationError),
    #[error("failed creating snapshot: {0}")]
    CreateSnapshot(#[from] SerializationError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

pub struct RaftMetadataStore {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    storage: RocksDbStorage,

    storage_id: StorageId,
    metadata_writer: Option<MetadataWriter>,
    health_status: Option<HealthStatus<MetadataServerStatus>>,

    request_tx: RequestSender,
    request_rx: RequestReceiver,

    provision_tx: ProvisionSender,
    provision_rx: Option<ProvisionReceiver>,

    join_cluster_tx: JoinClusterSender,
    join_cluster_rx: JoinClusterReceiver,

    status_tx: StatusSender,
}

impl RaftMetadataStore {
    pub async fn create(
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
        metadata_writer: Option<MetadataWriter>,
        health_status: HealthStatus<MetadataServerStatus>,
    ) -> Result<Self, BuildError> {
        health_status.update(MetadataServerStatus::StartingUp);

        let (request_tx, request_rx) = mpsc::channel(2);
        let (provision_tx, provision_rx) = mpsc::channel(1);
        let (join_cluster_tx, join_cluster_rx) = mpsc::channel(1);
        let (status_tx, _status_rx) = watch::channel(MetadataStoreSummary::default());

        let mut metadata_store_options =
            Configuration::updateable().map(|configuration| &configuration.metadata_store);
        let mut storage =
            RocksDbStorage::create(metadata_store_options.live_load(), rocksdb_options).await?;

        // todo unify with LogServer logic; turn into timestamp?
        // make sure that the storage is initialized with a storage id to be able to detect disk losses
        let storage_id = if let Some(storage_id) = storage
            .get_storage_id()
            .map_err(|err| BuildError::InitStorage(err.to_string()))?
        {
            storage_id
        } else {
            let storage_id = random();
            storage
                .store_storage_id(storage_id)
                .await
                .map_err(|err| BuildError::InitStorage(err.to_string()))?;
            storage_id
        };

        debug!("Obtained storage id: {storage_id}");

        Ok(Self {
            connection_manager: Arc::default(),
            storage,
            storage_id,
            health_status: Some(health_status),
            request_rx,
            request_tx,
            provision_tx,
            provision_rx: Some(provision_rx),
            join_cluster_tx,
            join_cluster_rx,
            status_tx,
            metadata_writer,
        })
    }

    pub(crate) fn request_sender(&self) -> RequestSender {
        self.request_tx.clone()
    }

    pub(crate) fn provision_sender(&self) -> ProvisionSender {
        self.provision_tx.clone()
    }

    pub(crate) fn join_cluster_handle(&self) -> JoinClusterHandle {
        JoinClusterHandle::new(self.join_cluster_tx.clone())
    }

    pub(crate) fn status_watch(&self) -> StatusWatch {
        self.status_tx.subscribe()
    }

    pub(crate) fn connection_manager(&self) -> Arc<ArcSwapOption<ConnectionManager<Message>>> {
        Arc::clone(&self.connection_manager)
    }

    pub async fn run(mut self) -> Result<(), Error> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let health_status = self.health_status.take().expect("to be present");

        let result = tokio::select! {
            _ = &mut shutdown => {
                debug!("Shutting down RaftMetadataStore");
                Ok(())
            },
            result = self.run_inner(&health_status) => {
                result.map(|_| ())
            },
        };

        health_status.update(MetadataServerStatus::Unknown);

        result
    }

    async fn run_inner(
        mut self,
        health_status: &HealthStatus<MetadataServerStatus>,
    ) -> Result<Never, Error> {
        if let Some(metadata_writer) = self.metadata_writer.as_mut() {
            // Try to read a persisted nodes configuration in order to learn about the addresses of our
            // potential peers and the metadata store states.
            if let Some(nodes_configuration) = self.storage.get_nodes_configuration()? {
                metadata_writer
                    .update(Arc::new(nodes_configuration))
                    .await?
            }
        }

        health_status.update(MetadataServerStatus::AwaitingProvisioning);
        let mut provisioned = self.await_provisioning().await?;

        loop {
            match provisioned {
                Provisioned::Active(active) => {
                    health_status.update(MetadataServerStatus::Active);
                    provisioned = Provisioned::Passive(active.run().await?);
                }
                Provisioned::Passive(passive) => {
                    health_status.update(MetadataServerStatus::Passive);
                    provisioned = Provisioned::Active(passive.run().await?);
                }
            }
        }
    }

    async fn await_provisioning(mut self) -> Result<Provisioned, Error> {
        let _ = self.status_tx.send(MetadataStoreSummary::Provisioning);
        let mut provision_rx = self.provision_rx.take().expect("must be present");

        let result = if let Some(configuration) = self.storage.get_raft_configuration()? {
            debug!(member_id = %configuration.my_member_id, "Found existing metadata store configuration.");
            Provisioned::Active(self.become_active(configuration)?)
        } else {
            let mut nodes_config_watcher =
                Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));

            if *nodes_config_watcher.borrow_and_update() > Version::INVALID {
                // The metadata store must have been provisioned if there exists a
                // NodesConfiguration. So let's move on.
                debug!("Detected a valid NodesConfiguration. This indicates that the metadata store cluster has been provisioned.");
                Provisioned::Passive(self.become_passive())
            } else {
                info!("Cluster has not been provisioned, yet. Awaiting the provision signal.");
                loop {
                    tokio::select! {
                        Some(request) = self.request_rx.recv() => {
                            // fail incoming requests while we are waiting for the provision signal
                            let request = request.into_request();
                            request.fail(RequestError::Unavailable("Metadata store has not been provisioned yet.".into(), None))
                        },
                        Some(request) = self.join_cluster_rx.recv() => {
                            let _ = request.response_tx.send(Err(JoinClusterError::NotActive(None)));
                        },
                        Some(request) = provision_rx.recv() => {
                            match self.initialize_storage(request.nodes_configuration).await {
                                Ok(raft_configuration) => {
                                    let _ = request.result_tx.send(Ok(true));
                                    debug!(member_id = %raft_configuration.my_member_id, "Successfully provisioned the metadata store.");
                                    let mut active = self.become_active(raft_configuration)?;
                                    active.campaign_immediately()?;
                                    break Provisioned::Active(active);
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
                                debug!("Detected a valid NodesConfiguration. This indicates that the metadata store cluster has been provisioned.");
                                break Provisioned::Passive(self.become_passive())
                            }
                        }
                    }
                }
            }
        };

        TaskCenter::spawn_unmanaged(TaskKind::Background, "provision-responder", async move {
            while let Some(request) = provision_rx.recv().await {
                let _ = request.result_tx.send(Ok(false));
            }
        })?;

        Ok(result)
    }

    async fn initialize_storage(
        &mut self,
        mut nodes_configuration: NodesConfiguration,
    ) -> anyhow::Result<RaftConfiguration> {
        assert!(
            self.storage.is_empty()?,
            "storage must be empty to get initialized"
        );

        let raft_configuration = self.derive_initial_configuration(&mut nodes_configuration)?;

        debug!("Initialize storage with nodes configuration: {nodes_configuration:?}");

        let initial_conf_state = ConfState::from((
            vec![to_raft_id(raft_configuration.my_member_id.node_id)],
            vec![],
        ));

        // initialize storage with an initial snapshot so that newly started nodes will fetch it
        // first to start with the same initial conf state.
        let mut snapshot = Snapshot::new();
        snapshot.mut_metadata().term = RAFT_INITIAL_LOG_TERM;
        snapshot.mut_metadata().index = RAFT_INITIAL_LOG_INDEX;
        snapshot.mut_metadata().set_conf_state(initial_conf_state);

        let value = serialize_value(&nodes_configuration)?;

        let request_data = WriteRequest::new(RequestKind::Put {
            precondition: Precondition::None,
            key: NODES_CONFIG_KEY.clone(),
            value,
        })
        .encode_to_vec()?;

        let entry = Entry {
            data: request_data.into(),
            term: RAFT_INITIAL_LOG_TERM,
            index: RAFT_INITIAL_LOG_INDEX + 1,
            ..Entry::default()
        };

        let mut txn = self.storage.txn();
        // it's important to first apply the snapshot so that the initial entry has the right index
        txn.apply_snapshot(&snapshot)?;
        txn.append(&vec![entry])?;
        txn.store_raft_configuration(&raft_configuration)?;
        txn.store_nodes_configuration(&nodes_configuration)?;
        txn.commit().await?;

        Ok(raft_configuration)
    }

    fn derive_initial_configuration(
        &self,
        nodes_configuration: &mut NodesConfiguration,
    ) -> Result<RaftConfiguration, InvalidConfiguration> {
        let configuration = Configuration::pinned();
        let node_id = prepare_initial_nodes_configuration(&configuration, nodes_configuration)?;

        // set our own raft node id to be Restate's plain node id
        let my_member_id = MemberId {
            node_id,
            storage_id: self.storage_id,
        };

        Ok(RaftConfiguration { my_member_id })
    }

    fn become_passive(self) -> Passive {
        let Self {
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            storage_id,
            status_tx,
            ..
        } = self;

        Passive::new(
            storage,
            connection_manager,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            storage_id,
            status_tx,
        )
    }

    fn become_active(self, raft_configuration: RaftConfiguration) -> Result<Active, Error> {
        let Self {
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            ..
        } = self;

        Active::create(
            raft_configuration,
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
        )
    }
}

impl MetadataStoreBackend for RaftMetadataStore {
    fn request_sender(&self) -> RequestSender {
        self.request_sender()
    }

    fn provision_sender(&self) -> Option<ProvisionSender> {
        Some(self.provision_sender())
    }

    fn status_watch(&self) -> Option<StatusWatch> {
        Some(self.status_watch())
    }

    fn run(self) -> impl Future<Output = anyhow::Result<()>> + Send + 'static {
        self.run().map_err(anyhow::Error::from)
    }
}

/// States of a provisioned metadata store. The metadata store can be either active or passive.
enum Provisioned {
    /// Being an active member of the metadata store cluster
    Active(Active),
    /// Not being a member of the metadata store cluster
    Passive(Passive),
}

#[allow(dead_code)]
struct Active {
    _logger: slog::Logger,

    raw_node: RawNode<RocksDbStorage>,
    networking: Networking<Message>,
    raft_rx: mpsc::Receiver<Message>,

    my_member_id: MemberId,
    kv_storage: KvMemoryStorage,
    is_leader: bool,
    pending_join_requests: HashMap<MemberId, oneshot::Sender<Result<(), JoinClusterError>>>,
    read_index_to_request_id: VecDeque<(u64, Ulid)>,

    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    metadata_writer: Option<MetadataWriter>,

    request_rx: RequestReceiver,
    join_cluster_rx: JoinClusterReceiver,
    status_tx: StatusSender,
}

impl Active {
    fn create(
        raft_configuration: RaftConfiguration,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
        storage: RocksDbStorage,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
        status_tx: StatusSender,
    ) -> Result<Self, Error> {
        let my_member_id = raft_configuration.my_member_id;

        let (raft_tx, raft_rx) = mpsc::channel(128);
        let new_connection_manager =
            ConnectionManager::new(to_raft_id(my_member_id.node_id), raft_tx);
        let mut networking = Networking::new(new_connection_manager.clone());

        networking.register_address(
            to_raft_id(my_member_id.node_id),
            Configuration::pinned().common.advertised_address.clone(),
        );

        // todo remove additional indirection from Arc
        connection_manager.store(Some(Arc::new(new_connection_manager)));

        // todo properly configure Raft instance
        let mut config = Config {
            id: to_raft_id(my_member_id.node_id),
            read_only_option: ReadOnlyOption::Safe,
            ..Default::default()
        };

        let drain = TracingSlogDrain;
        let logger = slog::Logger::root(drain, o!());

        let mut kv_storage = KvMemoryStorage::new(metadata_writer.clone());

        if let Ok(snapshot) = storage.snapshot(0, to_raft_id(my_member_id.node_id)) {
            config.applied = snapshot.get_metadata().get_index();

            if !snapshot.get_data().is_empty() {
                let mut data = snapshot.get_data();
                kv_storage.restore(&mut data)?;
            }
        }

        config.validate()?;

        let raw_node = RawNode::new(&config, storage, &logger)?;

        Ok(Active {
            _logger: logger,
            is_leader: false,
            my_member_id,
            raw_node,
            connection_manager,
            networking,
            raft_rx,
            kv_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            pending_join_requests: HashMap::default(),
            read_index_to_request_id: VecDeque::default(),
        })
    }

    /// Sets the Raft node up to start right away with a leader election.
    pub fn campaign_immediately(&mut self) -> Result<(), Error> {
        self.raw_node.campaign()?;
        Ok(())
    }

    #[instrument(level = "info", skip_all, fields(member_id = %self.my_member_id))]
    pub async fn run(mut self) -> Result<Passive, Error> {
        debug!("Run as active metadata store node");
        self.update_status();

        let mut tick_interval = time::interval(Duration::from_millis(100));
        tick_interval.set_missed_tick_behavior(MissedTickBehavior::Burst);

        let mut status_update_interval = time::interval(Duration::from_secs(5));

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
                    self.update_node_addresses(&Metadata::with_current(|m| m.nodes_config_ref()));
                },
                _ = tick_interval.tick() => {
                    self.raw_node.tick();
                },
                _ = status_update_interval.tick() => {
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
            debug!("Lost metadata store leadership");
            let known_leader = self.known_leader();

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
            debug!("Won metadata store leadership");
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
                self.raw_node.read_index(read_ctx);
                self.kv_storage
                    .register_read_only_request(read_only_request);
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
                    info!("Failed handling request: {err:?}");
                    callback.fail(err)
                } else {
                    self.kv_storage.register_callback(callback);
                }
            }
        }
    }

    fn handle_join_request(&mut self, join_cluster_request: JoinClusterRequest) {
        let (response_tx, joining_node_id, joining_storage_id) = join_cluster_request.into_inner();
        let joining_member_id =
            MemberId::new(PlainNodeId::from(joining_node_id), joining_storage_id);

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

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

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
            == MetadataServerState::Outsider
        {
            let _ = response_tx.send(Err(JoinClusterError::Outsider(joining_member_id.node_id)));
            return;
        }

        // It's possible to batch multiple new joining nodes into a single conf change if we want.
        // This will, however, require joint consensus.
        let mut conf_change_single = ConfChangeSingle::new();
        conf_change_single.change_type = ConfChangeType::AddNode;
        conf_change_single.node_id = to_raft_id(joining_member_id.node_id);

        let mut conf_change = ConfChangeV2::new();
        conf_change.set_changes(vec![conf_change_single].into());

        if let Err(err) = self.raw_node.propose_conf_change(Vec::new(), conf_change) {
            let response = match err {
                RaftError::ProposalDropped => JoinClusterError::ProposalDropped,
                err => JoinClusterError::Internal(err.to_string()),
            };

            let _ = response_tx.send(Err(response));
        } else {
            info!(
                "Triggered reconfiguration of metadata store cluster to add node '{}'",
                joining_member_id
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
            if !ready.snapshot().get_data().is_empty() {
                let mut data = ready.snapshot().get_data();
                self.kv_storage.restore(&mut data)?;
            }

            self.raw_node
                .mut_store()
                .apply_snapshot(ready.snapshot())
                .await?
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

    async fn handle_conf_change(&mut self, entry: Entry) -> Result<(), Error> {
        let cc_v2 = match entry.entry_type {
            EntryType::EntryNormal => {
                panic!("normal entries should be handled by handle_normal_entry")
            }
            EntryType::EntryConfChange => {
                let mut cc = ConfChange::default();
                cc.merge_from_bytes(&entry.data)
                    .map_err(Error::DecodeConfChange)?;
                cc.into_v2()
            }
            EntryType::EntryConfChangeV2 => {
                let mut cc = ConfChangeV2::default();
                cc.merge_from_bytes(&entry.data)
                    .map_err(Error::DecodeConfChange)?;
                cc
            }
        };

        self.raw_node
            .apply_conf_change(&cc_v2)
            .map_err(Error::ApplyConfChange)?;

        self.update_membership_in_nodes_configuration();

        self.create_snapshot(entry.index, entry.term).await?;

        self.answer_join_callbacks();
        self.update_leadership();
        self.update_node_addresses(&Metadata::with_current(|m| m.nodes_config_ref()));
        self.update_status();

        Ok(())
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

    async fn create_snapshot(&mut self, index: u64, term: u64) -> Result<(), Error> {
        let mut data = BytesMut::new();
        self.kv_storage.snapshot(&mut data)?;

        let mut snapshot = Snapshot::new();
        let mut metadata = SnapshotMetadata::new();
        metadata.set_index(index);
        metadata.set_term(term);
        metadata.set_conf_state(self.raw_node.raft.prs().conf().to_conf_state());
        snapshot.set_data(data.freeze());
        snapshot.set_metadata(metadata);

        debug!(%index, %term, "Created snapshot: '{}' bytes", snapshot.get_data().len());

        self.raw_node.mut_store().apply_snapshot(&snapshot).await?;

        Ok(())
    }

    fn update_membership_in_nodes_configuration(&mut self) {
        let mut new_nodes_configuration = self.kv_storage.last_seen_nodes_configuration().clone();
        let previous_version = new_nodes_configuration.version();

        for (node_id, node_config) in new_nodes_configuration.iter_mut() {
            if !self.is_member_plain_node_id(node_id) {
                node_config.metadata_server_config.metadata_server_state =
                    MetadataServerState::Outsider;
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

    fn update_node_addresses(&mut self, nodes_configuration: &NodesConfiguration) {
        trace!(
            "Update node addresses in networking based on NodesConfiguration '{}'",
            nodes_configuration.version()
        );
        for node_id in self.raw_node.raft.prs().conf().voters().ids().iter() {
            if let Ok(node_config) = nodes_configuration.find_node_by_id(PlainNodeId::from(
                u32::try_from(node_id).expect("node id is derived from PlainNodeId"),
            )) {
                self.networking
                    .register_address(node_id, node_config.address.clone());
            }
        }
    }

    fn update_status(&self) {
        self.status_tx.send_if_modified(|current_status| {
            // todo fix member id to contain correct storage id
            let current_leader = if self.raw_node.raft.leader_id == INVALID_ID {
                None
            } else {
                Some(MemberId::new(
                    to_plain_node_id(self.raw_node.raft.leader_id),
                    0,
                ))
            };

            if let MetadataStoreSummary::Active {
                leader,
                configuration,
            } = current_status
            {
                let mut modified = false;

                let members = self.current_members();
                if configuration.members != members {
                    // todo track the configuration id
                    configuration.members = members;
                    modified = true;
                }

                if *leader != current_leader {
                    *leader = current_leader;
                    modified = true;
                }

                modified
            } else {
                let members = self.current_members();

                *current_status = MetadataStoreSummary::Active {
                    leader: current_leader,
                    configuration: MetadataStoreConfiguration { id: 0, members },
                };

                true
            }
        });
    }

    fn current_members(&self) -> Vec<MemberId> {
        let members = self
            .raw_node
            .raft
            .prs()
            .conf()
            .voters()
            .ids()
            .iter()
            .map(|id| MemberId::new(to_plain_node_id(id), 0))
            .collect();
        members
    }

    fn is_member(&self, member_id: MemberId) -> bool {
        // todo check for storage id too
        self.raw_node
            .raft
            .prs()
            .conf()
            .voters()
            .contains(to_raft_id(member_id.node_id))
    }

    fn is_member_plain_node_id(&self, node_id: PlainNodeId) -> bool {
        // todo check for storage id too
        self.raw_node
            .raft
            .prs()
            .conf()
            .voters()
            .contains(to_raft_id(node_id))
    }

    /// Returns the known leader from the Raft instance or a random known leader from the
    /// current nodes configuration.
    fn known_leader(&self) -> Option<KnownLeader> {
        if self.raw_node.raft.leader_id == INVALID_ID {
            return Self::random_member();
        }

        let leader = to_plain_node_id(self.raw_node.raft.leader_id);

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        nodes_config
            .find_node_by_id(leader)
            .ok()
            .map(|node_config| KnownLeader {
                node_id: leader,
                address: node_config.address.clone(),
            })
            .or_else(Self::random_member)
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
            .choose(&mut thread_rng())
            .map(|(node_id, node_config)| KnownLeader {
                node_id,
                address: node_config.address.clone(),
            })
    }
}

#[allow(dead_code)]
struct Passive {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    storage: RocksDbStorage,
    request_rx: RequestReceiver,
    join_cluster_rx: JoinClusterReceiver,
    metadata_writer: Option<MetadataWriter>,
    storage_id: StorageId,
    status_tx: StatusSender,
}

impl Passive {
    fn new(
        storage: RocksDbStorage,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
        storage_id: StorageId,
        status_tx: StatusSender,
    ) -> Self {
        connection_manager.store(None);

        Passive {
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            storage_id,
            status_tx,
        }
    }

    #[instrument(level = "info", skip_all, fields(member_id = tracing::field::Empty))]
    async fn run(self) -> Result<Active, Error> {
        debug!("Run as passive metadata store node.");

        let Passive {
            connection_manager,
            mut storage,
            mut request_rx,
            mut join_cluster_rx,
            metadata_writer,
            storage_id,
            status_tx,
        } = self;

        let _ = status_tx.send(MetadataStoreSummary::Passive);

        // todo make configurable
        let mut join_retry_policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            None,
            Some(Duration::from_secs(5)),
        )
        .into_iter();

        // Persist latest NodesConfiguration so that we know about the MetadataServerState at least
        // as of now when restarting.
        storage
            .store_nodes_configuration(&Metadata::with_current(|m| m.nodes_config_ref()))
            .await?;

        let mut join_cluster: std::pin::Pin<&mut OptionFuture<_>> = std::pin::pin!(None.into());

        let mut nodes_config_watcher =
            Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));
        nodes_config_watcher.mark_changed();
        let my_node_name = Configuration::pinned().common.node_name().to_owned();
        let mut my_node_id = None;

        loop {
            tokio::select! {
                Some(request) = request_rx.recv() => {
                    let request = request.into_request();
                    request.fail(RequestError::Unavailable(
                        "Not being part of the metadata store cluster.".into(),
                        Active::random_member(),
                    ))
                },
                Some(request) = join_cluster_rx.recv() => {
                    let _ = request.response_tx.send(Err(JoinClusterError::NotActive(Active::random_member())));
                }
                Some(join_result) = &mut join_cluster => {
                    match join_result {
                        Ok(()) => {
                            let raft_configuration = RaftConfiguration {
                                my_member_id: MemberId::new(my_node_id.expect("must be set before"), storage_id),
                            };
                            storage.store_raft_configuration(&raft_configuration).await?;

                            return Active::create(
                                raft_configuration,
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
                                    join_cluster.set(Some(Self::join_cluster(known_leader, None, my_node_id.expect("must be set before"), storage_id).fuse()).into());
                                }
                                _ => {
                                    join_cluster.set(Some(Self::join_cluster(None, join_retry_policy.next(), my_node_id.expect("must be set before"), storage_id).fuse()).into());
                                }
                            }

                        }
                    }
                }
                _ = nodes_config_watcher.changed() => {
                    let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
                    if let Some(node_config) = nodes_config.find_node_by_name(&my_node_name) {
                        if my_node_id.is_none() {
                            my_node_id = Some(node_config.current_generation.as_plain());
                            Span::current().record("member_id", MemberId::new(my_node_id.unwrap(), self.storage_id).to_string());
                        }

                        if matches!(node_config.metadata_server_config.metadata_server_state, MetadataServerState::Member) && join_cluster.is_terminated() {
                            debug!("Node is part of the metadata store cluster. Trying to join the raft cluster.");
                            join_cluster.set(Some(Self::join_cluster(None, None, my_node_id.unwrap(), storage_id).fuse()).into());
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
        my_node_id: PlainNodeId,
        storage_id: StorageId,
    ) -> Result<(), JoinError> {
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
            let active_metadata_store_node = nodes_config.iter().filter_map(|(node, config)| {
                if config.has_role(Role::MetadataServer) && node != my_node_id && matches!(config.metadata_server_config.metadata_server_state, MetadataServerState::Member) {
                    Some(node)
                } else {
                    None
                }
            }).choose(&mut thread_rng()).ok_or(JoinError::Other("No other active metadata store present in the cluster. This indicates a misconfiguration.".into()))?;

            debug!(
                "Trying to join metadata store cluster at randomly chosen node '{}'",
                active_metadata_store_node
            );

            nodes_config
                .find_node_by_id(active_metadata_store_node)
                .expect("must be present")
                .address
                .clone()
        };

        let channel = create_tonic_channel(address, &Configuration::pinned().networking);

        let mut client = MetadataStoreNetworkSvcClient::new(channel);

        if let Err(status) = client
            .join_cluster(crate::network::grpc_svc::JoinClusterRequest {
                node_id: u32::from(my_node_id),
                storage_id,
            })
            .await
        {
            let known_leader = KnownLeader::from_status(&status);
            Err(JoinError::Rpc(status, known_leader))?
        };

        Ok(())
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

fn to_plain_node_id(id: u64) -> PlainNodeId {
    PlainNodeId::from(u32::try_from(id).expect("node id is derived from PlainNodeId"))
}

fn to_raft_id(plain_node_id: PlainNodeId) -> u64 {
    u64::from(u32::from(plain_node_id))
}
