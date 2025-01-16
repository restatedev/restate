// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::kv_memory_storage::KvMemoryStorage;
use crate::network::grpc_svc::metadata_store_network_svc_client::MetadataStoreNetworkSvcClient;
use crate::network::grpc_svc::JoinClusterRequest as ProtoJoinClusterRequest;
use crate::network::{ConnectionManager, Networking, KNOWN_LEADER_KEY};
use crate::omnipaxos::storage::RocksDbStorage;
use crate::omnipaxos::{BuildError, OmniPaxosConfiguration, OmniPaxosMessage};
use crate::{
    prepare_initial_nodes_configuration, InvalidConfiguration, JoinClusterError, JoinClusterHandle,
    JoinClusterReceiver, JoinClusterRequest, JoinClusterResponse, JoinClusterSender, KnownLeader,
    MemberId, MetadataStoreBackend, MetadataStoreConfiguration, MetadataStoreRequest,
    MetadataStoreSummary, ProvisionError, ProvisionReceiver, ProvisionSender, Request,
    RequestError, RequestKind, RequestReceiver, RequestSender, StatusSender, StatusWatch,
    StorageId,
};
use anyhow::Context;
use arc_swap::ArcSwapOption;
use futures::future::{FusedFuture, OptionFuture};
use futures::never::Never;
use futures::{FutureExt, TryFutureExt};
use omnipaxos::storage::{Entry, NoSnapshot, StopSign};
use omnipaxos::util::{ConfigurationId, LogEntry, NodeId};
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ProposeErr, ServerConfig};
use rand::seq::IteratorRandom;
use rand::{random, thread_rng};
use restate_core::metadata_store::{serialize_value, Precondition};
use restate_core::network::net_util::create_tonic_channel;
use restate_core::{cancellation_watcher, Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_types::config::{Configuration, RocksDbOptions};
use restate_types::errors::GenericError;
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{MetadataStoreState, NodesConfiguration, Role};
use restate_types::protobuf::common::MetadataStoreStatus;
use restate_types::retries::RetryPolicy;
use restate_types::storage::StorageEncodeError;
use restate_types::{PlainNodeId, Version};
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;
use tokio::time::MissedTickBehavior;
use tonic::Status;
use tracing::{debug, info, instrument, trace, warn};
use ulid::Ulid;

type OmniPaxos = omnipaxos::OmniPaxos<Request, RocksDbStorage<Request>>;

#[derive(Debug, thiserror::Error)]
#[error("failed accessing storage: {0}")]
pub struct StorageError(String);

impl From<Box<dyn std::error::Error + Send + Sync>> for StorageError {
    fn from(value: Box<dyn std::error::Error + Send + Sync>) -> Self {
        StorageError(value.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
enum DecidedEntriesError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Codec(#[from] StorageEncodeError),
}

#[derive(Debug, thiserror::Error)]
pub enum InitializeMetadataStoreError {
    #[error(transparent)]
    InvalidConfiguration(#[from] InvalidConfiguration),
    #[error("failed encoding nodes configuration: {0}")]
    Codec(#[from] StorageEncodeError),
    #[error("failed initializing the storage: {0}")]
    Storage(#[from] StorageError),
}

impl Entry for Request {
    type Snapshot = NoSnapshot;
}

pub struct OmniPaxosMetadataStore {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>>,
    rocksdb_storage: RocksDbStorage<Request>,
    storage_id: StorageId,
    metadata_writer: Option<MetadataWriter>,
    health_status: Option<HealthStatus<MetadataStoreStatus>>,

    request_tx: RequestSender,
    request_rx: RequestReceiver,

    provision_tx: ProvisionSender,
    provision_rx: Option<ProvisionReceiver>,

    join_cluster_tx: JoinClusterSender,
    join_cluster_rx: JoinClusterReceiver,

    status_tx: StatusSender,
}

impl OmniPaxosMetadataStore {
    pub async fn create(
        rocks_db_options: BoxedLiveLoad<RocksDbOptions>,
        metadata_writer: Option<MetadataWriter>,
        health_status: HealthStatus<MetadataStoreStatus>,
    ) -> Result<Self, BuildError> {
        health_status.update(MetadataStoreStatus::StartingUp);

        let (request_tx, request_rx) = mpsc::channel(2);
        let (provision_tx, provision_rx) = mpsc::channel(1);
        let (join_cluster_tx, join_cluster_rx) = mpsc::channel(1);
        let (status_tx, _status_rx) = watch::channel(MetadataStoreSummary::default());

        let rocksdb_storage =
            RocksDbStorage::create(&Configuration::pinned().metadata_store, rocks_db_options)
                .await?;

        // make sure that the storage is initialized with a storage id to be able to detect disk losses
        let storage_id = if let Some(storage_id) = rocksdb_storage
            .get_storage_id()
            .map_err(|err| BuildError::InitStorage(err.to_string()))?
        {
            storage_id
        } else {
            let storage_id = random();
            rocksdb_storage
                .set_storage_id(storage_id)
                .map_err(|err| BuildError::InitStorage(err.to_string()))?;
            storage_id
        };

        debug!("Obtained storage id: {storage_id}");

        // todo also store and validate that the node name has not changed to prevent assigning a different PlainNodeId

        Ok(Self {
            connection_manager: Arc::default(),
            rocksdb_storage,
            storage_id,
            metadata_writer,
            health_status: Some(health_status),
            request_tx,
            request_rx,
            provision_tx,
            provision_rx: Some(provision_rx),
            join_cluster_tx,
            join_cluster_rx,
            status_tx,
        })
    }

    pub(crate) fn request_sender(&self) -> RequestSender {
        self.request_tx.clone()
    }

    pub(crate) fn provision_sender(&self) -> ProvisionSender {
        self.provision_tx.clone()
    }

    pub(crate) fn status_watch(&self) -> Option<StatusWatch> {
        Some(self.status_tx.subscribe())
    }

    pub(crate) fn connection_manager(
        &self,
    ) -> Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>> {
        Arc::clone(&self.connection_manager)
    }

    pub(crate) fn join_cluster_handle(&self) -> JoinClusterHandle {
        JoinClusterHandle::new(self.join_cluster_tx.clone())
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let health_status = self.health_status.take().expect("to be present");

        let result = tokio::select! {
            _ = &mut shutdown => {
                debug!("Shutting down OmniPaxosMetadataStore");
                Ok(())
            },
            result = self.run_inner(&health_status) => {
                result.map(|_| ()).context("OmniPaxosMetadataStore failed")
            },
        };

        health_status.update(MetadataStoreStatus::Unknown);

        result
    }

    async fn run_inner(
        mut self,
        health_status: &HealthStatus<MetadataStoreStatus>,
    ) -> anyhow::Result<Never> {
        if let Some(metadata_writer) = self.metadata_writer.as_mut() {
            // Try to read a persisted nodes configuration in order to learn about the addresses of our
            // potential peers and the metadata store states.
            if let Some(nodes_configuration) = self
                .rocksdb_storage
                .get_nodes_configuration()
                .map_err(|err| BuildError::InitStorage(err.to_string()))?
            {
                metadata_writer
                    .update(Arc::new(nodes_configuration))
                    .await?
            }
        }

        health_status.update(MetadataStoreStatus::AwaitingProvisioning);
        let mut provisioned = self.await_provisioning().await?;

        loop {
            match provisioned {
                Provisioned::Active(active) => {
                    health_status.update(MetadataStoreStatus::Active);
                    provisioned = Provisioned::Passive(active.run().await?);
                }
                Provisioned::Passive(passive) => {
                    health_status.update(MetadataStoreStatus::Passive);
                    provisioned = Provisioned::Active(passive.run().await?);
                }
            }
        }
    }

    async fn await_provisioning(mut self) -> anyhow::Result<Provisioned> {
        let _ = self.status_tx.send(MetadataStoreSummary::Provisioning);
        let mut provision_rx = self.provision_rx.take().expect("must be present");

        let result = if let Some(configuration) = self.read_omni_paxos_configuration()? {
            debug!(member_id = %configuration.own_member_id, "Found existing metadata store configuration. Starting as active.");
            Provisioned::Active(self.become_active(configuration))
        } else {
            let mut nodes_config_watcher =
                Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));

            if *nodes_config_watcher.borrow_and_update() > Version::INVALID {
                // The metadata store must have been provisioned if there exists a
                // NodesConfiguration. So let's move on.
                debug!("Detected a valid NodesConfiguration. This indicates that the metadata store cluster has been provisioned. Starting as passive.");
                Provisioned::Passive(self.become_passive())
            } else {
                loop {
                    tokio::select! {
                        Some(request) = self.request_rx.recv() => {
                            // fail incoming requests while we are waiting for the provision signal
                            let (callback, _) = request.split_request();
                            callback.fail(RequestError::Unavailable("Metadata store has not been provisioned yet.".into()))
                        },
                        Some(request) = self.join_cluster_rx.recv() => {
                            let _ = request.response_tx.send(Err(JoinClusterError::NotActive(None)));
                        },
                        Some(request) = provision_rx.recv() => {
                            match self.initialize_storage(request.nodes_configuration) {
                                Ok(omni_paxos_configuration) => {
                                    let _ = request.result_tx.send(Ok(true));
                                    debug!(member_id = %omni_paxos_configuration.own_member_id, "Successfully provisioned the metadata store. Starting as active.");
                                    break Provisioned::Active(self.become_active(omni_paxos_configuration));
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
                                debug!("Detected a valid NodesConfiguration. This indicates that the metadata store cluster has been provisioned. Starting as passive.");
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

    fn initialize_storage(
        &mut self,
        nodes_configuration: NodesConfiguration,
    ) -> Result<OmniPaxosConfiguration, InitializeMetadataStoreError> {
        let (omni_paxos_configuration, nodes_configuration) =
            self.derive_initial_configuration(nodes_configuration)?;

        debug!("Initialize storage with nodes configuration: {nodes_configuration:?}");

        let value = serialize_value(&nodes_configuration)?;

        Self::prepare_storage(
            &mut self.rocksdb_storage,
            &omni_paxos_configuration,
            vec![Request {
                request_id: Ulid::new(),
                kind: RequestKind::Put {
                    precondition: Precondition::None,
                    key: NODES_CONFIG_KEY.clone(),
                    value,
                },
            }],
        )?;

        Ok(omni_paxos_configuration)
    }

    fn prepare_storage(
        rocksdb_storage: &mut RocksDbStorage<Request>,
        omni_paxos_configuration: &OmniPaxosConfiguration,
        log_prefix: Vec<Request>,
    ) -> Result<(), StorageError> {
        // we assume that log_prefix is complete, this will change once we support snapshots
        rocksdb_storage.batch_set_configuration(omni_paxos_configuration)?;
        rocksdb_storage.batch_append_on_prefix(0, log_prefix)?;
        // make sure we don't have a pending stop sign
        rocksdb_storage.batch_delete_stopsign();
        // make sure we start with a clean slate
        rocksdb_storage.batch_set_decided_idx(0)?;
        rocksdb_storage.commit_batch()?;

        Ok(())
    }

    fn derive_initial_configuration(
        &self,
        mut nodes_configuration: NodesConfiguration,
    ) -> Result<(OmniPaxosConfiguration, NodesConfiguration), InvalidConfiguration> {
        let configuration_id = 1;

        let configuration = Configuration::pinned();
        let restate_node_id = prepare_initial_nodes_configuration(
            &configuration,
            configuration_id,
            &mut nodes_configuration,
        )?;

        // set our own omni-paxos node id to be Restate's plain node id
        let own_member_id = MemberId {
            node_id: u64::from(u32::from(restate_node_id)),
            storage_id: self.storage_id,
        };

        let cluster_config = ClusterConfig {
            configuration_id,
            nodes: vec![own_member_id.node_id],
            flexible_quorum: None,
        };

        let mut members = HashMap::default();
        members.insert(own_member_id.node_id, own_member_id.storage_id);

        Ok((
            OmniPaxosConfiguration {
                own_member_id,
                cluster_config,
                members,
            },
            nodes_configuration,
        ))
    }

    fn read_omni_paxos_configuration(
        &self,
    ) -> Result<Option<OmniPaxosConfiguration>, StorageError> {
        self.rocksdb_storage.get_configuration().map_err(Into::into)
    }

    fn become_passive(self) -> Passive {
        let OmniPaxosMetadataStore {
            connection_manager,
            rocksdb_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            storage_id,
            status_tx,
            ..
        } = self;

        Passive::new(
            rocksdb_storage,
            connection_manager,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            storage_id,
            status_tx,
        )
    }

    fn become_active(self, omni_paxos_configuration: OmniPaxosConfiguration) -> Active {
        let OmniPaxosMetadataStore {
            connection_manager,
            rocksdb_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            ..
        } = self;

        Active::new(
            omni_paxos_configuration,
            connection_manager,
            rocksdb_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
        )
    }
}

enum Provisioned {
    Active(Active),
    Passive(Passive),
}

struct Active {
    omni_paxos: Option<OmniPaxos>,
    cluster_config: ClusterConfig,
    members: HashMap<NodeId, StorageId>,

    connection_manager: Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>>,
    networking: Networking<OmniPaxosMessage>,
    msg_rx: mpsc::Receiver<OmniPaxosMessage>,

    own_member_id: MemberId,
    is_leader: bool,

    last_applied_index: usize,

    pending_join_requests:
        HashMap<MemberId, oneshot::Sender<Result<JoinClusterResponse, JoinClusterError>>>,

    kv_storage: KvMemoryStorage,

    request_rx: RequestReceiver,
    join_cluster_rx: JoinClusterReceiver,

    metadata_writer: Option<MetadataWriter>,

    status_tx: StatusSender,
}

impl Active {
    fn new(
        omni_paxos_configuration: OmniPaxosConfiguration,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>>,
        rocksdb_storage: RocksDbStorage<Request>,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
        status_tx: StatusSender,
    ) -> Active {
        let own_member_id = omni_paxos_configuration.own_member_id;
        let cluster_config = omni_paxos_configuration.cluster_config;
        let members = omni_paxos_configuration.members;

        let (router_tx, router_rx) = mpsc::channel(128);
        let new_connection_manager = ConnectionManager::new(own_member_id.node_id, router_tx);
        let mut networking = Networking::new(new_connection_manager.clone());

        networking.register_address(
            own_member_id.node_id,
            Configuration::pinned().common.advertised_address.clone(),
        );

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

        for node_id in &cluster_config.nodes {
            if let Ok(node_config) = nodes_config.find_node_by_id(PlainNodeId::from(
                u32::try_from(*node_id).expect("node is derived from PlainNodeId"),
            )) {
                networking.register_address(*node_id, node_config.address.clone());
            }
        }

        // todo remove additional indirection from Arc
        connection_manager.store(Some(Arc::new(new_connection_manager)));

        let omni_paxos = Self::create_omni_paxos(
            own_member_id.node_id,
            cluster_config.clone(),
            rocksdb_storage,
        );

        Active {
            omni_paxos: Some(omni_paxos),
            cluster_config,
            members,
            connection_manager,
            networking,
            msg_rx: router_rx,
            own_member_id,
            is_leader: false,
            last_applied_index: 0,
            kv_storage: KvMemoryStorage::new(metadata_writer.clone()),
            request_rx,
            join_cluster_rx,
            pending_join_requests: HashMap::default(),
            metadata_writer,
            status_tx,
        }
    }

    fn create_omni_paxos(
        own_node_id: NodeId,
        cluster_config: ClusterConfig,
        rocksdb_storage: RocksDbStorage<Request>,
    ) -> OmniPaxos {
        let server_config = ServerConfig {
            pid: own_node_id,
            // todo make configurable
            election_tick_timeout: 5,
            resend_message_tick_timeout: 20,
            ..ServerConfig::default()
        };

        let op_config = OmniPaxosConfig {
            server_config,
            cluster_config,
        };

        op_config
            .build(rocksdb_storage)
            .expect("omni paxos configuration is valid")
    }

    fn omni_paxos(&self) -> &OmniPaxos {
        self.omni_paxos.as_ref().expect("to be present")
    }

    #[instrument(level = "info", skip_all, fields(member_id = %self.own_member_id))]
    pub(crate) async fn run(mut self) -> anyhow::Result<Passive> {
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
                Some(msg) = self.msg_rx.recv() => {
                    self.handle_omni_paxos_message(msg);
                },
                Ok(()) = nodes_config_watch.changed() => {
                    self.update_node_addresses(&Metadata::with_current(|m| m.nodes_config_ref()));
                },
                _ = tick_interval.tick() => {
                    self.omni_paxos.as_mut().expect("to be present").tick();
                },
                _ = status_update_interval.tick() => {
                    self.update_status();
                }
            }

            self.check_leadership();

            self.send_outgoing_messages();

            match self.handle_decided_entries() {
                Ok(DecidedEntriesResult::Continue) => {
                    // nothing to do
                }
                Ok(DecidedEntriesResult::Stop(rocksdb_storage)) => {
                    debug!("Stopping active metadata store node");
                    self.kv_storage.fail_callbacks(|| {
                        RequestError::Unavailable("stopping metadata store".into())
                    });
                    self.fail_join_callbacks(|| {
                        JoinClusterError::NotLeader(Self::random_known_leader())
                    });

                    return Ok(Passive::new(
                        rocksdb_storage,
                        self.connection_manager,
                        self.request_rx,
                        self.join_cluster_rx,
                        self.metadata_writer,
                        self.own_member_id.storage_id,
                        self.status_tx,
                    ));
                }
                Err(err) => {
                    self.kv_storage.fail_callbacks(|| {
                        RequestError::Unavailable("stopping metadata store".into())
                    });
                    self.fail_join_callbacks(|| {
                        JoinClusterError::NotLeader(Self::random_known_leader())
                    });
                    return Err(err.into());
                }
            }

            // check whether we have seen a higher configuration and have missed a possible reconfiguration
            if self.omni_paxos.as_ref().is_some_and(|omni_paxos| {
                omni_paxos.has_seen_higher_configuration() && omni_paxos.is_reconfigured().is_none()
            }) {
                debug!("Detected higher configuration. Stopping active metadata store node");
                self.kv_storage
                    .fail_callbacks(|| RequestError::Unavailable("stopping metadata store".into()));
                let known_leader = self.known_leader();
                self.fail_join_callbacks(|| JoinClusterError::NotLeader(known_leader.clone()));

                let rocksdb_storage = self
                    .omni_paxos
                    .take()
                    .expect("must be present")
                    .into_inner();

                return Ok(Passive::new(
                    rocksdb_storage,
                    self.connection_manager,
                    self.request_rx,
                    self.join_cluster_rx,
                    self.metadata_writer,
                    self.own_member_id.storage_id,
                    self.status_tx,
                ));
            }
        }
    }

    fn update_status(&self) {
        self.status_tx.send_if_modified(|current_status| {
            let current_leader = self
                .omni_paxos()
                .get_current_leader()
                .and_then(|(node_id, _)| {
                    self.members
                        .get(&node_id)
                        .map(|storage_id| MemberId::new(node_id, *storage_id))
                });

            if let MetadataStoreSummary::Active {
                leader,
                configuration,
            } = current_status
            {
                let mut modified = false;
                if configuration.id != self.cluster_config.configuration_id {
                    let members = self
                        .members
                        .iter()
                        .map(|(node_id, storage_id)| MemberId::new(*node_id, *storage_id))
                        .collect();

                    *configuration = MetadataStoreConfiguration {
                        id: self.cluster_config.configuration_id,
                        members,
                    };

                    modified = true;
                }

                if *leader != current_leader {
                    *leader = current_leader;
                    modified = true;
                }

                modified
            } else {
                let members = self
                    .members
                    .iter()
                    .map(|(node_id, storage_id)| MemberId::new(*node_id, *storage_id))
                    .collect();

                *current_status = MetadataStoreSummary::Active {
                    leader: current_leader,
                    configuration: MetadataStoreConfiguration {
                        id: self.cluster_config.configuration_id,
                        members,
                    },
                };

                true
            }
        });
    }

    fn update_node_addresses(&mut self, nodes_configuration: &NodesConfiguration) {
        for node_id in &self.cluster_config.nodes {
            if let Ok(node_config) = nodes_configuration.find_node_by_id(PlainNodeId::from(
                u32::try_from(*node_id).expect("node id is derived from PlainNodeId"),
            )) {
                self.networking
                    .register_address(*node_id, node_config.address.clone());
            }
        }
    }

    fn check_leadership(&mut self) {
        let previous_is_leader = self.is_leader;
        self.is_leader = self
            .omni_paxos()
            .get_current_leader()
            .is_some_and(|(node_id, _)| node_id == self.own_member_id.node_id);

        if previous_is_leader && !self.is_leader {
            debug!(configuration_id = %self.cluster_config.configuration_id, "Lost leadership");

            // we lost leadership :-( notify callers that their requests might not get committed
            // because we don't know whether the leader will start with the same log as we have.
            self.kv_storage
                .fail_callbacks(|| RequestError::Unavailable("lost leadership".into()));
            let known_leader = self.known_leader();
            self.fail_join_callbacks(|| JoinClusterError::NotLeader(known_leader.clone()));
        } else if !previous_is_leader && self.is_leader {
            debug!(configuration_id = %self.cluster_config.configuration_id, "Won leadership");
        }
    }

    fn handle_request(&mut self, request: MetadataStoreRequest) {
        let (callback, request) = request.split_request();
        trace!("Handle metadata store request: {request:?}");

        if !self.is_leader {
            callback.fail(RequestError::Unavailable("not leader".into()));
            return;
        }

        if let Err(err) = self
            .omni_paxos
            .as_mut()
            .expect("to be present")
            .append(request)
        {
            info!("Failed processing request: {err:?}");
            callback.fail(err)
        } else {
            self.kv_storage.register_callback(callback);
        }
    }

    fn handle_omni_paxos_message(&mut self, msg: OmniPaxosMessage) {
        trace!("Handle omni paxos message: {msg:?}");
        self.omni_paxos
            .as_mut()
            .expect("to be present")
            .handle_incoming(msg);
    }

    fn send_outgoing_messages(&mut self) {
        let outgoing_messages = self
            .omni_paxos
            .as_mut()
            .expect("to be present")
            .outgoing_messages();
        for outgoing_message in outgoing_messages.into_iter() {
            if let Err(err) = self.networking.try_send(outgoing_message) {
                trace!("Failed to send message: {:?}", err);
            }
        }
    }

    fn handle_decided_entries(&mut self) -> Result<DecidedEntriesResult, DecidedEntriesError> {
        let last_decided_index = self.omni_paxos().get_decided_idx();

        if self.last_applied_index < last_decided_index {
            for (idx, decided_entry) in self
                .omni_paxos()
                .read_decided_suffix(self.last_applied_index)
                .into_iter()
                .enumerate()
            {
                match decided_entry {
                    LogEntry::Decided(request) => {
                        self.kv_storage.handle_request(request);
                    }
                    LogEntry::Undecided(_) => {
                        panic!("Unexpected undecided entry")
                    }
                    LogEntry::Trimmed(_) => {
                        unimplemented!("We don't support trimming yet")
                    }
                    LogEntry::Snapshotted(_) => {
                        unimplemented!("We don't support snapshots yet")
                    }
                    LogEntry::StopSign(ss, decided) => {
                        assert!(decided, "we are handling only decided entries");
                        assert_eq!(
                            (idx + 1) + self.last_applied_index,
                            last_decided_index,
                            "StopSigns must be the last decided entries"
                        );
                        return self.handle_stop_sign(ss);
                    }
                }
            }

            self.last_applied_index = last_decided_index;
        }

        Ok(DecidedEntriesResult::Continue)
    }

    fn handle_stop_sign(
        &mut self,
        stop_sign: StopSign,
    ) -> Result<DecidedEntriesResult, DecidedEntriesError> {
        trace!("Handling stop sign");

        // remove the stop sign from the entries that are applied to the kv_storage because it is
        // virtual entry that is not part of the log
        let last_decided_index = self.omni_paxos().get_decided_idx() - 1;
        self.last_applied_index = last_decided_index;

        let metadata = Self::deserialize_omni_paxos_config_metadata(stop_sign.metadata.as_ref());
        let new_members = metadata.members;

        let mut rocksdb_storage = self.omni_paxos.take().expect("be present").into_inner();

        let new_nodes_configuration = self.update_membership_in_nodes_configuration(
            stop_sign.next_config.configuration_id,
            &new_members,
        );
        let request = self.put_new_nodes_configuration(&new_nodes_configuration)?;

        let new_cluster_config = stop_sign.next_config;
        let new_omni_paxos_configuration = OmniPaxosConfiguration {
            own_member_id: self.own_member_id,
            cluster_config: new_cluster_config.clone(),
            members: new_members,
        };

        Self::reset_storage_for_new_configuration(
            &mut rocksdb_storage,
            &new_omni_paxos_configuration,
            last_decided_index,
            vec![request],
        )?;

        if Self::is_member(self.own_member_id, &new_omni_paxos_configuration.members) {
            debug!(configuration_id = %new_cluster_config.configuration_id, "Continue as part of new configuration: {:?}", new_omni_paxos_configuration.cluster_config.nodes);

            let omni_paxos = Self::create_omni_paxos(
                new_omni_paxos_configuration.own_member_id.node_id,
                new_omni_paxos_configuration.cluster_config,
                rocksdb_storage,
            );

            self.omni_paxos = Some(omni_paxos);
            self.cluster_config = new_cluster_config;
            self.members = new_omni_paxos_configuration.members;

            self.answer_join_callbacks();
            self.check_leadership();

            self.update_node_addresses(&Metadata::with_current(|m| m.nodes_config_ref()));
            self.update_status();

            Ok(DecidedEntriesResult::Continue)
        } else {
            debug!(configuration_id = %new_cluster_config.configuration_id, "Stopping since I am no longer part of the new configuration.");

            // Node is no longer part of the configuration --> switch to passive.
            Ok(DecidedEntriesResult::Stop(rocksdb_storage))
        }
    }

    fn put_new_nodes_configuration(
        &mut self,
        new_nodes_configuration: &NodesConfiguration,
    ) -> Result<Request, DecidedEntriesError> {
        let request = Request::new(RequestKind::Put {
            key: NODES_CONFIG_KEY.clone(),
            value: serialize_value(new_nodes_configuration)?,
            precondition: Precondition::MatchesVersion(
                self.kv_storage.last_seen_nodes_configuration().version(),
            ),
        });
        Ok(request)
    }

    fn update_membership_in_nodes_configuration(
        &mut self,
        configuration_id: ConfigurationId,
        new_members: &HashMap<NodeId, StorageId>,
    ) -> NodesConfiguration {
        let mut new_nodes_configuration = self.kv_storage.last_seen_nodes_configuration().clone();

        for (node_id, node_config) in new_nodes_configuration.iter_mut() {
            let node_id = u64::from(u32::from(node_id));
            if new_members.contains_key(&node_id) {
                node_config.metadata_store_config.metadata_store_state =
                    MetadataStoreState::Active(configuration_id);
            } else if self.members.contains_key(&node_id)
                || matches!(
                    node_config.metadata_store_config.metadata_store_state,
                    MetadataStoreState::Active(_)
                )
            {
                // nodes that have been removed from the configuration are switched to passive
                node_config.metadata_store_config.metadata_store_state =
                    MetadataStoreState::Passive;
            }
            // Candidates stay candidates for the time being
        }

        new_nodes_configuration.increment_version();
        new_nodes_configuration
    }

    fn deserialize_omni_paxos_config_metadata(
        metadata: Option<&Vec<u8>>,
    ) -> OmniPaxosConfigMetadata {
        if let Some(metadata) = metadata {
            flexbuffers::from_slice(metadata).expect("members set is deserializable")
        } else {
            OmniPaxosConfigMetadata::default()
        }
    }

    fn serialize_omni_paxos_config_metadata(
        omni_paxos_config_metadata: &OmniPaxosConfigMetadata,
    ) -> Vec<u8> {
        flexbuffers::to_vec(omni_paxos_config_metadata)
            .expect("OmniPaxosConfigMetadata to be serializable")
    }

    fn reset_storage_for_new_configuration(
        rocksdb_storage: &mut RocksDbStorage<Request>,
        omni_paxos_configuration: &OmniPaxosConfiguration,
        last_decided_index: usize,
        log_prefix: Vec<Request>,
    ) -> Result<(), StorageError> {
        rocksdb_storage.batch_set_configuration(omni_paxos_configuration)?;
        rocksdb_storage.batch_set_decided_idx(last_decided_index)?;
        // append some extra log entries that are part of the new configuration
        rocksdb_storage.batch_append_on_prefix(last_decided_index, log_prefix)?;
        // delete stop sign and promise because we reset the storage for a new configuration
        rocksdb_storage.batch_delete_stopsign();
        rocksdb_storage.batch_delete_promise();
        rocksdb_storage.commit_batch()?;

        Ok(())
    }

    fn handle_join_request(&mut self, join_cluster_request: JoinClusterRequest) {
        let (response_tx, joining_node_id, joining_storage_id) = join_cluster_request.into_inner();
        let joining_member_id = MemberId::new(joining_node_id, joining_storage_id);

        trace!("Handle join request from node '{}'", joining_member_id);

        if !self.is_leader {
            let _ = response_tx.send(Err(JoinClusterError::NotLeader(self.known_leader())));
            return;
        }

        let is_reconfigured = self.omni_paxos().is_reconfigured();

        let (current_cluster_config, current_members) = if let Some(stop_sign) = is_reconfigured {
            let metadata =
                Self::deserialize_omni_paxos_config_metadata(stop_sign.metadata.as_ref());
            (
                Cow::Owned(stop_sign.next_config),
                Cow::Owned(metadata.members),
            )
        } else {
            (
                Cow::Borrowed(&self.cluster_config),
                Cow::Borrowed(&self.members),
            )
        };

        if Self::is_member(joining_member_id, &current_members) {
            let response = Self::prepare_join_cluster_response(
                self.omni_paxos(),
                joining_member_id,
                &current_cluster_config,
                current_members.into_owned(),
            );

            let _ = response_tx.send(Ok(response));
        } else {
            let mut new_cluster_config = self.cluster_config.clone();
            new_cluster_config.configuration_id += 1;
            new_cluster_config.nodes.push(joining_node_id);
            let mut new_members = current_members.into_owned();
            new_members.insert(joining_member_id.node_id, joining_member_id.storage_id);

            let config_metadata =
                Self::serialize_omni_paxos_config_metadata(&OmniPaxosConfigMetadata {
                    members: new_members,
                });

            if let Err(err) = self
                .omni_paxos
                .as_mut()
                .expect("to be present")
                .reconfigure(new_cluster_config, Some(config_metadata))
            {
                let response = match err {
                    ProposeErr::PendingReconfigEntry(_) => {
                        unreachable!("we were proposing a reconfiguration")
                    }
                    ProposeErr::PendingReconfigConfig(_, _) => {
                        JoinClusterError::PendingReconfiguration
                    }
                    ProposeErr::ConfigError(err, _, _) => {
                        JoinClusterError::ConfigError(err.to_string())
                    }
                };

                let _ = response_tx.send(Err(response));
            } else {
                debug!("Triggered reconfiguration of metadata store cluster");
                self.register_join_callback(joining_member_id, response_tx);
            }
        }
    }

    fn prepare_join_cluster_response(
        omni_paxos: &OmniPaxos,
        member_id: MemberId,
        current_cluster_config: &ClusterConfig,
        members: HashMap<NodeId, StorageId>,
    ) -> JoinClusterResponse {
        let metadata_store_config = flexbuffers::to_vec(&OmniPaxosConfiguration {
            own_member_id: member_id,
            cluster_config: current_cluster_config.clone(),
            members,
        })
        .expect("ClusterConfig to be serializable")
        .into();
        let log_entries = omni_paxos.read_decided_suffix(0);

        let log_entries: Vec<_> = log_entries
            .into_iter()
            .flat_map(|entry| {
                match entry {
                    LogEntry::Decided(request) => Some(request),
                    LogEntry::Undecided(_) => {
                        unreachable!("only reading decided suffix")
                    }
                    LogEntry::Trimmed(_) => {
                        unreachable!("we don't support trimming yet")
                    }
                    LogEntry::Snapshotted(_) => {
                        unreachable!("we don't support snapshotting yet")
                    }
                    // The stop sign should be the last entry and is not part of the actual
                    // log entries. If it exists, then we have checked the membership against
                    // the new cluster configuration. Therefore, we don't need to consider it.
                    LogEntry::StopSign(_, _) => None,
                }
            })
            .collect();

        let log_prefix = flexbuffers::to_vec(&log_entries)
            .expect("Requests to be serializable")
            .into();

        JoinClusterResponse {
            log_prefix,
            metadata_store_config,
        }
    }

    fn register_join_callback(
        &mut self,
        member_id: MemberId,
        reconfiguration_callback: oneshot::Sender<Result<JoinClusterResponse, JoinClusterError>>,
    ) {
        if let Some(previous_callback) = self
            .pending_join_requests
            .insert(member_id, reconfiguration_callback)
        {
            let _ = previous_callback.send(Err(JoinClusterError::ConcurrentRequest(
                PlainNodeId::from(
                    u32::try_from(member_id.node_id).expect("node id is derived from PlainNodeId"),
                ),
            )));
        }
    }

    fn answer_join_callbacks(&mut self) {
        let pending_join_request: Vec<_> = self.pending_join_requests.drain().collect();
        for (member_id, response_tx) in pending_join_request {
            if Self::is_member(member_id, &self.members) {
                let response = Self::prepare_join_cluster_response(
                    self.omni_paxos(),
                    member_id,
                    &self.cluster_config,
                    self.members.clone(),
                );
                let _ = response_tx.send(Ok(response));
            } else {
                // latest reconfiguration didn't include this node, fail it so that caller can retry
                let _ = response_tx.send(Err(JoinClusterError::Internal(format!(
                    "failed to include node '{}' in new configuration",
                    member_id
                ))));
            }
        }
    }

    fn fail_join_callbacks(&mut self, cause: impl Fn() -> JoinClusterError) {
        for (_, response_tx) in self.pending_join_requests.drain() {
            let _ = response_tx.send(Err(cause()));
        }
    }

    /// Checks whether the given node id with its storage id is part of the current set of members
    fn is_member(member_id: MemberId, members: &HashMap<NodeId, StorageId>) -> bool {
        members
            .get(&member_id.node_id)
            .is_some_and(|storage_id| *storage_id == member_id.storage_id)
    }

    /// Returns the known leader from the omni-paxos instance or a random known leader from the
    /// current nodes configuration.
    fn known_leader(&self) -> Option<KnownLeader> {
        let leader = self.omni_paxos().get_current_leader().map(|(node_id, _)| {
            PlainNodeId::new(u32::try_from(node_id).expect("node id is derived from PlainNodeId"))
        });

        leader
            .and_then(|leader| {
                let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
                nodes_config
                    .find_node_by_id(leader)
                    .ok()
                    .map(|node_config| KnownLeader {
                        node_id: leader,
                        address: node_config.address.clone(),
                    })
            })
            .or_else(Self::random_known_leader)
    }

    /// Returns a random known leader from the current nodes configuration.
    fn random_known_leader() -> Option<KnownLeader> {
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

        nodes_config
            .iter()
            .filter_map(|(node_id, node_config)| {
                if let MetadataStoreState::Active(_) =
                    node_config.metadata_store_config.metadata_store_state
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

enum DecidedEntriesResult {
    Continue,
    // Become passive
    Stop(RocksDbStorage<Request>),
}

#[allow(dead_code)]
struct Passive {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>>,
    rocksdb_storage: RocksDbStorage<Request>,
    request_rx: RequestReceiver,
    join_cluster_rx: JoinClusterReceiver,
    metadata_writer: Option<MetadataWriter>,
    storage_id: StorageId,
    status_tx: StatusSender,
}

impl Passive {
    fn new(
        rocksdb_storage: RocksDbStorage<Request>,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>>,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
        storage_id: StorageId,
        status_tx: StatusSender,
    ) -> Self {
        connection_manager.store(None);

        Passive {
            connection_manager,
            rocksdb_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            storage_id,
            status_tx,
        }
    }

    async fn run(self) -> anyhow::Result<Active> {
        debug!("Run as passive metadata store node.");

        let Passive {
            connection_manager,
            mut rocksdb_storage,
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

        rocksdb_storage
            .set_nodes_configuration(&Metadata::with_current(|m| m.nodes_config_ref()))
            .map_err(StorageError::from)?;

        let mut join_cluster: std::pin::Pin<&mut OptionFuture<_>> = std::pin::pin!(None.into());

        let mut nodes_config_watcher =
            Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));
        nodes_config_watcher.mark_changed();
        let my_node_name = Configuration::pinned().common.node_name().to_owned();

        loop {
            tokio::select! {
                Some(request) = request_rx.recv() => {
                    let (callback, _) = request.split_request();
                    callback.fail(RequestError::Unavailable(
                        "Not being part of the metadata store cluster.".into(),
                    ))
                },
                Some(request) = join_cluster_rx.recv() => {
                    // todo check whether we can answer the request if we were part of a previous configuration
                    let _ = request.response_tx.send(Err(JoinClusterError::NotActive(Active::random_known_leader())));
                }
                Some(join_configuration) = &mut join_cluster => {
                    let join_configuration: Result<JoinConfiguration, _> = join_configuration;
                    match join_configuration {
                        Ok(join_configuration) => {
                            OmniPaxosMetadataStore::prepare_storage(&mut rocksdb_storage, &join_configuration.omni_paxos_configuration, join_configuration.log_prefix)?;
                            return Ok(Active::new(
                                join_configuration.omni_paxos_configuration,
                                connection_manager,
                                rocksdb_storage,
                                request_rx,
                                join_cluster_rx,
                                metadata_writer,
                                status_tx));
                        },
                        Err(err) => {
                            debug!("Failed joining omni paxos cluster. Retrying. {err}");

                            match err {
                               JoinError::Rpc(_, known_leader) => {
                                    // if we have learned about a new leader, then try immediately rejoining
                                    join_cluster.set(Some(Self::join_cluster(known_leader, None, storage_id).fuse()).into());
                                }
                                _ => {
                                    join_cluster.set(Some(Self::join_cluster(None, join_retry_policy.next(), storage_id).fuse()).into());
                                }
                            }

                        }
                    }
                }
                _ = nodes_config_watcher.changed() => {
                    let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
                    let node_config = nodes_config.find_node_by_name(&my_node_name).expect("I must have registered before");

                    if matches!(node_config.metadata_store_config.metadata_store_state, MetadataStoreState::Active(_) | MetadataStoreState::Candidate) && join_cluster.is_terminated() {
                        debug!("Node is part of the metadata store cluster. Trying to join the omni paxos cluster.");
                        join_cluster.set(Some(Self::join_cluster(None, None, storage_id).fuse()).into());
                    } else {
                        debug!("Node is not part of the metadata store cluster. Waiting to become a candidate.");
                        join_cluster.set(None.into());
                    }
                }
            }
        }
    }

    async fn join_cluster(
        known_leader: Option<KnownLeader>,
        join_delay: Option<Duration>,
        storage_id: StorageId,
    ) -> Result<JoinConfiguration, JoinError> {
        if let Some(delay) = join_delay {
            time::sleep(delay).await
        }

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

        // We cannot assume that we have already joined the cluster and obtained our generational node id.
        // That's why we need to retrieve the plain node id based on our name from the latest known
        // NodesConfiguration. If the node has no assigned plain node id, then it first needs to obtain
        // it by following the regular join path before it can join the metadata store cluster.
        let my_node_id = if let Some(node_config) =
            nodes_config.find_node_by_name(Configuration::pinned().common.node_name())
        {
            node_config.current_generation.as_plain()
        } else {
            return Err(JoinError::Other(format!("The node with name '{}' has not obtained a node id yet. W/o the node id, it cannot join the metadata store cluster.", Configuration::pinned().common.node_name()).into()));
        };

        let address = if let Some(known_leader) = known_leader {
            debug!(
                "Trying to join metadata store at node '{}'",
                known_leader.node_id
            );
            known_leader.address
        } else {
            // pick random active metadata store node
            let active_metadata_store_node = nodes_config.iter().filter_map(|(node, config)| {
                if config.has_role(Role::MetadataStore) && node != my_node_id && matches!(config.metadata_store_config.metadata_store_state, MetadataStoreState::Active(_)) {
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

        let response = match client
            .join_cluster(ProtoJoinClusterRequest {
                node_id: u64::from(u32::from(my_node_id)),
                storage_id,
            })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let known_leader = if let Some(value) = status.metadata().get(KNOWN_LEADER_KEY) {
                    match value.to_str() {
                        Ok(value) => match serde_json::from_str(value) {
                            Ok(known_leader) => Some(known_leader),
                            Err(err) => {
                                debug!("failed parsing known leader from metadata: {err}");
                                None
                            }
                        },
                        Err(err) => {
                            debug!("failed parsing known leader from metadata: {err}");
                            None
                        }
                    }
                } else {
                    None
                };

                Err(JoinError::Rpc(status, known_leader))?
            }
        };

        // once the log grows beyond the configured grpc max message size (by default 4 MB) this
        // will no longer work :-( If we shared the snapshot we still have the same problem once the
        // snapshot grows beyond 4 MB. Then we need a separate channel (e.g. object store) or
        // support for chunked transfer.
        let log_prefix = flexbuffers::from_slice(response.log_prefix.as_ref())
            .map_err(|err| JoinError::Other(err.into()))?;
        let omni_paxos_configuration =
            flexbuffers::from_slice(response.metadata_store_config.as_ref())
                .map_err(|err| JoinError::Other(err.into()))?;

        Ok(JoinConfiguration {
            log_prefix,
            omni_paxos_configuration,
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum JoinError {
    #[error("rpc failed: status: {}, message: {}", _0.code(), _0.message())]
    Rpc(Status, Option<KnownLeader>),
    #[error("other error: {0}")]
    Other(GenericError),
}

#[derive(Clone, Debug)]
struct JoinConfiguration {
    omni_paxos_configuration: OmniPaxosConfiguration,
    log_prefix: Vec<Request>,
}

impl From<ProposeErr<Request>> for RequestError {
    fn from(err: ProposeErr<Request>) -> Self {
        match err {
            ProposeErr::PendingReconfigEntry(_) => {
                RequestError::Unavailable("reconfiguration in progress".into())
            }
            ProposeErr::PendingReconfigConfig(_, _) => RequestError::Internal(
                "cannot reconfigure while reconfiguration is in progress".into(),
            ),
            ProposeErr::ConfigError(_, _, _) => {
                RequestError::Internal("configuration error".into())
            }
        }
    }
}

impl MetadataStoreBackend for OmniPaxosMetadataStore {
    fn request_sender(&self) -> RequestSender {
        self.request_sender()
    }

    fn provision_sender(&self) -> Option<ProvisionSender> {
        Some(self.provision_sender())
    }

    fn status_watch(&self) -> Option<StatusWatch> {
        self.status_watch()
    }

    fn run(self) -> impl Future<Output = anyhow::Result<()>> + Send + 'static {
        self.run().map_err(anyhow::Error::from)
    }
}

#[serde_with::serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct OmniPaxosConfigMetadata {
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    members: HashMap<NodeId, StorageId>,
}
