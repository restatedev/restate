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
use crate::network::{ConnectionManager, Networking};
use crate::omnipaxos::storage::RocksDbStorage;
use crate::omnipaxos::{BuildError, MemberId, OmniPaxosConfiguration, OmniPaxosMessage, StorageId};
use crate::{
    JoinClusterError, JoinClusterHandle, JoinClusterReceiver, JoinClusterRequest,
    JoinClusterResponse, JoinClusterSender, MetadataStoreBackend, MetadataStoreRequest,
    ProvisionError, ProvisionReceiver, ProvisionSender, Request, RequestError, RequestKind,
    RequestReceiver, RequestSender,
};
use anyhow::{bail, Context};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use futures::never::Never;
use futures::TryFutureExt;
use omnipaxos::storage::{Entry, NoSnapshot, StopSign};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ProposeErr, ServerConfig};
use rand::seq::IteratorRandom;
use rand::{random, thread_rng};
use restate_core::metadata_store::{serialize_value, Precondition};
use restate_core::network::net_util::create_tonic_channel;
use restate_core::{cancellation_watcher, Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_types::config::{Configuration, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{
    LogServerConfig, MetadataStoreConfig, NodeConfig, NodesConfiguration, Role,
};
use restate_types::retries::RetryPolicy;
use restate_types::storage::StorageEncodeError;
use restate_types::{GenerationalNodeId, PlainNodeId, Version};
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, instrument, trace, warn};
use ulid::Ulid;

type OmniPaxos = omnipaxos::OmniPaxos<Request, RocksDbStorage<Request>>;

#[derive(Debug, thiserror::Error)]
#[error("failed accessing storage: {0}")]
pub struct StorageError(String);

impl From<Box<dyn std::error::Error>> for StorageError {
    fn from(value: Box<dyn std::error::Error>) -> Self {
        StorageError(value.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
enum DecidedEntriesError {
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Debug, thiserror::Error)]
#[error("invalid nodes configuration: {0}")]
pub struct InvalidConfiguration(String);

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

    request_tx: RequestSender,
    request_rx: RequestReceiver,

    provision_tx: ProvisionSender,
    provision_rx: Option<ProvisionReceiver>,

    join_cluster_tx: JoinClusterSender,
    join_cluster_rx: JoinClusterReceiver,
}

impl OmniPaxosMetadataStore {
    pub async fn create(
        rocks_db_options: BoxedLiveLoad<RocksDbOptions>,
        metadata_writer: Option<MetadataWriter>,
    ) -> Result<Self, BuildError> {
        let (request_tx, request_rx) = mpsc::channel(2);
        let (provision_tx, provision_rx) = mpsc::channel(1);
        let (join_cluster_tx, join_cluster_rx) = mpsc::channel(1);

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
            request_tx,
            request_rx,
            provision_tx,
            provision_rx: Some(provision_rx),
            join_cluster_tx,
            join_cluster_rx,
        })
    }

    pub(crate) fn request_sender(&self) -> RequestSender {
        self.request_tx.clone()
    }

    pub(crate) fn provision_sender(&self) -> ProvisionSender {
        self.provision_tx.clone()
    }

    pub(crate) fn connection_manager(
        &self,
    ) -> Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>> {
        Arc::clone(&self.connection_manager)
    }

    pub(crate) fn join_cluster_handle(&self) -> JoinClusterHandle {
        JoinClusterHandle::new(self.join_cluster_tx.clone())
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());

        tokio::select! {
            _ = &mut shutdown => {
                debug!("Shutting down OmniPaxosMetadataStore");
            },
            result = self.run_inner() => {
                result.context("OmniPaxosMetadataStore failed")?;
            }
        }

        Ok(())
    }

    async fn run_inner(mut self) -> anyhow::Result<Never> {
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

        let mut provisioned = self.await_provisioning().await?;

        loop {
            match provisioned {
                Provisioned::Active(active) => {
                    provisioned = Provisioned::Passive(active.run().await?);
                }
                Provisioned::Passive(passive) => {
                    provisioned = Provisioned::Active(passive.run().await?);
                }
            }
        }
    }

    async fn await_provisioning(mut self) -> anyhow::Result<Provisioned> {
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
                            let _ = request.response_tx.send(Err(JoinClusterError::NotActive));
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
        rocksdb_storage.commit_batch()?;

        Ok(())
    }

    fn derive_initial_configuration(
        &self,
        mut nodes_configuration: NodesConfiguration,
    ) -> Result<(OmniPaxosConfiguration, NodesConfiguration), InvalidConfiguration> {
        let configuration = Configuration::pinned();

        let restate_node_id = if let Some(node_config) =
            nodes_configuration.find_node_by_name(configuration.common.node_name())
        {
            if let Some(force_node_id) = configuration.common.force_node_id {
                if force_node_id != node_config.current_generation.as_plain() {
                    return Err(InvalidConfiguration(format!(
                        "nodes configuration has wrong plain node id; expected: {}, actual: {}",
                        force_node_id,
                        node_config.current_generation.as_plain()
                    )));
                }
            }

            node_config.current_generation.as_plain()
        } else {
            // give precedence to the force node id
            let current_generation = configuration
                .common
                .force_node_id
                .map(|node_id| node_id.with_generation(1))
                .unwrap_or_else(|| {
                    nodes_configuration
                        .max_plain_node_id()
                        .map(|node_id| node_id.next().with_generation(1))
                        .unwrap_or(GenerationalNodeId::INITIAL_NODE_ID)
                });

            let node_config = NodeConfig::new(
                configuration.common.node_name().to_owned(),
                current_generation,
                configuration.common.advertised_address.clone(),
                configuration.common.roles,
                LogServerConfig::default(),
                MetadataStoreConfig::default(),
            );

            nodes_configuration.upsert_node(node_config);

            current_generation.as_plain()
        };

        // set our own omni-paxos node id to be Restate's plain node id
        let own_member_id = MemberId {
            node_id: u64::from(u32::from(restate_node_id)),
            storage_id: self.storage_id,
        };

        let cluster_config = ClusterConfig {
            configuration_id: 1,
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
            ..
        } = self;

        Passive::new(
            rocksdb_storage,
            connection_manager,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            storage_id,
        )
    }

    fn become_active(self, omni_paxos_configuration: OmniPaxosConfiguration) -> Active {
        let OmniPaxosMetadataStore {
            connection_manager,
            rocksdb_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            ..
        } = self;

        Active::new(
            omni_paxos_configuration,
            connection_manager,
            rocksdb_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
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
}

impl Active {
    fn new(
        omni_paxos_configuration: OmniPaxosConfiguration,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>>,
        rocksdb_storage: RocksDbStorage<Request>,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
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

        let is_leader = omni_paxos
            .get_current_leader()
            .is_some_and(|(node_id, _)| node_id == own_member_id.node_id);

        Active {
            omni_paxos: Some(omni_paxos),
            cluster_config,
            members,
            connection_manager,
            networking,
            msg_rx: router_rx,
            own_member_id,
            is_leader,
            last_applied_index: 0,
            kv_storage: KvMemoryStorage::new(metadata_writer.clone()),
            request_rx,
            join_cluster_rx,
            pending_join_requests: HashMap::default(),
            metadata_writer,
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

    #[instrument(level = "info", skip_all, fields(member_id = %self.own_member_id))]
    pub(crate) async fn run(mut self) -> anyhow::Result<Passive> {
        debug!("Run as active metadata store node");

        let mut tick_interval = time::interval(Duration::from_millis(100));
        tick_interval.set_missed_tick_behavior(MissedTickBehavior::Burst);

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
                    self.fail_join_callbacks(|| JoinClusterError::NotLeader);

                    return Ok(Passive::new(
                        rocksdb_storage,
                        self.connection_manager,
                        self.request_rx,
                        self.join_cluster_rx,
                        self.metadata_writer,
                        self.own_member_id.storage_id,
                    ));
                }
                Err(err) => {
                    self.kv_storage.fail_callbacks(|| {
                        RequestError::Unavailable("stopping metadata store".into())
                    });
                    self.fail_join_callbacks(|| JoinClusterError::NotLeader);
                    return Err(err.into());
                }
            }
        }
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
            .omni_paxos
            .as_ref()
            .expect("to be present")
            .get_current_leader()
            .is_some_and(|(node_id, _)| node_id == self.own_member_id.node_id);

        if previous_is_leader && !self.is_leader {
            debug!(configuration_id = %self.cluster_config.configuration_id, "Lost leadership");

            // we lost leadership :-( notify callers that their requests might not get committed
            // because we don't know whether the leader will start with the same log as we have.
            self.kv_storage
                .fail_callbacks(|| RequestError::Unavailable("lost leadership".into()));
            self.fail_join_callbacks(|| JoinClusterError::NotLeader);
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
        let last_decided_index = self
            .omni_paxos
            .as_ref()
            .expect("to be present")
            .get_decided_idx();

        if self.last_applied_index < last_decided_index {
            if let Some(decided_entries) = self
                .omni_paxos
                .as_ref()
                .expect("to be present")
                .read_decided_suffix(self.last_applied_index)
            {
                for (idx, decided_entry) in decided_entries.into_iter().enumerate() {
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
        let last_decided_index = self
            .omni_paxos
            .as_ref()
            .expect("to be present")
            .get_decided_idx()
            - 1;
        self.last_applied_index = last_decided_index;

        let metadata = Self::deserialize_omni_paxos_config_metadata(stop_sign.metadata.as_ref());

        if Self::is_member(self.own_member_id, &metadata.members) {
            debug!(configuration_id = %stop_sign.next_config.configuration_id, "Continue as part of new configuration: {:?}", stop_sign.next_config.nodes);

            let mut rocksdb_storage = self.omni_paxos.take().expect("be present").into_inner();

            let new_cluster_config = stop_sign.next_config.clone();
            let omni_paxos_configuration = OmniPaxosConfiguration {
                own_member_id: self.own_member_id,
                cluster_config: stop_sign.next_config,
                members: metadata.members,
            };

            Self::reset_storage_for_new_configuration(
                &mut rocksdb_storage,
                &omni_paxos_configuration,
                last_decided_index,
            )?;

            let omni_paxos = Self::create_omni_paxos(
                omni_paxos_configuration.own_member_id.node_id,
                omni_paxos_configuration.cluster_config,
                rocksdb_storage,
            );

            self.omni_paxos = Some(omni_paxos);
            self.cluster_config = new_cluster_config;
            self.members = omni_paxos_configuration.members;

            self.answer_join_callbacks();
            self.check_leadership();

            self.update_node_addresses(&Metadata::with_current(|m| m.nodes_config_ref()));

            Ok(DecidedEntriesResult::Continue)
        } else {
            debug!(configuration_id = %stop_sign.next_config.configuration_id, "Stopping since I am no longer part of the new configuration.");

            let mut rocksdb_storage = self.omni_paxos.take().expect("to be present").into_inner();

            // remember the latest configuration we have seen for future checks
            Self::reset_storage_for_new_configuration(
                &mut rocksdb_storage,
                &OmniPaxosConfiguration {
                    own_member_id: self.own_member_id,
                    cluster_config: stop_sign.next_config,
                    members: metadata.members,
                },
                last_decided_index,
            )?;

            // Node is no longer part of the configuration --> switch to passive.
            Ok(DecidedEntriesResult::Stop(rocksdb_storage))
        }
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
    ) -> Result<(), StorageError> {
        rocksdb_storage.batch_set_configuration(omni_paxos_configuration)?;
        rocksdb_storage.batch_set_decided_idx(last_decided_index)?;
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
            let _ = response_tx.send(Err(JoinClusterError::NotLeader));
            return;
        }

        let is_reconfigured = self
            .omni_paxos
            .as_ref()
            .expect("to be present")
            .is_reconfigured();

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
                self.omni_paxos.as_ref().expect("to be present"),
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

        let log_prefix = if let Some(log_entries) = log_entries {
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

            flexbuffers::to_vec(&log_entries)
                .expect("Requests to be serializable")
                .into()
        } else {
            Bytes::new()
        };

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
                    self.omni_paxos.as_ref().expect("to be present"),
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
}

impl Passive {
    fn new(
        rocksdb_storage: RocksDbStorage<Request>,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>>,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
        storage_id: StorageId,
    ) -> Self {
        connection_manager.store(None);

        Passive {
            connection_manager,
            rocksdb_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            storage_id,
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
        } = self;

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
        // todo only try joining if MetadataStoreState::Candidate
        let mut join_cluster = std::pin::pin!(Self::join_cluster(None, storage_id));

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
                    let _ = request.response_tx.send(Err(JoinClusterError::NotActive));
                }
                join_configuration = &mut join_cluster => {
                    match join_configuration {
                        Ok(join_configuration) => {
                            OmniPaxosMetadataStore::prepare_storage(&mut rocksdb_storage, &join_configuration.omni_paxos_configuration, join_configuration.log_prefix)?;
                            return Ok(Active::new(join_configuration.omni_paxos_configuration, connection_manager, rocksdb_storage, request_rx, join_cluster_rx, metadata_writer));
                        },
                        Err(err) => {
                            debug!("Failed joining omni paxos cluster. Retrying. {err}");
                            join_cluster.set(Self::join_cluster(join_retry_policy.next(), storage_id));
                        }
                    }
                }
                // todo monitor NodesConfiguration changes to react to MetadataStoreState changes
            }
        }
    }

    async fn join_cluster(
        join_delay: Option<Duration>,
        storage_id: StorageId,
    ) -> anyhow::Result<JoinConfiguration> {
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
            bail!("The node with name '{}' has not obtained a node id yet. W/o the node id, it cannot join the metadata store cluster.", Configuration::pinned().common.node_name());
        };

        // pick random active metadata store node
        let active_metadata_store_node = nodes_config.iter().filter_map(|(node, config)| {
            if config.has_role(Role::MetadataStore) && node != my_node_id {
                Some(node)
            } else {
                None
            }
        }).choose(&mut thread_rng()).ok_or(anyhow::anyhow!("No other metadata store present in the cluster. This indicates a misconfiguration."))?;

        debug!(
            "Trying to join metadata store cluster at node '{}'",
            active_metadata_store_node
        );

        let address = nodes_config
            .find_node_by_id(active_metadata_store_node)
            .expect("must be present")
            .address
            .clone();
        let channel = create_tonic_channel(address, &Configuration::pinned().networking);

        let mut client = MetadataStoreNetworkSvcClient::new(channel);

        let response = client
            .join_cluster(ProtoJoinClusterRequest {
                node_id: u64::from(u32::from(my_node_id)),
                storage_id,
            })
            .await?
            .into_inner();

        // once the log grows beyond the configured grpc max message size (by default 4 MB) this
        // will no longer work :-( If we shared the snapshot we still have the same problem once the
        // snapshot grows beyond 4 MB. Then we need a separate channel (e.g. object store) or
        // support for chunked transfer.
        let log_prefix = flexbuffers::from_slice(response.log_prefix.as_ref())?;
        let omni_paxos_configuration =
            flexbuffers::from_slice(response.metadata_store_config.as_ref())?;

        Ok(JoinConfiguration {
            log_prefix,
            omni_paxos_configuration,
        })
    }
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
