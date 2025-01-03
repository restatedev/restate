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
use crate::network::{ConnectionManager, Networking};
use crate::omnipaxos::storage::RocksDbStorage;
use crate::omnipaxos::{BuildError, OmniPaxosConfiguration, OmniPaxosMessage};
use crate::{
    MetadataStoreBackend, MetadataStoreRequest, ProvisionError, ProvisionReceiver, ProvisionSender,
    Request, RequestError, RequestKind, RequestReceiver, RequestSender,
};
use anyhow::Context;
use arc_swap::ArcSwapOption;
use futures::never::Never;
use futures::TryFutureExt;
use omnipaxos::storage::{Entry, NoSnapshot};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ProposeErr, ServerConfig};
use restate_core::metadata_store::{serialize_value, Precondition};
use restate_core::{cancellation_watcher, Metadata, ShutdownError, TaskCenter, TaskKind};
use restate_types::config::{Configuration, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration};
use restate_types::storage::StorageEncodeError;
use restate_types::{GenerationalNodeId, Version};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, trace, warn};
use ulid::Ulid;

type OmniPaxos = omnipaxos::OmniPaxos<Request, RocksDbStorage<Request>>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Debug, thiserror::Error)]
#[error("failed accessing storage: {0}")]
pub struct StorageError(String);

#[derive(Debug, thiserror::Error)]
#[error("invalid nodes configuration: {0}")]
pub struct InvalidConfiguration(String);

#[derive(Debug, thiserror::Error)]
pub enum PrepareMetadataStoreError {
    #[error(transparent)]
    InvalidConfiguration(#[from] InvalidConfiguration),
    #[error("failed encoding nodes configuration: {0}")]
    Codec(#[from] StorageEncodeError),
    #[error("failed writing to the storage: {0}")]
    Storage(String),
}

impl Entry for Request {
    type Snapshot = NoSnapshot;
}

pub struct OmnipaxosMetadataStore {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>>,
    rocksdb_storage: RocksDbStorage<Request>,

    request_tx: RequestSender,
    request_rx: RequestReceiver,

    provision_tx: ProvisionSender,
    provision_rx: Option<ProvisionReceiver>,
}

impl OmnipaxosMetadataStore {
    pub async fn create(
        rocks_db_options: BoxedLiveLoad<RocksDbOptions>,
    ) -> Result<Self, BuildError> {
        let (request_tx, request_rx) = mpsc::channel(2);
        let (provision_tx, provision_rx) = mpsc::channel(1);

        let rocksdb_storage =
            RocksDbStorage::create(&Configuration::pinned().metadata_store, rocks_db_options)
                .await?;

        Ok(Self {
            connection_manager: Arc::default(),
            rocksdb_storage,
            request_tx,
            request_rx,
            provision_tx,
            provision_rx: Some(provision_rx),
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

    async fn run_inner(self) -> Result<Never, Error> {
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

    async fn await_provisioning(mut self) -> Result<Provisioned, Error> {
        let mut provision_rx = self.provision_rx.take().expect("must be present");

        let result = if let Some(configuration) = self.read_omni_paxos_configuration()? {
            debug!(peer_id = %configuration.own_peer_id, "Found existing metadata store configuration. Starting as active.");
            Provisioned::Active(self.become_active(configuration))
        } else {
            let mut nodes_config_watcher =
                Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));
            nodes_config_watcher.mark_changed();

            loop {
                tokio::select! {
                    Some(request) = self.request_rx.recv() => {
                        // fail incoming requests while we are waiting for the provision signal
                        let (callback, _) = request.split_request();
                        callback.fail(RequestError::Unavailable("Metadata store has not been provisioned yet.".into()))
                    },
                    Some(request) = provision_rx.recv() => {
                        match self.prepare_storage(request.nodes_configuration) {
                            Ok(omni_paxos_configuration) => {
                                let _ = request.result_tx.send(Ok(true));
                                debug!(peer_id = %omni_paxos_configuration.own_peer_id, "Successfully provisioned the metadata store. Starting as active.");
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
        };

        TaskCenter::spawn_unmanaged(TaskKind::Background, "provision-responder", async move {
            while let Some(request) = provision_rx.recv().await {
                let _ = request.result_tx.send(Ok(false));
            }
        })?;

        Ok(result)
    }

    fn prepare_storage(
        &mut self,
        nodes_configuration: NodesConfiguration,
    ) -> Result<OmniPaxosConfiguration, PrepareMetadataStoreError> {
        let (omni_paxos_configuration, nodes_configuration) =
            Self::derive_initial_configuration(nodes_configuration)?;

        debug!("Prepare storage with initial nodes configuration: {nodes_configuration:?}");

        let value = serialize_value(&nodes_configuration)?;

        self.rocksdb_storage
            .batch_set_configuration(&omni_paxos_configuration)
            .map_err(|err| PrepareMetadataStoreError::Storage(err.to_string()))?;
        self.rocksdb_storage
            .batch_append_entry(Request {
                request_id: Ulid::new(),
                kind: RequestKind::Put {
                    precondition: Precondition::None,
                    key: NODES_CONFIG_KEY.clone(),
                    value,
                },
            })
            .map_err(|err| PrepareMetadataStoreError::Storage(err.to_string()))?;
        self.rocksdb_storage
            .commit_batch()
            .map_err(|err| PrepareMetadataStoreError::Storage(err.to_string()))?;

        Ok(omni_paxos_configuration)
    }

    fn derive_initial_configuration(
        mut nodes_configuration: NodesConfiguration,
    ) -> Result<(OmniPaxosConfiguration, NodesConfiguration), InvalidConfiguration> {
        let configuration = Configuration::pinned();

        let current_generation = if let Some(node_config) =
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

            node_config.current_generation
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
            );

            nodes_configuration.upsert_node(node_config);

            current_generation
        };

        // derive our own node id from the current generation
        let own_node_id = u64::from(current_generation);

        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![own_node_id],
            flexible_quorum: None,
        };

        Ok((
            OmniPaxosConfiguration {
                own_peer_id: own_node_id,
                cluster_config,
            },
            nodes_configuration,
        ))
    }

    fn read_omni_paxos_configuration(
        &self,
    ) -> Result<Option<OmniPaxosConfiguration>, StorageError> {
        self.rocksdb_storage
            .get_configuration()
            .map_err(|err| StorageError(err.to_string()))
    }

    fn become_passive(self) -> Passive {
        let OmnipaxosMetadataStore {
            connection_manager,
            rocksdb_storage,
            request_rx,
            ..
        } = self;

        Passive {
            connection_manager,
            rocksdb_storage,
            request_rx,
        }
    }

    fn become_active(self, omni_paxos_configuration: OmniPaxosConfiguration) -> Active {
        debug!(peer_id = %omni_paxos_configuration.own_peer_id, "Start as active");
        let OmnipaxosMetadataStore {
            connection_manager,
            rocksdb_storage,
            request_rx,
            ..
        } = self;

        let own_node_id = omni_paxos_configuration.own_peer_id;

        let (router_tx, router_rx) = mpsc::channel(128);
        let new_connection_manager = ConnectionManager::new(own_node_id, router_tx);
        let mut networking = Networking::new(new_connection_manager.clone());
        networking.register_address(
            own_node_id,
            Configuration::pinned().common.advertised_address.clone(),
        );

        // todo remove additional indirection from Arc
        connection_manager.store(Some(Arc::new(new_connection_manager)));

        let server_config = ServerConfig {
            pid: own_node_id,
            // todo make configurable
            election_tick_timeout: 5,
            resend_message_tick_timeout: 20,
            ..ServerConfig::default()
        };

        let cluster_config = omni_paxos_configuration.cluster_config;

        let op_config = OmniPaxosConfig {
            server_config,
            cluster_config,
        };

        let omni_paxos = op_config
            .build(rocksdb_storage)
            .expect("omni paxos configuration is valid");

        let is_leader = omni_paxos
            .get_current_leader()
            .is_some_and(|(node_id, _)| node_id == own_node_id);

        Active {
            omni_paxos,
            networking,
            msg_rx: router_rx,
            own_node_id,
            is_leader,
            last_applied_index: 0,
            kv_storage: KvMemoryStorage::default(),
            request_rx,
        }
    }
}

enum Provisioned {
    Active(Active),
    Passive(Passive),
}

struct Active {
    omni_paxos: OmniPaxos,
    networking: Networking<OmniPaxosMessage>,
    msg_rx: mpsc::Receiver<OmniPaxosMessage>,

    own_node_id: NodeId,
    is_leader: bool,

    last_applied_index: usize,

    kv_storage: KvMemoryStorage,

    request_rx: RequestReceiver,
}

impl Active {
    pub(crate) async fn run(mut self) -> Result<Passive, Error> {
        let mut tick_interval = time::interval(Duration::from_millis(100));
        tick_interval.set_missed_tick_behavior(MissedTickBehavior::Burst);

        loop {
            tokio::select! {
                Some(request) = self.request_rx.recv() => {
                    self.handle_request(request);
                },
                Some(msg) = self.msg_rx.recv() => {
                    self.handle_omni_paxos_message(msg);
                },
                _ = tick_interval.tick() => {
                    self.omni_paxos.tick();
                },
            }

            self.check_leadership();

            self.send_outgoing_messages();
            self.handle_decided_entries();
        }
    }

    fn check_leadership(&mut self) {
        let previous_is_leader = self.is_leader;
        self.is_leader = self
            .omni_paxos
            .get_current_leader()
            .is_some_and(|(node_id, _)| node_id == self.own_node_id);

        if previous_is_leader && !self.is_leader {
            // we lost leadership :-(
            self.kv_storage
                .fail_callbacks(|| RequestError::Unavailable("lost leadership".into()));
        }
    }

    fn handle_request(&mut self, request: MetadataStoreRequest) {
        let (callback, request) = request.split_request();
        trace!("Handle metadata store request: {request:?}");

        if !self.is_leader {
            callback.fail(RequestError::Unavailable("not leader".into()));
            return;
        }

        if let Err(err) = self.omni_paxos.append(request) {
            info!("Failed processing request: {err:?}");
            callback.fail(err)
        } else {
            self.kv_storage.register_callback(callback);
        }
    }

    fn handle_omni_paxos_message(&mut self, msg: OmniPaxosMessage) {
        trace!("Handle omni paxos message: {msg:?}");
        self.omni_paxos.handle_incoming(msg);
    }

    fn send_outgoing_messages(&mut self) {
        let outgoing_messages = self.omni_paxos.outgoing_messages();
        for outgoing_message in outgoing_messages.into_iter() {
            if let Err(err) = self.networking.try_send(outgoing_message) {
                debug!("Failed to send message: {:?}", err);
            }
        }
    }

    fn handle_decided_entries(&mut self) {
        let last_decided_index = self.omni_paxos.get_decided_idx();

        if self.last_applied_index < last_decided_index {
            if let Some(decided_entries) =
                self.omni_paxos.read_decided_suffix(self.last_applied_index)
            {
                for decided_entry in decided_entries {
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
                        LogEntry::StopSign(_, _) => {
                            unimplemented!("We don't support reconfiguration yet")
                        }
                    }
                }
            }

            self.last_applied_index = last_decided_index;
        }
    }
}

#[allow(dead_code)]
struct Passive {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<OmniPaxosMessage>>>,
    rocksdb_storage: RocksDbStorage<Request>,
    request_rx: RequestReceiver,
}

impl Passive {
    async fn run(mut self) -> Result<Active, Error> {
        debug!("Run as passive metadata store node.");

        while let Some(request) = self.request_rx.recv().await {
            let (callback, _) = request.split_request();
            callback.fail(RequestError::Unavailable(
                "Not being part of the metadata store cluster.".into(),
            ))
        }

        futures::future::pending().await
    }
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

impl MetadataStoreBackend for OmnipaxosMetadataStore {
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
