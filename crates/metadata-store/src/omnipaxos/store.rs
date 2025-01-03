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
use crate::network::Networking;
use crate::omnipaxos::storage::RocksDbStorage;
use crate::omnipaxos::{BuildError, Error, OmniPaxosMessage};
use crate::{
    MetadataStoreBackend, MetadataStoreRequest, Request, RequestError, RequestReceiver,
    RequestSender,
};
use futures::TryFutureExt;
use omnipaxos::storage::{Entry, NoSnapshot};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ProposeErr, ServerConfig};
use restate_core::cancellation_watcher;
use restate_types::config::{Configuration, OmniPaxosOptions, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use std::future::Future;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, trace};

type OmniPaxos = omnipaxos::OmniPaxos<Request, RocksDbStorage<Request>>;

impl Entry for Request {
    type Snapshot = NoSnapshot;
}
pub struct OmnipaxosMetadataStore {
    networking: Networking<OmniPaxosMessage>,
    msg_rx: mpsc::Receiver<OmniPaxosMessage>,
    request_tx: RequestSender,
    request_rx: RequestReceiver,
    omni_paxos: OmniPaxos,

    own_node_id: NodeId,
    is_leader: bool,

    last_applied_index: usize,

    kv_storage: KvMemoryStorage,
}

impl OmnipaxosMetadataStore {
    pub async fn create(
        omni_paxos_options: &OmniPaxosOptions,
        rocks_db_options: BoxedLiveLoad<RocksDbOptions>,
        mut networking: Networking<OmniPaxosMessage>,
        msg_rx: mpsc::Receiver<OmniPaxosMessage>,
    ) -> Result<Self, BuildError> {
        let (request_tx, request_rx) = mpsc::channel(2);

        for (peer, address) in &omni_paxos_options.peers {
            networking.register_address(peer.get(), address.clone());
        }

        let rocksdb_storage =
            RocksDbStorage::create(&Configuration::pinned().metadata_store, rocks_db_options)
                .await?;

        let own_node_id = omni_paxos_options.id.get();

        // todo read configuration from persistent storage
        let server_config = ServerConfig {
            pid: own_node_id,
            // todo make configurable
            election_tick_timeout: 5,
            resend_message_tick_timeout: 20,
            ..ServerConfig::default()
        };

        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: omni_paxos_options
                .peers
                .keys()
                .map(|peer| peer.get())
                .collect(),
            ..ClusterConfig::default()
        };

        let op_config = OmniPaxosConfig {
            server_config,
            cluster_config,
        };

        let omni_paxos = op_config.build(rocksdb_storage)?;

        let is_leader = omni_paxos
            .get_current_leader()
            .is_some_and(|(node_id, _)| node_id == own_node_id);

        Ok(Self {
            own_node_id,
            omni_paxos,
            networking,
            msg_rx,
            request_tx,
            request_rx,
            kv_storage: KvMemoryStorage::default(),
            last_applied_index: 0,
            is_leader,
        })
    }

    pub(crate) fn request_sender(&self) -> RequestSender {
        self.request_tx.clone()
    }

    pub(crate) async fn run(mut self) -> Result<(), Error> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let mut tick_interval = time::interval(Duration::from_millis(100));
        tick_interval.set_missed_tick_behavior(MissedTickBehavior::Burst);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                },
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

        debug!("Shutting down OmniPaxosMetadataStore");

        Ok(())
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

    fn run(self) -> impl Future<Output = anyhow::Result<()>> + Send + 'static {
        self.run().map_err(anyhow::Error::from)
    }
}
