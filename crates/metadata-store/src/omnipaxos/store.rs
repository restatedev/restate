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
use crate::omnipaxos::OmniPaxosMessage;
use crate::{
    MetadataStoreBackend, MetadataStoreRequest, Request, RequestError, RequestReceiver,
    RequestSender,
};
use futures::TryFutureExt;
use omnipaxos::storage::{Entry, NoSnapshot};
use omnipaxos::util::LogEntry;
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ProposeErr, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use restate_core::cancellation_watcher;
use restate_types::config::{OmniPaxosOptions, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use std::future::Future;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info};

type OmniPaxos = omnipaxos::OmniPaxos<Request, MemoryStorage<Request>>;

impl Entry for Request {
    type Snapshot = NoSnapshot;
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("failed building OmniPaxos: {0}")]
    OmniPaxos(#[from] omnipaxos::errors::ConfigError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {}

pub struct OmnipaxosMetadataStore {
    _rocks_db_options: BoxedLiveLoad<RocksDbOptions>,
    networking: Networking<OmniPaxosMessage>,
    msg_rx: mpsc::Receiver<OmniPaxosMessage>,
    request_tx: RequestSender,
    request_rx: RequestReceiver,
    omni_paxos: OmniPaxos,

    last_applied_index: u64,

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

        // todo read configuration from persistent storage
        let server_config = ServerConfig {
            pid: omni_paxos_options.id.into(),
            // todo make configurable
            election_tick_timeout: 5,
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

        let omni_paxos = op_config.build(MemoryStorage::default())?;

        Ok(Self {
            omni_paxos,
            _rocks_db_options: rocks_db_options,
            networking,
            msg_rx,
            request_tx,
            request_rx,
            kv_storage: KvMemoryStorage::default(),
            last_applied_index: 0,
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

            self.send_outgoing_messages();
            self.handle_decided_entries();
        }

        debug!("Shutting down OmniPaxosMetadataStore");

        Ok(())
    }

    fn handle_request(&mut self, request: MetadataStoreRequest) {
        let (callback, request) = request.split_request();

        if let Err(err) = self.omni_paxos.append(request) {
            info!("Failed processing request: {err:?}");
            callback.fail(err)
        } else {
            self.kv_storage.register_callback(callback);
        }
    }

    fn handle_omni_paxos_message(&mut self, msg: OmniPaxosMessage) {
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
