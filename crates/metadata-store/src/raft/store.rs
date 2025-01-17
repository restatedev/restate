// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use crate::raft::storage;
use crate::raft::storage::RocksDbStorage;
use crate::{
    Callback, MetadataStoreBackend, ProvisionSender, Request, RequestError, RequestReceiver,
    RequestSender, StatusWatch,
};
use futures::TryFutureExt;
use protobuf::{Message as ProtobufMessage, ProtobufError};
use raft::prelude::{ConfChange, ConfChangeV2, ConfState, Entry, EntryType, Message};
use raft::{Config, RawNode};
use restate_core::{cancellation_watcher, MetadataWriter};
use restate_types::config::{Configuration, RaftOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::storage::StorageDecodeError;
use slog::o;
use std::future::Future;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};
use tracing_slog::TracingSlogDrain;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("failed creating raft node: {0}")]
    Raft(#[from] raft::Error),
    #[error("failed creating raft storage: {0}")]
    Storage(#[from] storage::BuildError),
    #[error("failed bootstrapping conf state: {0}")]
    BootstrapConfState(#[from] storage::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed appending entries: {0}")]
    Append(#[from] raft::Error),
    #[error("failed deserializing raft serialized requests: {0}")]
    DecodeRequest(StorageDecodeError),
    #[error("failed deserializing conf change: {0}")]
    DecodeConf(ProtobufError),
    #[error("failed applying conf change: {0}")]
    ApplyConfChange(raft::Error),
    #[error("failed reading/writing from/to storage: {0}")]
    Storage(#[from] storage::Error),
}

pub struct RaftMetadataStore {
    _logger: slog::Logger,
    raw_node: RawNode<RocksDbStorage>,
    networking: Networking<Message>,
    raft_rx: mpsc::Receiver<Message>,
    tick_interval: time::Interval,
    health_status: HealthStatus<MetadataServerStatus>,

    kv_storage: KvMemoryStorage,

    request_tx: RequestSender,
    request_rx: RequestReceiver,
}

impl RaftMetadataStore {
    pub async fn create(
        raft_options: &RaftOptions,
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
        mut networking: Networking<Message>,
        raft_rx: mpsc::Receiver<Message>,
        metadata_writer: Option<MetadataWriter>,
        health_status: HealthStatus<MetadataServerStatus>,
    ) -> Result<Self, BuildError> {
        health_status.update(MetadataServerStatus::StartingUp);
        let (request_tx, request_rx) = mpsc::channel(2);

        let config = Config {
            id: raft_options.id.get(),
            ..Default::default()
        };

        let mut metadata_store_options =
            Configuration::updateable().map(|configuration| &configuration.metadata_store);
        let mut storage =
            RocksDbStorage::create(metadata_store_options.live_load(), rocksdb_options).await?;

        // todo: Only write configuration on initialization
        let voters: Vec<_> = raft_options.peers.keys().map(|peer| peer.get()).collect();
        let conf_state = ConfState::from((voters, vec![]));
        storage.store_conf_state(conf_state).await?;

        // todo: Persist address information with configuration
        for (peer, address) in &raft_options.peers {
            networking.register_address(peer.get(), address.clone());
        }

        let drain = TracingSlogDrain;
        let logger = slog::Logger::root(drain, o!());

        let raw_node = RawNode::new(&config, storage, &logger)?;

        let mut tick_interval = time::interval(Duration::from_millis(100));
        tick_interval.set_missed_tick_behavior(MissedTickBehavior::Burst);

        Ok(Self {
            // we only need to keep it alive
            _logger: logger,
            raw_node,
            raft_rx,
            networking,
            tick_interval,
            health_status,
            kv_storage: KvMemoryStorage::new(metadata_writer),
            request_rx,
            request_tx,
        })
    }

    pub fn request_sender(&self) -> RequestSender {
        self.request_tx.clone()
    }

    pub async fn run(mut self) -> Result<(), Error> {
        self.health_status.update(MetadataServerStatus::Active);

        let mut cancellation = std::pin::pin!(cancellation_watcher());

        loop {
            tokio::select! {
                _ = &mut cancellation => {
                    break;
                },
                raft = self.raft_rx.recv() => {
                    if let Some(raft) = raft {
                        self.raw_node.step(raft)?;
                    } else {
                        break;
                    }
                }
                Some(request) = self.request_rx.recv() => {
                    // todo: Unclear whether every replica should be allowed to propose. Maybe
                    //  only the leader should propose and respond to clients.
                    let (callback, request) = request.split_request();

                    if let Err(err) = request
                        .encode_to_vec()
                        .map_err(Into::into)
                        .and_then(|request| self.raw_node
                            .propose(vec![], request)
                            .map_err(RequestError::from)) {
                        info!("Failed processing request: {err}");
                        callback.fail(err);
                        continue;
                    }

                    self.register_callback(callback);
                }
                _ = self.tick_interval.tick() => {
                    self.raw_node.tick();
                }
            }

            self.on_ready().await?;
        }

        self.health_status.update(MetadataServerStatus::Unknown);

        debug!("Stop running RaftMetadataStore.");

        Ok(())
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
            if let Err(err) = self
                .raw_node
                .mut_store()
                .apply_snapshot(ready.snapshot().clone())
            {
                warn!("failed applying snapshot: {err}");
            }
        }

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
            // update commit index in cached hard_state; no need to persist it though
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

        Ok(())
    }

    fn register_callback(&mut self, callback: Callback) {
        self.kv_storage.register_callback(callback);
    }

    fn send_messages(&mut self, messages: Vec<Message>) {
        for message in messages {
            if let Err(err) = self.networking.try_send(message) {
                debug!("failed sending message: {err}");
            }
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
                EntryType::EntryConfChange => self.handle_conf_change(entry).await?,
                EntryType::EntryConfChangeV2 => self.handle_conf_change_v2(entry).await?,
            }
        }

        Ok(())
    }

    fn handle_normal_entry(&mut self, entry: Entry) -> Result<(), Error> {
        let request = Request::decode_from_bytes(entry.data).map_err(Error::DecodeRequest)?;
        self.kv_storage.handle_request(request);

        Ok(())
    }

    async fn handle_conf_change(&mut self, entry: Entry) -> Result<(), Error> {
        let mut cc = ConfChange::default();
        cc.merge_from_bytes(&entry.data)
            .map_err(Error::DecodeConf)?;
        let cs = self
            .raw_node
            .apply_conf_change(&cc)
            .map_err(Error::ApplyConfChange)?;
        self.raw_node.mut_store().store_conf_state(cs).await?;
        Ok(())
    }

    async fn handle_conf_change_v2(&mut self, entry: Entry) -> Result<(), Error> {
        let mut cc = ConfChangeV2::default();
        cc.merge_from_bytes(&entry.data)
            .map_err(Error::DecodeConf)?;
        let cs = self
            .raw_node
            .apply_conf_change(&cc)
            .map_err(Error::ApplyConfChange)?;
        self.raw_node.mut_store().store_conf_state(cs).await?;
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

impl MetadataStoreBackend for RaftMetadataStore {
    fn request_sender(&self) -> RequestSender {
        self.request_sender()
    }

    fn provision_sender(&self) -> Option<ProvisionSender> {
        None
    }

    fn status_watch(&self) -> Option<StatusWatch> {
        None
    }

    fn run(self) -> impl Future<Output = anyhow::Result<()>> + Send + 'static {
        self.run().map_err(anyhow::Error::from)
    }
}
