// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::raft::networking::Networking;
use crate::raft::storage;
use crate::raft::storage::RocksDbStorage;
use crate::{
    MetadataStoreBackend, MetadataStoreRequest, PreconditionViolation, RequestError,
    RequestReceiver, RequestSender,
};
use assert2::let_assert;
use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use futures::TryFutureExt;
use protobuf::{Message as ProtobufMessage, ProtobufError};
use raft::prelude::{ConfChange, ConfChangeV2, ConfState, Entry, EntryType, Message};
use raft::{Config, RawNode};
use restate_core::cancellation_watcher;
use restate_core::metadata_store::{Precondition, VersionedValue};
use restate_types::config::{Configuration, RaftOptions, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use restate_types::storage::{StorageCodec, StorageDecodeError, StorageEncodeError};
use restate_types::{flexbuffers_storage_encode_decode, Version};
use slog::o;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};
use tracing_slog::TracingSlogDrain;
use ulid::Ulid;

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
    networking: Networking,
    raft_rx: mpsc::Receiver<Message>,
    tick_interval: time::Interval,

    callbacks: HashMap<Ulid, Callback>,
    kv_entries: HashMap<ByteString, VersionedValue>,

    request_tx: RequestSender,
    request_rx: RequestReceiver,
}

impl RaftMetadataStore {
    pub async fn create(
        raft_options: &RaftOptions,
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
        mut networking: Networking,
        raft_rx: mpsc::Receiver<Message>,
    ) -> Result<Self, BuildError> {
        let (request_tx, request_rx) = mpsc::channel(2);

        let config = Config {
            id: raft_options.id,
            ..Default::default()
        };

        let mut metadata_store_options =
            Configuration::updateable().map(|configuration| &configuration.metadata_store);
        let mut storage =
            RocksDbStorage::create(metadata_store_options.live_load(), rocksdb_options).await?;

        // todo: Only write configuration on initialization
        let voters: Vec<_> = raft_options.peers.keys().cloned().collect();
        let conf_state = ConfState::from((voters, vec![]));
        storage.store_conf_state(conf_state).await?;

        // todo: Persist address information with configuration
        for (peer, address) in &raft_options.peers {
            networking.register_address(*peer, address.clone());
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
            callbacks: HashMap::default(),
            kv_entries: HashMap::default(),
            request_rx,
            request_tx,
        })
    }

    pub fn request_sender(&self) -> RequestSender {
        self.request_tx.clone()
    }

    pub async fn run(mut self) -> Result<(), Error> {
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
                    let (callback, request) = Self::split_request(request);

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
        self.callbacks.insert(callback.request_id, callback);
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
        self.handle_request(request);

        Ok(())
    }

    fn handle_request(&mut self, request: Request) {
        match request.kind {
            RequestKind::Get { key } => {
                let result = self.get(key);
                if let Some(callback) = self.callbacks.remove(&request.request_id) {
                    callback.complete_get(result);
                }
            }
            RequestKind::GetVersion { key } => {
                let result = self.get_version(key);
                if let Some(callback) = self.callbacks.remove(&request.request_id) {
                    callback.complete_get_version(result);
                }
            }
            RequestKind::Put {
                key,
                value,
                precondition,
            } => {
                let result = self.put(key, value, precondition);
                if let Some(callback) = self.callbacks.remove(&request.request_id) {
                    callback.complete_put(result.map_err(Into::into));
                }
            }
            RequestKind::Delete { key, precondition } => {
                let result = self.delete(key, precondition);
                if let Some(callback) = self.callbacks.remove(&request.request_id) {
                    callback.complete_delete(result.map_err(Into::into));
                }
            }
        }
    }

    fn get(&self, key: ByteString) -> Option<VersionedValue> {
        self.kv_entries.get(&key).cloned()
    }

    fn get_version(&self, key: ByteString) -> Option<Version> {
        self.kv_entries.get(&key).map(|entry| entry.version)
    }

    fn put(
        &mut self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), PreconditionViolation> {
        match precondition {
            Precondition::None => {
                self.kv_entries.insert(key, value);
            }
            Precondition::DoesNotExist => {
                if self.kv_entries.contains_key(&key) {
                    return Err(PreconditionViolation::kv_pair_exists());
                }

                self.kv_entries.insert(key, value);
            }
            Precondition::MatchesVersion(expected_version) => {
                let actual_version = self.kv_entries.get(&key).map(|entry| entry.version);

                if actual_version == Some(expected_version) {
                    self.kv_entries.insert(key, value);
                } else {
                    return Err(PreconditionViolation::version_mismatch(
                        expected_version,
                        actual_version,
                    ));
                }
            }
        }

        Ok(())
    }

    fn delete(
        &mut self,
        key: ByteString,
        precondition: Precondition,
    ) -> Result<(), PreconditionViolation> {
        match precondition {
            Precondition::None => {
                self.kv_entries.remove(&key);
            }
            Precondition::DoesNotExist => {
                if self.kv_entries.contains_key(&key) {
                    return Err(PreconditionViolation::kv_pair_exists());
                }
            }
            Precondition::MatchesVersion(expected_version) => {
                let actual_version = self.kv_entries.get(&key).map(|entry| entry.version);

                if actual_version == Some(expected_version) {
                    self.kv_entries.remove(&key);
                } else {
                    return Err(PreconditionViolation::version_mismatch(
                        expected_version,
                        actual_version,
                    ));
                }
            }
        }

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

    fn split_request(request: MetadataStoreRequest) -> (Callback, Request) {
        let (request_kind, callback_kind) = match request {
            MetadataStoreRequest::Get { key, result_tx } => {
                (RequestKind::Get { key }, CallbackKind::Get { result_tx })
            }
            MetadataStoreRequest::GetVersion { key, result_tx } => (
                RequestKind::GetVersion { key },
                CallbackKind::GetVersion { result_tx },
            ),
            MetadataStoreRequest::Put {
                key,
                value,
                precondition,
                result_tx,
            } => (
                RequestKind::Put {
                    key,
                    value,
                    precondition,
                },
                CallbackKind::Put { result_tx },
            ),
            MetadataStoreRequest::Delete {
                key,
                precondition,
                result_tx,
            } => (
                RequestKind::Delete { key, precondition },
                CallbackKind::Delete { result_tx },
            ),
        };

        let request_id = Ulid::new();

        let callback = Callback {
            request_id,
            kind: callback_kind,
        };

        let request = Request {
            request_id,
            kind: request_kind,
        };

        (callback, request)
    }
}

struct Callback {
    request_id: Ulid,
    kind: CallbackKind,
}

impl Callback {
    fn fail(self, err: impl Into<RequestError>) {
        match self.kind {
            CallbackKind::Get { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            CallbackKind::GetVersion { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            CallbackKind::Put { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            CallbackKind::Delete { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
        };
    }

    fn complete_get(self, result: Option<VersionedValue>) {
        let_assert!(
            CallbackKind::Get { result_tx } = self.kind,
            "expected 'Get' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(Ok(result));
    }

    fn complete_get_version(self, result: Option<Version>) {
        let_assert!(
            CallbackKind::GetVersion { result_tx } = self.kind,
            "expected 'GetVersion' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(Ok(result));
    }

    fn complete_put(self, result: Result<(), RequestError>) {
        let_assert!(
            CallbackKind::Put { result_tx } = self.kind,
            "expected 'Put' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(result);
    }

    fn complete_delete(self, result: Result<(), RequestError>) {
        let_assert!(
            CallbackKind::Delete { result_tx } = self.kind,
            "expected 'Delete' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(result);
    }
}

enum CallbackKind {
    Get {
        result_tx: oneshot::Sender<Result<Option<VersionedValue>, RequestError>>,
    },
    GetVersion {
        result_tx: oneshot::Sender<Result<Option<Version>, RequestError>>,
    },
    Put {
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
    Delete {
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Request {
    request_id: Ulid,
    kind: RequestKind,
}

flexbuffers_storage_encode_decode!(Request);

impl Request {
    fn encode_to_vec(&self) -> Result<Vec<u8>, StorageEncodeError> {
        let mut buffer = BytesMut::new();
        // todo: Removing support for BufMut requires an extra copy from BytesMut to Vec :-(
        StorageCodec::encode(self, &mut buffer)?;
        Ok(buffer.to_vec())
    }

    fn decode_from_bytes(mut bytes: Bytes) -> Result<Self, StorageDecodeError> {
        StorageCodec::decode::<Request, _>(&mut bytes)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum RequestKind {
    Get {
        key: ByteString,
    },
    GetVersion {
        key: ByteString,
    },
    Put {
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    },
    Delete {
        key: ByteString,
        precondition: Precondition,
    },
}

impl From<raft::Error> for RequestError {
    fn from(value: raft::Error) -> Self {
        match value {
            err @ raft::Error::ProposalDropped => RequestError::Unavailable(err.into()),
            err => RequestError::Internal(err.into()),
        }
    }
}

impl MetadataStoreBackend for RaftMetadataStore {
    fn request_sender(&self) -> RequestSender {
        self.request_sender()
    }

    fn run(self) -> impl Future<Output = anyhow::Result<()>> + Send + 'static {
        self.run().map_err(anyhow::Error::from)
    }
}
