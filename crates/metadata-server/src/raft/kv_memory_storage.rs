// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use bytestring::ByteString;
use tracing::{debug, trace};
use ulid::Ulid;

use restate_core::MetadataWriter;
use restate_types::Version;
use restate_types::errors::ConversionError;
use restate_types::metadata::{Precondition, VersionedValue};
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::storage::StorageCodec;

use crate::{
    Callback, PreconditionViolation, ReadOnlyRequest, ReadOnlyRequestKind, RequestError,
    RequestKind, WriteRequest,
};
use restate_metadata_server_grpc::grpc;
use restate_metadata_server_grpc::grpc::MetadataServerSnapshot;

pub struct KvMemoryStorage {
    read_only_requests: HashMap<Ulid, ReadOnlyRequest>,
    callbacks: HashMap<Ulid, Callback>,
    kv_entries: HashMap<ByteString, VersionedValue>,
    metadata_writer: Option<MetadataWriter>,
    last_seen_nodes_configuration: Arc<NodesConfiguration>,
}

impl KvMemoryStorage {
    pub fn new(metadata_writer: Option<MetadataWriter>) -> Self {
        KvMemoryStorage {
            metadata_writer,
            read_only_requests: HashMap::default(),
            callbacks: HashMap::default(),
            kv_entries: HashMap::default(),
            last_seen_nodes_configuration: Arc::default(),
        }
    }

    pub fn register_read_only_request(&mut self, read_only_request: ReadOnlyRequest) {
        self.read_only_requests
            .insert(read_only_request.request_id, read_only_request);
    }

    pub fn fail_read_only_requests<F: Fn() -> RequestError>(&mut self, cause: F) {
        for (_, read_only_request) in self.read_only_requests.drain() {
            read_only_request.fail(cause())
        }
    }

    pub fn register_callback(&mut self, callback: Callback) {
        self.callbacks.insert(callback.request_id, callback);
    }

    pub fn fail_callbacks<F: Fn() -> RequestError>(&mut self, cause: F) {
        for (_, callback) in self.callbacks.drain() {
            callback.fail(cause())
        }
    }

    pub fn fail_pending_requests<F: Fn() -> RequestError>(&mut self, cause: F) {
        self.fail_read_only_requests(&cause);
        self.fail_callbacks(cause);
    }

    pub fn last_seen_nodes_configuration(&self) -> &NodesConfiguration {
        &self.last_seen_nodes_configuration
    }

    pub fn handle_request(&mut self, request: WriteRequest) {
        trace!("Handle request: {request:?}");
        match request.kind {
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

    pub fn handle_read_only_request(&mut self, request_id: Ulid) {
        trace!("Handle read-only request: {request_id:?}");
        if let Some(read_only_request) = self.read_only_requests.remove(&request_id) {
            match read_only_request.kind {
                ReadOnlyRequestKind::Get { key, result_tx } => {
                    let result = self.get(key);
                    // err if caller has gone
                    let _ = result_tx.send(Ok(result));
                }
                ReadOnlyRequestKind::GetVersion { key, result_tx } => {
                    let result = self.get_version(&key);
                    // err if caller has gone
                    let _ = result_tx.send(Ok(result));
                }
            }
        } else {
            debug!("Read-only request not found: {request_id}");
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        self.kv_entries.contains_key(key)
    }

    pub fn get(&self, key: ByteString) -> Option<VersionedValue> {
        self.kv_entries.get(&key).cloned()
    }

    pub fn get_version(&self, key: &ByteString) -> Option<Version> {
        self.kv_entries.get(key).map(|entry| entry.version)
    }

    pub fn put(
        &mut self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), PreconditionViolation> {
        match precondition {
            Precondition::None => {
                self.kv_entries.insert(key.clone(), value);
            }
            Precondition::DoesNotExist => {
                if self.kv_entries.contains_key(&key) {
                    return Err(PreconditionViolation::kv_pair_exists());
                }

                self.kv_entries.insert(key.clone(), value);
            }
            Precondition::MatchesVersion(expected_version) => {
                let actual_version = self.kv_entries.get(&key).map(|entry| entry.version);

                if actual_version == Some(expected_version) {
                    self.kv_entries.insert(key.clone(), value);
                } else {
                    return Err(PreconditionViolation::version_mismatch(
                        expected_version,
                        actual_version,
                    ));
                }
            }
        }

        // Not really happy about making the `KvMemoryStorage` aware of the NodesConfiguration. I
        // couldn't find a better way to let a restarting metadata store know about the latest
        // addresses of its peers which it reads from the NodesConfiguration. An alternative could
        // be to not support changing addresses. Changing addresses will also only be possible as
        // long as we maintain a quorum of running nodes. Otherwise, the nodes might not find each
        // other to form quorum.
        if key == NODES_CONFIG_KEY {
            self.update_last_seen_nodes_configuration();
        }

        Ok(())
    }

    fn update_last_seen_nodes_configuration(&mut self) {
        if let Some(mut data) = self
            .kv_entries
            .get(&NODES_CONFIG_KEY)
            .map(|versioned_value| versioned_value.value.as_ref())
        {
            match StorageCodec::decode::<NodesConfiguration, _>(&mut data) {
                Ok(nodes_configuration) => {
                    assert!(
                        self.last_seen_nodes_configuration.version()
                            <= nodes_configuration.version()
                    );
                    self.last_seen_nodes_configuration = Arc::new(nodes_configuration);

                    if let Some(metadata_writer) = self.metadata_writer.as_mut() {
                        metadata_writer.submit(Arc::clone(&self.last_seen_nodes_configuration));
                    }
                }
                Err(err) => {
                    debug!("Failed deserializing NodesConfiguration: {err}");
                }
            }
        }
    }

    pub fn delete(
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

    pub fn restore(&mut self, snapshot: MetadataServerSnapshot) -> Result<(), ConversionError> {
        debug!("Restore from snapshot");
        self.kv_entries.clear();

        for entry in snapshot.entries {
            let (key, versioned_value) = entry.try_into()?;
            self.kv_entries.insert(key, versioned_value);
        }

        self.update_last_seen_nodes_configuration();

        Ok(())
    }

    pub fn snapshot(&self, snapshot: &mut MetadataServerSnapshot) {
        debug!("Create snapshot");
        snapshot.entries = self
            .kv_entries
            .iter()
            .map(|(key, value)| grpc::KvEntry {
                key: key.clone().into_bytes(),
                value: Some(value.clone().into()),
            })
            .collect();
    }
}
