// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{Callback, PreconditionViolation, Request, RequestError, RequestKind};
use bytestring::ByteString;
use restate_core::metadata_store::{Precondition, VersionedValue};
use restate_core::MetadataWriter;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::storage::StorageCodec;
use restate_types::Version;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace};
use ulid::Ulid;

pub struct KvMemoryStorage {
    callbacks: HashMap<Ulid, Callback>,
    kv_entries: HashMap<ByteString, VersionedValue>,
    metadata_writer: Option<MetadataWriter>,
    last_seen_nodes_configuration: Arc<NodesConfiguration>,
}

impl KvMemoryStorage {
    pub fn new(metadata_writer: Option<MetadataWriter>) -> Self {
        KvMemoryStorage {
            metadata_writer,
            callbacks: HashMap::default(),
            kv_entries: HashMap::default(),
            last_seen_nodes_configuration: Arc::default(),
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

    pub fn last_seen_nodes_configuration(&self) -> &NodesConfiguration {
        &self.last_seen_nodes_configuration
    }

    pub fn handle_request(&mut self, request: Request) {
        trace!("Handle request: {request:?}");
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

    pub fn get(&self, key: ByteString) -> Option<VersionedValue> {
        self.kv_entries.get(&key).cloned()
    }

    pub fn get_version(&self, key: ByteString) -> Option<Version> {
        self.kv_entries.get(&key).map(|entry| entry.version)
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
            let mut data = self
                .kv_entries
                .get(&key)
                .expect("to be present")
                .value
                .as_ref();
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

        Ok(())
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
}
