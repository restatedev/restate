// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
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
use restate_types::Version;
use std::collections::HashMap;
use tracing::debug;
use ulid::Ulid;

#[derive(Default)]
pub struct KvMemoryStorage {
    callbacks: HashMap<Ulid, Callback>,
    kv_entries: HashMap<ByteString, VersionedValue>,
}

impl KvMemoryStorage {
    pub fn register_callback(&mut self, callback: Callback) {
        self.callbacks.insert(callback.request_id, callback);
        debug!("Pending callbacks: {}", self.callbacks.len());
    }

    pub fn fail_callbacks<F: Fn() -> RequestError>(&mut self, cause: F) {
        for (_, callback) in self.callbacks.drain() {
            callback.fail(cause())
        }
    }

    pub fn handle_request(&mut self, request: Request) {
        debug!("Handle request: {request:?}");
        debug!(
            "Pending callbacks before handling request: {}",
            self.callbacks.len()
        );
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

        debug!(
            "Pending callbacks after handling request: {}",
            self.callbacks.len()
        );
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
