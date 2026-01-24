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
use std::sync::Mutex;

use bytestring::ByteString;

use restate_types::Version;
use restate_types::metadata::{Precondition, VersionedValue};

use crate::metadata_store::{ProvisionedMetadataStore, ReadError, WriteError};

#[derive(Debug, Default)]
pub struct InMemoryMetadataStore {
    kv_pairs: Mutex<HashMap<ByteString, VersionedValue>>,
}

impl InMemoryMetadataStore {
    fn assert_precondition(
        precondition: Precondition,
        current_version: Option<Version>,
    ) -> Result<(), WriteError> {
        match precondition {
            Precondition::None => {
                // nothing to do
            }
            Precondition::DoesNotExist => {
                if current_version.is_some() {
                    return Err(WriteError::FailedPrecondition("kv-pair exists".to_owned()));
                }
            }
            Precondition::MatchesVersion(expected_version) => {
                if current_version != Some(expected_version) {
                    return Err(WriteError::FailedPrecondition(
                        "actual version does not match expected version".to_owned(),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl ProvisionedMetadataStore for InMemoryMetadataStore {
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        Ok(self.kv_pairs.lock().unwrap().get(&key).cloned())
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        Ok(self.kv_pairs.lock().unwrap().get(&key).map(|v| v.version))
    }

    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let mut guard = self.kv_pairs.lock().unwrap();
        let current_version = guard.get(&key).map(|v| v.version);

        Self::assert_precondition(precondition, current_version)?;

        guard.insert(key, value);

        Ok(())
    }

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let mut guard = self.kv_pairs.lock().unwrap();
        let current_version = guard.get(&key).map(|v| v.version);

        Self::assert_precondition(precondition, current_version)?;

        guard.remove(&key);

        Ok(())
    }
}
