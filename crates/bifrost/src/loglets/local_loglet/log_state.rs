// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use restate_types::flexbuffers_storage_encode_decode;
use restate_types::storage::StorageCodec;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::{error, trace, warn};

use crate::loglet::LogletOffset;
use crate::loglets::local_loglet::keys::{MetadataKey, MetadataKind};
use crate::SealReason;

use super::LogStoreError;

/// Bundles one or more updates to log state. LogState updates are applied via
/// a rocksdb merge operator
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LogStateUpdates {
    /// SmallVec is used to avoid heap allocation for the common case of small
    /// number of updates.
    updates: SmallVec<[LogStateUpdate; 1]>,
}

/// Represents a single update to the log state.
#[derive(Debug, Serialize, Deserialize)]
enum LogStateUpdate {
    ReleasePointer(u64),
    TrimPoint(u64),
    Seal(SealReason),
}

impl LogStateUpdates {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            updates: SmallVec::with_capacity(capacity),
        }
    }

    pub fn update_release_pointer(mut self, release_pointer: LogletOffset) -> Self {
        self.updates
            .push(LogStateUpdate::ReleasePointer(release_pointer.into()));
        self
    }

    pub fn update_trim_point(mut self, trim_point: LogletOffset) -> Self {
        self.updates
            .push(LogStateUpdate::TrimPoint(trim_point.into()));
        self
    }

    #[allow(dead_code)]
    pub fn seal(mut self, reason: SealReason) -> Self {
        self.updates.push(LogStateUpdate::Seal(reason));
        self
    }
}

impl LogStateUpdates {
    pub fn to_bytes(&self) -> Result<Bytes, LogStoreError> {
        let mut buf = BytesMut::default();
        self.encode(&mut buf)?;
        Ok(buf.freeze())
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), LogStoreError> {
        StorageCodec::encode(self, buf).map_err(|err| LogStoreError::Encode(Arc::new(err)))
    }

    pub fn from_slice(mut data: &[u8]) -> Result<Self, LogStoreError> {
        StorageCodec::decode(&mut data).map_err(|err| LogStoreError::Decode(Arc::new(err)))
    }
}

flexbuffers_storage_encode_decode!(LogStateUpdates);

#[derive(Debug, Serialize, Deserialize)]
pub struct LogState {
    pub release_pointer: u64,
    pub trim_point: u64,
    pub seal: Option<SealReason>,
}

impl Default for LogState {
    fn default() -> Self {
        Self {
            release_pointer: 0,
            trim_point: 0,
            seal: None,
        }
    }
}

impl LogState {
    pub fn to_bytes(&self) -> Result<Bytes, LogStoreError> {
        let mut buf = BytesMut::default();
        StorageCodec::encode(self, &mut buf).map_err(Arc::new)?;
        Ok(buf.freeze())
    }

    pub fn from_slice(mut data: &[u8]) -> Result<Self, LogStoreError> {
        StorageCodec::decode(&mut data).map_err(|err| LogStoreError::Decode(Arc::new(err)))
    }
}

flexbuffers_storage_encode_decode!(LogState);

pub fn log_state_full_merge(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let key = MetadataKey::from_slice(key);
    trace!(key = ?key, "log_state_full_merge");
    if key.kind != MetadataKind::LogState {
        warn!(key = ?key, "Merge is only supported for log-state");
        return None;
    }

    let mut log_state = existing_val
        .and_then(|f| {
            let decoded = LogState::from_slice(f);
            match decoded {
                Err(e) => {
                    error!("Failed to decode log state object: {}", e);
                    None
                }
                Ok(decoded) => Some(decoded),
            }
        })
        .unwrap_or_default();

    for op in operands {
        let updates = LogStateUpdates::from_slice(op);
        let updates = match updates {
            Err(e) => {
                error!("Failed to decode log state updates: {}", e);
                return None;
            }
            Ok(updates) => updates,
        };
        for update in updates.updates {
            match update {
                LogStateUpdate::ReleasePointer(offset) => {
                    // release pointer can only move forward
                    log_state.release_pointer = log_state.release_pointer.max(offset);
                }
                LogStateUpdate::TrimPoint(offset) => {
                    // trim point can only move forward
                    log_state.trim_point = log_state.trim_point.max(offset);
                }
                LogStateUpdate::Seal(reason) => {
                    // A log cannot be sealed twice.
                    if log_state.seal.is_none() {
                        // trim point can only move forward
                        log_state.seal = Some(reason);
                    }
                }
            }
        }
    }
    match log_state.to_bytes() {
        Ok(bytes) => Some(bytes.into()),
        Err(err) => {
            error!("Failed to encode log state updates: {}", err);
            None
        }
    }
}

pub fn log_state_partial_merge(
    key: &[u8],
    _unused: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let key = MetadataKey::from_slice(key);

    if key.kind != MetadataKind::LogState {
        warn!(key = ?key, "Merge is only supported for log-state");
        return None;
    }
    let mut merged = LogStateUpdates::with_capacity(operands.len());
    for op in operands {
        let updates = LogStateUpdates::from_slice(op);
        let mut updates = match updates {
            Err(e) => {
                error!(key = ?key,"Failed to decode log state updates: {}", e);
                return None;
            }
            Ok(updates) => updates,
        };

        merged.updates.append(&mut updates.updates);
    }
    match merged.to_bytes() {
        Ok(bytes) => Some(bytes.into()),
        Err(err) => {
            error!("Failed to encode log state updates: {}", err);
            None
        }
    }
}
