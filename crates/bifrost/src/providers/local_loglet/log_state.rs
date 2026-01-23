// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BytesMut;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::{error, trace, warn};

use restate_types::flexbuffers_storage_encode_decode;
use restate_types::logs::LogletOffset;
use restate_types::storage::StorageCodec;

use super::keys::{MetadataKey, MetadataKind};

use super::LogStoreError;

/// Bundles one or more updates to log state. LogState updates are applied via
/// a rocksdb merge operator
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LogStateUpdates {
    /// SmallVec is used to avoid heap allocation for the common case of small
    /// number of updates.
    /// Assumes one entry per LogStateUpdate variant at most since everything can be merged.
    updates: SmallVec<[LogStateUpdate; 3]>,
}

/// Represents a single update to the log state.
#[derive(Debug, Serialize, Deserialize)]
enum LogStateUpdate {
    ReleasePointer(u32),
    TrimPoint(u32),
    Seal,
}

impl LogStateUpdates {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            updates: SmallVec::with_capacity(capacity),
        }
    }

    pub fn update_release_pointer(mut self, release_pointer: LogletOffset) -> Self {
        // update existing release pointer if exists, otherwise, add to the vec.
        for update in &mut self.updates {
            if let LogStateUpdate::ReleasePointer(existing) = update {
                *existing = (*existing).max(release_pointer.into());
                return self;
            }
        }
        self.updates
            .push(LogStateUpdate::ReleasePointer(release_pointer.into()));
        self
    }

    pub fn update_trim_point(mut self, trim_point: LogletOffset) -> Self {
        // update existing release pointer if exists, otherwise, add to the vec.
        for update in &mut self.updates {
            if let LogStateUpdate::TrimPoint(existing) = update {
                *existing = (*existing).max(trim_point.into());
                return self;
            }
        }
        self.updates
            .push(LogStateUpdate::TrimPoint(trim_point.into()));
        self
    }

    pub fn seal(mut self) -> Self {
        // do nothing if we will seal already
        for update in &self.updates {
            if matches!(update, LogStateUpdate::Seal) {
                return self;
            }
        }
        self.updates.push(LogStateUpdate::Seal);
        self
    }

    pub fn merge(mut self, rhs: LogStateUpdates) -> Self {
        for update in rhs.updates {
            match update {
                LogStateUpdate::ReleasePointer(release_pointer) => {
                    self = self.update_release_pointer(LogletOffset::from(release_pointer));
                }
                LogStateUpdate::TrimPoint(trim_point) => {
                    self = self.update_trim_point(LogletOffset::from(trim_point));
                }
                LogStateUpdate::Seal => {
                    self = self.seal();
                }
            }
        }
        self
    }
}

impl LogStateUpdates {
    pub fn to_bytes(&self) -> Result<BytesMut, LogStoreError> {
        // trying to avoid buffer resizing by having plenty of space for serialization
        let mut buf = BytesMut::with_capacity(std::mem::size_of::<Self>() * 2);
        self.encode(&mut buf)?;
        Ok(buf)
    }

    pub fn encode_and_split(&self, buf: &mut BytesMut) -> Result<BytesMut, LogStoreError> {
        self.encode(buf)?;
        Ok(buf.split())
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), LogStoreError> {
        // trying to avoid buffer resizing by having plenty of space for serialization
        buf.reserve(std::mem::size_of::<Self>() * 2);
        StorageCodec::encode(self, buf)?;
        Ok(())
    }

    pub fn from_slice(mut data: &[u8]) -> Result<Self, LogStoreError> {
        Ok(StorageCodec::decode(&mut data)?)
    }
}

flexbuffers_storage_encode_decode!(LogStateUpdates);

/// DEPRECATED in v1.0
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
enum SealReason {
    Resharding,
    Other(String),
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LogState {
    pub release_pointer: u32,
    pub trim_point: u32,
    // deprecated and unused. Kept for v1 compatibility.
    #[serde(default)]
    seal: Option<SealReason>,
    #[serde(default)]
    pub sealed: bool,
}

impl LogState {
    pub fn to_bytes(&self) -> Result<BytesMut, LogStoreError> {
        // trying to avoid buffer resizing by having plenty of space for serialization
        let mut buf = BytesMut::with_capacity(std::mem::size_of::<Self>() * 2);
        self.encode(&mut buf)?;
        Ok(buf)
    }

    #[allow(unused)]
    pub fn encode_and_split(&self, buf: &mut BytesMut) -> Result<BytesMut, LogStoreError> {
        self.encode(buf)?;
        Ok(buf.split())
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), LogStoreError> {
        // trying to avoid buffer resizing by having plenty of space for serialization
        buf.reserve(std::mem::size_of::<Self>() * 2);
        StorageCodec::encode(self, buf)?;
        Ok(())
    }

    pub fn from_slice(mut data: &[u8]) -> Result<Self, LogStoreError> {
        Ok(StorageCodec::decode(&mut data)?)
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
                LogStateUpdate::Seal => {
                    log_state.sealed = true;
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
    // assuming one entry per LogStateUpdate variant at most since everything can be merged.
    let mut merged = LogStateUpdates::with_capacity(3);

    for op in operands {
        let updates = LogStateUpdates::from_slice(op);
        let updates = match updates {
            Err(e) => {
                error!(key = ?key,"Failed to decode log state updates: {}", e);
                return None;
            }
            Ok(updates) => updates,
        };

        // deduplicates all operations
        merged = merged.merge(updates);
    }
    match merged.to_bytes() {
        Ok(bytes) => Some(bytes.into()),
        Err(err) => {
            error!("Failed to encode log state updates: {}", err);
            None
        }
    }
}
