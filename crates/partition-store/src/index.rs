// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BufMut;
use zerocopy::IntoBytes;

use restate_storage_api::PrimaryKey;
use restate_types::sharding::PartitionId;

use crate::keys::{EncodeIndexKey, IndexKeyPrefix};

mod entry;
mod macros;
mod maintenance;
mod scan;
mod virtual_object;
mod vqueue;

pub use entry::{
    EntryByStage, EntryByStageKey, EntryByStageKeyView, EntryByStageService,
    EntryByStageServiceKey, EntryByStageServiceKeyView, EntryNextAtByStage, EntryNextAtByStageKey,
    EntryNextAtByStageKeyView, EntryNextAtByStageService, EntryNextAtByStageServiceKey,
    EntryNextAtByStageServiceKeyView,
};
pub use virtual_object::{
    EntryByVirtualObjectStage, EntryByVirtualObjectStageKey, EntryByVirtualObjectStageKeyView,
    EntryNextAtByVirtualObjectStage, EntryNextAtByVirtualObjectStageKey,
    EntryNextAtByVirtualObjectStageKeyView,
};
pub use vqueue::{BusyVQueue, BusyVQueueKey, BusyVQueueKeyView};

/// Identifies a secondary index and the primary-key type it references.
pub trait SecondaryIndex {
    /// The complete record identity stored as the terminal suffix of each key.
    type PrimaryKey: PrimaryKey;

    const INDEX_ID: IndexId;
}

/// A complete secondary key, including its required primary-key suffix.
/// Scan-prefix builders do not implement this trait.
pub trait SecondaryIndexKey: EncodeIndexKey {
    type Index: SecondaryIndex;

    fn primary_key(&self) -> &<Self::Index as SecondaryIndex>::PrimaryKey;

    /// Appends the fixed index identity and the complete payload to the caller's buffer.
    fn encode_key<B: BufMut>(&self, partition_id: PartitionId, buffer: &mut B) {
        buffer.put_slice(IndexKeyPrefix::of::<Self::Index>(partition_id).as_bytes());
        self.encode(buffer);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[allow(clippy::enum_variant_names)]
pub enum IndexId {
    /// Stage, service, descending transition time, canonical ID.
    EntryByStageService = 1,
    /// Stage, descending transition time, canonical ID.
    EntryByStage = 2,
    /// Stage, next transition time, sequence, canonical ID.
    EntryNextAtByStage = 3,
    BusyVQueue = 4,
    /// Service, scope, object key, stage, descending transition time, canonical ID.
    EntryByVirtualObjectStage = 5,
    /// Stage, service, next transition time, sequence, canonical ID.
    EntryNextAtByStageService = 6,
    /// Service, scope, object key, stage, next transition time, sequence, canonical ID.
    EntryNextAtByVirtualObjectStage = 7,
}

impl IndexId {
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::EntryByStageService => 1,
            Self::EntryByStage => 2,
            Self::EntryNextAtByStage => 3,
            Self::BusyVQueue => 4,
            Self::EntryByVirtualObjectStage => 5,
            Self::EntryNextAtByStageService => 6,
            Self::EntryNextAtByVirtualObjectStage => 7,
        }
    }
    pub const fn from_u32(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::EntryByStageService),
            2 => Some(Self::EntryByStage),
            3 => Some(Self::EntryNextAtByStage),
            4 => Some(Self::BusyVQueue),
            5 => Some(Self::EntryByVirtualObjectStage),
            6 => Some(Self::EntryNextAtByStageService),
            7 => Some(Self::EntryNextAtByVirtualObjectStage),
            _ => None,
        }
    }
}
