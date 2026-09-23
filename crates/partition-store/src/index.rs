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

pub use entry::{
    EntryByServiceStage, EntryByServiceStageKey, EntryByStage, EntryByStageKey, EntryNextAtByStage,
    EntryNextAtByStageKey,
};

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
    EntryByServiceStage = 1,
    EntryByStage = 2,
    EntryNextAtByStage = 3,
}

impl IndexId {
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::EntryByServiceStage => 1,
            Self::EntryByStage => 2,
            Self::EntryNextAtByStage => 3,
        }
    }
    pub const fn from_u32(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::EntryByServiceStage),
            2 => Some(Self::EntryByStage),
            3 => Some(Self::EntryNextAtByStage),
            _ => None,
        }
    }
}
