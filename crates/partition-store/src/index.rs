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

use restate_storage_api::Table;
use restate_types::sharding::PartitionId;

use crate::keys::{EncodeIndexKey, IndexKeyPrefix};

mod invocation;
mod macros;
mod maintenance;
mod scan;

pub use invocation::{InvocationByServiceStage, InvocationByServiceStageKey};

/// Identifies a secondary index and the primary table it references.
pub trait SecondaryIndex {
    /// The indexed records' table. Its primary-key type determines the required
    /// terminal suffix of a complete secondary key.
    type Table: Table;

    const INDEX_ID: IndexId;
}

/// A complete secondary key, including its table's required primary-key suffix.
/// Scan-prefix builders do not implement this trait.
pub trait SecondaryIndexKey: EncodeIndexKey {
    type Index: SecondaryIndex;

    fn primary_key(&self) -> &<<Self::Index as SecondaryIndex>::Table as Table>::PrimaryKey;

    /// Appends the fixed index identity and the complete payload to the caller's buffer.
    fn encode_key<B: BufMut>(&self, partition_id: PartitionId, buffer: &mut B) {
        buffer.put_slice(IndexKeyPrefix::of::<Self::Index>(partition_id).as_bytes());
        self.encode(buffer);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[allow(clippy::enum_variant_names)]
pub enum IndexId {
    InvocationByServiceStage = 1,
}

impl IndexId {
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::InvocationByServiceStage => 1,
        }
    }
    pub const fn from_u32(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::InvocationByServiceStage),
            _ => None,
        }
    }
}
