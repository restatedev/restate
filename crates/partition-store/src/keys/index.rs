// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use zerocopy::{TryFromBytes, big_endian};

use restate_storage_api::StorageError;
use restate_types::sharding::PartitionId;

use crate::index::{IndexId, SecondaryIndex};

use super::KeyKind;
use super::key_decoder::KeyDecoder;

const KEY_KIND: KeyKind = KeyKind::SecondaryIndex;

/// Prefix of secondary indexes in one physical partition.
///
/// Its ten-byte representation matches RocksDB's configured fixed-prefix extractor:
/// `xI (2) | partition padding (2) | partition id (2) | index id (4)`.
#[derive(
    zerocopy::ByteEq,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct IndexKeyPrefix {
    /// The key kind is always b'xI'.
    key_kind: [u8; 2],
    /// Zero padding that widens the persisted partition identifier to four bytes.
    padding: big_endian::U16,
    /// The partition that owns the statistic.
    partition_id: big_endian::U16,
    /// The raw globally unique index identifier.
    idx_id: big_endian::U32,
}

static_assertions::const_assert_eq!(size_of::<IndexKeyPrefix>(), crate::DB_PREFIX_LENGTH);
static_assertions::assert_eq_align!(IndexKeyPrefix, u8);

impl IndexKeyPrefix {
    /// Length of the persisted prefix.
    pub const SERIALIZED_LENGTH: usize = size_of::<Self>();

    /// Constructs the prefix for statistic `S` in a physical partition.
    pub fn of<I: SecondaryIndex + ?Sized>(partition_id: PartitionId) -> Self {
        Self {
            key_kind: *KEY_KIND.as_bytes(),
            padding: big_endian::U16::ZERO,
            partition_id: big_endian::U16::new(*partition_id),
            idx_id: big_endian::U32::new(I::INDEX_ID.as_u32()),
        }
    }

    fn key_kind(&self) -> Option<KeyKind> {
        KeyKind::from_bytes(&self.key_kind)
    }

    /// Returns the partition that owns the statistic.
    pub fn partition_id(&self) -> PartitionId {
        PartitionId::from(self.partition_id.get())
    }

    /// Returns the raw persisted index identifier.
    pub fn index_id_raw(&self) -> u32 {
        self.idx_id.get()
    }

    /// Returns the typed index identifier when it is known to this binary.
    pub fn index_id(&self) -> Option<IndexId> {
        IndexId::from_u32(self.index_id_raw())
    }

    /// Decodes and validates an index prefix, returning the index key payload decoder.
    pub fn decode_prefix(key: &[u8]) -> crate::Result<(&Self, KeyDecoder<'_, IndexKeyPrefix, 1>)> {
        KeyDecoder::new_index(key).decode_prefix()
    }
}

impl<'a> KeyDecoder<'a, IndexKeyPrefix, 0> {
    pub fn new_index(remaining: &'a [u8]) -> Self {
        Self {
            remaining,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn decode_prefix(
        &self,
    ) -> Result<(&'a IndexKeyPrefix, KeyDecoder<'a, IndexKeyPrefix, 1>), StorageError> {
        let Ok((prefix, payload)) = IndexKeyPrefix::try_ref_from_prefix(self.remaining) else {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "failed to decode index key prefix"
            )));
        };
        if prefix.key_kind() != Some(KEY_KIND) {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "invalid index key prefix"
            )));
        }
        if prefix.padding != big_endian::U16::ZERO {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "invalid index key partition padding"
            )));
        }

        Ok((
            prefix,
            KeyDecoder {
                remaining: payload,
                _marker: std::marker::PhantomData,
            },
        ))
    }
}
