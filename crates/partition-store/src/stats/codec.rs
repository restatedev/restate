// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes};
use zerocopy::{TryFromBytes, big_endian};

use restate_storage_api::StorageError;
use restate_types::sharding::PartitionId;

use crate::keys::header::{HeaderBounds, IndexScope, KeyHeader};
use crate::keys::{KeyDecoder, KeyKind};
use crate::registry::IndexKind;
use crate::table::ReadMetadata;

use super::{Stat, StatKind};

/// The header of a registered statistic's keys: its index identity and the
/// statistic's aggregation kind.
pub const HEADER_LENGTH: usize = AggregateKeyPrefix::SERIALIZED_LENGTH + 1;

/// Typed ownership metadata for the keys of statistic `S`. A read resolves the
/// Ready index of `S`'s definition and fails with `IndexNotReady` when there is none.
pub struct AggregateKeyHeader<S>(PhantomData<fn() -> S>);

impl<S: Stat + 'static> KeyHeader for AggregateKeyHeader<S> {
    type Scope = IndexScope;

    const KEY_KIND: KeyKind = KeyKind::Stats;
    const LENGTH: usize = HEADER_LENGTH;

    fn scope(metadata: &ReadMetadata<'_>) -> crate::Result<IndexScope> {
        Ok(IndexScope {
            partition: metadata.partition.partition_id,
            index: metadata.ready_index(S::DEFINITION, IndexKind::Aggregate)?,
        })
    }

    fn bounds(scope: IndexScope) -> Option<HeaderBounds> {
        let mut header = Vec::with_capacity(HEADER_LENGTH);
        super::encode_header::<S, _>(scope, &mut header);
        Some(HeaderBounds::prefix(Self::KEY_KIND, Bytes::from(header)))
    }
}

/// Identity of one registered statistic index in one physical partition.
///
/// Keys of registered statistics are
/// `key kind (2) | partition padding (2) | partition id (2) | index id (4) |
/// aggregation kind (1) | payload`. The ten-byte identity mirrors the secondary
/// index prefix, with the registry's `u32` index ID; ranges over an index
/// cover every aggregation kind. The aggregation kind tells the merge operator
/// and compaction filter how to process the value without consulting the
/// registry.
#[derive(
    zerocopy::ByteEq,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct AggregateKeyPrefix {
    key_kind: [u8; 2],
    padding: big_endian::U16,
    partition_id: big_endian::U16,
    id: big_endian::U32,
}

static_assertions::const_assert_eq!(size_of::<AggregateKeyPrefix>(), crate::DB_PREFIX_LENGTH);

impl AggregateKeyPrefix {
    pub const SERIALIZED_LENGTH: usize = size_of::<Self>();

    /// The identity of index `id` in a physical partition.
    pub fn new(partition_id: PartitionId, id: u32) -> Self {
        Self {
            key_kind: *KeyKind::Stats.as_bytes(),
            padding: big_endian::U16::ZERO,
            partition_id: big_endian::U16::new(*partition_id),
            id: big_endian::U32::new(id),
        }
    }

    pub fn partition_id(&self) -> PartitionId {
        PartitionId::from(self.partition_id.get())
    }

    pub fn id(&self) -> u32 {
        self.id.get()
    }

    /// Splits a key into its validated identity, aggregation kind and payload.
    pub fn decode(key: &[u8]) -> crate::Result<(&Self, StatKind, &[u8])> {
        let (prefix, rest) =
            Self::try_ref_from_prefix(key).map_err(|_| StorageError::DataIntegrityError)?;
        if prefix.key_kind != *KeyKind::Stats.as_bytes() || prefix.padding.get() != 0 {
            return Err(StorageError::DataIntegrityError);
        }
        let (kind, payload) = rest.split_first().ok_or(StorageError::DataIntegrityError)?;
        let kind = StatKind::from_repr(*kind).ok_or(StorageError::DataIntegrityError)?;
        Ok((prefix, kind, payload))
    }
}

/// Encodes the payload of a key for statistic `C`, without its header.
/// Use [`Stat::encode_key`] to append a complete key.
pub trait EncodeStatKey<C: Stat + ?Sized> {
    fn encode<B: BufMut>(&self, bytes: &mut B);
    fn serialized_length(&self) -> usize;
}

impl<T: EncodeStatKey<C>, C: Stat> EncodeStatKey<C> for &T {
    fn encode<B: BufMut>(&self, scratch: &mut B) {
        T::encode(*self, scratch)
    }

    fn serialized_length(&self) -> usize {
        T::serialized_length(*self)
    }
}

/// Decodes a key payload after its fixed prefix has been checked and removed.
pub trait DecodeStatKey<C: Stat + ?Sized> {
    fn decode(bytes: &mut &[u8]) -> crate::Result<Self>
    where
        Self: Sized;
}

/// Encoding of a materialized statistic value, distinct from a pending merge operand.
pub trait StatValueCodec {
    fn serialized_len(&self) -> usize;
    fn serialize_to<B: BufMut>(&self, bytes: &mut B);
    fn deserialize_from(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized;
}

/// Encoding of a pending update, including negative deltas to unsigned stored counters.
pub trait StatMergeValueCodec {
    fn serialized_len(&self) -> usize;
    fn serialize_to<B: BufMut>(&self, bytes: &mut B);
    fn deserialize_from(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized;
}

impl<'a, C> KeyDecoder<'a, C, 0> {
    pub fn try_full_decode<S>(mut self) -> Result<C, StorageError>
    where
        C: DecodeStatKey<S>,
        S: Stat,
    {
        let decoded = C::decode(&mut self.remaining)?;
        if !self.remaining.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(decoded)
    }
}

// ** Concrete Implementations for types **
impl StatValueCodec for u64 {
    fn serialized_len(&self) -> usize {
        size_of::<Self>()
    }

    fn serialize_to<B: BufMut>(&self, out: &mut B) {
        out.put_u64(*self);
    }

    fn deserialize_from(mut bytes: &[u8]) -> crate::Result<Self> {
        if bytes.remaining() != size_of::<Self>() {
            return Err(StorageError::DataIntegrityError);
        }

        Ok(bytes.get_u64())
    }
}

#[cfg(test)]
mod tests {
    use zerocopy::IntoBytes;

    use restate_types::sharding::PartitionId;

    use super::*;

    #[test]
    fn registered_stat_keys_mirror_the_index_identity() {
        let prefix = AggregateKeyPrefix::new(PartitionId::from(8), 70_000);
        assert_eq!(
            prefix.as_bytes(),
            b"zR\x00\x00\x00\x08\x00\x01\x11\x70",
            "kind, padded partition, u32 index ID"
        );
        let mut key = prefix.as_bytes().to_vec();
        key.push(StatKind::StageStatusBucketedGauge as u8);
        key.extend_from_slice(b"payload");
        let (decoded, kind, payload) = AggregateKeyPrefix::decode(&key).unwrap();
        assert_eq!(
            (decoded.partition_id(), decoded.id(), kind, payload),
            (
                PartitionId::from(8),
                70_000,
                StatKind::StageStatusBucketedGauge,
                &b"payload"[..]
            )
        );
        let corrupt = |at: usize, byte: u8| {
            let mut key = key.clone();
            key[at] = byte;
            AggregateKeyPrefix::decode(&key).is_err()
        };
        assert!(corrupt(1, b'S'), "other_key_kinds");
        assert!(corrupt(3, 1), "nonzero_padding");
        assert!(corrupt(10, u8::MAX), "unknown_aggregation_kind");
        assert!(
            AggregateKeyPrefix::decode(&key[..AggregateKeyPrefix::SERIALIZED_LENGTH]).is_err(),
            "missing_aggregation_kind"
        );
    }
}
