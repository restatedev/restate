// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut};
use zerocopy::{IntoBytes, TryFromBytes, big_endian};

use restate_storage_api::StorageError;
use restate_types::sharding::PartitionId;

use crate::keys::{EncodeTableKeyPrefix, KeyDecoder, KeyKind};

use super::{KEY_KIND, Stat, StatId, StatKind};

/// Fixed prefix shared by every key for one statistic in one physical partition.
///
/// Its ten-byte representation matches RocksDB's configured fixed-prefix extractor:
/// `key kind | partition padding | partition id | statistic id | statistic kind | reserved`.
#[derive(
    zerocopy::ByteEq,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct StatKeyPrefix {
    key_kind: [u8; 2],
    /// Zero padding that widens the persisted partition identifier to four bytes.
    padding: big_endian::U16,
    /// The partition that owns the statistic.
    partition_id: big_endian::U16,
    /// The raw globally unique statistic identifier.
    id: big_endian::U16,
    /// The aggregation algorithm used by the statistic.
    stat_kind: StatKind,
    /// Reserved for future use.
    reserved: u8,
}

static_assertions::const_assert_eq!(size_of::<StatKeyPrefix>(), crate::DB_PREFIX_LENGTH);
static_assertions::assert_eq_align!(StatKeyPrefix, u8);

impl StatKeyPrefix {
    /// Length of the persisted prefix.
    pub const SERIALIZED_LENGTH: usize = size_of::<Self>();

    /// Constructs the prefix for statistic `S` in a physical partition.
    pub fn of<S: Stat + ?Sized>(partition_id: PartitionId) -> Self {
        Self {
            key_kind: *KEY_KIND.as_bytes(),
            padding: big_endian::U16::ZERO,
            partition_id: big_endian::U16::new(*partition_id),
            id: big_endian::U16::new(S::STAT_ID.as_u16()),
            stat_kind: S::STAT_KIND,
            reserved: 0,
        }
    }

    fn key_kind(&self) -> Option<KeyKind> {
        KeyKind::from_bytes(&self.key_kind)
    }

    /// Returns the partition that owns the statistic.
    pub fn partition_id(&self) -> PartitionId {
        PartitionId::from(self.partition_id.get())
    }

    /// Returns the raw persisted statistic identifier.
    pub fn stat_id_raw(&self) -> u16 {
        self.id.get()
    }

    /// Returns the typed statistic identifier when it is known to this binary.
    pub fn stat_id(&self) -> Option<StatId> {
        StatId::from_repr(self.stat_id_raw())
    }

    pub fn stat_kind(&self) -> StatKind {
        self.stat_kind
    }

    /// Decodes and validates a statistics prefix, returning the statistic-specific key payload.
    pub fn decode_prefix(key: &[u8]) -> crate::Result<(&Self, KeyDecoder<'_, StatKeyPrefix, 1>)> {
        KeyDecoder::new_stat(key).decode_prefix()
    }
}

/// Types that can be encoded to a full statistic key in partition store.
pub trait EncodeStatKey<C: Stat + ?Sized> {
    fn encode<B: BufMut>(&self, bytes: &mut B);
    fn encoded_len(&self) -> usize;
}

impl<T: EncodeStatKey<C>, C: Stat> EncodeStatKey<C> for &T {
    fn encode<B: BufMut>(&self, scratch: &mut B) {
        T::encode(*self, scratch)
    }

    fn encoded_len(&self) -> usize {
        T::encoded_len(*self)
    }
}

pub trait DecodeStatKey<C: Stat + ?Sized> {
    fn decode(bytes: &mut &[u8]) -> crate::Result<Self>
    where
        Self: Sized;
}

pub trait StatValueCodec {
    fn serialized_len(&self) -> usize;
    fn serialize_to<B: BufMut>(&self, bytes: &mut B);
    fn deserialize_from(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized;
}

pub trait StatMergeValueCodec {
    fn serialized_len(&self) -> usize;
    fn serialize_to<B: BufMut>(&self, bytes: &mut B);
    fn deserialize_from(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized;
}

impl<'a> KeyDecoder<'a, StatKeyPrefix, 0> {
    /// Decoder starting point for all aggregated statistic keys.
    pub fn new_stat(remaining: &'a [u8]) -> Self {
        Self {
            remaining,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn decode_prefix(
        &self,
    ) -> Result<(&'a StatKeyPrefix, KeyDecoder<'a, StatKeyPrefix, 1>), StorageError> {
        let Ok((prefix, payload)) = StatKeyPrefix::try_ref_from_prefix(self.remaining) else {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "failed to decode aggregated statistic key prefix"
            )));
        };
        if prefix.key_kind() != Some(KEY_KIND) {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "invalid aggregated statistic key prefix"
            )));
        }
        if prefix.padding != big_endian::U16::ZERO {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "invalid aggregated statistic key partition padding"
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

impl<'a> KeyDecoder<'a, StatKeyPrefix, 1> {
    pub fn into_decoder<S, C>(self) -> KeyDecoder<'a, C, 0>
    where
        C: DecodeStatKey<S>,
        S: Stat,
    {
        KeyDecoder {
            remaining: self.remaining,
            _marker: std::marker::PhantomData,
        }
    }
}

impl EncodeTableKeyPrefix for StatKeyPrefix {
    const TABLE: crate::TableKind = crate::TableKind::Stats;
    const KEY_KIND: KeyKind = KeyKind::Stats;

    fn serialize_to<B: BufMut>(&self, bytes: &mut B) {
        bytes.put_slice(self.as_bytes());
    }

    fn serialize_key_kind<B: bytes::BufMut>(bytes: &mut B) {
        Self::KEY_KIND.serialize(bytes);
    }

    fn serialized_length(&self) -> usize {
        Self::SERIALIZED_LENGTH
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

    use restate_storage_api::stats::service_load::ServiceLoad;
    use restate_types::sharding::PartitionId;

    use super::*;

    #[test]
    fn stat_key_prefix_matches_rocksdb_prefix() {
        let prefix = StatKeyPrefix::of::<ServiceLoad>(PartitionId::from(8));
        assert_eq!(prefix.as_bytes(), b"ZS\x00\x00\x00\x08\x00\x01\x03\x00");

        let mut key = prefix.as_bytes().to_vec();
        key.extend_from_slice(b"payload");
        let (decoded, payload) = StatKeyPrefix::decode_prefix(&key).unwrap();
        assert_eq!(decoded.partition_id(), PartitionId::from(8));
        assert_eq!(decoded.stat_id(), Some(StatId::ServiceLoad));
        assert_eq!(decoded.stat_kind(), StatKind::StageStatusBucketedGauge);
        assert_eq!(payload.remaining, b"payload");

        key[6..8].copy_from_slice(&1023_u16.to_be_bytes());
        let decoded = StatKeyPrefix::decode_prefix(&key).unwrap().0;
        assert_eq!(decoded.stat_id_raw(), 1023);
        assert_eq!(decoded.stat_id(), None);

        key[9] = 1;
        assert!(StatKeyPrefix::decode_prefix(&key).is_ok());

        key[2..4].copy_from_slice(&1_u16.to_be_bytes());
        assert!(StatKeyPrefix::decode_prefix(&key).is_err());

        key[2..4].fill(0);
        key[8] = u8::MAX;
        assert!(StatKeyPrefix::decode_prefix(&key).is_err());
        assert!(
            StatKeyPrefix::decode_prefix(&key[..StatKeyPrefix::SERIALIZED_LENGTH - 1]).is_err()
        );
    }
}
