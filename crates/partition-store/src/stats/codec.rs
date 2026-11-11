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

use crate::keys::{EncodeTableKeyPrefix, KeyKind};

use super::{KEY_KIND, StatKind};

/// Fixed prefix shared by every key for one statistic in one physical partition.
///
/// The persisted layout is exactly ten bytes:
/// `key kind (2) | partition padding (2) | partition id (2) | statistic id (2) | kind (1) | reserved (1)`.
/// This is the statistic's fixed identity, independent of RocksDB's capped prefix
/// extractor. Scanning this prefix must use the short-prefix iterator configuration
/// when it is shorter than `PREFIX_EXTRACTOR_LENGTH`.
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
    /// Written as zero; currently ignored when decoding.
    reserved: u8,
}

static_assertions::const_assert_eq!(size_of::<StatKeyPrefix>(), crate::DB_PREFIX_LENGTH);
static_assertions::assert_eq_align!(StatKeyPrefix, u8);

impl StatKeyPrefix {
    /// Length of the persisted prefix.
    pub const SERIALIZED_LENGTH: usize = size_of::<Self>();

    /// Constructs the raw prefix
    #[cfg(test)]
    pub fn new(partition_id: PartitionId, id: u16, stat_kind: StatKind) -> Self {
        Self {
            key_kind: *KEY_KIND.as_bytes(),
            padding: big_endian::U16::ZERO,
            partition_id: big_endian::U16::new(*partition_id),
            id: big_endian::U16::new(id),
            stat_kind,
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

    pub fn stat_kind(&self) -> StatKind {
        self.stat_kind
    }

    /// Validates the key kind, zero padding, and aggregation kind, returning the
    /// prefix and unvalidated payload. Unknown statistic IDs and reserved bytes
    /// are preserved; the caller chooses the appropriate payload decoder.
    pub fn decode_prefix(key: &[u8]) -> crate::Result<(&Self, &[u8])> {
        let Ok((prefix, payload)) = Self::try_ref_from_prefix(key) else {
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
        Ok((prefix, payload))
    }
}

/// Identity of one registered statistic instance in one physical partition.
///
/// Keys of registered statistics are
/// `key kind (2) | partition padding (2) | partition id (2) | instance id (4) |
/// aggregation kind (1) | payload`. The ten-byte identity mirrors the secondary
/// index prefix, with the registry's `u32` physical ID; ranges over an instance
/// cover every aggregation kind. The aggregation kind tells the merge operator
/// and compaction filter how to process the value without consulting the
/// registry. [`StatKeyPrefix`] is the legacy layout with static `u16` IDs.
#[derive(
    zerocopy::ByteEq,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct RegisteredStatKeyPrefix {
    key_kind: [u8; 2],
    padding: big_endian::U16,
    partition_id: big_endian::U16,
    id: big_endian::U32,
}

static_assertions::const_assert_eq!(
    size_of::<RegisteredStatKeyPrefix>(),
    crate::DB_PREFIX_LENGTH
);

impl RegisteredStatKeyPrefix {
    pub const SERIALIZED_LENGTH: usize = size_of::<Self>();

    /// The identity of instance `id` in a physical partition.
    pub fn new(partition_id: PartitionId, id: u32) -> Self {
        Self {
            key_kind: *KeyKind::RegisteredStats.as_bytes(),
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
        if prefix.key_kind != *KeyKind::RegisteredStats.as_bytes() || prefix.padding.get() != 0 {
            return Err(StorageError::DataIntegrityError);
        }
        let (kind, payload) = rest.split_first().ok_or(StorageError::DataIntegrityError)?;
        let kind = StatKind::from_repr(*kind).ok_or(StorageError::DataIntegrityError)?;
        Ok((prefix, kind, payload))
    }
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

    use restate_types::sharding::PartitionId;

    use super::*;

    #[test]
    fn registered_stat_keys_mirror_the_index_identity() {
        let prefix = RegisteredStatKeyPrefix::new(PartitionId::from(8), 70_000);
        assert_eq!(
            prefix.as_bytes(),
            b"zR\x00\x00\x00\x08\x00\x01\x11\x70",
            "kind, padded partition, u32 instance ID"
        );
        let mut key = prefix.as_bytes().to_vec();
        key.push(StatKind::StageStatusBucketedGauge as u8);
        key.extend_from_slice(b"payload");
        let (decoded, kind, payload) = RegisteredStatKeyPrefix::decode(&key).unwrap();
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
            RegisteredStatKeyPrefix::decode(&key).is_err()
        };
        assert!(corrupt(1, b'S'), "other_key_kinds");
        assert!(corrupt(3, 1), "nonzero_padding");
        assert!(corrupt(10, u8::MAX), "unknown_aggregation_kind");
        assert!(
            RegisteredStatKeyPrefix::decode(&key[..RegisteredStatKeyPrefix::SERIALIZED_LENGTH])
                .is_err(),
            "missing_aggregation_kind"
        );
    }

    #[test]
    fn stat_key_prefix_preserves_layout_and_validates_header() {
        let prefix =
            StatKeyPrefix::new(PartitionId::from(8), 12, StatKind::StageStatusBucketedGauge);
        assert_eq!(prefix.as_bytes(), b"zS\x00\x00\x00\x08\x00\x0C\x03\x00");

        let mut key = prefix.as_bytes().to_vec();
        key.extend_from_slice(b"payload");
        let (decoded, payload) = StatKeyPrefix::decode_prefix(&key).unwrap();
        assert_eq!(decoded.partition_id(), PartitionId::from(8));
        assert_eq!(decoded.stat_id_raw(), 12);
        assert_eq!(decoded.stat_kind(), StatKind::StageStatusBucketedGauge);
        assert_eq!(payload, b"payload");

        key[6..8].copy_from_slice(&1023_u16.to_be_bytes());
        let decoded = StatKeyPrefix::decode_prefix(&key).unwrap().0;
        assert_eq!(decoded.stat_id_raw(), 1023);

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
