// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;

use restate_sharding::{PartitionKey, WithPartitionKey};

use crate::base62_util::base62_max_length_for_type;
use crate::errors::IdDecodeError;
use crate::id_util::{IdDecoder, IdEncoder, IdResourceType};
use crate::identifiers::{InvocationId, ResourceId};
use crate::vqueues::Seq;

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    zerocopy::Immutable,
    zerocopy::KnownLayout,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::Unaligned,
)]
#[repr(u8)]
pub enum BlobKind {
    Unknown = 0,
    Output = 1,
    State = 2,
}

/// Identifier of a blob within a partition.
///
/// Composed of a sequence number followed by a 16 bytes remainder. The byte representation
/// is big-endian, so ordering of the encoded bytes matches the derived [`Ord`].
#[derive(
    Eq,
    Hash,
    PartialEq,
    Clone,
    Copy,
    Debug,
    Ord,
    PartialOrd,
    zerocopy::Immutable,
    zerocopy::KnownLayout,
    zerocopy::IntoBytes,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct BlobId {
    sequence: Seq,
    remainder: [u8; 16],
}

impl BlobId {
    pub const RAW_BYTES_LEN: usize = size_of::<Seq>() + size_of::<u128>();

    pub const fn new(sequence: Seq, remainder: [u8; 16]) -> Self {
        Self {
            sequence,
            remainder,
        }
    }

    pub const fn sequence(&self) -> Seq {
        self.sequence
    }

    pub const fn remainder(&self) -> [u8; 16] {
        self.remainder
    }

    pub fn from_bytes(b: [u8; Self::RAW_BYTES_LEN]) -> Self {
        let (seq, remainder) = b.split_at(size_of::<Seq>());
        Self::new(
            Seq::from_bytes(seq.try_into().expect("checked length")),
            remainder.try_into().expect("checked length"),
        )
    }

    pub const fn to_bytes(self) -> [u8; Self::RAW_BYTES_LEN] {
        zerocopy::transmute!(self)
    }
}

/// Identifies a blob across partitions by combining the partition key with the [`BlobId`].
#[derive(Eq, Hash, PartialEq, Clone, Copy, PartialOrd, Ord)]
pub struct BaseBlobId {
    partition_key: PartitionKey,
    inner: BlobId,
}

impl BaseBlobId {
    pub const fn new(partition_key: PartitionKey, blob_id: BlobId) -> Self {
        Self {
            partition_key,
            inner: blob_id,
        }
    }

    pub fn generate_output_blob_id(invocation: &InvocationId, sequence: Seq) -> Self {
        Self::new(
            invocation.partition_key(),
            BlobId::new(sequence, invocation.invocation_uuid().to_bytes()),
        )
    }

    pub fn blob_id(&self) -> BlobId {
        self.inner
    }

    pub fn into_blob_id(self) -> BlobId {
        self.inner
    }

    pub fn from_slice(b: &[u8]) -> Result<Self, IdDecodeError> {
        if b.len() != Self::RAW_BYTES_LEN {
            return Err(IdDecodeError::Length);
        }
        let (pk, id) = b.split_at(size_of::<PartitionKey>());

        Ok(Self::new(
            PartitionKey::from_be_bytes(pk.try_into().expect("checked length")),
            BlobId::from_bytes(id.try_into().expect("checked length")),
        ))
    }

    pub fn to_bytes(self) -> [u8; Self::RAW_BYTES_LEN] {
        let mut buf = [0u8; Self::RAW_BYTES_LEN];
        let (pk, id) = buf.split_at_mut(size_of::<PartitionKey>());
        pk.copy_from_slice(&self.partition_key.to_be_bytes());
        id.copy_from_slice(&self.inner.to_bytes());
        buf
    }
}

impl WithPartitionKey for BaseBlobId {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

impl ResourceId for BaseBlobId {
    const RAW_BYTES_LEN: usize = size_of::<PartitionKey>() + BlobId::RAW_BYTES_LEN;
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Blob;

    type StrEncodedLen = generic_array::ConstArrayLength<
        // prefix + separator + version + suffix
        {
            Self::RESOURCE_TYPE.as_str().len()
                // separator + version
                + 2
                + base62_max_length_for_type::<PartitionKey>()
                + base62_max_length_for_type::<u64>()
                + base62_max_length_for_type::<u128>()
        },
    >;

    fn push_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        encoder.push_u64(self.partition_key);
        encoder.push_u64(self.inner.sequence.as_u64());
        encoder.push_u128(u128::from_be_bytes(self.inner.remainder));
    }
}

impl fmt::Display for BaseBlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut encoder = IdEncoder::new();
        self.push_to_encoder(&mut encoder);
        f.write_str(encoder.as_str())
    }
}

impl fmt::Debug for BaseBlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // use the same formatting for debug and display to show a consistent representation
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for BaseBlobId {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new(input)?;
        // Ensure we are decoding the right type
        if decoder.resource_type != Self::RESOURCE_TYPE {
            return Err(IdDecodeError::TypeMismatch);
        }

        let partition_key: PartitionKey = decoder.cursor.decode_next()?;
        let sequence: u64 = decoder.cursor.decode_next()?;
        let remainder: u128 = decoder.cursor.decode_next()?;
        let blob_id = BlobId::new(Seq::new(sequence), remainder.to_be_bytes());

        if decoder.cursor.remaining() != 0 {
            return Err(IdDecodeError::Length);
        }

        Ok(Self::new(partition_key, blob_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_blob_id_roundtrip() {
        let invocation_id = InvocationId::mock_random();
        let id = BaseBlobId::generate_output_blob_id(&invocation_id, Seq::new(42));

        // deterministic
        assert_eq!(
            id,
            BaseBlobId::generate_output_blob_id(&invocation_id, Seq::new(42))
        );
        assert_ne!(
            id,
            BaseBlobId::generate_output_blob_id(&invocation_id, Seq::new(43))
        );

        assert_eq!(id.blob_id().sequence(), Seq::new(42));
        assert_eq!(
            id.blob_id().remainder(),
            invocation_id.invocation_uuid().to_bytes()
        );
        assert_eq!(BlobId::from_bytes(id.blob_id().to_bytes()), id.blob_id());

        // string
        let encoded = id.to_string();
        assert!(encoded.starts_with("blob_1"));
        assert_eq!(encoded.len(), BaseBlobId::str_encoded_len());
        assert_eq!(encoded.parse::<BaseBlobId>(), Ok(id));
        assert_eq!(
            format!("{encoded}0").parse::<BaseBlobId>(),
            Err(IdDecodeError::Length)
        );
        assert_eq!(
            invocation_id.to_string().parse::<BaseBlobId>(),
            Err(IdDecodeError::TypeMismatch)
        );

        // bytes
        assert_eq!(BaseBlobId::from_slice(&id.to_bytes()), Ok(id));
        assert_eq!(
            BaseBlobId::from_slice(&id.to_bytes()[1..]),
            Err(IdDecodeError::Length)
        );
    }
}
