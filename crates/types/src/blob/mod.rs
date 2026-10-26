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

/// Identifies a blob across partitions.
///
/// Composed of the partition key, a sequence number and a 16 bytes remainder. The byte
/// representation is big-endian, so ordering of the encoded bytes matches the derived [`Ord`].
#[derive(Eq, Hash, PartialEq, Clone, Copy, PartialOrd, Ord)]
pub struct BlobId {
    partition_key: PartitionKey,
    sequence: Seq,
    remainder: [u8; 16],
}

impl BlobId {
    pub const fn new(partition_key: PartitionKey, sequence: Seq, remainder: [u8; 16]) -> Self {
        Self {
            partition_key,
            sequence,
            remainder,
        }
    }

    pub fn generate_output_blob_id(invocation: &InvocationId, sequence: Seq) -> Self {
        Self::new(
            invocation.partition_key(),
            sequence,
            invocation.invocation_uuid().to_bytes(),
        )
    }

    pub const fn sequence(&self) -> Seq {
        self.sequence
    }

    pub const fn remainder(&self) -> [u8; 16] {
        self.remainder
    }

    pub fn from_slice(b: &[u8]) -> Result<Self, IdDecodeError> {
        let b: &[u8; Self::RAW_BYTES_LEN] = b.try_into().map_err(|_| IdDecodeError::Length)?;
        let (pk, rest) = b
            .split_first_chunk::<{ size_of::<PartitionKey>() }>()
            .expect("checked length");
        let (seq, remainder) = rest
            .split_first_chunk::<{ size_of::<Seq>() }>()
            .expect("checked length");

        Ok(Self::new(
            PartitionKey::from_be_bytes(*pk),
            Seq::from_bytes(*seq),
            remainder.try_into().expect("checked length"),
        ))
    }

    pub fn to_bytes(self) -> [u8; Self::RAW_BYTES_LEN] {
        let mut buf = [0u8; Self::RAW_BYTES_LEN];
        let (pk, rest) = buf.split_at_mut(size_of::<PartitionKey>());
        let (seq, remainder) = rest.split_at_mut(size_of::<Seq>());
        pk.copy_from_slice(&self.partition_key.to_be_bytes());
        seq.copy_from_slice(&self.sequence.to_bytes());
        remainder.copy_from_slice(&self.remainder);
        buf
    }
}

impl WithPartitionKey for BlobId {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

impl ResourceId for BlobId {
    const RAW_BYTES_LEN: usize = size_of::<PartitionKey>() + size_of::<Seq>() + size_of::<u128>();
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
        encoder.push_u64(self.sequence.as_u64());
        encoder.push_u128(u128::from_be_bytes(self.remainder));
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut encoder = IdEncoder::new();
        self.push_to_encoder(&mut encoder);
        f.write_str(encoder.as_str())
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // use the same formatting for debug and display to show a consistent representation
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for BlobId {
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

        if decoder.cursor.remaining() != 0 {
            return Err(IdDecodeError::Length);
        }

        Ok(Self::new(
            partition_key,
            Seq::new(sequence),
            remainder.to_be_bytes(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_id_roundtrip() {
        let invocation_id = InvocationId::mock_random();
        let id = BlobId::generate_output_blob_id(&invocation_id, Seq::new(42));

        // deterministic
        assert_eq!(
            id,
            BlobId::generate_output_blob_id(&invocation_id, Seq::new(42))
        );
        assert_ne!(
            id,
            BlobId::generate_output_blob_id(&invocation_id, Seq::new(43))
        );

        assert_eq!(id.partition_key(), invocation_id.partition_key());
        assert_eq!(id.sequence(), Seq::new(42));
        assert_eq!(id.remainder(), invocation_id.invocation_uuid().to_bytes());

        // string
        let encoded = id.to_string();
        assert!(encoded.starts_with("blob_1"));
        assert_eq!(encoded.len(), BlobId::str_encoded_len());
        assert_eq!(encoded.parse::<BlobId>(), Ok(id));
        assert_eq!(
            format!("{encoded}0").parse::<BlobId>(),
            Err(IdDecodeError::Length)
        );
        assert_eq!(
            invocation_id.to_string().parse::<BlobId>(),
            Err(IdDecodeError::TypeMismatch)
        );

        // bytes: big-endian pk | seq | remainder, so byte order matches `Ord`
        let bytes = id.to_bytes();
        assert_eq!(bytes[..8], invocation_id.partition_key().to_be_bytes());
        assert_eq!(bytes[8..16], 42u64.to_be_bytes());
        assert_eq!(bytes[16..], invocation_id.invocation_uuid().to_bytes());
        assert_eq!(BlobId::from_slice(&id.to_bytes()), Ok(id));
        assert_eq!(
            BlobId::from_slice(&id.to_bytes()[1..]),
            Err(IdDecodeError::Length)
        );
    }
}
