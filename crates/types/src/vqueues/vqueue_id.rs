// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hash::Hash;
use std::str::FromStr;

use bytes::{Buf, BufMut};
use sha2::{Digest, Sha256};

use crate::base62_util::base62_max_length_for_type;
use crate::errors::IdDecodeError;
use crate::id_util::{IdDecoder, IdEncoder, IdSchemeVersion};
use crate::identifiers::{PartitionKey, ResourceId, WithPartitionKey};
use crate::{IdResourceType, PartitionedResourceId};

const DIGEST_LEN: usize = 16;
// We rely on this fact to encode the base62 using u128 representation
static_assertions::const_assert_eq!(DIGEST_LEN, size_of::<u128>());

// 25 bytes.
const RAW_VQUEUE_ID_LEN: usize = const { size_of::<PartitionKey>() + 1 + DIGEST_LEN };

/// Borrowing version of [`VQueueId`].
/// NOTE: keep in-sync with [`VQueueId`]
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, bilrost::Message)]
pub struct VQueueIdRef<'a>(#[bilrost(encoding(plainbytes))] &'a [u8; RAW_VQUEUE_ID_LEN]);

impl<'a> From<&'a VQueueId> for VQueueIdRef<'a> {
    #[inline]
    fn from(id: &'a VQueueId) -> Self {
        Self(&id.0)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, bilrost::Message)]
pub struct VQueueId(#[bilrost(encoding(plainbytes))] [u8; RAW_VQUEUE_ID_LEN]);

impl VQueueId {
    /// Creates the vqueue id by providing the raw digest bytes
    ///
    /// Panics if the digest is shorter than the constant [`DIGEST_LEN`]
    pub fn new(partition_key: PartitionKey, digest: &[u8]) -> Self {
        let mut buf = [0u8; RAW_VQUEUE_ID_LEN];
        {
            assert!(digest.len() >= DIGEST_LEN);
            let mut buf = &mut buf[..];
            buf.put_u64(partition_key);
            buf.put_u8(DIGEST_LEN as u8);
            buf.put_slice(&digest[0..DIGEST_LEN]);
        }

        Self(buf)
    }

    /// Creates the vqueue id by hashing the input slice
    /// Used for testing only
    pub fn custom(partition_key: PartitionKey, input: impl AsRef<[u8]>) -> Self {
        let result = Sha256::digest(input.as_ref());

        let mut buf = [0u8; RAW_VQUEUE_ID_LEN];
        {
            let mut buf = &mut buf[..];
            buf.put_u64(partition_key);
            buf.put_u8(DIGEST_LEN as u8);
            buf.put_slice(&result[0..DIGEST_LEN]);
        }

        Self(buf)
    }

    #[inline]
    pub fn partition_key(&self) -> PartitionKey {
        u64::from_be_bytes(self.0[0..size_of::<PartitionKey>()].try_into().unwrap())
    }

    /// The key is encoded as follows:
    /// - PartitionKey (u64) (big-endian)
    /// - u8 For the size of the rest of the bytes (to support future evolution) with max 255
    ///   bytes. and 0 being a special marker to indicate format change.
    /// - [u6; SIZE]
    pub fn encode_raw_bytes<B: BufMut>(&self, target: &mut B) {
        target.put_slice(&self.0);
    }

    pub fn from_raw_bytes<B: Buf>(source: &mut B) -> Self {
        let mut raw = [0u8; RAW_VQUEUE_ID_LEN];
        source.copy_to_slice(&mut raw);
        Self(raw)
    }

    // 25 bytes
    pub const fn serialized_length_fixed() -> usize {
        std::mem::size_of::<PartitionKey>() + 1 + DIGEST_LEN
    }
}

impl WithPartitionKey for VQueueId {
    #[inline]
    fn partition_key(&self) -> PartitionKey {
        self.partition_key()
    }
}

impl PartitionedResourceId for VQueueId {
    fn from_partition_key_and_slice(
        partition_key: crate::PartitionKey,
        remainder: &[u8],
    ) -> Result<Self, IdDecodeError>
    where
        Self: Sized,
    {
        let mut buf = [0u8; RAW_VQUEUE_ID_LEN];
        {
            let mut buf = &mut buf[..];
            buf.put_u64(partition_key);
            buf.put_slice(remainder);
        }
        Ok(Self(buf))
    }

    fn partition_key(&self) -> crate::PartitionKey {
        self.partition_key()
    }

    fn remainder_slice(&self) -> &[u8] {
        &self.0[size_of::<PartitionKey>()..]
    }
}

// needed when using hashbrown's entry_ref API to convert the key reference to a value
// lazily when inserting into the map.
impl From<&VQueueId> for VQueueId {
    fn from(value: &VQueueId) -> Self {
        value.clone()
    }
}

impl ResourceId for VQueueId {
    const RAW_BYTES_LEN: usize = RAW_VQUEUE_ID_LEN;
    const RESOURCE_TYPE: IdResourceType = IdResourceType::VQueue;

    type StrEncodedLen = generic_array::ConstArrayLength<
        // prefix + separator + version + suffix
        {
            Self::RESOURCE_TYPE.as_str().len()
                // separator + version
                + 2
                + base62_max_length_for_type::<PartitionKey>()
                + base62_max_length_for_type::<u8>()
                + base62_max_length_for_type::<u128>()
        },
    >;

    fn push_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        // v1 vqueue ID is 37c long
        encoder.push_u64(self.partition_key());
        let rest_bytes: [u8; DIGEST_LEN] =
            self.0[size_of::<PartitionKey>() + 1..].try_into().unwrap();
        encoder.push_u128(u128::from_be_bytes(rest_bytes));
    }
}

impl FromStr for VQueueId {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new(input)?;
        // Ensure we are decoding the right type
        if decoder.resource_type != Self::RESOURCE_TYPE {
            return Err(IdDecodeError::TypeMismatch);
        }
        if decoder.version != IdSchemeVersion::V1 {
            return Err(IdDecodeError::Version);
        }
        // VQueueID is 37c long, 33c without vq_1 prefix
        if decoder.cursor.remaining() != 33 {
            return Err(IdDecodeError::Length);
        }

        let mut buf = [0u8; RAW_VQUEUE_ID_LEN];
        {
            // so we can advance the slice as we decode
            let mut buf = &mut buf[..];
            // partition key (u64)
            let partition_key: PartitionKey = decoder.cursor.decode_next()?;
            // big-endian
            buf.put_u64(partition_key);
            buf.put_u8(DIGEST_LEN as u8);

            // what if we change the number of bytes?
            let rest: u128 = decoder.cursor.decode_next()?;
            buf.put_u128(rest);
        }

        Ok(Self(buf))
    }
}

impl std::fmt::Debug for VQueueId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for VQueueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // encode the id such that it is possible to do a string prefix search for a
        // partition key using the first 17 characters.
        let mut encoder = IdEncoder::new();
        self.push_to_encoder(&mut encoder);
        f.write_str(encoder.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_vqueue_id_roundtrip() {
        let id = VQueueId::custom(2, "test");
        let mut buf: Vec<u8> = Vec::with_capacity(100);
        id.encode_raw_bytes(&mut buf);
        let encoded = id.to_string();
        let decoded = VQueueId::from_str(&encoded).unwrap();
        assert_eq!(id, decoded);
        // just to be absolutely sure
        assert_eq!(id.0, decoded.0);
    }

    #[test]
    fn test_vqueue_id_partition_key() {
        let id = VQueueId::custom(88891122323, "test");
        assert_eq!(id.partition_key(), 88891122323);
    }

    #[test]
    fn test_vqueue_id_encode_decode() {
        let id = VQueueId::custom(2247781, "some_test_value");
        let mut buf = Vec::new();
        id.encode_raw_bytes(&mut buf);
        let decoded = VQueueId::from_raw_bytes(&mut buf.as_slice());
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_vqueue_ord_by_partition_key() {
        // vqueue ids's ord implementation should allow them to be sorted by their partition key
        // This means that if I collect a number of vqueue ids in a BtreeSet, I should be able to
        // scan through them by grouping partition key.
        let set: BTreeSet<_> = [
            VQueueId::custom(2, "pk2_item1"),
            VQueueId::custom(3, "pk3_item1"),
            VQueueId::custom(1, "pk1_item1"),
            VQueueId::custom(3, "pk3_item2"),
            VQueueId::custom(1, "pk1_item2"),
            VQueueId::custom(3, "pk3_item3"),
            VQueueId::custom(1, "pk1_item3"),
            VQueueId::custom(3, "pk3_item4"),
        ]
        .into_iter()
        .collect();

        // Example: batch iteration over a BTreeSet by partition key.
        let batched_sizes: Vec<_> = set
            .iter()
            .chunk_by(|id| id.partition_key())
            .into_iter()
            .map(|(partition_key, ids)| (partition_key, ids.count()))
            .collect();

        assert_eq!(batched_sizes, vec![(1, 3), (2, 1), (3, 4)]);
    }
}
