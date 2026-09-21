// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use zerocopy::{
    ByteEq, ByteHash, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned, big_endian,
};

use restate_sharding::{PartitionKey, WithPartitionKey};

use super::{CanonicalEntryId, InvocationId, ResourceId, StateMutationId};
use crate::errors::IdDecodeError;
use crate::id_util::{IdDecoder, IdEncoder};
use crate::vqueues::{EntryId, EntryKind, ParseError, Seq};

/// A partition-qualified resource identity, shared by all of its incarnations.
///
/// This adds a partition key to [`EntryId`]. For known entry kinds, its string
/// representation is the original [`InvocationId`] or [`StateMutationId`], without
/// a sequence suffix. Use [`Self::canonicalize`] when incarnation identity matters.
///
/// The 25-byte raw layout is `partition key (8B, big-endian) | kind (1B) | remainder (16B)`.
/// It is byte-aligned and ordered lexicographically by those fields. The same
/// layout forms the prefix of [`CanonicalEntryId`], allowing a borrowed base view.
///
/// See the [identifier module](crate::identifiers) for the relationship between
/// local, base, and canonical entry IDs and their validation boundaries.
#[derive(
    ByteEq,
    ByteHash,
    Immutable,
    IntoBytes,
    KnownLayout,
    Unaligned,
    TryFromBytes,
    Clone,
    Copy,
    Ord,
    PartialOrd,
)]
#[repr(C)]
pub struct BaseEntryId {
    partition_key: big_endian::U64,
    id: EntryId,
}

impl BaseEntryId {
    /// The size of a base entry ID's byte encoding.
    pub const RAW_BYTES_LEN: usize = size_of::<PartitionKey>() + size_of::<EntryId>();

    /// Attaches the caller-supplied partition key to a local entry ID.
    /// The relationship between the key and the resource is not validated here.
    pub const fn new(partition_key: PartitionKey, id: EntryId) -> Self {
        Self {
            partition_key: big_endian::U64::new(partition_key),
            id,
        }
    }

    /// Adds a caller-assigned incarnation sequence without generating or validating it.
    pub const fn canonicalize(self, seq: Seq) -> CanonicalEntryId {
        CanonicalEntryId::new(self, seq)
    }

    /// Separates the partition key from the local entry ID.
    pub const fn split(self) -> (PartitionKey, EntryId) {
        (self.partition_key.get(), self.id)
    }

    #[inline]
    pub const fn serialized_length_fixed() -> usize {
        Self::RAW_BYTES_LEN
    }

    /// Returns the partition key component.
    pub const fn partition_key(&self) -> PartitionKey {
        self.partition_key.get()
    }

    /// Returns the entry kind component.
    pub const fn kind(&self) -> EntryKind {
        self.id.kind()
    }

    /// Returns the entry ID without the partition key.
    pub const fn as_entry_id(&self) -> &EntryId {
        &self.id
    }

    /// Returns the local entry ID, discarding the partition key.
    pub const fn to_entry_id(self) -> EntryId {
        self.id
    }

    /// Returns the raw big-endian layout without copying or validating its contents.
    pub fn as_bytes(&self) -> &[u8; Self::RAW_BYTES_LEN] {
        zerocopy::transmute_ref!(self)
    }

    /// Returns a borrowed base entry ID backed by `bytes`.
    ///
    /// Requires exactly [`Self::RAW_BYTES_LEN`] bytes and a valid [`EntryKind`]
    /// discriminant, including the `Unknown` sentinel. The resource remainder is
    /// not validated; successful decoding does not guarantee a usable resource ID.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<&Self, ParseError> {
        if bytes.len() != Self::RAW_BYTES_LEN {
            return Err(ParseError::Length);
        }

        let id = Self::try_ref_from_bytes(bytes).map_err(|_| ParseError::MalformedId)?;

        Ok(id)
    }

    /// Extracts the partition key from a base or canonical resource ID prefix.
    /// This does not validate the remainder or sequence number.
    pub fn extract_partition_key(encoded: &str) -> Result<PartitionKey, IdDecodeError> {
        let mut decoder = IdDecoder::new(encoded)?;
        match decoder.resource_type {
            InvocationId::RESOURCE_TYPE | StateMutationId::RESOURCE_TYPE => {
                decoder.cursor.decode_next::<u64>()
            }
            _ => Err(IdDecodeError::TypeMismatch),
        }
    }

    /// Returns the invocation ID if this entry has invocation kind.
    ///
    /// The remainder must satisfy [`super::InvocationUuid`]'s nonzero requirement.
    /// A zero remainder triggers a debug assertion during conversion.
    pub fn to_invocation_id(self) -> Option<InvocationId> {
        self.id.to_invocation_id(self.partition_key())
    }

    /// Returns the state mutation ID if this entry has state-mutation kind.
    pub fn to_state_mutation_id(self) -> Option<StateMutationId> {
        self.id.to_state_mutation_id(self.partition_key())
    }
}

impl AsRef<EntryId> for BaseEntryId {
    fn as_ref(&self) -> &EntryId {
        &self.id
    }
}

static_assertions::assert_eq_size!(BaseEntryId, [u8; BaseEntryId::RAW_BYTES_LEN]);
static_assertions::assert_eq_align!(BaseEntryId, u8);

impl std::fmt::Debug for BaseEntryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl From<InvocationId> for BaseEntryId {
    #[inline]
    fn from(id: InvocationId) -> Self {
        Self::from(&id)
    }
}

impl From<&InvocationId> for BaseEntryId {
    #[inline]
    fn from(id: &InvocationId) -> Self {
        Self::new(id.partition_key(), EntryId::from(id))
    }
}

impl From<StateMutationId> for BaseEntryId {
    #[inline]
    fn from(id: StateMutationId) -> Self {
        Self::from(&id)
    }
}

impl From<&StateMutationId> for BaseEntryId {
    #[inline]
    fn from(id: &StateMutationId) -> Self {
        Self::new(id.partition_key(), EntryId::from(id))
    }
}

impl FromStr for BaseEntryId {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new(input)?;
        let kind = match decoder.resource_type {
            InvocationId::RESOURCE_TYPE => EntryKind::Invocation,
            StateMutationId::RESOURCE_TYPE => EntryKind::StateMutation,
            _ => return Err(IdDecodeError::TypeMismatch),
        };
        let partition_key = decoder.cursor.decode_next()?;
        let remainder = decoder.cursor.decode_next::<u128>()?.to_be_bytes();
        if decoder.cursor.remaining() != 0 {
            return Err(IdDecodeError::Length);
        }

        Ok(Self::new(partition_key, EntryId::new(kind, remainder)))
    }
}

impl Display for BaseEntryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fn fmt_resource<T: ResourceId>(id: &BaseEntryId, f: &mut Formatter<'_>) -> fmt::Result {
            let mut encoder = IdEncoder::<T>::new();
            encoder.push_u64(id.partition_key());
            encoder.push_u128(u128::from_be_bytes(*id.id.remainder_bytes()));
            f.write_str(encoder.as_str())
        }

        match self.id.kind() {
            EntryKind::Unknown => f.write_str("Unknown"),
            EntryKind::Invocation => fmt_resource::<InvocationId>(self, f),
            EntryKind::StateMutation => fmt_resource::<StateMutationId>(self, f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REMAINDER_LEN: usize = EntryId::REMAINDER_LEN;

    /// `Display` then `FromStr` must round-trip both variants, including edge
    /// remainders. `Debug` delegates to `Display`, so it must match too.
    #[test]
    fn display_from_str_round_trips() {
        let mut r_first = [0u8; REMAINDER_LEN];
        r_first[0] = 0xff;
        let mut r_last = [0u8; REMAINDER_LEN];
        r_last[REMAINDER_LEN - 1] = 0xff;

        let cases = [
            (0, EntryKind::Invocation, r_last),
            (42, EntryKind::Invocation, r_first),
            (u64::MAX, EntryKind::Invocation, [0xffu8; REMAINDER_LEN]),
            (0, EntryKind::StateMutation, r_last),
            (7, EntryKind::StateMutation, [0xabu8; REMAINDER_LEN]),
            (u64::MAX, EntryKind::StateMutation, [0xffu8; REMAINDER_LEN]),
        ];

        for (partition_key, kind, remainder) in cases {
            let id = BaseEntryId::new(partition_key, EntryId::new(kind, remainder));
            let encoded = id.to_string();
            assert_eq!(
                encoded,
                format!("{id:?}"),
                "Debug must delegate to Display for {id:?}"
            );
            let parsed: BaseEntryId = encoded.parse().expect("must parse its own Display output");
            assert_eq!(parsed, id, "round-trip mismatch for {encoded}");
            assert_eq!(id.as_entry_id().display(partition_key).to_string(), encoded);
            let canonical = id.canonicalize(Seq::MAX);
            assert_eq!(
                canonical.to_string().parse::<CanonicalEntryId>().unwrap(),
                canonical
            );
            let expected_prefix = match id.kind() {
                EntryKind::Invocation => "inv_",
                EntryKind::StateMutation => "mut_",
                EntryKind::Unknown => unreachable!(),
            };
            assert!(
                encoded.starts_with(expected_prefix),
                "expected {encoded} to start with {expected_prefix}"
            );
            assert_eq!(
                BaseEntryId::extract_partition_key(&encoded).unwrap(),
                id.partition_key(),
            );
        }
    }

    /// Base IDs preserve the resource ID's string and typed conversions.
    #[test]
    fn display_matches_underlying_resource_id() {
        let inv = InvocationId::mock_random();
        let inv_entry = BaseEntryId::from(inv);
        assert_eq!(inv_entry.to_string(), inv.to_string());
        assert_eq!(inv.to_string().parse::<BaseEntryId>().unwrap(), inv_entry);
        assert_eq!(inv_entry.to_invocation_id(), Some(inv));
        assert!(inv_entry.to_state_mutation_id().is_none());

        let sm = StateMutationId::generate(1234);
        let sm_entry = BaseEntryId::from(&sm);
        assert_eq!(sm_entry.to_string(), sm.to_string());
        assert_eq!(sm.to_string().parse::<BaseEntryId>().unwrap(), sm_entry);
        assert_eq!(sm_entry.to_state_mutation_id(), Some(sm));
        assert!(sm_entry.to_invocation_id().is_none());
    }

    /// Malformed strings, and well-formed ids of an unrelated resource type, must
    /// be rejected rather than silently mis-parsed.
    #[test]
    fn from_str_rejects_invalid_input() {
        for bad in ["", "not-an-id", "inv_", "xyz_1abc"] {
            assert!(
                bad.parse::<BaseEntryId>().is_err(),
                "expected {bad:?} to fail parsing"
            );
        }

        let service_id = InvocationId::mock_random()
            .to_string()
            .replace("inv_", "svc_");
        assert!(service_id.parse::<BaseEntryId>().is_err());
    }
}
