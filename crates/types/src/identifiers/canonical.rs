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
use std::num::ParseIntError;
use std::str::FromStr;

use zerocopy::{
    ByteEq, ByteHash, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned, big_endian,
};

use restate_sharding::PartitionKey;

use super::BaseEntryId;
use crate::errors::IdDecodeError;
use crate::id_util::ID_RESOURCE_SEPARATOR;
use crate::vqueues::{EntryId, EntryKind, ParseError, Seq};

/// Identifies one incarnation of a resource stored as a VQueue entry.
///
/// This combines [`BaseEntryId`] with a caller-assigned [`Seq`]. Equality and
/// hashing include both, so reusing a resource ID with a different sequence
/// produces a distinct canonical identity. The type does not allocate sequences
/// or check that they are unique. Its string form is the base resource ID followed
/// by `_` and the decimal sequence (for example, `inv_..._42`).
///
/// The 33-byte raw layout is `partition key (8B) | sequence (8B) | kind (1B) | remainder (16B)`.
/// Numeric fields are big-endian. Ordering compares these fields in layout order,
/// matching lexicographic raw-byte order, not queue scheduling order. Placing the
/// sequence before the resource groups creation sequences within a partition key
/// for incarnation-owned storage. [`Self::to_base_entry_id`] reconstructs the base
/// identity by value without allocating.
///
/// See the [identifier module](crate::identifiers) for the relationship between
/// local, base, and canonical entry IDs and their validation boundaries.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Copy,
    ByteEq,
    ByteHash,
    TryFromBytes,
    IntoBytes,
    KnownLayout,
    Immutable,
    Unaligned,
)]
#[repr(C)]
pub struct CanonicalEntryId {
    partition_key: big_endian::U64,
    seq: Seq,
    id: EntryId,
}

impl CanonicalEntryId {
    /// The size of a canonical entry ID's byte encoding.
    pub const RAW_BYTES_LEN: usize = BaseEntryId::RAW_BYTES_LEN + size_of::<Seq>();

    /// Combines a resource identity and a caller-assigned incarnation sequence.
    /// Equivalent to [`BaseEntryId::canonicalize`].
    pub const fn new(base: BaseEntryId, seq: Seq) -> Self {
        let (partition_key, id) = base.split();
        Self {
            partition_key: big_endian::U64::new(partition_key),
            seq,
            id,
        }
    }

    #[inline]
    pub const fn serialized_length_fixed() -> usize {
        Self::RAW_BYTES_LEN
    }

    /// Returns the partition key component.
    pub const fn partition_key(&self) -> PartitionKey {
        self.partition_key.get()
    }

    /// Returns the sequence number component.
    pub const fn seq(&self) -> Seq {
        self.seq
    }

    /// Returns an ID for the same base resource with a different sequence.
    /// Changing the sequence changes the canonical identity.
    #[must_use]
    pub const fn with_seq(mut self, seq: Seq) -> Self {
        self.seq = seq;
        self
    }

    /// Replaces the sequence in place, changing the canonical identity.
    pub const fn set_seq(&mut self, seq: Seq) {
        self.seq = seq;
    }

    /// Returns the entry kind component.
    pub const fn kind(&self) -> EntryKind {
        self.id.kind()
    }

    /// Returns the entry ID without the partition key and sequence number.
    /// The returned view alone cannot distinguish partitions or incarnations.
    pub const fn as_entry_id(&self) -> &EntryId {
        &self.id
    }

    /// Returns the resource identity without the sequence number.
    /// Use this explicitly for operations that do not distinguish incarnations.
    /// Reconstructs the base ID by value without allocating or validating it.
    pub const fn to_base_entry_id(self) -> BaseEntryId {
        BaseEntryId::new(self.partition_key(), self.id)
    }

    /// Returns the raw big-endian layout without copying or validating its contents.
    pub fn as_bytes(&self) -> &[u8; Self::RAW_BYTES_LEN] {
        zerocopy::transmute_ref!(self)
    }

    /// Returns a borrowed canonical entry ID backed by `bytes`.
    ///
    /// Requires exactly [`Self::RAW_BYTES_LEN`] bytes and a valid [`EntryKind`]
    /// discriminant, including `Unknown`. All `u64` sequences are representable;
    /// this does not validate resource invariants or incarnation uniqueness.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<&Self, ParseError> {
        if bytes.len() != Self::RAW_BYTES_LEN {
            return Err(ParseError::Length);
        }

        Self::try_ref_from_bytes(bytes).map_err(|_| ParseError::MalformedId)
    }

    /// Extracts the partition key from the resource ID prefix without validating
    /// the remainder or sequence number.
    pub fn extract_partition_key(encoded: &str) -> Result<PartitionKey, IdDecodeError> {
        BaseEntryId::extract_partition_key(encoded)
    }
}

impl std::fmt::Debug for CanonicalEntryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for CanonicalEntryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.kind() == EntryKind::Unknown {
            return f.write_str("Unknown");
        }
        write!(
            f,
            "{}{ID_RESOURCE_SEPARATOR}{}",
            self.to_base_entry_id(),
            self.seq
        )
    }
}

impl AsRef<[u8]> for CanonicalEntryId {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl FromStr for CanonicalEntryId {
    type Err = CanonicalIdParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (base, seq) = input
            .rsplit_once(ID_RESOURCE_SEPARATOR)
            .ok_or(CanonicalIdParseError::MissingSequence)?;
        let seq = Seq::new(seq.parse()?);
        Ok(Self::new(base.parse()?, seq))
    }
}

static_assertions::assert_eq_size!(CanonicalEntryId, [u8; CanonicalEntryId::RAW_BYTES_LEN]);
static_assertions::assert_eq_align!(CanonicalEntryId, u8);

/// Error returned when parsing a canonical identifier from its string representation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CanonicalIdParseError {
    #[error("missing sequence number suffix")]
    MissingSequence,
    #[error("invalid resource ID: {0}")]
    ResourceId(#[from] IdDecodeError),
    #[error("invalid sequence number: {0}")]
    InvalidSequence(#[from] ParseIntError),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::identifiers::{InvocationId, InvocationUuid, StateMutationId};

    use super::*;

    // Projecting away the sequence must require an explicit accessor.
    static_assertions::assert_not_impl_any!(CanonicalEntryId: AsRef<BaseEntryId>, AsRef<EntryId>);

    #[test]
    fn canonical_identity_includes_sequence_number() {
        for base in [
            BaseEntryId::from(InvocationId::mock_random()),
            BaseEntryId::from(StateMutationId::generate(123)),
        ] {
            let id = base.canonicalize(Seq::MIN);
            let updated = id.with_seq(Seq::MAX);
            assert_ne!(id, updated);
            assert_eq!(updated.to_base_entry_id(), base);
            assert_eq!(id.seq(), Seq::MIN);
            assert_eq!(updated.seq(), Seq::MAX);
            assert_eq!(HashMap::from([(id, 1), (updated, 2)]).len(), 2);
        }
    }

    #[test]
    fn entry_id_bytes_are_big_endian_and_decode_without_copying() {
        let invocation_id = InvocationId::from_parts(
            0x0102_0304_0506_0708,
            InvocationUuid::from_bytes([0x11; InvocationUuid::RAW_BYTES_LEN]),
        );
        let entry_id =
            BaseEntryId::from(invocation_id).canonicalize(Seq::new(0x0001_0203_0405_0607));

        let mut expected = [0x11; CanonicalEntryId::RAW_BYTES_LEN];
        expected[..8].copy_from_slice(&0x0102_0304_0506_0708_u64.to_be_bytes());
        expected[8..16].copy_from_slice(&0x0001_0203_0405_0607_u64.to_be_bytes());
        expected[16] = EntryKind::Invocation as u8;

        assert_eq!(AsRef::<[u8]>::as_ref(&entry_id), expected);
        let bytes = entry_id.as_bytes();
        assert_eq!(bytes, &expected);

        let decoded = CanonicalEntryId::try_from_bytes(bytes).unwrap();
        assert_eq!(decoded.as_bytes().as_ptr(), bytes.as_ptr());

        assert_eq!(decoded, &entry_id);
        assert_eq!(
            decoded.as_entry_id().as_bytes().as_ptr(),
            bytes[16..].as_ptr()
        );
        assert_eq!(decoded.as_entry_id().as_bytes(), &bytes[16..]);
        let base = decoded.to_base_entry_id();
        assert_eq!(base, BaseEntryId::from(invocation_id));
        assert_eq!(&base.as_bytes()[..8], &bytes[..8]);
        assert_eq!(&base.as_bytes()[8..], &bytes[16..]);
        assert_eq!(base.canonicalize(decoded.seq()), *decoded);
        assert_eq!(base.to_entry_id(), EntryId::from(invocation_id));

        assert!(matches!(
            CanonicalEntryId::try_from_bytes(&bytes[..bytes.len() - 1]),
            Err(ParseError::Length)
        ));
        assert!(matches!(
            CanonicalEntryId::try_from_bytes(&[0; CanonicalEntryId::RAW_BYTES_LEN + 1]),
            Err(ParseError::Length)
        ));

        expected[16] = 0xff;
        assert!(matches!(
            CanonicalEntryId::try_from_bytes(&expected),
            Err(ParseError::MalformedId)
        ));
        expected[16] = EntryKind::Unknown as u8;
        let unknown = CanonicalEntryId::try_from_bytes(&expected).unwrap();
        assert_eq!(unknown.to_string(), "Unknown");
        assert_eq!(unknown.to_base_entry_id().to_string(), "Unknown");

        // Structural decoding and base projection must not invoke EntryId::new:
        // both the Unknown sentinel and known kinds can have zero remainders.
        expected[17..].fill(0);
        for kind in [
            EntryKind::Unknown,
            EntryKind::Invocation,
            EntryKind::StateMutation,
        ] {
            expected[16] = kind as u8;
            let decoded = CanonicalEntryId::try_from_bytes(&expected).unwrap();
            let base = decoded.to_base_entry_id();
            assert_eq!(base.kind(), kind);
            assert_eq!(base.as_entry_id().remainder_bytes(), &[0; 16]);
            assert_eq!(base.canonicalize(decoded.seq()), *decoded);
        }
    }

    #[test]
    fn canonical_order_is_partition_then_sequence_then_resource() {
        let mut ids = Vec::new();
        // Nested iteration gives the expected logical order independently of Ord.
        for partition_key in [0, 1, 256, u64::MAX] {
            for seq in [Seq::MIN, Seq::new(1), Seq::new(256), Seq::MAX] {
                for kind in [EntryKind::Invocation, EntryKind::StateMutation] {
                    for remainder in [[1; 16], [2; 16]] {
                        let base = BaseEntryId::new(partition_key, EntryId::new(kind, remainder));
                        let id = base.canonicalize(seq);
                        assert_eq!(
                            CanonicalEntryId::try_from_bytes(id.as_bytes()).unwrap(),
                            &id
                        );
                        ids.push(id);
                    }
                }
            }
        }
        for pair in ids.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].as_bytes() < pair[1].as_bytes());
        }
        assert_eq!(
            ids.iter()
                .copied()
                .map(|id| (id, ()))
                .collect::<HashMap<_, _>>()
                .len(),
            ids.len()
        );
    }

    #[test]
    fn canonical_id_strings_round_trip() {
        let invocation_id = InvocationId::from_parts(
            0x0102_0304_0506_0708,
            InvocationUuid::from_bytes([0x11; InvocationUuid::RAW_BYTES_LEN]),
        );
        let state_mutation_id =
            StateMutationId::from_partition_key_and_bytes(0x0807_0605_0403_0201, [0x22; 16]);
        for (base, resource_string) in [
            (BaseEntryId::from(invocation_id), invocation_id.to_string()),
            (
                BaseEntryId::from(&state_mutation_id),
                state_mutation_id.to_string(),
            ),
        ] {
            for seq in [Seq::MIN, Seq::new(42), Seq::MAX] {
                let id = base.canonicalize(seq);
                let encoded = format!("{resource_string}_{seq}");
                assert_eq!(id.to_string(), encoded);
                assert_eq!(encoded.parse::<CanonicalEntryId>().unwrap(), id);
            }
        }
    }

    #[test]
    fn canonical_id_strings_reject_invalid_input() {
        let invocation_id = InvocationId::mock_random();

        assert!(matches!(
            "missing-sequence".parse::<CanonicalEntryId>(),
            Err(CanonicalIdParseError::MissingSequence)
        ));
        for seq in ["invalid", "-1", "18446744073709551616"] {
            assert!(matches!(
                format!("{invocation_id}_{seq}").parse::<CanonicalEntryId>(),
                Err(CanonicalIdParseError::InvalidSequence(_))
            ));
        }
        assert!(matches!(
            "invalid_1".parse::<CanonicalEntryId>(),
            Err(CanonicalIdParseError::ResourceId(_))
        ));
        assert!(matches!(
            format!("{invocation_id}_trailing_1").parse::<CanonicalEntryId>(),
            Err(CanonicalIdParseError::ResourceId(IdDecodeError::Length))
        ));
    }
}
