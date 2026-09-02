// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::fmt::{self, Display, Formatter};
use std::mem::size_of;
use std::num::ParseIntError;
use std::str::FromStr;

use zerocopy::byteorder::big_endian::U64;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use restate_sharding::{PartitionKey, WithPartitionKey};

use super::{InvocationId, InvocationUuid, StateMutationId};
use crate::errors::IdDecodeError;
use crate::vqueues::{EntryKind, ParseError, Seq};

const REMAINDER_LEN: usize = 16;

/// Canonical unique identifier for resources that can be stored as entries in VQueues.
///
/// The fields are encoded in order as a big-endian partition key, big-endian sequence number,
/// entry kind, and resource identifier remainder.
///
/// The byte encoding is byte-wise (lexicographic) order of the fields in that order:
/// - partition key
/// - sequencer number
/// - entry kind
/// - remainder
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(C)]
pub struct CanonicalEntryId {
    partition_key: U64,
    seq: U64,
    kind: EntryKind,
    remainder: [u8; REMAINDER_LEN],
}

impl CanonicalEntryId {
    /// The size of a canonical entry ID's byte encoding.
    pub const RAW_BYTES_LEN: usize = size_of::<U64>() * 2 + size_of::<EntryKind>() + REMAINDER_LEN;

    pub(crate) fn new(
        partition_key: PartitionKey,
        seq: Seq,
        kind: EntryKind,
        remainder: [u8; REMAINDER_LEN],
    ) -> Self {
        Self {
            partition_key: U64::new(partition_key),
            seq: U64::new(seq.as_u64()),
            kind,
            remainder,
        }
    }

    /// Returns the partition key component.
    pub const fn partition_key(&self) -> PartitionKey {
        self.partition_key.get()
    }

    /// Returns the sequence number component.
    pub const fn seq(&self) -> Seq {
        Seq::new(self.seq.get())
    }

    /// Returns the same canonical entry ID with a different sequence number.
    #[must_use]
    pub const fn with_seq(mut self, seq: Seq) -> Self {
        self.seq = U64::new(seq.as_u64());
        self
    }

    /// Returns the entry kind component.
    pub const fn kind(&self) -> EntryKind {
        self.kind
    }

    pub(crate) const fn remainder(&self) -> &[u8; REMAINDER_LEN] {
        &self.remainder
    }

    /// Returns the canonical big-endian byte encoding without copying.
    pub fn as_bytes(&self) -> &[u8; Self::RAW_BYTES_LEN] {
        zerocopy::transmute_ref!(self)
    }

    /// Returns a borrowed canonical entry ID backed by `bytes`.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<&Self, ParseError> {
        if bytes.len() != Self::RAW_BYTES_LEN {
            return Err(ParseError::Length);
        }

        let id = Self::try_ref_from_bytes(bytes)
            .map_err(|_| ParseError::UnknownEntryKind(bytes[size_of::<U64>() * 2]))?;

        if id.seq.get() > Seq::MAX.as_u64() {
            return Err(ParseError::SequenceOutOfRange(id.seq.get()));
        }

        Ok(id)
    }
}

impl AsRef<[u8]> for CanonicalEntryId {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
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
    #[error("sequence number exceeds 56 bits: {0}")]
    SequenceOutOfRange(u64),
}

fn split_resource_id_and_seq(input: &str) -> Result<(&str, Seq), CanonicalIdParseError> {
    let (resource_id, seq) = input
        .rsplit_once('_')
        .ok_or(CanonicalIdParseError::MissingSequence)?;
    let seq = seq.parse::<u64>()?;
    if seq > Seq::MAX.as_u64() {
        return Err(CanonicalIdParseError::SequenceOutOfRange(seq));
    }

    Ok((resource_id, Seq::new(seq)))
}

/// A unique invocation ID that distinguishes different incarnations of an invocation.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct CanonicalInvocationId(CanonicalEntryId);

impl CanonicalInvocationId {
    /// Creates a canonical ID for an invocation at the given sequence number.
    pub fn new(invocation_id: InvocationId, seq: Seq) -> Self {
        Self(CanonicalEntryId::new(
            invocation_id.partition_key(),
            seq,
            EntryKind::Invocation,
            invocation_id.invocation_uuid().to_bytes(),
        ))
    }

    /// Returns the same canonical invocation ID with a different sequence number.
    #[must_use]
    pub const fn with_seq(self, seq: Seq) -> Self {
        Self(self.0.with_seq(seq))
    }
}

impl Display for CanonicalInvocationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let entry_id: &CanonicalEntryId = self.borrow();
        let invocation_id = InvocationId::from_parts(
            entry_id.partition_key(),
            InvocationUuid::from_bytes(*entry_id.remainder()),
        );
        write!(f, "{invocation_id}_{}", entry_id.seq())
    }
}

impl FromStr for CanonicalInvocationId {
    type Err = CanonicalIdParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (invocation_id, seq) = split_resource_id_and_seq(input)?;
        Ok(Self::new(invocation_id.parse()?, seq))
    }
}

impl From<CanonicalInvocationId> for CanonicalEntryId {
    fn from(value: CanonicalInvocationId) -> Self {
        value.0
    }
}

impl PartialEq<CanonicalEntryId> for CanonicalInvocationId {
    fn eq(&self, other: &CanonicalEntryId) -> bool {
        self.0 == *other
    }
}

impl PartialEq<CanonicalInvocationId> for CanonicalEntryId {
    fn eq(&self, other: &CanonicalInvocationId) -> bool {
        *self == other.0
    }
}

impl Borrow<CanonicalEntryId> for CanonicalInvocationId {
    fn borrow(&self) -> &CanonicalEntryId {
        &self.0
    }
}

impl AsRef<CanonicalEntryId> for CanonicalInvocationId {
    fn as_ref(&self) -> &CanonicalEntryId {
        self.borrow()
    }
}

/// A unique state mutation ID that distinguishes different attempts of a state mutation.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct CanonicalStateMutationId(CanonicalEntryId);

impl CanonicalStateMutationId {
    /// Creates a canonical ID for a state mutation at the given sequence number.
    pub fn new(state_mutation_id: StateMutationId, seq: Seq) -> Self {
        Self(CanonicalEntryId::new(
            state_mutation_id.partition_key(),
            seq,
            EntryKind::StateMutation,
            state_mutation_id.to_remainder_bytes(),
        ))
    }

    /// Returns the same canonical state mutation ID with a different sequence number.
    #[must_use]
    pub const fn with_seq(self, seq: Seq) -> Self {
        Self(self.0.with_seq(seq))
    }
}

impl Display for CanonicalStateMutationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let entry_id: &CanonicalEntryId = self.borrow();
        let state_mutation_id = StateMutationId::from_partition_key_and_bytes(
            entry_id.partition_key(),
            *entry_id.remainder(),
        );
        write!(f, "{state_mutation_id}_{}", entry_id.seq())
    }
}

impl FromStr for CanonicalStateMutationId {
    type Err = CanonicalIdParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (state_mutation_id, seq) = split_resource_id_and_seq(input)?;
        Ok(Self::new(state_mutation_id.parse()?, seq))
    }
}

impl From<CanonicalStateMutationId> for CanonicalEntryId {
    fn from(value: CanonicalStateMutationId) -> Self {
        value.0
    }
}

impl PartialEq<CanonicalEntryId> for CanonicalStateMutationId {
    fn eq(&self, other: &CanonicalEntryId) -> bool {
        self.0 == *other
    }
}

impl PartialEq<CanonicalStateMutationId> for CanonicalEntryId {
    fn eq(&self, other: &CanonicalStateMutationId) -> bool {
        *self == other.0
    }
}

impl Borrow<CanonicalEntryId> for CanonicalStateMutationId {
    fn borrow(&self) -> &CanonicalEntryId {
        &self.0
    }
}

impl AsRef<CanonicalEntryId> for CanonicalStateMutationId {
    fn as_ref(&self) -> &CanonicalEntryId {
        self.borrow()
    }
}

static_assertions::assert_eq_size!(CanonicalInvocationId, CanonicalEntryId);
static_assertions::assert_eq_align!(CanonicalInvocationId, CanonicalEntryId);
static_assertions::assert_eq_size!(CanonicalStateMutationId, CanonicalEntryId);
static_assertions::assert_eq_align!(CanonicalStateMutationId, CanonicalEntryId);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn typed_ids_borrow_as_entry_ids_without_copying() {
        let invocation_id = CanonicalInvocationId::new(InvocationId::mock_random(), Seq::new(42));
        let entry_id: &CanonicalEntryId = invocation_id.as_ref();

        assert!(std::ptr::addr_eq(&invocation_id, entry_id));
        assert!(invocation_id == *entry_id);
        assert!(*entry_id == invocation_id);
        assert!(invocation_id.with_seq(Seq::MAX) != *entry_id);
        assert_eq!(entry_id.seq(), Seq::new(42));
        assert_eq!(entry_id.kind(), EntryKind::Invocation);
        assert_eq!(HashMap::from([(invocation_id, 1)]).get(entry_id), Some(&1));

        let state_mutation_id = CanonicalStateMutationId::new(
            StateMutationId::from_partition_key_and_bytes(123, [0x22; 16]),
            Seq::new(43),
        );
        let entry_id: &CanonicalEntryId = state_mutation_id.as_ref();

        assert!(std::ptr::addr_eq(&state_mutation_id, entry_id));
        assert!(state_mutation_id == *entry_id);
        assert!(*entry_id == state_mutation_id);
        assert!(state_mutation_id.with_seq(Seq::MAX) != *entry_id);
        assert_eq!(entry_id.partition_key(), 123);
        assert_eq!(entry_id.seq(), Seq::new(43));
        assert_eq!(entry_id.kind(), EntryKind::StateMutation);
        assert_eq!(
            HashMap::from([(state_mutation_id, 2)]).get(entry_id),
            Some(&2)
        );
    }

    #[test]
    fn canonical_ids_can_replace_their_sequence_number() {
        let invocation_id = InvocationId::mock_random();
        let canonical_invocation_id = CanonicalInvocationId::new(invocation_id, Seq::new(42));
        let updated_invocation_id = canonical_invocation_id.with_seq(Seq::MAX);

        assert_eq!(canonical_invocation_id.as_ref().seq(), Seq::new(42));
        assert!(updated_invocation_id == CanonicalInvocationId::new(invocation_id, Seq::MAX));

        let state_mutation_id = StateMutationId::generate(123);
        let canonical_state_mutation_id =
            CanonicalStateMutationId::new(state_mutation_id, Seq::MAX);
        let updated_state_mutation_id = canonical_state_mutation_id.with_seq(Seq::MIN);

        assert_eq!(canonical_state_mutation_id.as_ref().seq(), Seq::MAX);
        assert_eq!(updated_state_mutation_id.as_ref().seq(), Seq::MIN);
        assert_eq!(
            updated_state_mutation_id.as_ref().partition_key(),
            canonical_state_mutation_id.as_ref().partition_key()
        );
        assert_eq!(
            updated_state_mutation_id.as_ref().kind(),
            canonical_state_mutation_id.as_ref().kind()
        );
        assert_eq!(
            updated_state_mutation_id.as_ref().remainder(),
            canonical_state_mutation_id.as_ref().remainder()
        );
    }

    #[test]
    fn entry_id_bytes_are_big_endian_and_decode_without_copying() {
        let invocation_id = InvocationId::from_parts(
            0x0102_0304_0506_0708,
            InvocationUuid::from_bytes([0x11; InvocationUuid::RAW_BYTES_LEN]),
        );
        let canonical = CanonicalInvocationId::new(invocation_id, Seq::new(0x0001_0203_0405_0607));

        let mut expected = [0x11; CanonicalEntryId::RAW_BYTES_LEN];
        expected[..8].copy_from_slice(&0x0102_0304_0506_0708_u64.to_be_bytes());
        expected[8..16].copy_from_slice(&0x0001_0203_0405_0607_u64.to_be_bytes());
        expected[16] = EntryKind::Invocation as u8;

        let entry_id: &CanonicalEntryId = canonical.as_ref();
        assert_eq!(AsRef::<[u8]>::as_ref(entry_id), expected);

        let bytes = entry_id.as_bytes();
        assert_eq!(bytes, &expected);

        let decoded = CanonicalEntryId::try_from_bytes(bytes).unwrap();
        assert_eq!(decoded.as_bytes().as_ptr(), bytes.as_ptr());

        assert!(matches!(
            CanonicalEntryId::try_from_bytes(&bytes[..bytes.len() - 1]),
            Err(ParseError::Length)
        ));

        expected[16] = 0xff;
        assert!(matches!(
            CanonicalEntryId::try_from_bytes(&expected),
            Err(ParseError::UnknownEntryKind(0xff))
        ));

        expected[8] = 1;
        expected[16] = EntryKind::Invocation as u8;
        assert!(matches!(
            CanonicalEntryId::try_from_bytes(&expected),
            Err(ParseError::SequenceOutOfRange(seq)) if seq == (1 << 56) | 0x0001_0203_0405_0607
        ));
    }

    #[test]
    fn canonical_id_strings_round_trip() {
        let invocation_id = InvocationId::from_parts(
            0x0102_0304_0506_0708,
            InvocationUuid::from_bytes([0x11; InvocationUuid::RAW_BYTES_LEN]),
        );
        let invocation_seq = Seq::new(42);
        let canonical_invocation_id = CanonicalInvocationId::new(invocation_id, invocation_seq);
        let invocation_string = format!("{invocation_id}_{invocation_seq}");

        assert_eq!(canonical_invocation_id.to_string(), invocation_string);
        assert!(
            invocation_string
                .parse::<CanonicalInvocationId>()
                .is_ok_and(|parsed| parsed == canonical_invocation_id)
        );

        let state_mutation_id =
            StateMutationId::from_partition_key_and_bytes(0x0807_0605_0403_0201, [0x22; 16]);
        let state_mutation_seq = Seq::MAX;
        let state_mutation_string = format!("{state_mutation_id}_{state_mutation_seq}");
        let canonical_state_mutation_id =
            CanonicalStateMutationId::new(state_mutation_id, state_mutation_seq);

        assert_eq!(
            canonical_state_mutation_id.to_string(),
            state_mutation_string
        );
        assert!(
            state_mutation_string
                .parse::<CanonicalStateMutationId>()
                .is_ok_and(|parsed| parsed == canonical_state_mutation_id)
        );
    }

    #[test]
    fn canonical_id_strings_reject_invalid_input() {
        let invocation_id = InvocationId::mock_random();

        assert!(matches!(
            "missing-sequence".parse::<CanonicalInvocationId>(),
            Err(CanonicalIdParseError::MissingSequence)
        ));
        assert!(matches!(
            format!("{invocation_id}_invalid").parse::<CanonicalInvocationId>(),
            Err(CanonicalIdParseError::InvalidSequence(_))
        ));
        assert!(matches!(
            format!("{invocation_id}_{}", Seq::MAX.as_u64() + 1)
                .parse::<CanonicalInvocationId>(),
            Err(CanonicalIdParseError::SequenceOutOfRange(seq))
                if seq == Seq::MAX.as_u64() + 1
        ));

        let state_mutation_id = StateMutationId::generate(123);
        assert!(matches!(
            format!("{state_mutation_id}_1").parse::<CanonicalInvocationId>(),
            Err(CanonicalIdParseError::ResourceId(
                IdDecodeError::TypeMismatch
            ))
        ));
    }
}
