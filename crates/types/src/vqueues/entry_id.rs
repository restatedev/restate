// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::{InvocationId, InvocationUuid, PartitionKey, StateMutationId};

use super::ParseError;

/// The length of the remainder bytes of an entry id.
const REMAINDER_LEN: usize = 16;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::FromRepr, bilrost::Enumeration,
)]
#[repr(u8)]
pub enum EntryKind {
    /// Must not be used as input when encoding but it can be observed when decoding
    /// if the raw bytes did not form a known entry kind.
    #[bilrost(0)]
    Unknown = 0x0,
    #[bilrost(1)]
    Invocation = b'i', // 0x69
    #[bilrost(2)]
    StateMutation = b's', // 0x73
}

impl EntryKind {
    pub const fn serialized_length_fixed() -> usize {
        std::mem::size_of::<Self>()
    }
}

#[derive(
    derive_more::Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash, bilrost::Message,
)]
pub struct EntryId {
    #[bilrost(tag(1))]
    kind: EntryKind,
    // The remainder of the original resource identifier but without the partition-key prefix.
    // to reconstruct the original resource, you'll need to supply the partition_key.
    #[bilrost(tag(2), encoding(plainbytes))]
    #[debug(skip)]
    remainder: [u8; REMAINDER_LEN],
}

impl EntryId {
    pub const REMAINDER_LEN: usize = REMAINDER_LEN;
    #[inline]
    pub const fn serialized_length_fixed() -> usize {
        // 1 byte for kind + 16 bytes for remainder (REMAINDER_LEN)
        EntryKind::serialized_length_fixed() + Self::REMAINDER_LEN
    }

    pub fn new(kind: EntryKind, remainder: [u8; Self::REMAINDER_LEN]) -> Self {
        assert_ne!(kind, EntryKind::Unknown, "cannot build unknown entry id");
        Self { kind, remainder }
    }

    #[inline]
    pub const fn kind(&self) -> EntryKind {
        self.kind
    }

    pub fn to_bytes(&self) -> [u8; Self::serialized_length_fixed()] {
        assert_ne!(
            self.kind,
            EntryKind::Unknown,
            "cannot encode unknown entry id"
        );
        let mut buf = [0u8; Self::serialized_length_fixed()];
        buf[0] = self.kind as u8;
        buf[1..].copy_from_slice(&self.remainder);
        buf
    }

    pub fn try_from_bytes(
        bytes: &[u8; Self::serialized_length_fixed()],
    ) -> Result<Self, ParseError> {
        let kind = EntryKind::from_repr(bytes[0]);
        let Some(kind) = kind else {
            return Err(ParseError::UnknownEntryKind(bytes[0]));
        };

        Ok(Self {
            kind,
            remainder: bytes[1..].try_into().unwrap(),
        })
    }

    #[inline]
    pub fn remainder_bytes(&self) -> &[u8; Self::REMAINDER_LEN] {
        &self.remainder
    }

    #[inline]
    pub fn to_remainder_bytes(&self) -> [u8; Self::REMAINDER_LEN] {
        self.remainder
    }

    #[inline]
    pub fn display(&self, partition_key: PartitionKey) -> EntryIdDisplay<'_> {
        EntryIdDisplay {
            partition_key,
            id: self,
        }
    }

    /// Returns the [`InvocationId`] if this is a [`EntryKind::Invocation`].
    #[inline]
    pub fn to_invocation_id(&self, partition_key: PartitionKey) -> Option<InvocationId> {
        match self.kind() {
            EntryKind::Invocation => Some(InvocationId::from_parts(
                partition_key,
                InvocationUuid::from_bytes(self.remainder),
            )),
            _ => None,
        }
    }

    /// Returns the [`StateMutationId`] if this is a [`EntryKind::StateMutation`].
    #[inline]
    pub fn to_state_mutation_id(&self, partition_key: PartitionKey) -> Option<StateMutationId> {
        match self.kind() {
            EntryKind::StateMutation => Some(StateMutationId::from_partition_key_and_bytes(
                partition_key,
                self.remainder,
            )),
            _ => None,
        }
    }
}

impl From<&InvocationId> for EntryId {
    #[inline]
    fn from(id: &InvocationId) -> Self {
        Self {
            kind: EntryKind::Invocation,
            remainder: id.invocation_uuid().to_bytes(),
        }
    }
}

impl From<InvocationId> for EntryId {
    #[inline]
    fn from(id: InvocationId) -> Self {
        Self {
            kind: EntryKind::Invocation,
            remainder: id.invocation_uuid().to_bytes(),
        }
    }
}

impl From<StateMutationId> for EntryId {
    fn from(mutation_id: StateMutationId) -> Self {
        Self {
            kind: EntryKind::StateMutation,
            remainder: mutation_id.to_remainder_bytes(),
        }
    }
}

impl From<&StateMutationId> for EntryId {
    fn from(mutation_id: &StateMutationId) -> Self {
        Self {
            kind: EntryKind::StateMutation,
            remainder: mutation_id.to_remainder_bytes(),
        }
    }
}

pub struct EntryIdDisplay<'a> {
    pub(crate) partition_key: PartitionKey,
    pub(crate) id: &'a EntryId,
}

impl std::fmt::Display for EntryIdDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.id.kind {
            EntryKind::Unknown => f.write_str("Unknown"),
            EntryKind::Invocation => std::fmt::Display::fmt(
                &InvocationId::from_parts(
                    self.partition_key,
                    InvocationUuid::from_bytes(self.id.remainder),
                ),
                f,
            ),
            EntryKind::StateMutation => std::fmt::Display::fmt(
                &StateMutationId::from_partition_key_and_bytes(
                    self.partition_key,
                    self.id.remainder,
                ),
                f,
            ),
        }
    }
}
