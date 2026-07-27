// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use crate::errors::IdDecodeError;
use crate::id_util::{IdDecoder, IdEncoder};
use crate::identifiers::{InvocationId, InvocationUuid, PartitionKey, ResourceId, StateMutationId};

use super::ParseError;

/// The length of the remainder bytes of an entry id.
const REMAINDER_LEN: usize = 16;

/// Identifiers that can be used as entry ids in vqueues.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum VQueueEntryId {
    Invocation(PartitionKey, [u8; REMAINDER_LEN]),
    StateMutation(PartitionKey, [u8; REMAINDER_LEN]),
}

impl VQueueEntryId {
    pub fn partition_key(&self) -> PartitionKey {
        match self {
            Self::Invocation(partition_key, _) => *partition_key,
            Self::StateMutation(partition_key, _) => *partition_key,
        }
    }

    pub fn kind(&self) -> EntryKind {
        match self {
            Self::Invocation(_, _) => EntryKind::Invocation,
            Self::StateMutation(_, _) => EntryKind::StateMutation,
        }
    }

    pub fn remainder(&self) -> &[u8; REMAINDER_LEN] {
        match self {
            Self::Invocation(_, remainder) => remainder,
            Self::StateMutation(_, remainder) => remainder,
        }
    }

    pub fn into_remainder(self) -> [u8; REMAINDER_LEN] {
        match self {
            Self::Invocation(_, remainder) => remainder,
            Self::StateMutation(_, remainder) => remainder,
        }
    }

    /// Extracts the partition key if the entry ids are of a valid type
    pub fn extract_partition_key(encoded: &str) -> Result<PartitionKey, IdDecodeError> {
        let mut decoder = IdDecoder::new(encoded)?;
        match decoder.resource_type {
            InvocationId::RESOURCE_TYPE | StateMutationId::RESOURCE_TYPE => {
                decoder.cursor.decode_next::<u64>()
            }
            _ => Err(IdDecodeError::TypeMismatch),
        }
    }
}

// It's critical that this matches the memory ordering of the EntryStatusKey
// That is, the partition key is matched first, then the entry kind (as u8)
// then the remainder (bytewise)
impl PartialOrd for VQueueEntryId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VQueueEntryId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partition_key()
            .cmp(&other.partition_key())
            .then_with(|| self.kind().cmp(&other.kind()))
            .then_with(|| self.remainder().cmp(other.remainder()))
    }
}

impl FromStr for VQueueEntryId {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new(input)?;
        // Ensure we are decoding the right type
        match decoder.resource_type {
            InvocationId::RESOURCE_TYPE => {
                let partition_key: PartitionKey = decoder.cursor.decode_next()?;
                let raw: [u8; REMAINDER_LEN] = decoder.cursor.decode_next::<u128>()?.to_be_bytes();

                if decoder.cursor.remaining() != 0 {
                    return Err(IdDecodeError::Length);
                }

                Ok(Self::Invocation(partition_key, raw))
            }
            StateMutationId::RESOURCE_TYPE => {
                let partition_key: PartitionKey = decoder.cursor.decode_next()?;
                let raw: [u8; REMAINDER_LEN] = decoder.cursor.decode_next::<u128>()?.to_be_bytes();

                if decoder.cursor.remaining() != 0 {
                    return Err(IdDecodeError::Length);
                }

                Ok(Self::StateMutation(partition_key, raw))
            }
            _ => Err(IdDecodeError::TypeMismatch),
        }
    }
}

impl std::fmt::Display for VQueueEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invocation(partition_key, remainder) => {
                let mut encoder = IdEncoder::<InvocationId>::new();
                encoder.push_u64(*partition_key);
                encoder.push_u128(u128::from_be_bytes(*remainder));
                f.write_str(encoder.as_str())
            }
            Self::StateMutation(partition_key, remainder) => {
                let mut encoder = IdEncoder::<StateMutationId>::new();
                encoder.push_u64(*partition_key);
                encoder.push_u128(u128::from_be_bytes(*remainder));
                f.write_str(encoder.as_str())
            }
        }
    }
}

impl std::fmt::Debug for VQueueEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<VQueueEntryId> for EntryId {
    fn from(value: VQueueEntryId) -> Self {
        EntryId::new(value.kind(), value.into_remainder())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::FromRepr,
    bilrost::Enumeration,
    strum::Display,
)]
#[repr(u8)]
#[strum(serialize_all = "kebab-case")]
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

mod bilrost_encoding {
    use bilrost::encoding::{DistinguishedProxiable, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind, Enumeration};

    use super::EntryKind;

    impl Proxiable for EntryKind {
        type Proxy = u32;

        fn encode_proxy(&self) -> Self::Proxy {
            <EntryKind as Enumeration>::to_number(self)
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self =
                <EntryKind as Enumeration>::try_from_number(proxy).unwrap_or(EntryKind::Unknown);
            Ok(())
        }
    }

    impl DistinguishedProxiable for EntryKind {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonicity::Canonical)
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Fixed)
        to encode proxied type (EntryKind)
        with encoding (bilrost::encoding::Fixed)
        including distinguished
    );
}

#[derive(
    derive_more::Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash, bilrost::Message,
)]
pub struct EntryId {
    #[bilrost(tag(1), encoding(fixed))]
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

    pub fn to_bytes(self) -> [u8; Self::serialized_length_fixed()] {
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
    pub fn to_remainder_bytes(self) -> [u8; Self::REMAINDER_LEN] {
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
    pub fn to_invocation_id(self, partition_key: PartitionKey) -> Option<InvocationId> {
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
    pub fn to_state_mutation_id(self, partition_key: PartitionKey) -> Option<StateMutationId> {
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

#[cfg(test)]
mod tests {
    use bilrost::{Message, OwnedMessage};

    use crate::identifiers::WithPartitionKey;

    use super::*;

    #[test]
    fn fixed_encoding_round_trips_entry_kind() {
        #[derive(Debug, PartialEq, bilrost::Message)]
        struct EncodedEntryKind {
            #[bilrost(tag(1), encoding(fixed))]
            kind: EntryKind,
        }

        let value = EncodedEntryKind {
            kind: EntryKind::Invocation,
        };
        let encoded = value.encode_to_bytes();

        assert_eq!(encoded.as_ref(), &[0x06, 1, 0, 0, 0]);
        assert_eq!(EncodedEntryKind::decode(encoded).unwrap(), value);
    }

    #[test]
    fn entry_id_bilrost_round_trips_with_fixed_kind() {
        let value = EntryId::new(EntryKind::Invocation, [1; EntryId::REMAINDER_LEN]);
        let encoded = value.encode_to_bytes();

        assert_eq!(encoded.len(), 23);
        assert_eq!(EntryId::decode(encoded).unwrap(), value);
    }

    /// `Display` then `FromStr` must round-trip both variants, including edge
    /// remainders. `Debug` delegates to `Display`, so it must match too.
    #[test]
    fn display_from_str_round_trips() {
        let mut r_first = [0u8; REMAINDER_LEN];
        r_first[0] = 0xff;
        let mut r_last = [0u8; REMAINDER_LEN];
        r_last[REMAINDER_LEN - 1] = 0xff;

        let cases = [
            VQueueEntryId::Invocation(0, [0u8; REMAINDER_LEN]),
            VQueueEntryId::Invocation(42, r_first),
            VQueueEntryId::Invocation(u64::MAX, [0xffu8; REMAINDER_LEN]),
            VQueueEntryId::StateMutation(0, r_last),
            VQueueEntryId::StateMutation(7, [0xabu8; REMAINDER_LEN]),
            VQueueEntryId::StateMutation(u64::MAX, [0xffu8; REMAINDER_LEN]),
        ];

        for id in cases {
            let encoded = id.to_string();
            assert_eq!(
                encoded,
                format!("{id:?}"),
                "Debug must delegate to Display for {id:?}"
            );
            let parsed: VQueueEntryId = encoded.parse().expect("must parse its own Display output");
            assert_eq!(parsed, id, "round-trip mismatch for {encoded}");
            // The encoded prefix identifies the kind.
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
                VQueueEntryId::extract_partition_key(&encoded).unwrap(),
                id.partition_key(),
            );
        }
    }

    /// A `VQueueEntryId` built from a real resource id must produce the exact same
    /// string as that resource id, and parsing it back must reproduce the parts.
    #[test]
    fn display_matches_underlying_resource_id() {
        let inv = InvocationId::mock_random();
        let inv_entry =
            VQueueEntryId::Invocation(inv.partition_key(), inv.invocation_uuid().to_bytes());
        assert_eq!(inv_entry.to_string(), inv.to_string());
        assert_eq!(inv.to_string().parse::<VQueueEntryId>().unwrap(), inv_entry,);

        let sm = StateMutationId::generate(1234);
        let sm_entry = VQueueEntryId::StateMutation(sm.partition_key(), sm.to_remainder_bytes());
        assert_eq!(sm_entry.to_string(), sm.to_string());
        assert_eq!(sm.to_string().parse::<VQueueEntryId>().unwrap(), sm_entry,);
    }

    /// Malformed strings, and well-formed ids of an unrelated resource type, must
    /// be rejected rather than silently mis-parsed.
    #[test]
    fn from_str_rejects_invalid_input() {
        for bad in ["", "not-an-id", "inv_", "xyz_1abc"] {
            assert!(
                bad.parse::<VQueueEntryId>().is_err(),
                "expected {bad:?} to fail parsing"
            );
        }

        // A valid id of a different resource type must be rejected (TypeMismatch),
        // not decoded as an entry id.
        let service_id = InvocationId::mock_random()
            .to_string()
            .replace("inv_", "svc_");
        assert!(service_id.parse::<VQueueEntryId>().is_err());
    }
}
