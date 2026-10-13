// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;

use bytes::BufMut;

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_types::identifiers::{CanonicalEntryId, ResourceId};
use restate_types::vqueues::{EntryKind, Seq, VQueueId};

use super::{IndexFieldDecode, IndexFieldEncode};

impl IndexFieldEncode for VQueueId {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_slice(self.as_raw_bytes());
    }

    fn serialized_length(&self) -> usize {
        Self::RAW_BYTES_LEN
    }
}

impl IndexFieldDecode for VQueueId {
    type Owned = Self;
    type Encoded<'a> = &'a [u8; Self::RAW_BYTES_LEN];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self> {
        Ok(Self::from_raw_bytes(&mut encoded.as_ref()))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let Some((encoded, remaining)) = source.split_first_chunk() else {
            return Err(StorageError::DataIntegrityError);
        };
        *source = remaining;
        Ok(encoded)
    }
}

impl IndexFieldEncode for CanonicalEntryId {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        assert_ne!(
            self.kind(),
            EntryKind::Unknown,
            "unknown entry kind cannot be encoded in an index key"
        );
        target.put_slice(self.as_bytes());
    }

    fn serialized_length(&self) -> usize {
        Self::RAW_BYTES_LEN
    }
}

impl IndexFieldDecode for CanonicalEntryId {
    type Owned = Self;
    type Encoded<'a> = &'a [u8; Self::RAW_BYTES_LEN];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self> {
        let id = Self::try_from_bytes(encoded).map_err(|_| StorageError::DataIntegrityError)?;
        if id.kind() == EntryKind::Unknown {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(*id)
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let Some((encoded, remaining)) = source.split_first_chunk() else {
            return Err(StorageError::DataIntegrityError);
        };
        *source = remaining;
        Ok(encoded)
    }
}

impl IndexFieldEncode for Seq {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_u64(self.as_u64());
    }

    fn serialized_length(&self) -> usize {
        size_of::<u64>()
    }
}

impl IndexFieldDecode for Seq {
    type Owned = Self;
    type Encoded<'a> = &'a [u8; size_of::<u64>()];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self> {
        Ok(Self::from_bytes(*encoded))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        u64::take_encoded(source)
    }
}

impl IndexFieldEncode for RoughTimestamp {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_u32(self.as_u32());
    }

    fn serialized_length(&self) -> usize {
        size_of::<u32>()
    }
}

impl IndexFieldDecode for RoughTimestamp {
    type Owned = Self;
    type Encoded<'a> = &'a [u8; size_of::<u32>()];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self> {
        let seconds = u32::from_be_bytes(*encoded);
        // The constructor clamps; reject the unrepresentable value on disk instead.
        if seconds > Self::MAX.as_u32() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(Self::new(seconds))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let Some((encoded, remaining)) = source.split_first_chunk() else {
            return Err(StorageError::DataIntegrityError);
        };
        *source = remaining;
        Ok(encoded)
    }
}

/// Keeps all HLC bits while reversing their bytewise order. Logical predicate
/// bounds must be reversed too before this codec can be used for filter binding.
impl IndexFieldEncode for Reverse<UniqueTimestamp> {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_u64(!self.0.as_u64());
    }

    fn serialized_length(&self) -> usize {
        size_of::<u64>()
    }
}

impl IndexFieldDecode for Reverse<UniqueTimestamp> {
    type Owned = Self;
    type Encoded<'a> = &'a [u8; size_of::<u64>()];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self> {
        UniqueTimestamp::try_from(!u64::from_be_bytes(*encoded))
            .map(Reverse)
            .map_err(|_| StorageError::DataIntegrityError)
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        u64::take_encoded(source)
    }
}

impl IndexFieldEncode for Reverse<u64> {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_u64(!self.0);
    }

    fn serialized_length(&self) -> usize {
        size_of::<u64>()
    }
}

impl IndexFieldDecode for Reverse<u64> {
    type Owned = Self;
    type Encoded<'a> = &'a [u8; size_of::<u64>()];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self> {
        Ok(Reverse(!u64::from_be_bytes(*encoded)))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        u64::take_encoded(source)
    }
}

// Index codes are independent of EntryKind's raw discriminants. Keep them in
// as_str() order, leaving gaps for future variants without renumbering stored keys.
impl IndexFieldEncode for EntryKind {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_u8(match self {
            Self::Invocation => 10,
            Self::StateMutation => 20,
            Self::Unknown => panic!("unknown entry kind cannot be encoded in an index key"),
        });
    }

    fn serialized_length(&self) -> usize {
        size_of::<u8>()
    }
}

impl IndexFieldDecode for EntryKind {
    type Owned = EntryKind;
    type Encoded<'a> = &'a [u8; 1];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Owned> {
        match encoded[0] {
            10 => Ok(Self::Invocation),
            20 => Ok(Self::StateMutation),
            _ => Err(StorageError::DataIntegrityError),
        }
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let Some((encoded, remaining)) = source.split_first_chunk() else {
            return Err(StorageError::DataIntegrityError);
        };
        *source = remaining;
        Ok(encoded)
    }
}

// Like EntryKind, these codes follow name order rather than raw discriminants.
// Gaps are reserved for future variants; existing codes must remain stable.
impl IndexFieldEncode for Stage {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_u8(match self {
            Self::Finished => 10,
            Self::Inbox => 20,
            Self::Paused => 30,
            Self::Running => 40,
            Self::Suspended => 50,
            Self::Unknown => panic!("unknown stage cannot be encoded in an index key"),
        });
    }

    fn serialized_length(&self) -> usize {
        size_of::<u8>()
    }
}

impl IndexFieldDecode for Stage {
    type Owned = Stage;
    type Encoded<'a> = &'a [u8; 1];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Owned> {
        match encoded[0] {
            10 => Ok(Self::Finished),
            20 => Ok(Self::Inbox),
            30 => Ok(Self::Paused),
            40 => Ok(Self::Running),
            50 => Ok(Self::Suspended),
            _ => Err(StorageError::DataIntegrityError),
        }
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let Some((encoded, remaining)) = source.split_first_chunk() else {
            return Err(StorageError::DataIntegrityError);
        };
        *source = remaining;
        Ok(encoded)
    }
}

impl IndexFieldEncode for Status {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_u8(match self {
            Self::BackingOff => 10,
            Self::Cancelled => 20,
            Self::Failed => 30,
            Self::Killed => 40,
            Self::New => 50,
            Self::Scheduled => 60,
            Self::Started => 70,
            Self::Succeeded => 80,
            Self::Unknown => 90,
            Self::Yielded => 100,
        });
    }

    fn serialized_length(&self) -> usize {
        size_of::<u8>()
    }
}

impl IndexFieldDecode for Status {
    type Owned = Status;
    type Encoded<'a> = &'a [u8; 1];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Owned> {
        match encoded[0] {
            10 => Ok(Self::BackingOff),
            20 => Ok(Self::Cancelled),
            30 => Ok(Self::Failed),
            40 => Ok(Self::Killed),
            50 => Ok(Self::New),
            60 => Ok(Self::Scheduled),
            70 => Ok(Self::Started),
            80 => Ok(Self::Succeeded),
            90 => Ok(Self::Unknown),
            100 => Ok(Self::Yielded),
            _ => Err(StorageError::DataIntegrityError),
        }
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let Some((encoded, remaining)) = source.split_first_chunk() else {
            return Err(StorageError::DataIntegrityError);
        };
        *source = remaining;
        Ok(encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vqueue_ids_preserve_order_and_round_trip() {
        let mut previous = None;
        for id in [
            VQueueId::new(0, &[0; 16]),
            VQueueId::new(255, &[255; 16]),
            VQueueId::new(256, &[0; 16]),
            VQueueId::new(256, &[255; 16]),
            VQueueId::new(u64::MAX, &[255; 16]),
        ] {
            let mut bytes = Vec::new();
            id.encode_field(&mut bytes);
            assert_eq!(bytes.len(), id.serialized_length());
            assert_eq!(bytes.len(), VQueueId::RAW_BYTES_LEN);
            assert_eq!(bytes, id.as_raw_bytes());
            if let Some(previous) = previous {
                assert!(previous < bytes);
            }
            previous = Some(bytes.clone());

            for len in 0..bytes.len() {
                let mut truncated = &bytes[..len];
                assert!(matches!(
                    VQueueId::decode_field(&mut truncated),
                    Err(StorageError::DataIntegrityError)
                ));
                assert_eq!(truncated, &bytes[..len]);
            }

            bytes.extend_from_slice(b"suffix");
            let mut remaining = bytes.as_slice();
            let encoded = VQueueId::take_encoded(&mut remaining).unwrap();
            assert_eq!(encoded.as_slice(), id.as_raw_bytes());
            assert_eq!(VQueueId::decode_encoded(encoded).unwrap(), id);
            assert_eq!(remaining, b"suffix");

            let mut remaining = bytes.as_slice();
            assert_eq!(VQueueId::decode_field(&mut remaining).unwrap(), id);
            assert_eq!(remaining, b"suffix");
        }
    }

    #[test]
    fn rough_timestamps_preserve_order_and_reject_invalid_encoding() {
        let mut previous = None;
        for timestamp in [
            RoughTimestamp::RESTATE_EPOCH,
            RoughTimestamp::new(255),
            RoughTimestamp::new(256),
            RoughTimestamp::MAX,
        ] {
            let mut bytes = Vec::new();
            timestamp.encode_field(&mut bytes);
            assert_eq!(bytes.len(), size_of::<u32>());
            assert_eq!(bytes, timestamp.as_u32().to_be_bytes());
            if let Some(previous) = previous {
                assert!(previous < bytes);
            }
            previous = Some(bytes.clone());
            bytes.push(42);
            let mut remaining = bytes.as_slice();
            assert_eq!(
                RoughTimestamp::decode_field(&mut remaining).unwrap(),
                timestamp
            );
            assert_eq!(remaining, [42]);
        }
        assert!(RoughTimestamp::decode_encoded(&u32::MAX.to_be_bytes()).is_err());
        for len in 0..size_of::<u32>() {
            assert!(RoughTimestamp::decode_field(&mut &[0; 4][..len]).is_err());
        }
    }

    #[test]
    fn stage_index_codec_matches_string_order_and_round_trips() {
        check_enum_index_codec(
            Stage::as_str,
            Some(Stage::Unknown),
            &[
                (Stage::Finished, 10),
                (Stage::Inbox, 20),
                (Stage::Paused, 30),
                (Stage::Running, 40),
                (Stage::Suspended, 50),
            ],
        );
        assert!(std::panic::catch_unwind(|| Stage::Unknown.encode_field(&mut Vec::new())).is_err());
    }

    #[test]
    fn status_index_codec_matches_string_order_and_round_trips() {
        check_enum_index_codec(
            Status::as_str,
            None,
            &[
                (Status::BackingOff, 10),
                (Status::Cancelled, 20),
                (Status::Failed, 30),
                (Status::Killed, 40),
                (Status::New, 50),
                (Status::Scheduled, 60),
                (Status::Started, 70),
                (Status::Succeeded, 80),
                (Status::Unknown, 90),
                (Status::Yielded, 100),
            ],
        );
    }

    fn check_enum_index_codec<C>(
        name: fn(C) -> &'static str,
        unencodable: Option<C>,
        codes: &[(C, u8)],
    ) where
        C: IndexFieldEncode
            + IndexFieldDecode<Owned = C>
            + strum::VariantArray
            + Copy
            + Eq
            + std::fmt::Debug
            + std::fmt::Display,
    {
        let mut by_name: Vec<_> = C::VARIANTS
            .iter()
            .copied()
            .filter(|value| Some(*value) != unencodable)
            .collect();
        by_name.sort_by_key(|&value| name(value));
        assert_eq!(
            by_name,
            codes.iter().map(|&(value, _)| value).collect::<Vec<_>>()
        );
        assert!(codes.windows(2).all(|pair| pair[0].1 < pair[1].1));

        for &(value, byte) in codes {
            assert_eq!(value.to_string(), name(value));
            let mut bytes = Vec::new();
            value.encode_field(&mut bytes);
            assert_eq!(bytes, [byte]);
            assert_eq!(value.serialized_length(), 1);

            bytes.extend_from_slice(b"suffix");
            let mut remaining = bytes.as_slice();
            let field = C::take_encoded(&mut remaining).unwrap();
            assert_eq!(field.as_ref(), &[byte]);
            assert_eq!(field.as_ref().as_ptr(), bytes.as_ptr());
            assert_eq!(C::decode_encoded(field).unwrap(), value);
            assert_eq!(remaining, b"suffix");

            let mut remaining = bytes.as_slice();
            assert_eq!(C::decode_field(&mut remaining).unwrap(), value);
            assert_eq!(remaining, b"suffix");
        }

        for byte in u8::MIN..=u8::MAX {
            if codes.iter().any(|&(_, code)| code == byte) {
                continue;
            }
            let bytes = [byte, 42];
            let mut remaining = bytes.as_slice();
            let field = C::take_encoded(&mut remaining).unwrap();
            assert_eq!(field.as_ref(), &[byte]);
            assert_eq!(remaining, &[42]);
            assert!(matches!(
                C::decode_encoded(field),
                Err(StorageError::DataIntegrityError)
            ));
        }
        assert!(C::decode_field(&mut [].as_slice()).is_err());
    }

    #[test]
    fn entry_kind_index_codec_uses_ordered_bytes_and_defers_value_validation() {
        let mut by_name: Vec<_> = <EntryKind as strum::VariantArray>::VARIANTS
            .iter()
            .copied()
            .filter(|kind| *kind != EntryKind::Unknown)
            .collect();
        by_name.sort_by_key(|kind| kind.as_str());
        assert_eq!(by_name, [EntryKind::Invocation, EntryKind::StateMutation]);

        for (kind, byte) in by_name.into_iter().zip([10, 20]) {
            assert_eq!(kind.to_string(), kind.as_str());
            assert_eq!(kind.as_str().parse::<EntryKind>().unwrap(), kind);
            let mut bytes = Vec::new();
            kind.encode_field(&mut bytes);
            assert_eq!(bytes, [byte]);
            assert_eq!(kind.serialized_length(), bytes.len());

            bytes.extend_from_slice(b"suffix");
            let mut input = bytes.as_slice();
            let field = EntryKind::take_encoded(&mut input).unwrap();
            assert_eq!(field, &[byte]);
            assert_eq!(field.as_ptr(), bytes.as_ptr());
            assert_eq!(EntryKind::decode_encoded(field).unwrap(), kind);
            assert_eq!(input, b"suffix");

            let mut input = bytes.as_slice();
            assert_eq!(EntryKind::decode_field(&mut input).unwrap(), kind);
            assert_eq!(input, b"suffix");
        }

        // Borrowing a field only checks its length. Decoding rejects every
        // unassigned code, including the raw enum discriminants and Unknown.
        for byte in u8::MIN..=u8::MAX {
            if [10, 20].contains(&byte) {
                continue;
            }
            let bytes = [byte, 42];
            let mut input = bytes.as_slice();
            let field = EntryKind::take_encoded(&mut input).unwrap();
            assert_eq!(field, &[byte]);
            assert_eq!(input, &[42]);
            assert!(matches!(
                EntryKind::decode_encoded(field),
                Err(StorageError::DataIntegrityError)
            ));
        }
        assert!(EntryKind::decode_field(&mut [].as_slice()).is_err());
        assert!(
            std::panic::catch_unwind(|| EntryKind::Unknown.encode_field(&mut Vec::new())).is_err()
        );
    }
}
