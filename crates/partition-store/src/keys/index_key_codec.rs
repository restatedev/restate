// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ordered field encodings for queue identities, enums, and timestamps.
//!
//! These are index representations, independent of the types' other storage or
//! wire formats. Enum codes follow name order and leave gaps for future variants;
//! existing codes must remain stable. `Stage::Unknown` and `EntryKind::Unknown`
//! cannot be encoded, while `Status::Unknown` is a supported value.
//!
//! Timestamp fields store their full physical representation: seconds for
//! `RoughTimestamp`, and complemented HLC bits for `Reverse<UniqueTimestamp>`.
//! Millisecond filter literals require a separate translation in the filter codec.
//! Taking a fixed-width field checks its length; decoding performs value checks.

use std::cmp::Reverse;

use bytes::BufMut;

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_types::identifiers::{CanonicalEntryId, ResourceId};
use restate_types::vqueues::{EntryKind, Seq, VQueueId};

use super::fields::U64Codec;
use super::{FieldCodec, OrderedFieldCodec};

pub enum VQueueIdCodec {}

impl OrderedFieldCodec for VQueueIdCodec {}

impl FieldCodec for VQueueIdCodec {
    type Value = VQueueId;
    type Ref<'a> = &'a VQueueId;
    type Encoded<'a> = &'a [u8; VQueueId::RAW_BYTES_LEN];

    fn borrow(value: &VQueueId) -> &VQueueId {
        value
    }

    fn encode_ref<B: BufMut>(value: &VQueueId, target: &mut B) {
        target.put_slice(value.as_raw_bytes());
    }

    fn serialized_length_ref(_: &VQueueId) -> usize {
        VQueueId::RAW_BYTES_LEN
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<VQueueId> {
        Ok(VQueueId::from_raw_bytes(&mut encoded.as_ref()))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let Some((encoded, remaining)) = source.split_first_chunk() else {
            return Err(StorageError::DataIntegrityError);
        };
        *source = remaining;
        Ok(encoded)
    }
}

pub enum CanonicalEntryIdCodec {}

impl OrderedFieldCodec for CanonicalEntryIdCodec {}

impl FieldCodec for CanonicalEntryIdCodec {
    type Value = CanonicalEntryId;
    type Ref<'a> = &'a CanonicalEntryId;
    type Encoded<'a> = &'a [u8; CanonicalEntryId::RAW_BYTES_LEN];

    fn borrow(value: &CanonicalEntryId) -> &CanonicalEntryId {
        value
    }

    fn encode_ref<B: BufMut>(value: &CanonicalEntryId, target: &mut B) {
        assert_ne!(
            value.kind(),
            EntryKind::Unknown,
            "unknown entry kind cannot be encoded in an index key"
        );
        target.put_slice(value.as_bytes());
    }

    fn serialized_length_ref(_: &CanonicalEntryId) -> usize {
        CanonicalEntryId::RAW_BYTES_LEN
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<CanonicalEntryId> {
        let id = CanonicalEntryId::try_from_bytes(encoded)
            .map_err(|_| StorageError::DataIntegrityError)?;
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

pub enum SeqCodec {}

impl OrderedFieldCodec for SeqCodec {}

impl FieldCodec for SeqCodec {
    type Value = Seq;
    type Ref<'a> = &'a Seq;
    type Encoded<'a> = &'a [u8; size_of::<u64>()];

    fn borrow(value: &Seq) -> &Seq {
        value
    }

    fn encode_ref<B: BufMut>(value: &Seq, target: &mut B) {
        target.put_u64(value.as_u64());
    }

    fn serialized_length_ref(_: &Seq) -> usize {
        size_of::<u64>()
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Seq> {
        Ok(Seq::from_bytes(*encoded))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        U64Codec::take_encoded(source)
    }
}

pub enum RoughTimestampCodec {}

impl OrderedFieldCodec for RoughTimestampCodec {}

impl FieldCodec for RoughTimestampCodec {
    type Value = RoughTimestamp;
    type Ref<'a> = &'a RoughTimestamp;
    type Encoded<'a> = &'a [u8; size_of::<u32>()];

    fn borrow(value: &RoughTimestamp) -> &RoughTimestamp {
        value
    }

    fn encode_ref<B: BufMut>(value: &RoughTimestamp, target: &mut B) {
        target.put_u32(value.as_u32());
    }

    fn serialized_length_ref(_: &RoughTimestamp) -> usize {
        size_of::<u32>()
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<RoughTimestamp> {
        let seconds = u32::from_be_bytes(*encoded);
        // The constructor clamps; reject the unrepresentable value on disk instead.
        if seconds > RoughTimestamp::MAX.as_u32() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(RoughTimestamp::new(seconds))
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
pub enum ReverseUniqueTimestampCodec {}

impl OrderedFieldCodec for ReverseUniqueTimestampCodec {}

impl FieldCodec for ReverseUniqueTimestampCodec {
    type Value = Reverse<UniqueTimestamp>;
    type Ref<'a> = &'a Reverse<UniqueTimestamp>;
    type Encoded<'a> = &'a [u8; size_of::<u64>()];

    fn borrow(value: &Self::Value) -> Self::Ref<'_> {
        value
    }

    fn encode_ref<B: BufMut>(value: Self::Ref<'_>, target: &mut B) {
        target.put_u64(!value.0.as_u64());
    }

    fn serialized_length_ref(_: Self::Ref<'_>) -> usize {
        size_of::<u64>()
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Value> {
        UniqueTimestamp::try_from(!u64::from_be_bytes(*encoded))
            .map(Reverse)
            .map_err(|_| StorageError::DataIntegrityError)
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        U64Codec::take_encoded(source)
    }
}

pub enum ReverseU64Codec {}

impl OrderedFieldCodec for ReverseU64Codec {}

impl FieldCodec for ReverseU64Codec {
    type Value = Reverse<u64>;
    type Ref<'a> = &'a Reverse<u64>;
    type Encoded<'a> = &'a [u8; size_of::<u64>()];

    fn borrow(value: &Self::Value) -> Self::Ref<'_> {
        value
    }

    fn encode_ref<B: BufMut>(value: Self::Ref<'_>, target: &mut B) {
        target.put_u64(!value.0);
    }

    fn serialized_length_ref(_: Self::Ref<'_>) -> usize {
        size_of::<u64>()
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Value> {
        Ok(Reverse(!u64::from_be_bytes(*encoded)))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        U64Codec::take_encoded(source)
    }
}

// Index codes are independent of EntryKind's raw discriminants. Keep them in
// as_str() order, leaving gaps for future variants without renumbering stored keys.
pub enum EntryKindCodec {}

impl OrderedFieldCodec for EntryKindCodec {}

impl FieldCodec for EntryKindCodec {
    type Value = EntryKind;
    type Ref<'a> = &'a EntryKind;
    type Encoded<'a> = &'a [u8; 1];

    fn borrow(value: &EntryKind) -> &EntryKind {
        value
    }

    fn encode_ref<B: BufMut>(value: &EntryKind, target: &mut B) {
        target.put_u8(match value {
            EntryKind::Invocation => 10,
            EntryKind::StateMutation => 20,
            EntryKind::Unknown => panic!("unknown entry kind cannot be encoded in an index key"),
        });
    }

    fn serialized_length_ref(_: &EntryKind) -> usize {
        size_of::<u8>()
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<EntryKind> {
        match encoded[0] {
            10 => Ok(EntryKind::Invocation),
            20 => Ok(EntryKind::StateMutation),
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

/// Stable one-byte ranks in stage-name order, independent of raw discriminants
/// and lifecycle progression. Gaps are reserved; existing ranks must not change.
pub enum StageRankCodec {}

impl OrderedFieldCodec for StageRankCodec {}

impl FieldCodec for StageRankCodec {
    type Value = Stage;
    type Ref<'a> = &'a Stage;
    type Encoded<'a> = &'a [u8; 1];

    fn borrow(value: &Stage) -> &Stage {
        value
    }

    fn encode_ref<B: BufMut>(value: &Stage, target: &mut B) {
        target.put_u8(match value {
            Stage::Finished => 10,
            Stage::Inbox => 20,
            Stage::Paused => 30,
            Stage::Running => 40,
            Stage::Suspended => 50,
            Stage::Unknown => panic!("unknown stage cannot be encoded in an index key"),
        });
    }

    fn serialized_length_ref(_: &Stage) -> usize {
        size_of::<u8>()
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Stage> {
        match encoded[0] {
            10 => Ok(Stage::Finished),
            20 => Ok(Stage::Inbox),
            30 => Ok(Stage::Paused),
            40 => Ok(Stage::Running),
            50 => Ok(Stage::Suspended),
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

pub enum StatusCodec {}

impl OrderedFieldCodec for StatusCodec {}

impl FieldCodec for StatusCodec {
    type Value = Status;
    type Ref<'a> = &'a Status;
    type Encoded<'a> = &'a [u8; 1];

    fn borrow(value: &Status) -> &Status {
        value
    }

    fn encode_ref<B: BufMut>(value: &Status, target: &mut B) {
        target.put_u8(match value {
            Status::BackingOff => 10,
            Status::Cancelled => 20,
            Status::Failed => 30,
            Status::Killed => 40,
            Status::New => 50,
            Status::Scheduled => 60,
            Status::Started => 70,
            Status::Succeeded => 80,
            Status::Unknown => 90,
            Status::Yielded => 100,
        });
    }

    fn serialized_length_ref(_: &Status) -> usize {
        size_of::<u8>()
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Status> {
        match encoded[0] {
            10 => Ok(Status::BackingOff),
            20 => Ok(Status::Cancelled),
            30 => Ok(Status::Failed),
            40 => Ok(Status::Killed),
            50 => Ok(Status::New),
            60 => Ok(Status::Scheduled),
            70 => Ok(Status::Started),
            80 => Ok(Status::Succeeded),
            90 => Ok(Status::Unknown),
            100 => Ok(Status::Yielded),
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
            VQueueIdCodec::encode_field(&id, &mut bytes);
            assert_eq!(bytes.len(), VQueueIdCodec::serialized_length(&id));
            assert_eq!(bytes.len(), VQueueId::RAW_BYTES_LEN);
            assert_eq!(bytes, id.as_raw_bytes());
            if let Some(previous) = previous {
                assert!(previous < bytes);
            }
            previous = Some(bytes.clone());

            for len in 0..bytes.len() {
                let mut truncated = &bytes[..len];
                assert!(matches!(
                    VQueueIdCodec::decode_field(&mut truncated),
                    Err(StorageError::DataIntegrityError)
                ));
                assert_eq!(truncated, &bytes[..len]);
            }

            bytes.extend_from_slice(b"suffix");
            let mut remaining = bytes.as_slice();
            let encoded = VQueueIdCodec::take_encoded(&mut remaining).unwrap();
            assert_eq!(encoded.as_slice(), id.as_raw_bytes());
            assert_eq!(VQueueIdCodec::decode_encoded(encoded).unwrap(), id);
            assert_eq!(remaining, b"suffix");

            let mut remaining = bytes.as_slice();
            assert_eq!(VQueueIdCodec::decode_field(&mut remaining).unwrap(), id);
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
            RoughTimestampCodec::encode_field(&timestamp, &mut bytes);
            assert_eq!(bytes.len(), size_of::<u32>());
            assert_eq!(bytes, timestamp.as_u32().to_be_bytes());
            if let Some(previous) = previous {
                assert!(previous < bytes);
            }
            previous = Some(bytes.clone());
            bytes.push(42);
            let mut remaining = bytes.as_slice();
            assert_eq!(
                RoughTimestampCodec::decode_field(&mut remaining).unwrap(),
                timestamp
            );
            assert_eq!(remaining, [42]);
        }
        assert!(RoughTimestampCodec::decode_encoded(&u32::MAX.to_be_bytes()).is_err());
        for len in 0..size_of::<u32>() {
            assert!(RoughTimestampCodec::decode_field(&mut &[0; 4][..len]).is_err());
        }
    }

    #[test]
    fn stage_index_codec_matches_string_order_and_round_trips() {
        check_enum_index_codec::<StageRankCodec>(
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
        assert!(
            std::panic::catch_unwind(|| StageRankCodec::encode_field(
                &Stage::Unknown,
                &mut Vec::new()
            ))
            .is_err()
        );
    }

    #[test]
    fn status_index_codec_matches_string_order_and_round_trips() {
        check_enum_index_codec::<StatusCodec>(
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
        name: fn(C::Value) -> &'static str,
        unencodable: Option<C::Value>,
        codes: &[(C::Value, u8)],
    ) where
        C: OrderedFieldCodec,
        C::Value: strum::VariantArray + Copy + Eq + std::fmt::Debug + std::fmt::Display,
    {
        let mut by_name: Vec<_> = <C::Value as strum::VariantArray>::VARIANTS
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
            C::encode_field(&value, &mut bytes);
            assert_eq!(bytes, [byte]);
            assert_eq!(C::serialized_length(&value), 1);

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
            EntryKindCodec::encode_field(&kind, &mut bytes);
            assert_eq!(bytes, [byte]);
            assert_eq!(EntryKindCodec::serialized_length(&kind), bytes.len());

            bytes.extend_from_slice(b"suffix");
            let mut input = bytes.as_slice();
            let field = EntryKindCodec::take_encoded(&mut input).unwrap();
            assert_eq!(field, &[byte]);
            assert_eq!(field.as_ptr(), bytes.as_ptr());
            assert_eq!(EntryKindCodec::decode_encoded(field).unwrap(), kind);
            assert_eq!(input, b"suffix");

            let mut input = bytes.as_slice();
            assert_eq!(EntryKindCodec::decode_field(&mut input).unwrap(), kind);
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
            let field = EntryKindCodec::take_encoded(&mut input).unwrap();
            assert_eq!(field, &[byte]);
            assert_eq!(input, &[42]);
            assert!(matches!(
                EntryKindCodec::decode_encoded(field),
                Err(StorageError::DataIntegrityError)
            ));
        }
        assert!(EntryKindCodec::decode_field(&mut [].as_slice()).is_err());
        assert!(
            std::panic::catch_unwind(|| EntryKindCodec::encode_field(
                &EntryKind::Unknown,
                &mut Vec::new()
            ))
            .is_err()
        );
    }
}
