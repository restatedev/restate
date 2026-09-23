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

use super::{EncodedMemCmpStr, IndexFieldDecode, IndexFieldEncode};

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
    type Encoded = [u8; Self::RAW_BYTES_LEN];

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self> {
        Ok(Self::from_raw_bytes(&mut encoded.as_ref()))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
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
    type Encoded = [u8; Self::RAW_BYTES_LEN];

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self> {
        let id = Self::try_from_bytes(encoded).map_err(|_| StorageError::DataIntegrityError)?;
        if id.kind() == EntryKind::Unknown {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(*id)
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
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
    type Encoded = [u8; size_of::<u64>()];

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self> {
        Ok(Self::from_bytes(*encoded))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
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
    type Encoded = [u8; size_of::<u32>()];

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self> {
        let seconds = u32::from_be_bytes(*encoded);
        // The constructor clamps; reject the unrepresentable value on disk instead.
        if seconds > Self::MAX.as_u32() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(Self::new(seconds))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
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
    type Encoded = [u8; size_of::<u64>()];

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self> {
        UniqueTimestamp::try_from(!u64::from_be_bytes(*encoded))
            .map(Reverse)
            .map_err(|_| StorageError::DataIntegrityError)
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
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
    type Encoded = [u8; size_of::<u64>()];

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self> {
        Ok(Reverse(!u64::from_be_bytes(*encoded)))
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
        u64::take_encoded(source)
    }
}

impl IndexFieldEncode for EntryKind {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        assert_ne!(
            *self,
            EntryKind::Unknown,
            "unknown entry kind cannot be encoded in an index key"
        );
        target.put_slice(self.as_mem_cmp_str().as_bytes());
    }

    fn serialized_length(&self) -> usize {
        self.as_mem_cmp_str().encoded_len()
    }
}

impl IndexFieldDecode for EntryKind {
    type Owned = EntryKind;
    type Encoded = EncodedMemCmpStr;

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self::Owned> {
        EntryKind::from_mem_cmp_str(encoded)
            .filter(|kind| *kind != EntryKind::Unknown)
            .ok_or(StorageError::DataIntegrityError)
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
        str::take_encoded(source)
    }
}

impl IndexFieldEncode for Stage {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_slice(self.as_mem_cmp_str().as_bytes());
    }

    fn serialized_length(&self) -> usize {
        self.as_mem_cmp_str().encoded_len()
    }
}

impl IndexFieldDecode for Stage {
    type Owned = Stage;
    type Encoded = EncodedMemCmpStr;

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self::Owned> {
        Stage::from_mem_cmp_str(encoded).ok_or(StorageError::DataIntegrityError)
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
        str::take_encoded(source)
    }
}

impl IndexFieldEncode for Status {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_slice(self.as_mem_cmp_str().as_bytes());
    }

    fn serialized_length(&self) -> usize {
        self.as_mem_cmp_str().encoded_len()
    }
}

impl IndexFieldDecode for Status {
    type Owned = Status;
    type Encoded = EncodedMemCmpStr;

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self::Owned> {
        Status::from_mem_cmp_str(encoded).ok_or(StorageError::DataIntegrityError)
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
        str::take_encoded(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static_assertions::assert_impl_all!(VQueueId: restate_storage_api::PrimaryKey);
    static_assertions::assert_not_impl_any!(Option<VQueueId>: restate_storage_api::PrimaryKey);

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
        let stages = [
            Stage::Unknown,
            Stage::Inbox,
            Stage::Running,
            Stage::Suspended,
            Stage::Paused,
            Stage::Finished,
        ];

        assert_eq!(stages.len(), <Stage as strum::EnumCount>::COUNT);

        let mut by_name = stages;
        by_name.sort_by_key(|stage| stage.as_str());

        let mut by_encoded = stages;
        by_encoded.sort_by_key(|stage| {
            let mut bytes = Vec::new();
            stage.encode_field(&mut bytes);
            bytes
        });

        assert_eq!(by_encoded, by_name);
        assert_eq!(
            by_name,
            [
                Stage::Finished,
                Stage::Inbox,
                Stage::Paused,
                Stage::Running,
                Stage::Suspended,
                Stage::Unknown,
            ]
        );
        assert_eq!(Stage::Running.as_mem_cmp_str().as_bytes(), b"running\0\x07");

        for stage in by_name {
            let mut bytes = Vec::new();
            stage.encode_field(&mut bytes);
            assert_eq!(bytes, stage.as_mem_cmp_str().as_bytes());

            let mut remaining = bytes.as_slice();
            assert_eq!(
                Stage::take_encoded(&mut remaining).unwrap(),
                stage.as_mem_cmp_str()
            );
            assert!(remaining.is_empty());

            let mut bytes = bytes.as_slice();
            assert_eq!(Stage::decode_field(&mut bytes).unwrap(), stage);
            assert!(bytes.is_empty());
        }

        let mut invalid = Vec::new();
        "invalid".encode_field(&mut invalid);
        assert!(Stage::decode_field(&mut invalid.as_slice()).is_err());
        let mut remaining = invalid.as_slice();
        assert_eq!(
            Stage::take_encoded(&mut remaining).unwrap().as_bytes(),
            invalid
        );
        assert!(remaining.is_empty());
        assert!(Stage::decode_field(&mut [].as_slice()).is_err());
    }

    #[test]
    fn status_index_codec_matches_string_order_and_round_trips() {
        let statuses = [
            Status::Unknown,
            Status::New,
            Status::Scheduled,
            Status::Started,
            Status::BackingOff,
            Status::Yielded,
            Status::Killed,
            Status::Cancelled,
            Status::Failed,
            Status::Succeeded,
        ];

        assert_eq!(statuses.len(), <Status as strum::EnumCount>::COUNT);

        let mut by_name = statuses;
        by_name.sort_by_key(|status| status.as_str());

        let mut by_encoded = statuses;
        by_encoded.sort_by_key(|status| {
            let mut bytes = Vec::new();
            status.encode_field(&mut bytes);
            bytes
        });

        assert_eq!(by_encoded, by_name);
        assert_eq!(
            by_name,
            [
                Status::BackingOff,
                Status::Cancelled,
                Status::Failed,
                Status::Killed,
                Status::New,
                Status::Scheduled,
                Status::Started,
                Status::Succeeded,
                Status::Unknown,
                Status::Yielded,
            ]
        );
        assert_eq!(
            Status::BackingOff.as_mem_cmp_str().as_bytes(),
            b"backing-\x09off\0\0\0\0\0\x03"
        );

        for status in by_name {
            assert_eq!(status.to_string(), status.as_str());

            let mut bytes = Vec::new();
            status.encode_field(&mut bytes);
            assert_eq!(bytes, status.as_mem_cmp_str().as_bytes());

            let mut remaining = bytes.as_slice();
            assert_eq!(
                Status::take_encoded(&mut remaining).unwrap(),
                status.as_mem_cmp_str()
            );
            assert!(remaining.is_empty());

            let mut bytes = bytes.as_slice();
            assert_eq!(Status::decode_field(&mut bytes).unwrap(), status);
            assert!(bytes.is_empty());
        }

        let mut invalid = Vec::new();
        "invalid".encode_field(&mut invalid);
        assert!(Status::decode_field(&mut invalid.as_slice()).is_err());
        let mut remaining = invalid.as_slice();
        assert_eq!(
            Status::take_encoded(&mut remaining).unwrap().as_bytes(),
            invalid
        );
        assert!(remaining.is_empty());
        assert!(Status::decode_field(&mut [].as_slice()).is_err());
    }

    #[test]
    fn entry_kind_index_codec_uses_display_names_and_defers_value_validation() {
        let mut by_name = Vec::new();
        let mut by_encoded = Vec::new();
        for &kind in <EntryKind as strum::VariantArray>::VARIANTS {
            let name = kind.to_string();
            assert_eq!(kind.as_str(), name);
            let mut encoded_name = Vec::new();
            name.as_str().encode_field(&mut encoded_name);
            assert_eq!(kind.as_mem_cmp_str().as_bytes(), encoded_name);
            assert_eq!(
                EntryKind::from_mem_cmp_str(kind.as_mem_cmp_str()),
                Some(kind)
            );
            if kind == EntryKind::Unknown {
                assert!(EntryKind::decode_encoded(kind.as_mem_cmp_str()).is_err());
                continue;
            }
            assert_eq!(name.parse::<EntryKind>().unwrap(), kind);

            let mut bytes = Vec::new();
            kind.encode_field(&mut bytes);
            assert_eq!(bytes, encoded_name);
            assert_eq!(kind.serialized_length(), bytes.len());
            by_name.push((name, kind));
            by_encoded.push((bytes.clone(), kind));

            bytes.extend_from_slice(b"suffix");
            let mut input = bytes.as_slice();
            let field = EntryKind::take_encoded(&mut input).unwrap();
            assert_eq!(field, kind.as_mem_cmp_str());
            assert_eq!(EntryKind::decode_encoded(field).unwrap(), kind);
            assert_eq!(input, b"suffix");
        }
        by_name.sort_by(|(a, _), (b, _)| a.cmp(b));
        by_encoded.sort_by(|(a, _), (b, _)| a.cmp(b));
        assert_eq!(
            by_name
                .into_iter()
                .map(|(_, kind)| kind)
                .collect::<Vec<_>>(),
            by_encoded
                .into_iter()
                .map(|(_, kind)| kind)
                .collect::<Vec<_>>()
        );

        let unknown_name = crate::encoded_mem_cmp_str!("future-kind");
        let mut input = unknown_name.as_bytes();
        let field = EntryKind::take_encoded(&mut input).unwrap();
        assert!(input.is_empty());
        assert!(EntryKind::decode_encoded(field).is_err());
        assert!(
            EntryKind::decode_field(
                &mut &unknown_name.as_bytes()[..unknown_name.encoded_len() - 1]
            )
            .is_err()
        );
        assert!(EntryKind::decode_field(&mut [10].as_slice()).is_err());
    }
}
