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

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::index::EntryByService;
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_types::ServiceName;
use restate_types::identifiers::CanonicalEntryId;

use super::macros::define_secondary_index;

define_secondary_index!(
    /// Entries ordered by service, VQueue stage, and newest transition first.
    EntryByServiceStage,
    key: EntryByServiceStageKey {
        service_name: ServiceName => str,
        stage: Stage,
        transitioned_at: Reverse<UniqueTimestamp>,
        // todo: add status?
        canonical_id: CanonicalEntryId (primary_key),
    }
);

define_secondary_index!(
    /// Entries ordered by stage and newest transition first.
    EntryByStage,
    key: EntryByStageKey {
        stage: Stage,
        transitioned_at: Reverse<UniqueTimestamp>,
        status: Status,
        canonical_id: CanonicalEntryId (primary_key),
    }
);

define_secondary_index!(
    /// Entries lined up in a stage ordered by their next transition time.
    EntryNextAtByStage,
    key: EntryNextAtByStageKey {
        stage: Stage,
        next_at: RoughTimestamp,
        status: Status,
        canonical_id: CanonicalEntryId (primary_key),
    }
);

crate::keys::macros::define_index_key_filter!(
    EntryByServiceStageKey; [EntryByService];
    service_name: ServiceName,
    stage: Stage,
    transitioned_at: Reverse<UniqueTimestamp>,
    canonical_id: CanonicalEntryId,
);

#[cfg(test)]
mod tests {
    use restate_types::identifiers::{BaseEntryId, InvocationId, InvocationUuid, PartitionKey};
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::{EntryId, EntryKind, Seq};

    use crate::index::{IndexId, SecondaryIndex, SecondaryIndexKey};
    use crate::keys::{IndexFieldDecode, IndexKeyPrefix};

    use super::*;

    static_assertions::assert_impl_all!(EntryByServiceStageKey: SecondaryIndexKey);
    static_assertions::assert_impl_all!(EntryByStageKey: SecondaryIndexKey);
    static_assertions::assert_impl_all!(EntryNextAtByStageKey: SecondaryIndexKey);
    static_assertions::assert_not_impl_any!(EntryByServiceStageKeyPrefix<Vec<u8>, 3>: SecondaryIndexKey);
    static_assertions::assert_not_impl_any!(EntryByStageKeyPrefix<Vec<u8>, 3>: SecondaryIndexKey);
    static_assertions::assert_not_impl_any!(EntryNextAtByStageKeyPrefix<Vec<u8>, 3>: SecondaryIndexKey);
    static_assertions::assert_type_eq_all!(
        <EntryByServiceStage as SecondaryIndex>::PrimaryKey,
        CanonicalEntryId
    );
    static_assertions::assert_impl_all!(InvocationId: restate_storage_api::PrimaryKey);
    static_assertions::assert_impl_all!(CanonicalEntryId: restate_storage_api::PrimaryKey);
    static_assertions::assert_not_impl_any!(Option<InvocationId>: restate_storage_api::PrimaryKey);
    static_assertions::assert_not_impl_any!(Option<CanonicalEntryId>: restate_storage_api::PrimaryKey);

    #[test]
    fn secondary_key_round_trip_preserves_required_primary_suffix() {
        let partition = PartitionId::from(8);
        for kind in [EntryKind::Invocation, EntryKind::StateMutation] {
            let id = BaseEntryId::new(3337, EntryId::new(kind, [42; EntryId::REMAINDER_LEN]))
                .canonicalize(Seq::MAX);
            let at = UniqueTimestamp::try_from_parts(100, 7).unwrap();
            let key = EntryByServiceStageKey::borrowed("svc", Stage::Running, Reverse(at), id);
            assert_eq!(*key.primary_key(), id);

            let mut bytes = Vec::new();
            key.encode_key(partition, &mut bytes);
            assert_eq!(
                &bytes[..IndexKeyPrefix::SERIALIZED_LENGTH],
                b"ZI\0\0\0\x08\0\0\0\x01"
            );
            assert_eq!(&bytes[bytes.len() - id.as_bytes().len()..], id.as_bytes());

            let mut prefix = Vec::new();
            let builder = EntryByServiceStageKey::prefix(partition, &mut prefix)
                .service_name("svc")
                .stage(Stage::Running)
                .transitioned_at(Reverse(at));
            builder.canonical_id(id);
            assert_eq!(prefix, bytes);

            let (header, remaining) = IndexKeyPrefix::decode_prefix(&bytes).unwrap();
            assert_eq!(header.partition_id(), partition);
            assert_eq!(header.index_id(), Some(IndexId::EntryByServiceStage));
            assert_eq!(
                header.index_id_raw(),
                EntryByServiceStage::INDEX_ID.as_u32()
            );
            assert_eq!(IndexId::from_u32(0), None);
            assert_eq!(IndexId::from_u32(u32::MAX), None);
            let decoded = remaining
                .into_decoder::<EntryByServiceStageKey>()
                .decode_all()
                .unwrap();
            assert_eq!(decoded.service_name.as_str(), "svc");
            assert_eq!(decoded.stage, Stage::Running);
            assert_eq!(decoded.transitioned_at, Reverse(at));
            assert_eq!(*decoded.primary_key(), id);
            let mut owned_bytes = Vec::new();
            decoded.encode_key(partition, &mut owned_bytes);
            assert_eq!(owned_bytes, bytes);

            let (_, remaining) = IndexKeyPrefix::decode_prefix(&bytes).unwrap();
            let (_, decoder) = remaining
                .into_decoder::<EntryByServiceStageKey>()
                .take_service_name()
                .unwrap();
            let (_, decoder) = decoder.take_stage().unwrap();
            let (time, decoder) = decoder.take_transitioned_at().unwrap();
            assert_eq!(time.as_bytes(), &(!at.as_u64()).to_be_bytes());
            assert_eq!(time.decode().unwrap(), Reverse(at));
            assert_eq!(decoder.take_canonical_id().unwrap().decode().unwrap(), id);

            // The suffix cannot be missing or partial, and full decoding rejects trailing data.
            for missing in 1..=id.as_bytes().len() {
                let (_, remaining) =
                    IndexKeyPrefix::decode_prefix(&bytes[..bytes.len() - missing]).unwrap();
                assert!(
                    remaining
                        .into_decoder::<EntryByServiceStageKey>()
                        .decode_all()
                        .is_err()
                );
            }
            bytes.push(0);
            let (_, remaining) = IndexKeyPrefix::decode_prefix(&bytes).unwrap();
            assert!(
                remaining
                    .into_decoder::<EntryByServiceStageKey>()
                    .decode_all()
                    .is_err()
            );
            assert!(
                CanonicalEntryId::decode_encoded(&[0; CanonicalEntryId::RAW_BYTES_LEN]).is_err()
            );
            let mut invalid_kind = *id.as_bytes();
            invalid_kind[size_of::<PartitionKey>()] = 0xff;
            assert!(CanonicalEntryId::decode_encoded(&invalid_kind).is_err());
            assert!(Reverse::<UniqueTimestamp>::decode_encoded(&[0; 8]).is_err());
        }
    }

    #[test]
    fn stage_indexes_round_trip_with_opposite_time_ordering() {
        let partition = PartitionId::MIN;
        let base = BaseEntryId::new(3337, EntryId::new(EntryKind::StateMutation, [42; 16]));
        let mut transitions = Vec::new();
        let mut next_times = Vec::new();
        for (i, (at, next_at)) in [
            (UniqueTimestamp::MIN, RoughTimestamp::RESTATE_EPOCH),
            (
                UniqueTimestamp::try_from_parts(100, 1).unwrap(),
                RoughTimestamp::new(1),
            ),
            (UniqueTimestamp::MAX, RoughTimestamp::MAX),
        ]
        .into_iter()
        .enumerate()
        {
            let id = base.canonicalize(Seq::new(i as u64));
            let mut bytes = Vec::new();
            EntryByStageKey::borrowed(Stage::Inbox, Reverse(at), Status::Scheduled, id)
                .encode_key(partition, &mut bytes);
            let (prefix, payload) = IndexKeyPrefix::decode_prefix(&bytes).unwrap();
            assert_eq!(prefix.index_id(), Some(IndexId::EntryByStage));
            let decoded = payload
                .into_decoder::<EntryByStageKey>()
                .decode_all()
                .unwrap();
            assert_eq!(
                (
                    decoded.stage,
                    decoded.transitioned_at,
                    decoded.status,
                    *decoded.primary_key()
                ),
                (Stage::Inbox, Reverse(at), Status::Scheduled, id)
            );
            transitions.push((bytes, id));

            let mut bytes = Vec::new();
            EntryNextAtByStageKey::borrowed(Stage::Inbox, next_at, Status::Scheduled, id)
                .encode_key(partition, &mut bytes);
            let (prefix, payload) = IndexKeyPrefix::decode_prefix(&bytes).unwrap();
            assert_eq!(prefix.index_id(), Some(IndexId::EntryNextAtByStage));
            let decoded = payload
                .into_decoder::<EntryNextAtByStageKey>()
                .decode_all()
                .unwrap();
            assert_eq!(
                (
                    decoded.stage,
                    decoded.next_at,
                    decoded.status,
                    *decoded.primary_key()
                ),
                (Stage::Inbox, next_at, Status::Scheduled, id)
            );
            next_times.push((bytes, id));
        }
        transitions.sort();
        next_times.sort();
        assert_eq!(
            transitions
                .into_iter()
                .map(|(_, id)| id.seq())
                .collect::<Vec<_>>(),
            [Seq::new(2), Seq::new(1), Seq::new(0)]
        );
        assert_eq!(
            next_times
                .into_iter()
                .map(|(_, id)| id.seq())
                .collect::<Vec<_>>(),
            [Seq::new(0), Seq::new(1), Seq::new(2)]
        );
    }

    #[test]
    fn secondary_key_order_is_service_stage_newest_transition_then_primary_key() {
        let id = |partition, uuid| {
            BaseEntryId::from(InvocationId::from_parts(
                partition,
                InvocationUuid::from_u128(uuid),
            ))
            .canonicalize(Seq::new(1))
        };
        let at = UniqueTimestamp::try_from_parts(100, 1).unwrap();
        let later = UniqueTimestamp::try_from_parts(100, 2).unwrap();
        let mut expected = vec![
            (
                "svc",
                Stage::Running,
                Reverse(at),
                id(8, 1).with_seq(Seq::MAX),
            ),
            (
                "svc",
                Stage::Running,
                Reverse(at),
                id(8, 1).with_seq(Seq::new(0)),
            ),
            ("svc", Stage::Running, Reverse(at), id(8, 2)),
            ("svc", Stage::Running, Reverse(at), id(8, 1)),
            ("svc", Stage::Running, Reverse(at), id(7, 9)),
            ("svc", Stage::Running, Reverse(later), id(8, 3)),
            (
                "svc",
                Stage::Running,
                Reverse(UniqueTimestamp::MIN),
                id(8, 1),
            ),
            (
                "svc",
                Stage::Running,
                Reverse(UniqueTimestamp::MAX),
                id(8, 1),
            ),
            ("svc", Stage::Inbox, Reverse(later), id(8, 1)),
            ("another", Stage::Running, Reverse(at), id(8, 1)),
        ];
        let mut encoded: Vec<_> = expected
            .iter()
            .map(|&(service, stage, at, id)| {
                let mut bytes = Vec::new();
                EntryByServiceStageKey::borrowed(service, stage, at, id)
                    .encode_key(PartitionId::MIN, &mut bytes);
                (bytes, (service, stage, at, id))
            })
            .collect();
        encoded.sort_by(|(a, _), (b, _)| a.cmp(b));
        expected.sort_by_key(|&(service, stage, at, id)| (service, stage.as_str(), at, id));
        assert_eq!(
            encoded.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
            expected
        );
    }
}
