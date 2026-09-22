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

use restate_clock::UniqueTimestamp;
use restate_storage_api::index::InvocationByService;
use restate_storage_api::invocation_status_table::InvocationStatusTable;
use restate_storage_api::vqueue_table::Stage;
use restate_types::ServiceName;
use restate_types::identifiers::InvocationId;

use super::macros::define_secondary_index;

define_secondary_index!(
    /// Invocation entries ordered by service, VQueue stage, and newest transition first.
    InvocationByServiceStage,
    table: InvocationStatusTable,
    key: InvocationByServiceStageKey {
        service_name: ServiceName => str,
        stage: Stage,
        transitioned_at: Reverse<UniqueTimestamp>,
        invocation_id: InvocationId (primary_key),
    }
);

// Timestamp predicates remain residual until logical time ranges can be bound
// safely to the descending full-HLC field. The physical schema still includes it.
crate::keys::macros::define_index_key_filter!(
    InvocationByServiceStageKey; [InvocationByService];
    service_name: ServiceName,
    stage: Stage,
    invocation_id: InvocationId,
);

#[cfg(test)]
mod tests {
    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::sharding::PartitionId;

    use crate::index::{IndexId, SecondaryIndex, SecondaryIndexKey};
    use crate::keys::{IndexFieldDecode, IndexKeyPrefix};

    use super::*;

    static_assertions::assert_impl_all!(InvocationByServiceStageKey: SecondaryIndexKey);
    static_assertions::assert_not_impl_any!(InvocationByServiceStageKeyPrefix<Vec<u8>, 3>: SecondaryIndexKey);

    #[test]
    fn secondary_key_round_trip_preserves_required_primary_suffix() {
        let partition = PartitionId::from(8);
        let id = InvocationId::from_parts(3337, InvocationUuid::from_u128(42));
        let at = UniqueTimestamp::try_from_parts(100, 7).unwrap();
        let key = InvocationByServiceStageKey::borrowed("svc", Stage::Running, Reverse(at), id);
        assert_eq!(*key.primary_key(), id);

        let mut bytes = Vec::new();
        key.encode_key(partition, &mut bytes);
        assert_eq!(
            &bytes[..IndexKeyPrefix::SERIALIZED_LENGTH],
            b"ZI\0\0\0\x08\0\0\0\x01"
        );
        assert_eq!(&bytes[bytes.len() - id.to_bytes().len()..], id.to_bytes());

        let mut prefix = Vec::new();
        let builder = InvocationByServiceStageKey::prefix(partition, &mut prefix)
            .service_name("svc")
            .stage(Stage::Running)
            .transitioned_at(Reverse(at));
        builder.invocation_id(id);
        assert_eq!(prefix, bytes);

        let (header, remaining) = IndexKeyPrefix::decode_prefix(&bytes).unwrap();
        assert_eq!(header.partition_id(), partition);
        assert_eq!(header.index_id(), Some(IndexId::InvocationByServiceStage));
        assert_eq!(
            header.index_id_raw(),
            InvocationByServiceStage::INDEX_ID.as_u32()
        );
        assert_eq!(IndexId::from_u32(0), None);
        assert_eq!(IndexId::from_u32(u32::MAX), None);
        let decoded = remaining
            .into_decoder::<InvocationByServiceStageKey>()
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
            .into_decoder::<InvocationByServiceStageKey>()
            .take_service_name()
            .unwrap();
        let (_, decoder) = decoder.take_stage().unwrap();
        let (time, decoder) = decoder.take_transitioned_at().unwrap();
        assert_eq!(time.as_bytes(), &(!at.as_u64()).to_be_bytes());
        assert_eq!(time.decode().unwrap(), Reverse(at));
        assert_eq!(decoder.take_invocation_id().unwrap().decode().unwrap(), id);

        // The suffix cannot be missing or partial, and full decoding rejects trailing data.
        for missing in 1..=id.to_bytes().len() {
            let (_, remaining) =
                IndexKeyPrefix::decode_prefix(&bytes[..bytes.len() - missing]).unwrap();
            assert!(
                remaining
                    .into_decoder::<InvocationByServiceStageKey>()
                    .decode_all()
                    .is_err()
            );
        }
        bytes.push(0);
        let (_, remaining) = IndexKeyPrefix::decode_prefix(&bytes).unwrap();
        assert!(
            remaining
                .into_decoder::<InvocationByServiceStageKey>()
                .decode_all()
                .is_err()
        );
        assert!(InvocationId::decode_encoded(&[0; 24]).is_err());
        assert!(Reverse::<UniqueTimestamp>::decode_encoded(&[0; 8]).is_err());
    }

    #[test]
    fn secondary_key_order_is_service_stage_newest_transition_then_primary_key() {
        let id =
            |partition, uuid| InvocationId::from_parts(partition, InvocationUuid::from_u128(uuid));
        let at = UniqueTimestamp::try_from_parts(100, 1).unwrap();
        let later = UniqueTimestamp::try_from_parts(100, 2).unwrap();
        let mut expected = vec![
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
                InvocationByServiceStageKey::borrowed(service, stage, at, id)
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
