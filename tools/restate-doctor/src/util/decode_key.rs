// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decoding helpers for partition-store keys with custom layouts.

use restate_partition_store::index::{
    BusyVQueueKey, EntryByStageKey, EntryByStageServiceKey, EntryByVirtualObjectStageKey,
    EntryNextAtByStageKey, EntryNextAtByStageServiceKey, EntryNextAtByVirtualObjectStageKey,
    IndexId,
};
use restate_partition_store::keys::IndexKeyPrefix;
use restate_partition_store::stats::AggregateKeyPrefix;

use super::hex_encode;

/// Decodes legacy static index identities and their known payloads. Registered
/// index IDs start at 64 and retain their payload as hex without a registry.
pub fn decode_secondary_index_key(key: &[u8]) -> Option<String> {
    let (prefix, payload) = IndexKeyPrefix::decode_prefix(key).ok()?;
    let details = match prefix.index_id() {
        Some(index @ IndexId::EntryByStageService) => {
            let key = payload
                .into_decoder::<EntryByStageServiceKey>()
                .decode_all()
                .ok()?;
            // Decoding removes the descending-order transformation. Preserve the
            // full HLC alongside Unix milliseconds so logical ticks remain visible.
            let transitioned_at = key.transitioned_at.0;
            format!(
                "index={index}, stage={}, service_name={:?}, transitioned_at_unix_ms={}, transitioned_at_hlc={}, canonical_id={}, entry_id={}",
                key.stage,
                key.service_name.as_str(),
                transitioned_at.to_unix_millis().as_u64(),
                transitioned_at.as_u64(),
                key.canonical_id,
                key.canonical_id.to_base_entry_id(),
            )
        }
        Some(index @ IndexId::EntryByStage) => {
            let key = payload
                .into_decoder::<EntryByStageKey>()
                .decode_all()
                .ok()?;
            let transitioned_at = key.transitioned_at.0;
            format!(
                "index={index}, stage={}, transitioned_at_unix_ms={}, transitioned_at_hlc={}, canonical_id={}, entry_id={}",
                key.stage,
                transitioned_at.to_unix_millis().as_u64(),
                transitioned_at.as_u64(),
                key.canonical_id,
                key.canonical_id.to_base_entry_id(),
            )
        }
        Some(index @ IndexId::EntryNextAtByStage) => {
            let key = payload
                .into_decoder::<EntryNextAtByStageKey>()
                .decode_all()
                .ok()?;
            format!(
                "index={index}, stage={}, next_at_unix_ms={}, seq={}, canonical_id={}, entry_id={}",
                key.stage,
                key.next_at.as_unix_millis().as_u64(),
                key.seq,
                key.canonical_id,
                key.canonical_id.to_base_entry_id(),
            )
        }
        Some(index @ IndexId::BusyVQueue) => {
            let key = payload.into_decoder::<BusyVQueueKey>().decode_all().ok()?;
            format!(
                "index={index}, total_non_completed={}, last_modified_unix_ms={}, last_modified_hlc={}, scope={:?}, vqueue_id={}",
                key.total_non_completed.0,
                key.last_modified.0.to_unix_millis().as_u64(),
                key.last_modified.0.as_u64(),
                key.scope,
                key.vqueue_id,
            )
        }
        Some(index @ IndexId::EntryNextAtByStageService) => {
            let key = payload
                .into_decoder::<EntryNextAtByStageServiceKey>()
                .decode_all()
                .ok()?;
            format!(
                "index={index}, stage={}, service_name={:?}, next_at_unix_ms={}, seq={}, canonical_id={}, entry_id={}",
                key.stage,
                key.service_name.as_str(),
                key.next_at.as_unix_millis().as_u64(),
                key.seq,
                key.canonical_id,
                key.canonical_id.to_base_entry_id(),
            )
        }
        Some(index @ IndexId::EntryByVirtualObjectStage) => {
            let key = payload
                .into_decoder::<EntryByVirtualObjectStageKey>()
                .decode_all()
                .ok()?;
            format!(
                "index={index}, service_name={:?}, scope={:?}, key={:?}, stage={}, transitioned_at_unix_ms={}, transitioned_at_hlc={}, canonical_id={}, entry_id={}",
                key.service_name.as_str(),
                key.scope,
                key.key,
                key.stage,
                key.transitioned_at.0.to_unix_millis().as_u64(),
                key.transitioned_at.0.as_u64(),
                key.canonical_id,
                key.canonical_id.to_base_entry_id(),
            )
        }
        Some(index @ IndexId::EntryNextAtByVirtualObjectStage) => {
            let key = payload
                .into_decoder::<EntryNextAtByVirtualObjectStageKey>()
                .decode_all()
                .ok()?;
            format!(
                "index={index}, service_name={:?}, scope={:?}, key={:?}, stage={}, next_at_unix_ms={}, seq={}, canonical_id={}, entry_id={}",
                key.service_name.as_str(),
                key.scope,
                key.key,
                key.stage,
                key.next_at.as_unix_millis().as_u64(),
                key.seq,
                key.canonical_id,
                key.canonical_id.to_base_entry_id(),
            )
        }
        None => format!("payload=0x{}", hex_encode(payload.remaining)),
    };

    Some(format!(
        "index_id={}, partition_id={}, {details}",
        prefix.index_id_raw(),
        prefix.partition_id(),
    ))
}

/// Statistics are keyed by a replica-local index ID; the payload's meaning
/// depends on the registered definition, so it is shown raw.
pub fn decode_stat_key(key: &[u8]) -> Option<String> {
    let (prefix, kind, payload) = AggregateKeyPrefix::decode(key).ok()?;
    Some(format!(
        "partition={}, index={}, stat_kind={kind:?}, payload={payload:02x?}",
        prefix.partition_id(),
        prefix.id()
    ))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cmp::Reverse;

    use restate_partition_store::index::SecondaryIndexKey;
    use restate_partition_store::keys::{KeyEncode, KeyKind};
    use restate_storage_api::vqueue_table::Stage;
    use restate_types::clock::{RoughTimestamp, UniqueTimestamp};
    use restate_types::identifiers::{BaseEntryId, CanonicalEntryId, InvocationId, InvocationUuid};
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::{Seq, VQueueId};
    use zerocopy::IntoBytes;

    use super::*;
    use crate::util::decode_value::{DecodedContent, decode_value};

    /// An entry index key under its static ID, which predates the registry.
    fn legacy_index_key(id: IndexId, key: impl KeyEncode) -> Vec<u8> {
        let mut bytes = IndexKeyPrefix::new(PartitionId::from(7), id.as_u32())
            .as_bytes()
            .to_vec();
        key.encode(&mut bytes);
        bytes
    }

    #[test]
    fn decodes_secondary_index_keys_and_handles_unknown_or_malformed_keys() {
        let id = InvocationId::from_parts(3337, InvocationUuid::from_u128(42));
        let canonical_id = BaseEntryId::from(id).canonicalize(Seq::MAX);
        let at = UniqueTimestamp::try_from_parts(100, 7).unwrap();
        let key = legacy_index_key(
            IndexId::EntryByStageService,
            EntryByStageServiceKey::borrowed(Stage::Running, "Morder", Reverse(at), canonical_id),
        );
        assert_eq!(
            decode_secondary_index_key(&key).unwrap(),
            format!(
                "index_id=1, partition_id=7, index=EntryByStageService, stage=running, service_name=\"Morder\", transitioned_at_unix_ms={}, transitioned_at_hlc={}, canonical_id={canonical_id}, entry_id={id}",
                at.to_unix_millis().as_u64(),
                at.as_u64(),
            )
        );

        for len in 0..key.len() {
            assert!(
                decode_secondary_index_key(&key[..len]).is_none(),
                "length {len}"
            );
        }
        let mut trailing = key.clone();
        trailing.push(0);
        assert!(decode_secondary_index_key(&trailing).is_none());
        let mut bad_padding = key.clone();
        bad_padding[2] = 1;
        assert!(decode_secondary_index_key(&bad_padding).is_none());
        let mut bad_id = key.clone();
        let primary_start = key.len() - CanonicalEntryId::RAW_BYTES_LEN;
        bad_id[primary_start..].fill(0);
        assert!(decode_secondary_index_key(&bad_id).is_none());
        let mut bad_timestamp = key.clone();
        bad_timestamp[primary_start - size_of::<u64>()..primary_start].fill(0);
        assert!(decode_secondary_index_key(&bad_timestamp).is_none());

        let mut unknown = key;
        unknown[6..10].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(
            decode_secondary_index_key(&unknown).unwrap(),
            format!(
                "index_id={}, partition_id=7, payload=0x{}",
                u32::MAX,
                hex_encode(&unknown[IndexKeyPrefix::SERIALIZED_LENGTH..])
            )
        );

        let key = legacy_index_key(
            IndexId::EntryByStage,
            EntryByStageKey::borrowed(Stage::Running, Reverse(at), canonical_id),
        );
        assert_eq!(
            decode_secondary_index_key(&key).unwrap(),
            format!(
                "index_id=2, partition_id=7, index=EntryByStage, stage=running, transitioned_at_unix_ms={}, transitioned_at_hlc={}, canonical_id={canonical_id}, entry_id={id}",
                at.to_unix_millis().as_u64(),
                at.as_u64(),
            )
        );
        assert!(decode_secondary_index_key(&key[..key.len() - 1]).is_none());

        let mut key = legacy_index_key(
            IndexId::EntryNextAtByStage,
            EntryNextAtByStageKey::borrowed(
                Stage::Inbox,
                RoughTimestamp::MAX,
                canonical_id.seq(),
                canonical_id,
            ),
        );
        assert_eq!(
            decode_secondary_index_key(&key).unwrap(),
            format!(
                "index_id=3, partition_id=7, index=EntryNextAtByStage, stage=inbox, next_at_unix_ms={}, seq={}, canonical_id={canonical_id}, entry_id={id}",
                RoughTimestamp::MAX.as_unix_millis().as_u64(),
                canonical_id.seq(),
            )
        );
        key.push(0);
        assert!(decode_secondary_index_key(&key).is_none());

        let key = legacy_index_key(
            IndexId::EntryNextAtByStageService,
            EntryNextAtByStageServiceKey::borrowed(
                Stage::Inbox,
                "Morder",
                RoughTimestamp::MAX,
                canonical_id.seq(),
                canonical_id,
            ),
        );
        assert_eq!(
            decode_secondary_index_key(&key).unwrap(),
            format!(
                "index_id=6, partition_id=7, index=EntryNextAtByStageService, stage=inbox, service_name=\"Morder\", next_at_unix_ms={}, seq={}, canonical_id={canonical_id}, entry_id={id}",
                RoughTimestamp::MAX.as_unix_millis().as_u64(),
                canonical_id.seq(),
            )
        );
        for scope in [None, Some("tenant")] {
            let key = legacy_index_key(
                IndexId::EntryByVirtualObjectStage,
                EntryByVirtualObjectStageKey::borrowed(
                    "Morder",
                    scope,
                    "object",
                    Stage::Suspended,
                    Reverse(at),
                    canonical_id,
                ),
            );
            assert_eq!(
                decode_secondary_index_key(&key).unwrap(),
                format!(
                    "index_id=5, partition_id=7, index=EntryByVirtualObjectStage, service_name=\"Morder\", scope={scope:?}, key=\"object\", stage=suspended, transitioned_at_unix_ms={}, transitioned_at_hlc={}, canonical_id={canonical_id}, entry_id={id}",
                    at.to_unix_millis().as_u64(),
                    at.as_u64(),
                )
            );
            let key = legacy_index_key(
                IndexId::EntryNextAtByVirtualObjectStage,
                EntryNextAtByVirtualObjectStageKey::borrowed(
                    "Morder",
                    scope,
                    "object",
                    Stage::Suspended,
                    RoughTimestamp::MAX,
                    canonical_id.seq(),
                    canonical_id,
                ),
            );
            assert_eq!(
                decode_secondary_index_key(&key).unwrap(),
                format!(
                    "index_id=7, partition_id=7, index=EntryNextAtByVirtualObjectStage, service_name=\"Morder\", scope={scope:?}, key=\"object\", stage=suspended, next_at_unix_ms={}, seq={}, canonical_id={canonical_id}, entry_id={id}",
                    RoughTimestamp::MAX.as_unix_millis().as_u64(),
                    canonical_id.seq(),
                )
            );
        }

        let qid = VQueueId::custom(3337, "busy");
        let mut key = Vec::new();
        BusyVQueueKey::borrowed(Reverse(42), Reverse(at), Some("tenant"), qid.clone())
            .encode_key(PartitionId::from(7), &mut key);
        assert_eq!(
            decode_secondary_index_key(&key).unwrap(),
            format!(
                "index_id=4, partition_id=7, index=BusyVQueue, total_non_completed=42, last_modified_unix_ms={}, last_modified_hlc={}, scope=Some(\"tenant\"), vqueue_id={qid}",
                at.to_unix_millis().as_u64(),
                at.as_u64(),
            )
        );
        let mut value = vec![Stage::Inbox as u8];
        value.extend_from_slice(&42_u64.to_be_bytes());
        assert_eq!(
            decode_value(KeyKind::SecondaryIndex, &key, &value).to_string(),
            "StageCounts([(Inbox, 42)])"
        );
        assert_eq!(
            decode_value(KeyKind::SecondaryIndex, &key, &[]).to_string(),
            "StageCounts([])"
        );
        assert!(matches!(
            decode_value(KeyKind::SecondaryIndex, &key, &value[..8]).content,
            DecodedContent::Error(_)
        ));
    }

    /// A raw index key: `xI | padding | partition | index ID | payload`.
    fn raw_index_key(padding: u16, partition: u16, index_id: u32, payload: &[u8]) -> Vec<u8> {
        let mut key = b"xI".to_vec();
        key.extend_from_slice(&padding.to_be_bytes());
        key.extend_from_slice(&partition.to_be_bytes());
        key.extend_from_slice(&index_id.to_be_bytes());
        key.extend_from_slice(payload);
        key
    }

    #[test]
    fn index_keys_keep_their_payload_raw_and_malformed_prefixes_are_refused() {
        // A registered index's ID says nothing about its layout: the payload stays raw.
        let key = raw_index_key(0, 7, 64, b"payload");
        assert_eq!(
            decode_secondary_index_key(&key).as_deref(),
            Some(
                format!(
                    "index_id=64, partition_id=7, payload=0x{}",
                    hex_encode(b"payload")
                )
                .as_str()
            )
        );
        assert_eq!(
            decode_secondary_index_key(&raw_index_key(0, 7, 65, b"")).as_deref(),
            Some("index_id=65, partition_id=7, payload=0x")
        );
        // A truncated header, a nonzero padding or another key kind are refused.
        assert_eq!(decode_secondary_index_key(&key[..9]), None);
        assert_eq!(
            decode_secondary_index_key(&raw_index_key(1, 7, 64, b"")),
            None
        );
        let mut other = key;
        other[..2].copy_from_slice(b"zR");
        assert_eq!(decode_secondary_index_key(&other), None);
    }
}
