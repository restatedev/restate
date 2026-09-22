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
    EntryByServiceStageKey, EntryByStageKey, EntryNextAtByStageKey, IndexId,
};
use restate_partition_store::keys::IndexKeyPrefix;
use restate_partition_store::stats::aggregated::{
    DeploymentLoadKey, ServiceLoadKey, VirtualObjectLoadKey,
};
use restate_partition_store::stats::{StatId, StatKeyPrefix};
use restate_storage_api::stats::deployment_load::DeploymentLoad;
use restate_storage_api::stats::service_load::ServiceLoad;
use restate_storage_api::stats::virtual_object_load::VirtualObjectLoad;

use super::hex_encode;

/// Decodes the index identity and known payloads, preserving unknown payloads as hex.
pub fn decode_secondary_index_key(key: &[u8]) -> Option<String> {
    let (prefix, payload) = IndexKeyPrefix::decode_prefix(key).ok()?;
    let details = match prefix.index_id() {
        Some(index @ IndexId::EntryByServiceStage) => {
            let key = payload
                .into_decoder::<EntryByServiceStageKey>()
                .decode_all()
                .ok()?;
            // Decoding removes the descending-order transformation. Preserve the
            // full HLC alongside Unix milliseconds so logical ticks remain visible.
            let transitioned_at = key.transitioned_at.0;
            format!(
                "index={index}, service_name={:?}, stage={}, transitioned_at_unix_ms={}, transitioned_at_hlc={}, canonical_id={}, entry_id={}",
                key.service_name.as_str(),
                key.stage,
                transitioned_at.to_unix_millis().as_u64(),
                transitioned_at.as_u64(),
                key.canonical_id,
                key.canonical_id.as_base_entry_id(),
            )
        }
        Some(index @ IndexId::EntryByStage) => {
            let key = payload
                .into_decoder::<EntryByStageKey>()
                .decode_all()
                .ok()?;
            let transitioned_at = key.transitioned_at.0;
            format!(
                "index={index}, stage={}, transitioned_at_unix_ms={}, transitioned_at_hlc={}, status={}, canonical_id={}, entry_id={}",
                key.stage,
                transitioned_at.to_unix_millis().as_u64(),
                transitioned_at.as_u64(),
                key.status,
                key.canonical_id,
                key.canonical_id.as_base_entry_id(),
            )
        }
        Some(index @ IndexId::EntryNextAtByStage) => {
            let key = payload
                .into_decoder::<EntryNextAtByStageKey>()
                .decode_all()
                .ok()?;
            format!(
                "index={index}, stage={}, next_at_unix_ms={}, status={}, canonical_id={}, entry_id={}",
                key.stage,
                key.next_at.as_unix_millis().as_u64(),
                key.status,
                key.canonical_id,
                key.canonical_id.as_base_entry_id(),
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

/// Decodes an aggregated statistic key, including known statistic-specific payloads.
pub fn decode_aggregated_stat_key(key: &[u8]) -> Option<String> {
    let (prefix, payload) = StatKeyPrefix::decode_prefix(key).ok()?;
    let stat_id = prefix.stat_id_raw();
    let details = match prefix.stat_id() {
        Some(StatId::ServiceLoad) => {
            let key = payload
                .into_decoder::<ServiceLoad, ServiceLoadKey>()
                .try_full_decode::<ServiceLoad>()
                .ok()?;
            format!(
                "service_name={:?}, kind={}, handler={:?}",
                key.service_name, key.kind, key.handler
            )
        }
        Some(StatId::DeploymentLoad) => {
            let key = payload
                .into_decoder::<DeploymentLoad, DeploymentLoadKey>()
                .try_full_decode::<DeploymentLoad>()
                .ok()?;
            format!(
                "deployment_id={}, service_name={}",
                key.deployment_id, key.service_name
            )
        }
        Some(StatId::VirtualObjectLoad) => {
            let key = payload
                .into_decoder::<VirtualObjectLoad, VirtualObjectLoadKey>()
                .try_full_decode::<VirtualObjectLoad>()
                .ok()?;
            format!(
                "scope={:?}, service_name={}, key={}, kind={}, handler={:?}",
                key.scope, key.service_name, key.key, key.kind, key.handler,
            )
        }
        None => format!("payload=0x{}", hex_encode(payload.remaining)),
    };

    Some(format!(
        "stat_kind={:?}, stat_id={}, partition_id={}, {details}",
        prefix.stat_kind(),
        stat_id,
        prefix.partition_id()
    ))
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use restate_partition_store::index::SecondaryIndexKey;
    use restate_partition_store::stats::Stat;
    use restate_storage_api::vqueue_table::{Stage, Status};
    use restate_types::ServiceName;
    use restate_types::clock::{RoughTimestamp, UniqueTimestamp};
    use restate_types::identifiers::{BaseEntryId, CanonicalEntryId, InvocationId, InvocationUuid};
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::{EntryKind, Seq};

    use super::*;

    fn service_load_key() -> Vec<u8> {
        let mut key = Vec::new();
        ServiceLoad::encode_key(
            PartitionId::from(7),
            ServiceLoadKey {
                service_name: ServiceName::new("greeter"),
                kind: EntryKind::Invocation,
                handler: None,
            },
            &mut key,
        );
        key
    }

    #[test]
    fn decodes_known_statistic_keys() {
        let key = service_load_key();
        let decoded = decode_aggregated_stat_key(&key).unwrap();
        assert!(decoded.contains("stat_kind=StageStatusBucketedGauge"));
        assert!(decoded.contains("stat_id=1, partition_id=7"));
        assert!(decoded.contains("service_name=") && decoded.contains("greeter"));

        assert!(decode_aggregated_stat_key(&key[..StatKeyPrefix::SERIALIZED_LENGTH - 1]).is_none());

        let mut trailing_bytes = key;
        trailing_bytes.push(0);
        assert!(decode_aggregated_stat_key(&trailing_bytes).is_none());

        let mut unknown = service_load_key();
        unknown[6..8].copy_from_slice(&1023_u16.to_be_bytes());
        let decoded = decode_aggregated_stat_key(&unknown).unwrap();
        assert!(decoded.contains("stat_id=1023") && decoded.contains("payload=0x"));

        let mut key = Vec::new();
        DeploymentLoad::encode_key(
            PartitionId::from(9),
            DeploymentLoadKey::borrowed("deployment-1", "greeter"),
            &mut key,
        );
        let decoded = decode_aggregated_stat_key(&key).unwrap();
        assert!(decoded.contains("stat_id=2, partition_id=9"));
        assert!(decoded.contains("deployment_id=") && decoded.contains("deployment-1"));
    }

    #[test]
    fn decodes_secondary_index_keys_and_handles_unknown_or_malformed_keys() {
        let id = InvocationId::from_parts(3337, InvocationUuid::from_u128(42));
        let canonical_id = BaseEntryId::from(id).canonicalize(Seq::MAX);
        let at = UniqueTimestamp::try_from_parts(100, 7).unwrap();
        let mut key = Vec::new();
        EntryByServiceStageKey::borrowed("Morder", Stage::Running, Reverse(at), canonical_id)
            .encode_key(PartitionId::from(7), &mut key);
        assert_eq!(
            decode_secondary_index_key(&key).unwrap(),
            format!(
                "index_id=1, partition_id=7, index=EntryByServiceStage, service_name=\"Morder\", stage=running, transitioned_at_unix_ms={}, transitioned_at_hlc={}, canonical_id={canonical_id}, entry_id={id}",
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

        let mut key = Vec::new();
        EntryByStageKey::borrowed(Stage::Running, Reverse(at), Status::Started, canonical_id)
            .encode_key(PartitionId::from(7), &mut key);
        assert_eq!(
            decode_secondary_index_key(&key).unwrap(),
            format!(
                "index_id=2, partition_id=7, index=EntryByStage, stage=running, transitioned_at_unix_ms={}, transitioned_at_hlc={}, status=started, canonical_id={canonical_id}, entry_id={id}",
                at.to_unix_millis().as_u64(),
                at.as_u64(),
            )
        );
        assert!(decode_secondary_index_key(&key[..key.len() - 1]).is_none());

        let mut key = Vec::new();
        EntryNextAtByStageKey::borrowed(
            Stage::Inbox,
            RoughTimestamp::MAX,
            Status::Scheduled,
            canonical_id,
        )
        .encode_key(PartitionId::from(7), &mut key);
        assert_eq!(
            decode_secondary_index_key(&key).unwrap(),
            format!(
                "index_id=3, partition_id=7, index=EntryNextAtByStage, stage=inbox, next_at_unix_ms={}, status=scheduled, canonical_id={canonical_id}, entry_id={id}",
                RoughTimestamp::MAX.as_unix_millis().as_u64(),
            )
        );
        key.push(0);
        assert!(decode_secondary_index_key(&key).is_none());
    }
}
