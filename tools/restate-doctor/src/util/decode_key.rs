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

use restate_partition_store::stats::aggregated::{
    DeploymentStats, DeploymentStatsKey, ServiceStats, ServiceStatsKey, VirtualObjectStats,
    VirtualObjectStatsKey,
};
use restate_partition_store::stats::{Stat, StatId, StatKeyPrefix};

use super::hex_encode;

/// Decodes an aggregated statistic key, including known statistic-specific payloads.
pub fn decode_aggregated_stat_key(key: &[u8]) -> Option<String> {
    let (prefix, payload) = StatKeyPrefix::decode_prefix(key).ok()?;
    let stat_id = prefix.stat_id_raw();
    let details = match prefix.stat_id() {
        Some(StatId::ServiceStats) => {
            if prefix.stat_kind() != ServiceStats::STAT_KIND {
                return None;
            }
            let key = payload
                .into_decoder::<ServiceStats, ServiceStatsKey>()
                .try_full_decode::<ServiceStats>()
                .ok()?;
            format!(
                "service_name={:?}, kind={}, handler={:?}",
                key.service_name, key.kind, key.handler
            )
        }
        Some(StatId::DeploymentStats) => {
            if prefix.stat_kind() != DeploymentStats::STAT_KIND {
                return None;
            }
            let key = payload
                .into_decoder::<DeploymentStats, DeploymentStatsKey>()
                .try_full_decode::<DeploymentStats>()
                .ok()?;
            format!(
                "deployment_id={:?}, service_name={:?}",
                key.deployment_id, key.service_name
            )
        }
        Some(StatId::VirtualObjectStats) => {
            if prefix.stat_kind() != VirtualObjectStats::STAT_KIND {
                return None;
            }
            let key = payload
                .into_decoder::<VirtualObjectStats, VirtualObjectStatsKey>()
                .try_full_decode::<VirtualObjectStats>()
                .ok()?;
            format!(
                "service_name={:?}, scope={:?}, key={:?}, handler={:?}, kind={}, partition_key={}",
                key.service_name, key.scope, key.key, key.handler, key.kind, key.partition_key,
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
    use restate_partition_store::stats::StatKind;
    use restate_types::ServiceName;
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::EntryKind;

    use super::*;

    fn service_stats_key() -> Vec<u8> {
        let mut key = Vec::new();
        ServiceStats::encode_key(
            PartitionId::from(7),
            ServiceStatsKey {
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
        let key = service_stats_key();
        let decoded = decode_aggregated_stat_key(&key).unwrap();
        assert!(decoded.contains("stat_kind=StageStatusBucketedGauge"));
        assert!(decoded.contains("stat_id=1, partition_id=7"));
        assert!(decoded.contains("service_name=") && decoded.contains("greeter"));

        let mut wrong_kind = key.clone();
        wrong_kind[8] = StatKind::AggregatedGauge as u8;
        assert!(decode_aggregated_stat_key(&wrong_kind).is_none());

        assert!(decode_aggregated_stat_key(&key[..StatKeyPrefix::SERIALIZED_LENGTH - 1]).is_none());

        let mut trailing_bytes = key;
        trailing_bytes.push(0);
        assert!(decode_aggregated_stat_key(&trailing_bytes).is_none());

        let mut unknown = service_stats_key();
        unknown[6..8].copy_from_slice(&1023_u16.to_be_bytes());
        let decoded = decode_aggregated_stat_key(&unknown).unwrap();
        assert!(decoded.contains("stat_id=1023") && decoded.contains("payload=0x"));

        let mut key = Vec::new();
        DeploymentStats::encode_key(
            PartitionId::from(9),
            DeploymentStatsKey::borrowed("deployment-1", "greeter"),
            &mut key,
        );
        let decoded = decode_aggregated_stat_key(&key).unwrap();
        assert!(decoded.contains("stat_id=2, partition_id=9"));
        assert!(decoded.contains("deployment_id=") && decoded.contains("deployment-1"));

        key.clear();
        VirtualObjectStats::encode_key(
            PartitionId::from(9),
            VirtualObjectStatsKey::borrowed(
                "counter",
                Some("tenant"),
                "object",
                None::<&str>,
                EntryKind::StateMutation,
                3337,
            ),
            &mut key,
        );
        let decoded = decode_aggregated_stat_key(&key).unwrap();
        assert!(decoded.contains("stat_id=3, partition_id=9"));
        assert!(decoded.contains("scope=Some(\"tenant\")"));
        assert!(decoded.contains("key=\"object\""));
        assert!(decoded.contains("handler=None"));
        assert!(decoded.contains("partition_key=3337"));
    }
}
