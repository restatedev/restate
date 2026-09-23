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
    DeploymentLoadKey, ServiceLoadKey, VirtualObjectLoadKey,
};
use restate_partition_store::stats::{StatId, StatKeyPrefix};
use restate_storage_api::stats::deployment_load::DeploymentLoad;
use restate_storage_api::stats::service_load::ServiceLoad;
use restate_storage_api::stats::virtual_object_load::VirtualObjectLoad;

use super::hex_encode;

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
    use restate_partition_store::stats::Stat;
    use restate_types::ServiceName;
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::EntryKind;

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
}
