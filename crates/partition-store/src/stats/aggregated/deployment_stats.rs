// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::codecs::{ServiceNameCodec, StringCodec};
use crate::stats::macros::define_aggregated_stat;

use super::StageStatusGauge;

define_aggregated_stat!(
    /// Key for current entry counts grouped by pinned deployment and service.
    ///
    /// The payload orders `deployment_id -> service_name` within each physical
    /// partition. Values combine handlers, scopes, and entry kinds into sparse
    /// stage/status counters. Unpinned entries do not contribute; repinning moves
    /// the contribution even when the entry's stage/status is unchanged.
    table: DeploymentStats,
    value: StageStatusGauge,
    key: DeploymentStatsKey(
        deployment_id: StringCodec => str,
        service_name: ServiceNameCodec => str,
    ),
);

#[cfg(test)]
mod tests {
    use restate_types::sharding::PartitionId;

    use crate::stats::Stat;

    use super::*;

    #[test]
    fn generated_prefixes_match_complete_key_field_boundaries() {
        let partition_id = PartitionId::from(8);
        let mut key = Vec::new();
        DeploymentStats::encode_key(
            partition_id,
            DeploymentStatsKey::borrowed("dp_1", "alpha"),
            &mut key,
        );

        let mut prefix = Vec::new();
        for fields in 0..=2 {
            prefix.clear();
            let builder = DeploymentStatsKey::prefix(partition_id, &mut prefix);
            match fields {
                0 => {}
                1 => {
                    builder.deployment_id("dp_1");
                }
                _ => {
                    builder.deployment_id("dp_1").service_name("alpha");
                }
            }
            assert!(key.starts_with(&prefix));
        }
        assert_eq!(prefix, key);
    }
}
