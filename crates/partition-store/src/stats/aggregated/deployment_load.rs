// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::stats::deployment_load::DeploymentLoad;
use restate_types::ServiceName;
use restate_util_string::ReString;

use crate::stats::macros::define_aggregated_stat;

use super::StageGauge;

// A gauge for the number of vqueue entries for a stage in deployment id.
define_aggregated_stat!(
    table: DeploymentLoad,
    value: StageGauge,
    key: DeploymentLoadKey(
        deployment_id: ReString => str,
        service_name: ServiceName => str,
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
        DeploymentLoad::encode_key(
            partition_id,
            DeploymentLoadKey::borrowed("dp_1", "alpha"),
            &mut key,
        );

        let mut prefix = Vec::new();
        for fields in 0..=2 {
            prefix.clear();
            let builder = DeploymentLoadKey::prefix(partition_id, &mut prefix);
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
    }
}
