// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_partition_store::stats::aggregated::{DeploymentLoadKey, StageCounts};

use super::schema::SysDeploymentStatsBuilder;

#[inline]
pub(crate) fn append_deployment_stats_row(
    builder: &mut SysDeploymentStatsBuilder,
    key: DeploymentLoadKey,
    counts: StageCounts,
) {
    for (stage, value) in counts.iter() {
        let mut row = builder.row();

        row.deployment_id(&key.deployment_id);
        row.fmt_service_name(&key.service_name);
        row.fmt_stage(stage);
        row.num_entries(value);
    }
}
