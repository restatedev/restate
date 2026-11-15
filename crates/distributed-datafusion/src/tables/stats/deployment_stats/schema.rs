// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_query_api::tables::stats::deployment_stats::DeploymentStats;

crate::tables::table_adapter!(pub(crate) SysDeploymentStatsTable, DeploymentStats);

/// The registered instances of every partition (see `tables::stats`).
#[derive(Debug, Clone)]
pub(crate) struct DeploymentStatsSource;

impl crate::QuerySource for DeploymentStatsSource {
    type Schema = DeploymentStats;

    fn identity() -> restate_util_string::ReString {
        "sys_deployment_stats.v2".into()
    }
}

impl crate::sharding::PartitionSource for DeploymentStatsSource {}
