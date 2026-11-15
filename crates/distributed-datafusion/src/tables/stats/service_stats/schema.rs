// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_query_api::tables::stats::service_stats::ServiceStats;

crate::tables::table_adapter!(pub(crate) SysServiceStatsTable, ServiceStats);

/// The registered instances of every partition (see `tables::stats`).
#[derive(Debug, Clone)]
pub(crate) struct ServiceStatsSource;

impl crate::QuerySource for ServiceStatsSource {
    type Schema = ServiceStats;

    fn identity() -> restate_util_string::ReString {
        "sys_service_stats.v2".into()
    }
}

impl crate::sharding::PartitionSource for ServiceStatsSource {}
