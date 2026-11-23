// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_query_api::tables::stats::virtual_object_stats::VirtualObjectStats;

crate::tables::table_adapter!(pub(crate) SysVirtualObjectStatsTable, VirtualObjectStats);

/// The registered instances of every partition (see `tables::stats`).
#[derive(Debug, Clone)]
pub(crate) struct VirtualObjectStatsSource;

impl crate::QuerySource for VirtualObjectStatsSource {
    type Schema = VirtualObjectStats;

    fn identity() -> restate_util_string::ReString {
        "sys_virtual_object_stats.v2".into()
    }
}

impl crate::sharding::PartitionSource for VirtualObjectStatsSource {
    const ORDERS: &'static [restate_storage_query_api::ordering::NativeOrder] =
        <<restate_partition_store::stats::aggregated::VirtualObjectStatsBinding as restate_storage_query_api::SchemaBinding>::Table as restate_storage_query_api::Table>::NATIVE_ORDERS;
}
