// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enum_map::EnumMap;

use restate_partition_store::stats::aggregated::{StageCounts, VirtualObjectLoadKey};
use restate_storage_api::vqueue_table::Stage;
use restate_types::sharding::PartitionId;

use super::schema::SysVirtualObjectStatsBuilder;

#[inline]
pub(crate) fn append_virtual_object_stats_row(
    builder: &mut SysVirtualObjectStatsBuilder,
    partition_id: PartitionId,
    key: VirtualObjectLoadKey,
    counts: StageCounts,
) {
    let mut row = builder.row();
    row.partition_id(partition_id.into());

    if row.is_service_name_defined() {
        row.fmt_service_name(&key.service_name);
    }

    if row.is_key_defined() {
        row.key(&key.key);
    }

    if let Some(scope) = &key.scope {
        row.scope(scope);
    }

    if let Some(handler) = &key.handler {
        row.handler(handler);
    }

    if row.is_kind_defined() {
        row.fmt_kind(key.kind);
    }

    if row.is_partition_key_defined() {
        row.partition_key(key.partition_key);
    }

    // Stored counts are sparse; fill absent stages with zero before appending
    // exactly one value to each projected column.
    let counts: EnumMap<Stage, u64> = counts.iter().collect();
    row.num_inbox(counts[Stage::Inbox]);
    row.num_running(counts[Stage::Running]);
    row.num_suspended(counts[Stage::Suspended]);
    row.num_paused(counts[Stage::Paused]);
    row.num_finished(counts[Stage::Finished]);
}
