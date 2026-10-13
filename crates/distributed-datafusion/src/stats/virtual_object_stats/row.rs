// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_partition_store::stats::aggregated::{StageStatusCounts, VirtualObjectLoadKey};

use super::schema::SysVirtualObjectStatsBuilder;

#[inline]
pub(crate) fn append_virtual_object_stats_row(
    builder: &mut SysVirtualObjectStatsBuilder,
    key: VirtualObjectLoadKey,
    counts: StageStatusCounts,
) {
    for (bucket, value) in counts.iter() {
        let mut row = builder.row();
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

        row.fmt_stage(bucket.stage);
        row.fmt_status(bucket.status);
        row.num_entries(value);
    }
}
