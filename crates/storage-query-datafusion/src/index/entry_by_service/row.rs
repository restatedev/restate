// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_partition_store::index::EntryByServiceStageKey;
use restate_types::sharding::PartitionId;

use super::schema::IdxEntryByServiceBuilder;

pub(super) fn append_row(
    builder: &mut IdxEntryByServiceBuilder,
    partition_id: PartitionId,
    key: EntryByServiceStageKey,
) {
    let mut row = builder.row();
    row.partition_id(partition_id.into());
    row.service_name(key.service_name.as_str());
    row.stage(key.stage.as_str());
    row.transitioned_at(key.transitioned_at.0.to_unix_millis().as_u64() as i64);
    if row.is_canonical_id_defined() {
        row.fmt_canonical_id(key.canonical_id);
    }
    if row.is_entry_id_defined() {
        row.fmt_entry_id(key.canonical_id.as_base_entry_id());
    }
    row.partition_key(key.canonical_id.partition_key());
}
