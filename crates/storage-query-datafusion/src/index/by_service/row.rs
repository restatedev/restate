// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_partition_store::index::EntryByStageServiceKeyView;
use restate_storage_api::Result;

use super::schema::IdxEntryByServiceBuilder;

pub(super) fn append_row(
    builder: &mut IdxEntryByServiceBuilder,
    key: EntryByStageServiceKeyView<'_>,
) -> Result<()> {
    let mut row = builder.row();
    if row.is_service_name_defined() {
        row.service_name(key.service_name.decode()?);
    }

    if row.is_stage_defined() {
        let stage = key.stage.decode()?;
        row.stage(stage.as_str());
    }

    if row.is_transitioned_at_defined() {
        let transitioned_at = key.transitioned_at.decode()?;
        row.transitioned_at(transitioned_at.0.to_unix_millis().as_u64() as i64);
    }

    if row.is_canonical_id_defined() || row.is_entry_id_defined() || row.is_partition_key_defined()
    {
        let id = key.canonical_id.decode()?;

        if row.is_canonical_id_defined() {
            row.fmt_canonical_id(id);
        }

        if row.is_entry_id_defined() {
            row.fmt_entry_id(id.to_base_entry_id());
        }

        if row.is_partition_key_defined() {
            row.partition_key(id.partition_key());
        }
    }

    Ok(())
}
