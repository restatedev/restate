// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::blob_table::BlobEntry;
use restate_types::blob::BlobKind;
use restate_types::identifiers::WithPartitionKey;

use super::schema::SysBlobBuilder;

#[inline]
pub(crate) fn append_blob_row(builder: &mut SysBlobBuilder, entry: BlobEntry<'_>) {
    let mut row = builder.row();
    row.partition_key(entry.id.partition_key());
    if row.is_id_defined() {
        row.fmt_id(entry.id);
    }
    if row.is_created_at_defined() {
        row.created_at(entry.id.timestamp().to_unix_millis().as_u64() as i64);
    }
    row.kind(match entry.kind {
        BlobKind::Generic => "generic",
        BlobKind::Output => "output",
        BlobKind::State => "state",
    });
    row.ref_count(entry.ref_count.into());
    row.payload_length(entry.payload.len() as u64);
}
