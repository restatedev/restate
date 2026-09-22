// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;

use restate_storage_api::vqueue_table::{EntryChange, EntryContext, EntryStateRef};

use crate::PartitionStoreTransaction;
use crate::index::InvocationByServiceStageKey;

/// Maintains the invocation index alongside the source entry's lifecycle writes.
/// This alone does not establish index completeness for pre-existing stores;
/// query access must wait for a separate activation/backfill step.
pub(super) fn on_entry_change(
    storage: &mut PartitionStoreTransaction<'_>,
    context: &EntryContext<'_>,
    change: &EntryChange<'_>,
) {
    let fields = |entry: &EntryStateRef<'_>| (entry.stage, entry.stats.transitioned_at);
    let old = change.before().map(fields);
    let new = change.after().map(fields);
    // Service and primary identity are stable for this entry. Status/metadata-only
    // changes need no encoding, and must not overwrite the existing index entry.
    if old == new {
        return;
    }
    let entry = match change {
        EntryChange::Insert { after } | EntryChange::Update { after, .. } => after,
        EntryChange::Delete { before } => before,
    };
    let Some(invocation_id) = entry.base_entry_id(context).to_invocation_id() else {
        return;
    };
    let key = |(stage, at)| {
        InvocationByServiceStageKey::borrowed(
            context.target.service(),
            stage,
            Reverse(at),
            invocation_id,
        )
    };
    let old = old.map(key);
    let new = new.map(key);
    storage.update_secondary_index(old.as_ref(), new.as_ref());
}
