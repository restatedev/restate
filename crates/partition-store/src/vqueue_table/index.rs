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
use crate::index::{
    EntryByServiceStageKey, EntryByStageKey, EntryNextAtByStageKey, SecondaryIndexKey,
};

/// Maintains the entry indexes alongside the source entry's lifecycle writes.
/// This alone does not establish index completeness for pre-existing stores;
/// query access must wait for a separate activation/backfill step.
pub(super) fn on_entry_change(
    storage: &mut PartitionStoreTransaction<'_>,
    context: &EntryContext<'_>,
    change: &EntryChange<'_>,
) {
    let canonical_id = |entry: &EntryStateRef<'_>| {
        entry
            .entry_key
            .to_canonical_entry_id(context.qid.partition_key())
    };
    update_index(
        storage,
        change,
        |entry| {
            (
                entry.stage,
                Reverse(entry.stats.transitioned_at),
                canonical_id(entry),
            )
        },
        |(stage, at, id)| EntryByServiceStageKey::borrowed(context.target.service(), stage, at, id),
    );
    update_index(
        storage,
        change,
        |entry| {
            (
                entry.stage,
                Reverse(entry.stats.transitioned_at),
                entry.status,
                canonical_id(entry),
            )
        },
        |(stage, at, status, id)| EntryByStageKey::borrowed(stage, at, status, id),
    );
    update_index(
        storage,
        change,
        |entry| {
            (
                entry.stage,
                entry.entry_key.run_at(),
                entry.status,
                canonical_id(entry),
            )
        },
        |(stage, next_at, status, id)| EntryNextAtByStageKey::borrowed(stage, next_at, status, id),
    );
}

/// Compare each index's projection independently before allocating or encoding keys.
/// Unchanged membership must not overwrite a key and break its SingleDelete lifetime.
fn update_index<P: Eq, K: SecondaryIndexKey>(
    storage: &mut PartitionStoreTransaction<'_>,
    change: &EntryChange<'_>,
    project: impl Fn(&EntryStateRef<'_>) -> P,
    key: impl Fn(P) -> K,
) {
    let old = change.before().map(&project);
    let new = change.after().map(project);
    if old == new {
        return;
    }
    let old = old.map(&key);
    let new = new.map(key);
    storage.update_secondary_index(old.as_ref(), new.as_ref());
}
