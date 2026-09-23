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

use restate_clock::UniqueTimestamp;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{EntryChange, EntryContext, EntryStateRef};
use restate_types::vqueues::VQueueId;

use crate::index::{
    BusyVQueueKey, EntryByStageKey, EntryByStageServiceKey, EntryByVirtualObjectStageKey,
    EntryNextAtByStageKey, EntryNextAtByStageServiceKey, EntryNextAtByVirtualObjectStageKey,
    SecondaryIndexKey,
};
use crate::stats::StatValueCodec;
use crate::stats::aggregated::StageCounts;
use crate::{PartitionStoreTransaction, StorageAccess};

/// Snapshot only the indexed fields before mutating metadata. Scope and identity
/// are unchanged by metadata updates and can be borrowed when constructing keys.
#[derive(PartialEq, Eq)]
pub(super) struct VQueueIndexState {
    total_non_completed: u64,
    last_modified: UniqueTimestamp,
    counts: StageCounts,
}

impl VQueueIndexState {
    pub(super) fn new(meta: &VQueueMeta) -> Self {
        Self {
            total_non_completed: meta.len(),
            last_modified: meta.stats().last_modified_at(),
            counts: meta.stats().into(),
        }
    }
}

pub(super) fn on_vqueue_change(
    storage: &mut PartitionStoreTransaction<'_>,
    qid: &VQueueId,
    meta: &VQueueMeta,
    old: Option<&VQueueIndexState>,
    new: Option<&VQueueIndexState>,
) {
    if old == new {
        return;
    }
    let old_len = old.map_or(0, |state| state.counts.serialized_len());
    let new_len = new.map_or(0, |state| state.counts.serialized_len());
    let values = {
        let buffer = storage.cleared_value_buffer_mut(old_len + new_len);
        if let Some(old) = old {
            old.counts.serialize_to(buffer);
        }
        if let Some(new) = new {
            new.counts.serialize_to(buffer);
        }
        buffer.split()
    };
    let key = |state: &VQueueIndexState| {
        BusyVQueueKey::borrowed(
            Reverse(state.total_non_completed),
            Reverse(state.last_modified),
            meta.scope().as_ref().map(|scope| scope.as_str()),
            qid.clone(),
        )
    };
    let old = old.map(key);
    let new = new.map(key);
    storage.update_covering_secondary_index(
        old.as_ref().map(|key| (key, &values[..old_len])),
        new.as_ref().map(|key| (key, &values[old_len..])),
    );
}

/// Maintains the entry indexes alongside the source entry's lifecycle writes.
/// This alone does not establish index completeness for pre-existing stores.
/// Raw index inspection is possible, but using the index as an authoritative
/// query access path requires a separate activation/backfill step.
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
    let transitioned = |entry: &EntryStateRef<'_>| {
        (
            entry.stage,
            Reverse(entry.stats.transitioned_at),
            canonical_id(entry),
        )
    };
    let next_at =
        |entry: &EntryStateRef<'_>| (entry.stage, entry.entry_key.run_at(), canonical_id(entry));
    update_index(storage, change, transitioned, |(stage, at, id)| {
        EntryByStageServiceKey::borrowed(stage, context.target.service(), at, id)
    });
    update_index(storage, change, transitioned, |(stage, at, id)| {
        EntryByStageKey::borrowed(stage, at, id)
    });
    update_index(storage, change, next_at, |(stage, at, id)| {
        EntryNextAtByStageKey::borrowed(stage, at, id.seq(), id)
    });
    update_index(storage, change, next_at, |(stage, at, id)| {
        EntryNextAtByStageServiceKey::borrowed(stage, context.target.service(), at, id.seq(), id)
    });
    if let Some(key) = context.target.virtual_object_key() {
        update_index(storage, change, transitioned, |(stage, at, id)| {
            EntryByVirtualObjectStageKey::borrowed(
                context.target.service(),
                context.target.scope(),
                key,
                stage,
                at,
                id,
            )
        });
        update_index(storage, change, next_at, |(stage, at, id)| {
            EntryNextAtByVirtualObjectStageKey::borrowed(
                context.target.service(),
                context.target.scope(),
                key,
                stage,
                at,
                id.seq(),
                id,
            )
        });
    }
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
