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

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef, Stage};
use restate_types::config::Configuration;
use restate_types::identifiers::CanonicalEntryId;
use restate_types::vqueues::VQueueId;

use crate::index::{
    BusyVQueueKey, EntryByStage, EntryByStageKey, EntryByStageService, EntryByStageServiceKey,
    EntryByVirtualObjectStage, EntryByVirtualObjectStageKey, EntryNextAtByStage,
    EntryNextAtByStageKey, EntryNextAtByStageService, EntryNextAtByStageServiceKey,
    EntryNextAtByVirtualObjectStage, EntryNextAtByVirtualObjectStageKey, RegisteredIndex,
    SecondaryIndex, SecondaryIndexKey,
};
use crate::keys::header::IndexScope;
use crate::registry::{Family, IndexDefinition, IndexKind};
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

/// The entry indexes are registered definitions, applied to each maintained index
/// on every entry write (see [`crate::registry`]). A before-image is present only
/// when the index already reflects the entry (Ready, token, or committed walk
/// cursor); an uncovered entry only gets its new key.
macro_rules! entry_index {
    ($(#[$meta:meta])* $definition:ident, $apply:ident, $index:ty, $project:ident, $key:expr) => {
        $(#[$meta])*
        pub(crate) static $definition: IndexDefinition = IndexDefinition {
            name: <$index as RegisteredIndex>::DEFINITION,
            kind: IndexKind::Secondary,
            apply: $apply,
        };

        fn $apply(
            storage: &mut PartitionStoreTransaction<'_>,
            id: u32,
            context: &EntryContext<'_>,
            before: Option<&EntryStateRef<'_>>,
            after: Option<&EntryStateRef<'_>>,
        ) {
            update_index(storage, id, context, before, after, $project, $key);
        }
    };
}

entry_index!(
    /// Stage, service, descending transition time.
    ENTRY_BY_STAGE_SERVICE,
    apply_entry_by_stage_service,
    EntryByStageService,
    transitioned,
    |context, (stage, at, id)| Some(EntryByStageServiceKey::borrowed(
        stage,
        context.target.service(),
        at,
        id
    ))
);
entry_index!(
    /// Stage, descending transition time.
    ENTRY_BY_STAGE,
    apply_entry_by_stage,
    EntryByStage,
    transitioned,
    |_, (stage, at, id)| Some(EntryByStageKey::borrowed(stage, at, id))
);
entry_index!(
    /// Stage, next transition time.
    ENTRY_NEXT_AT_BY_STAGE,
    apply_entry_next_at_by_stage,
    EntryNextAtByStage,
    next_at,
    |_, (stage, at, id)| Some(EntryNextAtByStageKey::borrowed(stage, at, id.seq(), id))
);
entry_index!(
    /// Stage, service, next transition time.
    ENTRY_NEXT_AT_BY_STAGE_SERVICE,
    apply_entry_next_at_by_stage_service,
    EntryNextAtByStageService,
    next_at,
    |context, (stage, at, id)| Some(EntryNextAtByStageServiceKey::borrowed(
        stage,
        context.target.service(),
        at,
        id.seq(),
        id
    ))
);
entry_index!(
    /// Virtual object, stage, descending transition time. Only keyed targets.
    ENTRY_BY_VIRTUAL_OBJECT_STAGE,
    apply_entry_by_virtual_object_stage,
    EntryByVirtualObjectStage,
    transitioned,
    |context, (stage, at, id)| {
        let target = context.target;
        target.virtual_object_key().map(|key| {
            EntryByVirtualObjectStageKey::borrowed(
                target.service(),
                target.scope(),
                key,
                stage,
                at,
                id,
            )
        })
    }
);
entry_index!(
    /// Virtual object, stage, next transition time. Only keyed targets.
    ENTRY_NEXT_AT_BY_VIRTUAL_OBJECT_STAGE,
    apply_entry_next_at_by_virtual_object_stage,
    EntryNextAtByVirtualObjectStage,
    next_at,
    |context, (stage, at, id)| {
        let target = context.target;
        target.virtual_object_key().map(|key| {
            EntryNextAtByVirtualObjectStageKey::borrowed(
                target.service(),
                target.scope(),
                key,
                stage,
                at,
                id.seq(),
                id,
            )
        })
    }
);

/// The entry indexes, released and controlled together.
#[allow(dead_code)] // Consumed when the registry is reconciled at open.
pub(crate) const ENTRY_INDEXES: Family = Family {
    definitions: &[
        &ENTRY_BY_STAGE_SERVICE,
        &ENTRY_BY_STAGE,
        &ENTRY_NEXT_AT_BY_STAGE,
        &ENTRY_NEXT_AT_BY_STAGE_SERVICE,
        &ENTRY_BY_VIRTUAL_OBJECT_STAGE,
        &ENTRY_NEXT_AT_BY_VIRTUAL_OBJECT_STAGE,
    ],
    maintain: indexes_v1,
    reset: no_reset,
};

#[allow(dead_code)] // Consumed when the registry is reconciled at open.
fn indexes_v1(config: &Configuration) -> bool {
    config.common.experimental.is_indexes_v1_enabled()
}

/// Disabling the entry indexes has no option yet.
#[allow(dead_code)] // Consumed when the registry is reconciled at open.
fn no_reset(_: &Configuration) -> bool {
    false
}

type Transitioned = (Stage, Reverse<UniqueTimestamp>, CanonicalEntryId);
type NextAt = (Stage, RoughTimestamp, CanonicalEntryId);

fn transitioned(context: &EntryContext<'_>, entry: &EntryStateRef<'_>) -> Transitioned {
    (
        entry.stage,
        Reverse(entry.stats.transitioned_at),
        entry
            .entry_key
            .to_canonical_entry_id(context.qid.partition_key()),
    )
}

fn next_at(context: &EntryContext<'_>, entry: &EntryStateRef<'_>) -> NextAt {
    (
        entry.stage,
        entry.entry_key.run_at(),
        entry
            .entry_key
            .to_canonical_entry_id(context.qid.partition_key()),
    )
}

/// Compares the index's projections before allocating or encoding keys.
/// Unchanged membership must not overwrite a key and break its SingleDelete
/// lifetime. `key` is `None` for entries outside the index.
fn update_index<'a, P: Eq, K>(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'a>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
    project: fn(&EntryContext<'_>, &EntryStateRef<'_>) -> P,
    key: impl Fn(&EntryContext<'a>, P) -> Option<K>,
) where
    K: SecondaryIndexKey,
    K::Index: SecondaryIndex<Scope = IndexScope>,
{
    let old = before.map(|entry| project(context, entry));
    let new = after.map(|entry| project(context, entry));
    if old == new {
        return;
    }
    let old = old.and_then(|old| key(context, old));
    let new = new.and_then(|new| key(context, new));
    storage.update_secondary_index_with_id(id, old.as_ref(), new.as_ref());
}
