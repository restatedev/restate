// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The entry indexes: secondary indexes over VQueue entries, released together
//! and switched on by `experimental-enable-track-entry-indexes`.
//!
//! **Entries by service and handler, newest created first**
//! (`entry_by_service_created/1`) holds one key for every entry, of both kinds,
//! in every stage, including finished entries kept for their retention:
//!
//! ```text
//! xI | partition | index ID | service | handler? | created_at ↓ | canonical entry ID
//!                             "Utils"   "call"     t9             inv_…
//!                             "Utils"   "call"     t7             inv_…
//!                             "Utils"   "run"      t8             inv_…
//!                             "object"  NULL       t6             (a state mutation)
//! ```
//!
//! So the entries of one service and handler are one contiguous run, newest
//! first, and a query naming only the service reads one run per handler. The
//! handler is NULL for state mutations, and NULL sorts first within its service.
//!
//! Service and handler come from the entry's target, which every write already
//! passes (`EntryContext`), so the entry header stays as it is. None of the key's
//! fields changes after the entry is created, so only creating, adopting and
//! deleting an entry writes the index; stage and status changes don't.
//!
//! **Finished entries by outcome, service and handler, newest finished first**
//! (`finished_entry_by_outcome/1`) holds one key for every entry whose stored
//! stage is Finished, of both kinds, whatever its stored status:
//!
//! ```text
//! xI | partition | index ID | status | service | handler? | finished ↓ | canonical entry ID
//!                             failed   "Utils"   "call"     t9           inv_…
//!                             killed   "Utils"   "call"     t4           inv_…
//!                             succeeded "Utils"  "call"     t8           inv_…
//! ```
//!
//! The status is the entry's stored status, and "finished" its stored
//! `transitioned_at`: when it entered Finished, or, for an invocation migrated
//! already completed, the transition time the migration recorded. Neither is
//! claimed to equal an invocation's completion time.
//!
//! **Active entries by stage, service and handler, newest created first**
//! (`active_entry_by_stage/1`) holds one key for every entry whose stored stage
//! is one of the four known active stages, Inbox, Running, Suspended and Paused,
//! of both kinds:
//!
//! ```text
//! xI | partition | index ID | stage | service | handler? | created_at ↓ | canonical entry ID
//!                             inbox   "Utils"   "call"     t9             inv_…
//!                             running "Utils"   "call"     t3             inv_…
//! ```
//!
//! An entry in any other stage, Finished or a stage this binary doesn't know, has
//! no key. The key moves with the stage; the processing status isn't part of it.
//!
//! All three indexes project an entry's old and new state to their optional keys
//! and write only when the two differ.

use std::cmp::Reverse;

use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef, Stage};
use restate_types::config::Configuration;

use crate::PartitionStoreTransaction;
use crate::index::RegisteredIndex;
use crate::keys::codecs::{
    CanonicalEntryIdCodec, ReverseUniqueTimestampCodec, ServiceNameCodec, StageRankCodec,
    StatusCodec, StringCodec,
};
use crate::registry::{Family, IndexDefinition, IndexKind};

crate::index::macros::define_secondary_index!(
    /// Entries by service and handler, newest created first.
    EntryByServiceCreated,
    definition: "entry_by_service_created/1",
    introduced: (1, 8, 2),
    key: EntryByServiceCreatedKey {
        target_service_name: ServiceNameCodec => str,
        target_handler_name: Option<StringCodec> => str,
        created_at: ReverseUniqueTimestampCodec,
        canonical_id: CanonicalEntryIdCodec (primary_key),
    }
);

// A query on invocations restricts the index through the invocation columns its
// fields are named after. Every invocation's key has a handler; only state
// mutations, which a query on invocations skips, have none.
crate::keys::macros::define_key_selection!(
    EntryByServiceCreatedKey;
    [restate_storage_query_api::tables::index::EntryByServiceCreated];
    target_service_name,
    target_handler_name => present,
    created_at,
);

crate::index::macros::define_secondary_index!(
    /// Finished entries by stored status, service and handler, newest finished
    /// first.
    FinishedEntryByOutcome,
    definition: "finished_entry_by_outcome/1",
    introduced: (1, 8, 2),
    // `status` is the stored status, the outcome; `transitioned_at` is when
    // the entry entered Finished. Named after the entry table's columns, so
    // that a query's conditions on them restrict the index.
    key: FinishedEntryByOutcomeKey {
        status: StatusCodec,
        target_service_name: ServiceNameCodec => str,
        target_handler_name: Option<StringCodec> => str,
        transitioned_at: ReverseUniqueTimestampCodec,
        canonical_id: CanonicalEntryIdCodec (primary_key),
    }
);

// A query on entries restricts the index through the entry table's status and
// transition time; the entry table shows no service or handler.
crate::keys::macros::define_key_selection!(
    FinishedEntryByOutcomeKey;
    [restate_storage_query_api::tables::index::FinishedEntryByOutcome];
    status,
    target_service_name,
    target_handler_name,
    transitioned_at,
);

crate::index::macros::define_secondary_index!(
    /// Entries in a known active stage by stage, service and handler, newest
    /// created first.
    ActiveEntryByStage,
    definition: "active_entry_by_stage/1",
    introduced: (1, 8, 2),
    key: ActiveEntryByStageKey {
        stage: StageRankCodec,
        target_service_name: ServiceNameCodec => str,
        target_handler_name: Option<StringCodec> => str,
        created_at: ReverseUniqueTimestampCodec,
        canonical_id: CanonicalEntryIdCodec (primary_key),
    }
);

// A query on entries restricts the index through the entry table's stage and
// creation time; the entry table shows no service or handler.
crate::keys::macros::define_key_selection!(
    ActiveEntryByStageKey;
    [restate_storage_query_api::tables::index::ActiveEntryByStage];
    stage,
    target_service_name,
    target_handler_name,
    created_at,
);

/// Every entry by service and handler, newest created first.
pub(crate) static ENTRY_BY_SERVICE_CREATED: IndexDefinition = IndexDefinition {
    name: EntryByServiceCreated::DEFINITION,
    introduced: EntryByServiceCreated::INTRODUCED,
    kind: IndexKind::Secondary,
    apply: apply_entry_by_service_created,
    can_fill: true,
};

/// Every entry whose stored stage is Finished, by stored status, service and
/// handler, newest finished first.
pub(crate) static FINISHED_ENTRY_BY_OUTCOME: IndexDefinition = IndexDefinition {
    name: FinishedEntryByOutcome::DEFINITION,
    introduced: FinishedEntryByOutcome::INTRODUCED,
    kind: IndexKind::Secondary,
    apply: apply_finished_entry_by_outcome,
    can_fill: true,
};

/// Every entry in Inbox, Running, Suspended or Paused, by stage, service and
/// handler, newest created first.
pub(crate) static ACTIVE_ENTRY_BY_STAGE: IndexDefinition = IndexDefinition {
    name: ActiveEntryByStage::DEFINITION,
    introduced: ActiveEntryByStage::INTRODUCED,
    kind: IndexKind::Secondary,
    apply: apply_active_entry_by_stage,
    can_fill: true,
};

/// The entry indexes, released and controlled together.
pub(crate) const ENTRY_INDEXES: Family = Family {
    definitions: &[
        &ENTRY_BY_SERVICE_CREATED,
        &FINISHED_ENTRY_BY_OUTCOME,
        &ACTIVE_ENTRY_BY_STAGE,
    ],
    maintain: track_entry_indexes,
    reset: entry_indexes_reset,
};

fn track_entry_indexes(config: &Configuration) -> bool {
    config.common.experimental.is_track_entry_indexes_enabled()
}

fn entry_indexes_reset(config: &Configuration) -> bool {
    config.common.experimental.is_entry_indexes_reset_enabled()
}

/// Removes the key of `before` and adds the key of `after`. Both share the
/// entry's target, and the rest of the key never changes after creation, so an
/// update writes nothing.
fn apply_entry_by_service_created(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let project = |entry: &EntryStateRef<'_>| {
        (
            Reverse(entry.stats.created_at),
            entry
                .entry_key
                .to_canonical_entry_id(context.qid.partition_key()),
        )
    };
    let (old, new) = (before.map(project), after.map(project));
    if old != new {
        let key = |(created_at, canonical_id)| {
            EntryByServiceCreatedKey::borrowed(
                context.target.service(),
                context.target.handler(),
                created_at,
                canonical_id,
            )
        };
        storage.update_secondary_index_with_id(id, old.map(key).as_ref(), new.map(key).as_ref());
    }
}

/// Removes the key of `before` and adds the key of `after`, each only if its
/// entry's stored stage is Finished.
fn apply_finished_entry_by_outcome(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let project = |entry: &EntryStateRef<'_>| {
        (entry.stage == Stage::Finished).then(|| {
            (
                entry.status,
                Reverse(entry.stats.transitioned_at),
                entry
                    .entry_key
                    .to_canonical_entry_id(context.qid.partition_key()),
            )
        })
    };
    let (old, new) = (before.and_then(project), after.and_then(project));
    if old != new {
        let key = |(outcome, finished_at, canonical_id): &(_, _, _)| {
            FinishedEntryByOutcomeKey::borrowed(
                *outcome,
                context.target.service(),
                context.target.handler(),
                *finished_at,
                *canonical_id,
            )
        };
        storage.update_secondary_index_with_id(
            id,
            old.as_ref().map(key).as_ref(),
            new.as_ref().map(key).as_ref(),
        );
    }
}

/// Whether `active_entry_by_stage/1` holds an entry in `stage`: exactly the four
/// known active stages. A new stage needs a decision here, and a new definition
/// if it changes this one's meaning.
fn is_active(stage: Stage) -> bool {
    match stage {
        Stage::Inbox | Stage::Running | Stage::Suspended | Stage::Paused => true,
        Stage::Finished | Stage::Unknown => false,
    }
}

/// Removes the key of `before` and adds the key of `after`, each only if its
/// entry's stored stage is a known active one ([`is_active`]).
fn apply_active_entry_by_stage(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let project = |entry: &EntryStateRef<'_>| {
        is_active(entry.stage).then(|| {
            (
                entry.stage,
                Reverse(entry.stats.created_at),
                entry
                    .entry_key
                    .to_canonical_entry_id(context.qid.partition_key()),
            )
        })
    };
    let (old, new) = (before.and_then(project), after.and_then(project));
    if old != new {
        let key = |(stage, created_at, canonical_id): &(_, _, _)| {
            ActiveEntryByStageKey::borrowed(
                *stage,
                context.target.service(),
                context.target.handler(),
                *created_at,
                *canonical_id,
            )
        };
        storage.update_secondary_index_with_id(
            id,
            old.as_ref().map(key).as_ref(),
            new.as_ref().map(key).as_ref(),
        );
    }
}

#[cfg(test)]
mod tests;
