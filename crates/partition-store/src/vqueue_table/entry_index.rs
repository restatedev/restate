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
//! **Entries by creation hour and service, newest created first**
//! (`entry_by_created/1`) holds one key for every entry, like the first index,
//! but leads with the start of the hour in which the entry was created,
//! so that the newest entries of all services are the first keys:
//!
//! ```text
//! xI | partition | index ID | hour ↓ | service | created_at ↓ | canonical entry ID
//!                             h9       "Other"   t9             inv_…
//!                             h9       "Utils"   t8             inv_…
//!                             h8       "Utils"   t7             inv_…
//! value: handler?, scope?
//! ```
//!
//! A page of the newest entries merges one run per service active in its hours;
//! a service's page reads one run per hour. The value holds the entry's handler
//! and scope, which a read can return without fetching the entry. Like the first
//! index, key and value never change after the entry is created.
//!
//! **Entries by stage and status, newest created first**
//! (`entry_by_status_created/1`) and **newest transitioned first**
//! (`entry_by_status_transitioned/1`) hold one key for every entry in a known
//! stage, of both kinds, and lead with its stored stage and status. Below that,
//! they are laid out like `entry_by_created/1`, by the creation time in the
//! first and by the stored `transitioned_at`, when the entry entered its stage,
//! in the second:
//!
//! ```text
//! xI | partition | index ID | stage | status | hour ↓ | service | time ↓ | canonical entry ID
//!                             finished killed    h9       "Utils"   t9       inv_…
//!                             finished succeeded h9       "Other"   t9       inv_…
//!                             finished succeeded h9       "Utils"   t8       inv_…
//!                             finished succeeded h8       "Utils"   t7       inv_…
//!                             running  started   h9       "Utils"   t9       inv_…
//! value: handler?, scope?
//! ```
//!
//! Each stage and status is one contiguous range over all hours and services.
//! Finished entries that succeeded, typically the bulk of a partition, are one
//! range that a page of any other outcome or stage never reads; a page of
//! killed entries in the past hours reads only the killed range's newest hours.
//! The key moves whenever the entry's stage or status changes, and, in the
//! second index, when its `transitioned_at` does. An entry in a stage this
//! binary doesn't know has no key.
//!
//! All six indexes project an entry's old and new state to their optional keys
//! and write only when the two differ.

use std::cmp::Reverse;
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use bytes::{Buf, BufMut};
use prost::encoding::{decode_varint, encode_varint, encoded_len_varint};

use restate_clock::time::MillisSinceEpoch;
use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef, Stage, Status};
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::{
    EntryByServiceCreated as InvocationsByCreation, EntryByServiceCreatedClause,
    EntryByServiceCreatedColumn,
};
use restate_types::config::Configuration;
use restate_types::vqueues::CanonicalEntryId;

use crate::PartitionStoreTransaction;
use crate::index::{IndexValue, IndexValueRef, RegisteredIndex, SecondaryIndex, SecondaryIndexKey};
use crate::keys::codecs::{
    CanonicalEntryIdCodec, ReverseRoughTimestampCodec, ReverseUniqueTimestampCodec,
    ServiceNameCodec, StageRankCodec, StatusCodec, StringCodec,
};
use crate::keys::header::IndexScope;
use crate::keys::selection::codec::{FieldPredicateCodec, prepare_hour_buckets};
use crate::keys::selection::{KeySelection, PrepareKeySelection};
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

/// Every entry by creation hour and service, newest created first, with its
/// handler and scope.
pub(crate) static ENTRY_BY_CREATED: IndexDefinition = IndexDefinition {
    name: EntryByCreated::DEFINITION,
    introduced: EntryByCreated::INTRODUCED,
    kind: IndexKind::Secondary,
    apply: apply_entry_by_created,
    can_fill: true,
};

/// Every entry in a known stage by stage, status, creation hour and service,
/// newest created first, with its handler and scope.
pub(crate) static ENTRY_BY_STATUS_CREATED: IndexDefinition = IndexDefinition {
    name: EntryByStatusCreated::DEFINITION,
    introduced: EntryByStatusCreated::INTRODUCED,
    kind: IndexKind::Secondary,
    apply: apply_entry_by_status_created,
    can_fill: true,
};

/// Every entry in a known stage by stage, status, transition hour and service,
/// newest transitioned first, with its handler and scope.
pub(crate) static ENTRY_BY_STATUS_TRANSITIONED: IndexDefinition = IndexDefinition {
    name: EntryByStatusTransitioned::DEFINITION,
    introduced: EntryByStatusTransitioned::INTRODUCED,
    kind: IndexKind::Secondary,
    apply: apply_entry_by_status_transitioned,
    can_fill: true,
};

/// The entry indexes, released and controlled together.
pub(crate) const ENTRY_INDEXES: Family = Family {
    definitions: &[
        &ENTRY_BY_SERVICE_CREATED,
        &FINISHED_ENTRY_BY_OUTCOME,
        &ACTIVE_ENTRY_BY_STAGE,
        &ENTRY_BY_CREATED,
        &ENTRY_BY_STATUS_CREATED,
        &ENTRY_BY_STATUS_TRANSITIONED,
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

impl ActiveEntryByStage {
    /// Whether `active_entry_by_stage/1` holds an entry in `stage`: exactly the
    /// four known active stages. A new stage needs a decision here, and a new
    /// definition if it changes this one's meaning. Queries decide which
    /// queries the index can serve from this too.
    pub fn holds(stage: Stage) -> bool {
        match stage {
            Stage::Inbox | Stage::Running | Stage::Suspended | Stage::Paused => true,
            Stage::Finished | Stage::Unknown => false,
        }
    }
}

/// Removes the key of `before` and adds the key of `after`, each only if its
/// entry's stored stage is a known active one ([`ActiveEntryByStage::holds`]).
fn apply_active_entry_by_stage(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let project = |entry: &EntryStateRef<'_>| {
        ActiveEntryByStage::holds(entry.stage).then(|| {
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

crate::index::macros::define_secondary_index!(
    /// Entries by creation hour and service, newest created first; each key
    /// holds the entry's handler and scope ([`EntryTargetValue`]).
    ///
    /// Since v1.8.2
    EntryByCreated,
    definition: "entry_by_created/1",
    introduced: (1, 8, 2),
    key: EntryByCreatedKey {
        created_hour: ReverseRoughTimestampCodec,
        target_service_name: ServiceNameCodec => str,
        created_at: ReverseUniqueTimestampCodec,
        canonical_id: CanonicalEntryIdCodec (primary_key),
    },
    value: EntryTargetValue,
);

// A query on invocations restricts `entry_by_created/1` through the invocation
// columns its fields are named after, and the creation hour through
// `created_at`: the hours that may hold a matching time. The handler is in the
// value, which the read checks before fetching the entry's status.
impl PrepareKeySelection<InvocationsByCreation> for EntryByCreatedKey {
    fn prepare_candidates(
        selection: &Selection<InvocationsByCreation>,
    ) -> crate::Result<KeySelection<Self>> {
        KeySelection::for_key_fields_with(
            selection,
            |clause, literals| match clause {
                EntryByServiceCreatedClause::TargetServiceName(predicate) => {
                    ServiceNameCodec::try_prepare_value(predicate, literals)
                }
                EntryByServiceCreatedClause::CreatedAt(predicate) => {
                    ReverseUniqueTimestampCodec::try_prepare_value(predicate, literals)
                }
                _ => Ok(None),
            },
            |tag, literals| {
                if tag != "created_hour" {
                    return Ok(None);
                }
                Ok(match created_times(selection) {
                    Some(CreatedTimes::Clause(times)) => {
                        Some(prepare_hour_buckets(times, literals))
                    }
                    Some(CreatedTimes::Range(times)) => {
                        Some(prepare_hour_buckets(&times, literals))
                    }
                    None => None,
                })
            },
        )
    }
}

/// The creation times every match has: one of its equalities or IN lists, or
/// its ranges intersected.
enum CreatedTimes<'a> {
    Clause(&'a ValuePredicate<MillisSinceEpoch>),
    Range(ValuePredicate<MillisSinceEpoch>),
}

/// The creation times every match of `selection` has: one of its `created_at`
/// equalities or IN lists, each of which every match satisfies, or else its
/// ranges intersected. `None` when nothing restricts the creation time.
fn created_times(selection: &Selection<InvocationsByCreation>) -> Option<CreatedTimes<'_>> {
    let Selection::Predicates(predicates) = selection else {
        return None;
    };
    let (mut lower, mut upper) = (Unbounded, Unbounded);
    let mut ranged = false;
    for clause in predicates.for_column(EntryByServiceCreatedColumn::CreatedAt) {
        let EntryByServiceCreatedClause::CreatedAt(predicate) = clause else {
            continue;
        };
        match predicate {
            ValuePredicate::Equal(_) | ValuePredicate::In(_) => {
                return Some(CreatedTimes::Clause(predicate));
            }
            ValuePredicate::Range {
                lower: low,
                upper: high,
            } => {
                lower = tighter(lower, *low, |a, b| a > b);
                upper = tighter(upper, *high, |a, b| a < b);
                ranged = true;
            }
        }
    }
    ranged.then_some(CreatedTimes::Range(ValuePredicate::Range { lower, upper }))
}

/// The stricter of two bounds on the same side: `beyond(a, b)` says whether `a`
/// is further into the interval than `b`; at the same value, exclusive wins.
fn tighter(
    current: Bound<MillisSinceEpoch>,
    other: Bound<MillisSinceEpoch>,
    beyond: impl Fn(u64, u64) -> bool,
) -> Bound<MillisSinceEpoch> {
    let value = |bound: &Bound<MillisSinceEpoch>| match bound {
        Included(millis) | Excluded(millis) => Some(millis.as_u64()),
        Unbounded => None,
    };
    match (value(&current), value(&other)) {
        (_, None) => current,
        (None, Some(_)) => other,
        (Some(a), Some(b)) if beyond(b, a) => other,
        (Some(a), Some(b)) if a == b && matches!(other, Excluded(_)) => other,
        _ => current,
    }
}

/// The start of the hour in which `at` falls: the bucket that orders the keys of
/// `entry_by_created/1` and the two status indexes, newest first.
pub fn hour_bucket(at: UniqueTimestamp) -> RoughTimestamp {
    RoughTimestamp::from_unix_millis_clamped(at.to_unix_millis()).floor_to_hour()
}

/// What `entry_by_created/1` and the two status indexes store with each key: the
/// parts of the entry's target their keys don't hold, its handler, none for a
/// state mutation, and its scope. Both are fixed when the entry is created.
pub enum EntryTargetValue {}

impl IndexValue for EntryTargetValue {
    type Ref<'a> = EntryTargetValueRef<'a>;
}

/// An [`EntryTargetValue`], borrowing its strings from the entry or the stored
/// bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryTargetValueRef<'a> {
    pub handler: Option<&'a str>,
    pub scope: Option<&'a str>,
}

// Encoded as a byte of flags, which fields are present, followed by each present
// field's length (a varint) and UTF-8 bytes, handler first.
const HAS_HANDLER: u8 = 0b01;
const HAS_SCOPE: u8 = 0b10;

impl<'a> IndexValueRef<'a> for EntryTargetValueRef<'a> {
    fn encoded_len(&self) -> usize {
        let field = |value: Option<&str>| {
            value.map_or(0, |value| {
                encoded_len_varint(value.len() as u64) + value.len()
            })
        };
        1 + field(self.handler) + field(self.scope)
    }

    fn encode<B: BufMut>(&self, buffer: &mut B) {
        let mut flags = 0;
        if self.handler.is_some() {
            flags |= HAS_HANDLER;
        }
        if self.scope.is_some() {
            flags |= HAS_SCOPE;
        }
        buffer.put_u8(flags);
        for value in [self.handler, self.scope].into_iter().flatten() {
            encode_varint(value.len() as u64, buffer);
            buffer.put_slice(value.as_bytes());
        }
    }

    fn decode(bytes: &'a [u8]) -> crate::Result<Self> {
        let (&flags, mut rest) = bytes
            .split_first()
            .ok_or(StorageError::DataIntegrityError)?;
        if flags & !(HAS_HANDLER | HAS_SCOPE) != 0 {
            return Err(StorageError::DataIntegrityError);
        }
        let mut field = |present: bool| -> crate::Result<Option<&'a str>> {
            if !present {
                return Ok(None);
            }
            let len = decode_varint(&mut rest).map_err(|_| StorageError::DataIntegrityError)?;
            let len = usize::try_from(len).map_err(|_| StorageError::DataIntegrityError)?;
            if rest.remaining() < len {
                return Err(StorageError::DataIntegrityError);
            }
            let (value, tail) = rest.split_at(len);
            rest = tail;
            std::str::from_utf8(value)
                .map(Some)
                .map_err(|_| StorageError::DataIntegrityError)
        };
        let handler = field(flags & HAS_HANDLER != 0)?;
        let scope = field(flags & HAS_SCOPE != 0)?;
        if !rest.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(Self { handler, scope })
    }
}

/// Removes the key of `before` and adds the key of `after`, each with the
/// entry's handler and scope. Key and value are fixed when the entry is
/// created, so an update writes nothing.
fn apply_entry_by_created(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let project = |entry: &EntryStateRef<'_>| {
        (
            entry.stats.created_at,
            entry
                .entry_key
                .to_canonical_entry_id(context.qid.partition_key()),
        )
    };
    let (old, new) = (before.map(project), after.map(project));
    if old != new {
        let value = EntryTargetValueRef {
            handler: context.target.handler(),
            scope: context.target.scope(),
        };
        let key = |(created_at, canonical_id)| {
            EntryByCreatedKey::borrowed(
                Reverse(hour_bucket(created_at)),
                context.target.service(),
                Reverse(created_at),
                canonical_id,
            )
        };
        let (old, new) = (old.map(key), new.map(key));
        storage.update_secondary_index_entry_with_id(
            id,
            old.as_ref().map(|key| (key, value)),
            new.as_ref().map(|key| (key, value)),
        );
    }
}

crate::index::macros::define_secondary_index!(
    /// Entries in a known stage by stored stage and status, creation hour and
    /// service, newest created first; each key holds the entry's handler and
    /// scope ([`EntryTargetValue`]).
    ///
    /// Since v1.8.2
    EntryByStatusCreated,
    definition: "entry_by_status_created/1",
    introduced: (1, 8, 2),
    key: EntryByStatusCreatedKey {
        stage: StageRankCodec,
        status: StatusCodec,
        created_hour: ReverseRoughTimestampCodec,
        target_service_name: ServiceNameCodec => str,
        created_at: ReverseUniqueTimestampCodec,
        canonical_id: CanonicalEntryIdCodec (primary_key),
    },
    value: EntryTargetValue,
);

crate::index::macros::define_secondary_index!(
    /// Entries in a known stage by stored stage and status, transition hour and
    /// service, newest transitioned first; each key holds the entry's handler
    /// and scope ([`EntryTargetValue`]).
    ///
    /// Since v1.8.2
    EntryByStatusTransitioned,
    definition: "entry_by_status_transitioned/1",
    introduced: (1, 8, 2),
    key: EntryByStatusTransitionedKey {
        stage: StageRankCodec,
        status: StatusCodec,
        transitioned_hour: ReverseRoughTimestampCodec,
        target_service_name: ServiceNameCodec => str,
        transitioned_at: ReverseUniqueTimestampCodec,
        canonical_id: CanonicalEntryIdCodec (primary_key),
    },
    value: EntryTargetValue,
);

/// Whether the two status indexes hold an entry in `stage`: every stage this
/// binary knows. A new stage needs a rank in [`StageRankCodec`], a decision
/// here, and new definition names, since it changes both indexes' membership
/// (README rule 1): a binary that doesn't know the stage projects no key for
/// it, and would leave a newer binary's key behind. The same holds for a new
/// [`StatusCodec`] rank. Queries decide which queries the indexes can serve
/// from this too.
pub fn status_indexes_hold(stage: Stage) -> bool {
    match stage {
        Stage::Inbox | Stage::Running | Stage::Suspended | Stage::Paused | Stage::Finished => true,
        Stage::Unknown => false,
    }
}

/// Removes the key of `before` and adds the key of `after` in
/// `entry_by_status_created/1`.
fn apply_entry_by_status_created(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    apply_by_status(
        storage,
        id,
        context,
        before,
        after,
        |stats| stats.created_at,
        |(stage, status, created_at, canonical_id)| {
            EntryByStatusCreatedKey::borrowed(
                stage,
                status,
                Reverse(hour_bucket(created_at)),
                context.target.service(),
                Reverse(created_at),
                canonical_id,
            )
        },
    );
}

/// Removes the key of `before` and adds the key of `after` in
/// `entry_by_status_transitioned/1`.
fn apply_entry_by_status_transitioned(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    apply_by_status(
        storage,
        id,
        context,
        before,
        after,
        |stats| stats.transitioned_at,
        |(stage, status, transitioned_at, canonical_id)| {
            EntryByStatusTransitionedKey::borrowed(
                stage,
                status,
                Reverse(hour_bucket(transitioned_at)),
                context.target.service(),
                Reverse(transitioned_at),
                canonical_id,
            )
        },
    );
}

/// Removes the key of `before` and adds the key of `after`, each only if its
/// entry's stage is known ([`status_indexes_hold`]), with the
/// entry's handler and scope. `time` picks the timestamp that orders the index,
/// and `key` lays the key out from the stage, status, that time and the entry's
/// ID: a change of any of them moves the key.
fn apply_by_status<K>(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
    time: fn(&EntryStatistics) -> UniqueTimestamp,
    key: impl Fn((Stage, Status, UniqueTimestamp, CanonicalEntryId)) -> K,
) where
    K: SecondaryIndexKey,
    K::Index: SecondaryIndex<Scope = IndexScope, Value = EntryTargetValue>,
{
    let project = |entry: &EntryStateRef<'_>| {
        status_indexes_hold(entry.stage).then(|| {
            (
                entry.stage,
                entry.status,
                time(entry.stats),
                entry
                    .entry_key
                    .to_canonical_entry_id(context.qid.partition_key()),
            )
        })
    };
    let (old, new) = (before.and_then(project), after.and_then(project));
    if old != new {
        let value = EntryTargetValueRef {
            handler: context.target.handler(),
            scope: context.target.scope(),
        };
        let (old, new) = (old.map(&key), new.map(&key));
        storage.update_secondary_index_entry_with_id(
            id,
            old.as_ref().map(|key| (key, value)),
            new.as_ref().map(|key| (key, value)),
        );
    }
}

#[cfg(test)]
mod tests;
