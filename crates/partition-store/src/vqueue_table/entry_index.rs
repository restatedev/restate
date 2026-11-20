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
//! Each index takes the entry's service, handler and scope from its target,
//! which every write passes (`EntryContext`), so the entry header stays as it
//! is.
//!
//! **Entries by creation hour and service, newest created first**
//! (`entry_by_created/1`) holds one key for every entry, of both kinds, in
//! every stage, including finished entries kept for their retention. It leads
//! with the start of the hour in which the entry was created, so that the
//! newest entries of all services are the first keys:
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
//! and scope, which a read can check or return without fetching the entry. Key
//! and value never change after the entry is created, so only creating,
//! adopting and deleting an entry writes the index; stage and status changes
//! don't. It replaced `entry_by_service_created/1`, a run per service and
//! handler for all time, which is retired: a store that maintained it drops it
//! at open.
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
//! The status indexes replaced `active_entry_by_stage/1`, a run per active
//! stage, service and handler, and `finished_entry_by_outcome/1`, a run per
//! outcome, service and handler of Finished entries: their pages read the
//! status indexes' lanes in the same order. Both are retired: a store that
//! maintained them drops them at open.
//!
//! Every index projects an entry's old and new state to its optional key and
//! writes only when the two differ.

use std::cmp::Reverse;
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use bytes::{Buf, BufMut};
use prost::encoding::{decode_varint, encode_varint, encoded_len_varint};

use restate_clock::time::MillisSinceEpoch;
use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef, Stage, Status};
use restate_storage_query_api::SelectionSchema;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::{
    EntryByServiceCreated as InvocationsByCreation, EntryByServiceCreatedClause,
    EntryByServiceCreatedColumn, EntryByStatusCreated as InvocationsByStatus,
    EntryByStatusCreatedClause, EntryByStatusCreatedColumn,
    EntryStatusByStatusCreated as EntriesByStatusCreated, EntryStatusByStatusCreatedClause,
    EntryStatusByStatusCreatedColumn,
    EntryStatusByStatusTransitioned as EntriesByStatusTransitioned,
    EntryStatusByStatusTransitionedClause, EntryStatusByStatusTransitionedColumn,
};
use restate_types::config::Configuration;
use restate_types::vqueues::{CanonicalEntryId, EffectiveStatus};
use restate_util_string::ReString;

use crate::PartitionStoreTransaction;
use crate::index::{IndexValue, IndexValueRef, RegisteredIndex, SecondaryIndex, SecondaryIndexKey};
#[cfg(test)]
use crate::keys::codecs::StringCodec;
use crate::keys::codecs::{
    CanonicalEntryIdCodec, ReverseRoughTimestampCodec, ReverseUniqueTimestampCodec,
    ServiceNameCodec, StageRankCodec, StatusCodec,
};
use crate::keys::header::IndexScope;
use crate::keys::selection::codec::{FieldPredicateCodec, prepare_hour_buckets};
use crate::keys::selection::{KeySelection, PrepareKeySelection, PreparedFieldPredicate};
use crate::registry::{Family, IndexDefinition, IndexKind};

// `entry_by_service_created/1`, the first entry index, keyed every entry by
// service and handler, newest created first. `entry_by_created/1` replaced it
// for the invocation list, and nothing reads it: stores retire it at open
// (README rule 1). Kept for tests only: its runs, a service and handler each,
// are the run merge's fixture, and a store holding it shows the retirement.
#[cfg(test)]
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
#[cfg(test)]
crate::keys::macros::define_key_selection!(
    EntryByServiceCreatedKey;
    [restate_storage_query_api::tables::index::EntryByServiceCreated];
    target_service_name,
    target_handler_name => present,
    created_at,
);

/// Every entry by service and handler, newest created first. Retired; for
/// tests only.
#[cfg(test)]
pub(crate) static ENTRY_BY_SERVICE_CREATED: IndexDefinition = IndexDefinition {
    name: EntryByServiceCreated::DEFINITION,
    introduced: EntryByServiceCreated::INTRODUCED,
    kind: IndexKind::Secondary,
    apply: apply_entry_by_service_created,
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
#[cfg(test)]
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
                let times = created_times(
                    selection,
                    EntryByServiceCreatedColumn::CreatedAt,
                    |clause| match clause {
                        EntryByServiceCreatedClause::CreatedAt(times) => Some(times),
                        _ => None,
                    },
                );
                Ok(hour_buckets(times, literals))
            },
        )
    }
}

/// The hours that may hold a time in `times`, if anything restricts them.
fn hour_buckets(
    times: Option<CreatedTimes<'_>>,
    literals: &mut Vec<u8>,
) -> Option<PreparedFieldPredicate> {
    match times? {
        CreatedTimes::Clause(times) => Some(prepare_hour_buckets(times, literals)),
        CreatedTimes::Range(times) => Some(prepare_hour_buckets(&times, literals)),
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
/// `column` is the creation time's column, and `times` its predicate in a
/// clause.
fn created_times<'a, T: SelectionSchema>(
    selection: &'a Selection<T>,
    column: T::Column,
    times: impl Fn(&'a T::Clause) -> Option<&'a ValuePredicate<MillisSinceEpoch>>,
) -> Option<CreatedTimes<'a>> {
    let Selection::Predicates(predicates) = selection else {
        return None;
    };
    let (mut lower, mut upper) = (Unbounded, Unbounded);
    let mut ranged = false;
    for predicate in predicates.for_column(column).iter().filter_map(times) {
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

/// The stages in the order the two status indexes key them
/// ([`StageRankCodec`]): every stage they hold.
const KEYED_STAGES: [Stage; 5] = [
    Stage::Finished,
    Stage::Inbox,
    Stage::Paused,
    Stage::Running,
    Stage::Suspended,
];

/// Every stored status, in the order the two status indexes key them
/// ([`StatusCodec`]).
const KEYED_STATUSES: [Status; 10] = [
    Status::BackingOff,
    Status::Cancelled,
    Status::Failed,
    Status::Killed,
    Status::New,
    Status::Scheduled,
    Status::Started,
    Status::Succeeded,
    Status::Unknown,
    Status::Yielded,
];

/// The bit of `stage` in [`Keyed::stages`]: its position in [`KEYED_STAGES`].
const fn stage_bit(stage: Stage) -> u8 {
    let mut at = 0;
    while at < KEYED_STAGES.len() {
        if KEYED_STAGES[at] as u8 == stage as u8 {
            return 1 << at;
        }
        at += 1;
    }
    0
}

/// The bit of `status` in [`Keyed::statuses`]: its position in
/// [`KEYED_STATUSES`].
const fn status_bit(status: Status) -> u16 {
    let mut at = 0;
    while at < KEYED_STATUSES.len() {
        if KEYED_STATUSES[at] as u8 == status as u8 {
            return 1 << at;
        }
        at += 1;
    }
    0
}

/// Sets of stored stages and statuses, a bit for each of [`KEYED_STAGES`] and
/// [`KEYED_STATUSES`] by position. They stand for every pair of one of the
/// stages with one of the statuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Keyed {
    stages: u8,
    statuses: u16,
}

impl Keyed {
    const ALL: Self = Self {
        stages: (1 << KEYED_STAGES.len()) - 1,
        statuses: (1 << KEYED_STATUSES.len()) - 1,
    };
    const NONE: Self = Self {
        stages: 0,
        statuses: 0,
    };
    /// The entries of completed invocations: Finished, whatever the stored
    /// status. Only completing an invocation (`end_invocation`, terminating an
    /// inboxed or scheduled one) moves its entry to Finished, under the same
    /// retention that keeps the invocation's completed status; the migration
    /// to vqueues adds completed invocations as Finished.
    const COMPLETED: Self = Self {
        stages: stage_bit(Stage::Finished),
        statuses: Self::ALL.statuses,
    };
    /// The entries of invocations that haven't completed: any other stage.
    const ACTIVE: Self = Self {
        stages: Self::ALL.stages & !Self::COMPLETED.stages,
        statuses: Self::ALL.statuses,
    };
    /// The entries of invocations that completed with a failure: Finished, with
    /// any stored status but Succeeded. Completing with a failure stores
    /// Failed, Cancelled or Killed; so does the migration. No release stored
    /// Succeeded for a failure.
    const FAILED: Self = Self {
        stages: Self::COMPLETED.stages,
        statuses: Self::ALL.statuses & !status_bit(Status::Succeeded),
    };

    fn union(self, other: Self) -> Self {
        Self {
            stages: self.stages | other.stages,
            statuses: self.statuses | other.statuses,
        }
    }

    fn intersect(self, other: Self) -> Self {
        Self {
            stages: self.stages & other.stages,
            statuses: self.statuses & other.statuses,
        }
    }

    /// The entries of the invocations whose value satisfies `predicate`, with
    /// `of` mapping one value. A range is no restriction.
    fn matching<V>(predicate: &ValuePredicate<V>, of: impl Fn(&V) -> Self) -> Self {
        match predicate {
            ValuePredicate::Equal(value) => of(value),
            ValuePredicate::In(values) => values
                .iter()
                .fold(Self::NONE, |keyed, value| keyed.union(of(value))),
            ValuePredicate::Range { .. } => Self::ALL,
        }
    }

    /// The entries of invocations with the public `status`.
    fn of_status(status: &ReString) -> Self {
        match status.as_str() {
            "completed" => Self::COMPLETED,
            "inboxed" | "scheduled" | "invoked" | "suspended" | "paused" => Self::ACTIVE,
            // No invocation has another status.
            _ => Self::NONE,
        }
    }

    /// The entries in `stage`, in any status.
    fn of_stage(stage: &Stage) -> Self {
        Self {
            stages: stage_bit(*stage),
            statuses: Self::ALL.statuses,
        }
    }

    /// The pairs `selection`'s clauses allow, each clause mapped by `of`, or
    /// left out if it doesn't restrict them.
    fn of_selection<T: SelectionSchema>(
        selection: &Selection<T>,
        of: impl Fn(&T::Clause) -> Option<Self>,
    ) -> Self {
        match selection {
            Selection::All => Self::ALL,
            Selection::Empty => Self::NONE,
            Selection::Predicates(predicates) => predicates
                .iter()
                .flat_map(|(_, clauses)| clauses.iter())
                .filter_map(of)
                .fold(Self::ALL, Self::intersect),
        }
    }

    /// The pairs of the stages `selection`'s conditions on the entry's stage
    /// allow, each kept only if its effective status satisfies every condition
    /// on the entry's `status`: exactly the pairs of the entries it may match,
    /// as an entry's effective status is a function of its stored pair.
    fn of_entry_selection<T: SelectionSchema>(
        selection: &Selection<T>,
        stage: impl Fn(&T::Clause) -> Option<&ValuePredicate<Stage>>,
        status: impl Fn(&T::Clause) -> Option<&ValuePredicate<EffectiveStatus>>,
    ) -> impl Iterator<Item = (Stage, Status)> {
        let conditions: Vec<_> = match selection {
            Selection::Predicates(predicates) => predicates
                .iter()
                .flat_map(|(_, clauses)| clauses.iter())
                .filter_map(&status)
                .cloned()
                .collect(),
            Selection::All | Selection::Empty => Vec::new(),
        };
        Self::of_selection(selection, |clause| {
            stage(clause).map(|stage| Self::matching(stage, Self::of_stage))
        })
        .pairs()
        .filter(move |&(stage, stored)| {
            let effective = EffectiveStatus::new(stage, stored);
            conditions
                .iter()
                .all(|condition| condition.matches(&effective))
        })
    }

    /// Every pair of an allowed stage and an allowed status, in key order.
    fn pairs(self) -> impl Iterator<Item = (Stage, Status)> {
        let member = |bits: u16, at: usize| bits & (1 << at) != 0;
        KEYED_STAGES
            .into_iter()
            .enumerate()
            .filter(move |&(at, _)| member(self.stages.into(), at))
            .flat_map(move |(_, stage)| {
                KEYED_STATUSES
                    .into_iter()
                    .enumerate()
                    .filter(move |&(at, _)| member(self.statuses, at))
                    .map(move |(_, status)| (stage, status))
            })
    }

    /// The entries of invocations with the public `completion_result`, NULL
    /// for those that haven't completed.
    fn of_completion_result(result: &Option<ReString>) -> Self {
        match result.as_ref().map(ReString::as_str) {
            None => Self::ACTIVE,
            Some("success") => Self::COMPLETED,
            Some("failure") => Self::FAILED,
            Some(_) => Self::NONE,
        }
    }
}

impl EntryByStatusCreatedKey {
    /// The stages and stored statuses whose keys can hold the entry of an
    /// invocation `selection` matches, in key order: a superset, derived from
    /// its conditions on `status` and `completion_result`. The caller keeps
    /// its predicate. Each pair is a lane of the index
    /// ([`Self::prepare_lanes`]).
    pub fn lanes(
        selection: &Selection<InvocationsByStatus>,
    ) -> impl Iterator<Item = (Stage, Status)> {
        Keyed::of_selection(selection, |clause| match clause {
            EntryByStatusCreatedClause::Status(status) => {
                Some(Keyed::matching(status, Keyed::of_status))
            }
            EntryByStatusCreatedClause::CompletionResult(result) => {
                Some(Keyed::matching(result, Keyed::of_completion_result))
            }
            _ => None,
        })
        .pairs()
    }

    /// The stages and stored statuses whose keys can hold an entry
    /// `selection` matches, in key order: those its conditions on the entry's
    /// stage and effective status allow. Each pair is a lane
    /// ([`Self::prepare_entry_lanes`]).
    pub fn entry_lanes(
        selection: &Selection<EntriesByStatusCreated>,
    ) -> impl Iterator<Item = (Stage, Status)> {
        Keyed::of_entry_selection(
            selection,
            |clause| match clause {
                EntryStatusByStatusCreatedClause::Stage(stage) => Some(stage),
                _ => None,
            },
            |clause| match clause {
                EntryStatusByStatusCreatedClause::Status(status) => Some(status),
                _ => None,
            },
        )
    }

    /// The selection of the keys of entries `selection` may match in the lanes
    /// of every one of `stages` with every one of `statuses`: their creation
    /// time, and the hours that may hold it. The caller keeps its predicate.
    pub fn prepare_entry_lanes(
        selection: &Selection<EntriesByStatusCreated>,
        stages: &[Stage],
        statuses: &[Status],
    ) -> crate::Result<KeySelection<Self>> {
        KeySelection::for_key_fields_with(
            selection,
            |clause, literals| match clause {
                EntryStatusByStatusCreatedClause::CreatedAt(predicate) => {
                    ReverseUniqueTimestampCodec::try_prepare_value(predicate, literals)
                }
                // The stage and status select the lanes.
                _ => Ok(None),
            },
            |tag, literals| match tag {
                "stage" => pinned_stages(stages, literals),
                "status" => pinned_statuses(statuses, literals),
                "created_hour" => {
                    let times = created_times(
                        selection,
                        EntryStatusByStatusCreatedColumn::CreatedAt,
                        |clause| match clause {
                            EntryStatusByStatusCreatedClause::CreatedAt(times) => Some(times),
                            _ => None,
                        },
                    );
                    Ok(hour_buckets(times, literals))
                }
                _ => Ok(None),
            },
        )
    }

    /// The selection of the keys `selection` may match in the lanes of every
    /// one of `stages` with every one of `statuses`: their service and creation
    /// time, and the hours that may hold that time. The caller keeps its
    /// predicate.
    pub fn prepare_lanes(
        selection: &Selection<InvocationsByStatus>,
        stages: &[Stage],
        statuses: &[Status],
    ) -> crate::Result<KeySelection<Self>> {
        KeySelection::for_key_fields_with(
            selection,
            |clause, literals| match clause {
                EntryByStatusCreatedClause::TargetServiceName(predicate) => {
                    ServiceNameCodec::try_prepare_value(predicate, literals)
                }
                EntryByStatusCreatedClause::CreatedAt(predicate) => {
                    ReverseUniqueTimestampCodec::try_prepare_value(predicate, literals)
                }
                // The public status and completion result select the lanes;
                // the handler is in each key's value.
                _ => Ok(None),
            },
            |tag, literals| match tag {
                "stage" => pinned_stages(stages, literals),
                "status" => pinned_statuses(statuses, literals),
                "created_hour" => {
                    let times =
                        created_times(selection, EntryByStatusCreatedColumn::CreatedAt, |clause| {
                            match clause {
                                EntryByStatusCreatedClause::CreatedAt(times) => Some(times),
                                _ => None,
                            }
                        });
                    Ok(hour_buckets(times, literals))
                }
                _ => Ok(None),
            },
        )
    }
}

impl EntryByStatusTransitionedKey {
    /// The stages and stored statuses whose keys can hold an entry
    /// `selection` matches, in key order: those its conditions on the entry's
    /// stage and effective status allow. Each pair is a lane
    /// ([`Self::prepare_entry_lanes`]).
    pub fn entry_lanes(
        selection: &Selection<EntriesByStatusTransitioned>,
    ) -> impl Iterator<Item = (Stage, Status)> {
        Keyed::of_entry_selection(
            selection,
            |clause| match clause {
                EntryStatusByStatusTransitionedClause::Stage(stage) => Some(stage),
                _ => None,
            },
            |clause| match clause {
                EntryStatusByStatusTransitionedClause::Status(status) => Some(status),
                _ => None,
            },
        )
    }

    /// The selection of the keys of entries `selection` may match in the lanes
    /// of every one of `stages` with every one of `statuses`: their transition
    /// time, and the hours that may hold it. The caller keeps its predicate.
    pub fn prepare_entry_lanes(
        selection: &Selection<EntriesByStatusTransitioned>,
        stages: &[Stage],
        statuses: &[Status],
    ) -> crate::Result<KeySelection<Self>> {
        KeySelection::for_key_fields_with(
            selection,
            |clause, literals| match clause {
                EntryStatusByStatusTransitionedClause::TransitionedAt(predicate) => {
                    ReverseUniqueTimestampCodec::try_prepare_value(predicate, literals)
                }
                // The stage and status select the lanes.
                _ => Ok(None),
            },
            |tag, literals| match tag {
                "stage" => pinned_stages(stages, literals),
                "status" => pinned_statuses(statuses, literals),
                "transitioned_hour" => {
                    let times = created_times(
                        selection,
                        EntryStatusByStatusTransitionedColumn::TransitionedAt,
                        |clause| match clause {
                            EntryStatusByStatusTransitionedClause::TransitionedAt(times) => {
                                Some(times)
                            }
                            _ => None,
                        },
                    );
                    Ok(hour_buckets(times, literals))
                }
                _ => Ok(None),
            },
        )
    }
}

/// A status index's stage field, pinned to `stages`.
fn pinned_stages(
    stages: &[Stage],
    literals: &mut Vec<u8>,
) -> crate::Result<Option<PreparedFieldPredicate>> {
    StageRankCodec::try_prepare_value(&ValuePredicate::In(stages.to_vec()), literals)
}

/// A status index's status field, pinned to `statuses`.
fn pinned_statuses(
    statuses: &[Status],
    literals: &mut Vec<u8>,
) -> crate::Result<Option<PreparedFieldPredicate>> {
    StatusCodec::try_prepare_value(&ValuePredicate::In(statuses.to_vec()), literals)
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
