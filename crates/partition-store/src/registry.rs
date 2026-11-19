// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The local registry of a partition store's indexes and one-time migrations.
//!
//! The registry holds two kinds of entries, numbered by one counter:
//!
//! | Entry | Life | Run by | Defined in |
//! |---|---|---|---|
//! | [`Index`] | Backfilling ─▶ Ready ─▶ Dropped | its build job, on a walk | an index definition, below |
//! | [`Migration`] | InProgress ─▶ Complete | its job, on a walk | an `OnlineMigration` (`migrations::online`) |
//!
//! This page describes indexes; [`migration`] describes migrations.
//!
//! An **index definition** is code-defined, with a stable name that includes its
//! semantic version, such as `"entry_by_stage_service/1"`. Changing its
//! membership, key or value encoding means a new name. An **index** is one
//! physical build of a definition in this store: an **index ID** that prefixes
//! every key of the build, so that its keys form one range, and that entries
//! hold as a token while the index is being built, plus its [`IndexStatus`] and,
//! while it is being backfilled, the walk doing so (see [`crate::walk`]). An
//! aggregate index, a statistic, stores counters grouped by the definition's
//! keys rather than one key per member entry.
//!
//! The registry is local to a replica and is not replicated: replicas may assign
//! different index IDs to the same definition. Queries therefore name the
//! definition and resolve the index ID against an admitted snapshot.
//!
//! The registry, its walks and its jobs are stored under one key kind,
//! one key per record ([`records`]).
//!
//! # IDs
//!
//! One counter issues IDs for secondary and aggregate indexes and migrations
//! alike, starting at [`FIRST_ID`], so that an entry's tokens are never
//! ambiguous and an entry's job can have the entry's ID. IDs are never reused,
//! not even after an index is dropped, and fit the `u32` of both key prefixes.
//! Registering a definition that already has a maintained index returns that
//! index; ordinary reopening therefore consumes no IDs.
//!
//! ```
//! use restate_partition_store::registry::{IndexKind, IndexStatus, Registry};
//!
//! let mut registry = Registry::default();
//! let by_service = registry.register("entry_by_stage_service/1", IndexKind::Secondary).unwrap();
//! let gauges = registry.register("service_gauges/1", IndexKind::Aggregate).unwrap();
//! assert_eq!((by_service, gauges), (64, 65));
//! // A maintained index is returned as is.
//! assert_eq!(registry.register("entry_by_stage_service/1", IndexKind::Secondary).unwrap(), 64);
//!
//! // Dropping keeps the ID reserved; registering again builds a fresh index.
//! registry.drop_index(64).unwrap();
//! assert_eq!(registry.register("entry_by_stage_service/1", IndexKind::Secondary).unwrap(), 66);
//! assert_eq!(registry.get(64).unwrap().state, IndexStatus::Dropped);
//! ```

use restate_util_string::ReString;

mod active;
mod backfill;
pub mod migration;
mod reconcile;
pub mod records;

#[cfg(test)]
pub(crate) use active::TEST_INTRODUCED;
pub(crate) use active::{Family, INDEX_DEFINITIONS, IndexDefinition, PublishedRegistry};
pub(crate) use backfill::{IndexBackfill, start as start_backfill};
pub use migration::{Migration, MigrationOnUnknown, MigrationState, MigrationTimeline};
pub(crate) use reconcile::{FAMILIES, FenceState, Requests};

/// Whether this binary's definition `name` fills rows itself (`Some(true)`, an
/// active one) or relies on writes and deletions (`Some(false)`, a passive one);
/// `None` if this binary doesn't know it.
pub fn fill_capability(name: &str) -> Option<bool> {
    INDEX_DEFINITIONS
        .iter()
        .find(|definition| definition.name == name)
        .map(|definition| definition.can_fill)
}

/// The first ID the registry issues. Lower IDs belong to the static index (1–7)
/// and statistic (1–3) identities that predate the registry.
///
/// This floor constrains registry envelopes only: an ID below it is invalid.
/// Opening a partition deletes the legacy status gauges
/// (`PartitionStore::remove_legacy_status_gauges`); keys under the static index
/// identities are not handled here.
pub const FIRST_ID: u32 = 64;

/// The first walk ID the registry issues: a store without a registry has
/// issued none.
pub(crate) const FIRST_WALK_ID: u64 = 1;

/// The persisted registry format this binary reads and writes. It versions the
/// whole record family: the registry, its walks and its jobs (see [`records`]).
const FORMAT: u32 = 1;

/// What an index stores for each entry.
///
/// **Compatibility.** A kind this binary doesn't know makes the whole registry
/// unreadable (bilrost rejects unknown enumeration values), which fails the open;
/// nor could this binary delete the keys of such an index. A release adding a
/// kind raises the version floor before registering one, so older binaries
/// refuse the store with a clear version error. This binary refuses a store whose
/// floor is above its version the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum IndexKind {
    #[bilrost(0)]
    Unknown,
    /// Keys: one key per member entry, ordered by the definition's fields, for
    /// example "entries by stage and service".
    #[bilrost(1)]
    Secondary,
    /// Counters: one bucket per group, maintained with merge deltas, for example
    /// "how many entries of each service are in each stage". A statistic.
    #[bilrost(2)]
    Aggregate,
}

/// Where an index is in its life. Writes maintain a `Backfilling` or `Ready`
/// index; queries read only a `Ready` one.
///
/// ```text
/// register ──▶ Backfilling ──(walk reflects every entry)──▶ Ready
///                  │                                          │
///                  └──────────── drop ───────────────────────┘──▶ Dropped
/// ```
///
/// **Compatibility.** As for [`IndexKind`]: a status this binary doesn't know
/// fails the open, so a release adding one raises the version floor first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum IndexStatus {
    #[bilrost(0)]
    Unknown,
    /// Maintained on every write, but incomplete: entries written since the index
    /// was registered hold its token and are reflected; older entries are not
    /// reflected until a walk reaches them. Queries must not use it.
    #[bilrost(1)]
    Backfilling,
    /// Maintained, and every entry is reflected: queries may use it.
    #[bilrost(2)]
    Ready,
    /// Neither maintained nor readable; its keys were deleted. The ID stays
    /// reserved, so a stale token in an entry never matches a new index.
    #[bilrost(3)]
    Dropped,
}

/// Why an index was dropped, stored as its number in
/// [`IndexTimeline::drop_reason`].
///
/// **Compatibility.** The timeline stores the number, not a bilrost enumeration:
/// an enumeration value a binary doesn't know fails decoding, which here would
/// make the whole registry unreadable. A binary that finds a number it doesn't
/// know keeps it through rewrites and shows it as unknown, so a release can add a
/// reason without a version floor. Never reuse a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Its family's reset was requested.
    Reset = 1,
    /// The binary that opened the store doesn't know its definition.
    UnknownDefinition = 2,
}

impl DropReason {
    /// The reason stored as `number`, if this binary knows it.
    pub fn from_number(number: u32) -> Option<Self> {
        match number {
            1 => Some(Self::Reset),
            2 => Some(Self::UnknownDefinition),
            _ => None,
        }
    }
}

/// When an index went through its states, for operators. Every field is `None`
/// (or 0) until its event happens, or when unknown. Nothing reads it to decide
/// anything. Times are wall-clock milliseconds since the epoch on this replica.
#[derive(Debug, Clone, Default, PartialEq, Eq, bilrost::Message)]
pub struct IndexTimeline {
    #[bilrost(1)]
    pub registered_at: Option<u64>,
    /// The Restate version that registered it.
    #[bilrost(2)]
    pub registered_by: Option<ReString>,
    #[bilrost(3)]
    pub ready_at: Option<u64>,
    #[bilrost(4)]
    pub dropped_at: Option<u64>,
    /// Why it was dropped: a [`DropReason`] number, 0 when not dropped.
    #[bilrost(5)]
    pub drop_reason: u32,
}

impl std::fmt::Display for IndexTimeline {
    /// The times, then the drop reason by name, or `unknown(N)` for a number this
    /// binary doesn't know.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "registered={:?} registered_by={:?} ready={:?} dropped={:?}",
            self.registered_at, self.registered_by, self.ready_at, self.dropped_at
        )?;
        match (self.drop_reason, DropReason::from_number(self.drop_reason)) {
            (0, _) => Ok(()),
            (_, Some(reason)) => write!(f, " drop_reason={reason:?}"),
            (number, None) => write!(f, " drop_reason=unknown({number})"),
        }
    }
}

impl IndexTimeline {
    /// Records that the index was dropped at `at` for `reason`.
    pub(crate) fn dropped(&mut self, at: u64, reason: DropReason) {
        self.dropped_at = Some(at);
        self.drop_reason = reason as u32;
    }
}

/// One physical build of a definition in this store: its ID, status and, while
/// it is being backfilled, the walk doing so.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct Index {
    #[bilrost(1)]
    pub definition: ReString,
    #[bilrost(2)]
    pub kind: IndexKind,
    #[bilrost(3)]
    pub id: u32,
    #[bilrost(4)]
    pub state: IndexStatus,
    /// The walk backfilling this Backfilling index, once started.
    #[bilrost(5)]
    pub walk: Option<u64>,
    #[bilrost(6)]
    pub timeline: IndexTimeline,
}

impl Index {
    /// Maintained on writes: Backfilling or Ready.
    pub fn is_maintained(&self) -> bool {
        matches!(self.state, IndexStatus::Backfilling | IndexStatus::Ready)
    }
}

/// A query needs an index that is not Ready in the partition snapshot it reads:
/// not registered, still being backfilled, dropped, or the registry is unreadable.
/// Reading it anyway could miss entries, so the read fails instead.
#[derive(Debug, thiserror::Error)]
#[error("'{definition}' is not ready on partition {partition}: {reason}")]
pub struct IndexNotReady {
    pub definition: ReString,
    pub partition: restate_types::identifiers::PartitionId,
    pub reason: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("all {} IDs of this store's registry are allocated", u32::MAX - FIRST_ID + 1)]
    Exhausted,
    #[error("unsupported registry format {0}; this binary supports format {FORMAT}")]
    UnsupportedFormat(u32),
    #[error("'{0}' is registered as a different kind")]
    KindMismatch(ReString),
    #[error("indexes must be secondary or aggregate")]
    UnknownKind,
    #[error("no entry has ID {0}")]
    UnknownId(u32),
    #[error("migration {0} is not in progress")]
    NotInProgress(u32),
    #[error("index {id} cannot change from {from:?} to {to:?}")]
    InvalidTransition {
        id: u32,
        from: IndexStatus,
        to: IndexStatus,
    },
    #[error("malformed registry: {0}")]
    Malformed(#[from] bilrost::DecodeError),
    #[error("invalid registry: {0}")]
    Invalid(&'static str),
}

/// The indexes and migrations of one partition store, and the counters that
/// issue their IDs and their walks' IDs. Persisted tags never change meaning
/// (README rule 8).
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct Registry {
    /// A newer format must not be rewritten by this binary: re-encoding would
    /// drop fields it does not know. It versions the whole record family.
    #[bilrost(1)]
    format: u32,
    /// The next ID to issue. Beyond `u32::MAX` the counter is exhausted.
    #[bilrost(2)]
    next_id: u64,
    #[bilrost(3)]
    indexes: Vec<Index>,
    /// The next walk ID to issue, never 0; `u64::MAX` is never issued.
    #[bilrost(4)]
    next_walk_id: u64,
    /// The one-time migrations, complete ones included, in registration order.
    #[bilrost(5)]
    migrations: Vec<Migration>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            format: FORMAT,
            next_id: u64::from(FIRST_ID),
            indexes: Vec::new(),
            next_walk_id: FIRST_WALK_ID,
            migrations: Vec::new(),
        }
    }
}

impl Registry {
    /// Decodes a persisted registry and checks the invariants that allocation and
    /// maintenance rely on; a registry violating them is an error, not repaired.
    pub fn decode(bytes: &[u8]) -> Result<Self, RegistryError> {
        let registry = <Self as bilrost::OwnedMessage>::decode(bytes)?;
        if registry.format != FORMAT {
            return Err(RegistryError::UnsupportedFormat(registry.format));
        }
        registry.validate()?;
        Ok(registry)
    }

    fn validate(&self) -> Result<(), RegistryError> {
        let invalid = |reason| Err(RegistryError::Invalid(reason));
        if self.next_id < u64::from(FIRST_ID) {
            return invalid("the ID counter is below the first ID");
        }
        if self.next_walk_id == 0 {
            return invalid("the walk ID counter is zero");
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut active = std::collections::BTreeSet::new();
        let mut issued = |id: u32| {
            if id < FIRST_ID || u64::from(id) >= self.next_id {
                return invalid("an ID was not issued by the counter");
            }
            if !ids.insert(id) {
                return invalid("an ID is registered twice");
            }
            Ok(())
        };
        for entry in &self.indexes {
            issued(entry.id)?;
            if entry.kind == IndexKind::Unknown || entry.state == IndexStatus::Unknown {
                return invalid("an index has an unknown kind or state");
            }
            if entry.is_maintained() && !active.insert(&entry.definition) {
                return invalid("a definition has more than one maintained index");
            }
            if entry.walk.is_some() && entry.state != IndexStatus::Backfilling {
                return invalid("only a Backfilling index can be backfilled");
            }
        }
        // Several entries may link one walk: a later release's walk may run a
        // migration and the builds that require it. The open decides whether
        // this binary can run such a walk (`crate::walk::Walk::is_known`).
        let mut definitions = std::collections::BTreeSet::new();
        for migration in &self.migrations {
            issued(migration.id)?;
            if !definitions.insert(&migration.definition) {
                return invalid("a migration is registered twice");
            }
            // A state this binary doesn't know is a later release's: kept as is.
            if migration.state == 0 {
                return invalid("a migration has no state");
            }
            if migration.walk.is_some() && migration.state() == Some(MigrationState::Complete) {
                return invalid("a Complete migration has a walk");
            }
        }
        Ok(())
    }

    pub fn format(&self) -> u32 {
        self.format
    }

    /// The ID the next walk gets (see [`crate::walk::Walks::start`]).
    pub fn next_walk_id(&self) -> u64 {
        self.next_walk_id
    }

    /// Issues [`Self::next_walk_id`] to a walk that starts. The caller checks
    /// that it isn't `u64::MAX`, which is never issued.
    pub(crate) fn issue_walk_id(&mut self) -> u64 {
        let id = self.next_walk_id;
        self.next_walk_id += 1;
        id
    }

    /// Links a Backfilling index to the walk backfilling it.
    pub(crate) fn link(&mut self, id: u32, walk: u64) -> Result<(), RegistryError> {
        use IndexStatus::Backfilling;
        self.transition(id, &[Backfilling], Backfilling)?.walk = Some(walk);
        Ok(())
    }

    /// The indexes a walk backfills.
    pub fn members(&self, walk: u64) -> impl Iterator<Item = &Index> {
        self.indexes
            .iter()
            .filter(move |entry| entry.walk == Some(walk))
    }

    pub fn encode(&self) -> Vec<u8> {
        bilrost::Message::encode_to_vec(self)
    }

    /// Returns the maintained index of `definition`, registering a new one in
    /// state Backfilling with a fresh ID if there is none. Errors leave the
    /// registry unchanged.
    pub fn register(&mut self, definition: &str, kind: IndexKind) -> Result<u32, RegistryError> {
        if kind == IndexKind::Unknown {
            return Err(RegistryError::UnknownKind);
        }
        if let Some(active) = self.maintained(definition) {
            if active.kind != kind {
                return Err(RegistryError::KindMismatch(active.definition.clone()));
            }
            return Ok(active.id);
        }
        let id = self.issue_id()?;
        self.indexes.push(Index {
            definition: ReString::from(definition),
            kind,
            id,
            state: IndexStatus::Backfilling,
            walk: None,
            timeline: IndexTimeline::default(),
        });
        Ok(id)
    }

    /// Issues the next ID, to an index or a migration.
    fn issue_id(&mut self) -> Result<u32, RegistryError> {
        let id = u32::try_from(self.next_id).map_err(|_| RegistryError::Exhausted)?;
        self.next_id += 1;
        Ok(id)
    }

    /// The maintained (Backfilling or Ready) index of `definition`, if any.
    pub fn maintained(&self, definition: &str) -> Option<&Index> {
        self.indexes
            .iter()
            .find(|entry| entry.is_maintained() && entry.definition == definition)
    }

    pub fn get(&self, id: u32) -> Option<&Index> {
        self.indexes.iter().find(|entry| entry.id == id)
    }

    /// The index `id`, which must currently be in one of `from`.
    fn transition(
        &mut self,
        id: u32,
        from: &[IndexStatus],
        to: IndexStatus,
    ) -> Result<&mut Index, RegistryError> {
        let entry = self
            .indexes
            .iter_mut()
            .find(|entry| entry.id == id)
            .ok_or(RegistryError::UnknownId(id))?;
        if !from.contains(&entry.state) {
            return Err(RegistryError::InvalidTransition {
                id,
                from: entry.state,
                to,
            });
        }
        Ok(entry)
    }

    /// All indexes, including dropped ones, in allocation order.
    pub fn indexes(&self) -> &[Index] {
        &self.indexes
    }

    /// The timeline of index `id`, to record what happens to it.
    pub(crate) fn timeline_mut(&mut self, id: u32) -> Option<&mut IndexTimeline> {
        self.indexes
            .iter_mut()
            .find(|entry| entry.id == id)
            .map(|entry| &mut entry.timeline)
    }

    /// Unlinks a Backfilling index from its walk; it stays Backfilling, waiting
    /// for another pass.
    pub fn unlink(&mut self, id: u32) -> Result<(), RegistryError> {
        use IndexStatus::Backfilling;
        self.transition(id, &[Backfilling], Backfilling)?.walk = None;
        Ok(())
    }

    /// Completes a Backfilling index: unlinks its walk and makes it Ready.
    pub fn set_ready(&mut self, id: u32) -> Result<(), RegistryError> {
        use IndexStatus::{Backfilling, Ready};
        let entry = self.transition(id, &[Backfilling], Ready)?;
        entry.walk = None;
        entry.state = Ready;
        Ok(())
    }

    /// Stops maintaining a maintained index, keeping its ID reserved. The caller
    /// deletes its keys in the same batch that persists this change.
    pub fn drop_index(&mut self, id: u32) -> Result<(), RegistryError> {
        use IndexStatus::{Backfilling, Dropped, Ready};
        let entry = self.transition(id, &[Backfilling, Ready], Dropped)?;
        entry.walk = None;
        entry.state = Dropped;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_fields_are_skipped_and_unknown_drop_reasons_kept() {
        // How a binary sees a later release's record: as one without the fields
        // added since. Here, this binary's record without the timeline.
        #[derive(bilrost::Message)]
        struct OlderIndex {
            #[bilrost(1)]
            definition: ReString,
            #[bilrost(2)]
            kind: IndexKind,
            #[bilrost(3)]
            id: u32,
            #[bilrost(4)]
            state: IndexStatus,
            #[bilrost(5)]
            walk: Option<u64>,
        }
        let index = Index {
            definition: ReString::from_static("stat/1"),
            kind: IndexKind::Aggregate,
            id: 64,
            state: IndexStatus::Backfilling,
            walk: Some(1),
            timeline: IndexTimeline {
                registered_at: Some(5),
                registered_by: Some(ReString::from_static("1.8.2")),
                ..IndexTimeline::default()
            },
        };
        let encoded = bilrost::Message::encode_to_vec(&index);
        let older = <OlderIndex as bilrost::OwnedMessage>::decode(encoded.as_slice()).unwrap();
        assert_eq!(
            (older.id, older.walk),
            (64, Some(1)),
            "the timeline is skipped"
        );
        let rewritten = bilrost::Message::encode_to_vec(&older);
        let back = <Index as bilrost::OwnedMessage>::decode(rewritten.as_slice()).unwrap();
        assert_eq!(
            back.timeline,
            IndexTimeline::default(),
            "and lost by a rewrite"
        );
        assert_eq!(
            (back.id, back.state, back.walk),
            (index.id, index.state, index.walk)
        );

        // A drop reason a later release added survives this binary's rewrite.
        let mut dropped = index;
        dropped.timeline.drop_reason = 99;
        let back = <Index as bilrost::OwnedMessage>::decode(
            bilrost::Message::encode_to_vec(&dropped).as_slice(),
        )
        .unwrap();
        assert_eq!(back.timeline.drop_reason, 99);
        assert!(
            back.timeline
                .to_string()
                .ends_with(" drop_reason=unknown(99)")
        );
        // Known reasons show by name.
        let mut reset = back.timeline;
        reset.dropped(7, DropReason::Reset);
        assert!(
            reset
                .to_string()
                .ends_with(" dropped=Some(7) drop_reason=Reset")
        );
    }

    #[test]
    fn ids_are_shared_monotonic_and_bounded() {
        let mut registry = Registry::default();
        assert_eq!(registry.register("a/1", IndexKind::Secondary), Ok(FIRST_ID));
        assert_eq!(
            registry.register("b/1", IndexKind::Aggregate),
            Ok(FIRST_ID + 1),
            "one_counter_for_both_kinds"
        );
        assert_eq!(
            registry.register("b/1", IndexKind::Secondary),
            Err(RegistryError::KindMismatch("b/1".into()))
        );
        let unchanged = registry.clone();
        assert_eq!(
            registry.register("c/1", IndexKind::Unknown),
            Err(RegistryError::UnknownKind)
        );
        assert_eq!(registry, unchanged, "rejected_registrations_change_nothing");
        registry.link(64, 7).unwrap();
        assert_eq!(
            Registry::decode(&registry.encode())
                .unwrap()
                .get(64)
                .unwrap()
                .walk,
            Some(7),
            "links_persist"
        );
        registry.set_ready(64).unwrap();
        assert_eq!(registry.get(64).unwrap().state, IndexStatus::Ready);
        assert_eq!(registry.get(64).unwrap().walk, None);

        // Round trips preserve the counter: reopening consumes no IDs.
        let mut reopened = Registry::decode(&registry.encode()).unwrap();
        assert_eq!(reopened, registry);
        assert_eq!(reopened.register("a/1", IndexKind::Secondary), Ok(64));
        reopened.drop_index(64).unwrap();
        assert!(reopened.maintained("a/1").is_none());
        assert_eq!(
            reopened.register("a/1", IndexKind::Secondary),
            Ok(66),
            "disabled_ids_stay_reserved"
        );
        assert_eq!(reopened.indexes().len(), 3);

        // The last ID fits the key prefixes' u32; the next registration fails
        // without changing anything.
        reopened.next_id = u64::from(u32::MAX);
        assert_eq!(reopened.register("c/1", IndexKind::Secondary), Ok(u32::MAX));
        let exhausted = reopened.clone();
        assert_eq!(
            reopened.register("d/1", IndexKind::Secondary),
            Err(RegistryError::Exhausted)
        );
        assert_eq!(reopened, exhausted);
        assert_eq!(
            Registry::decode(&reopened.encode()).unwrap(),
            reopened,
            "exhaustion_is_persisted"
        );

        let newer = Registry {
            format: FORMAT + 1,
            ..Registry::default()
        };
        assert_eq!(
            Registry::decode(&newer.encode()),
            Err(RegistryError::UnsupportedFormat(FORMAT + 1))
        );

        // The walk counter persists with the registry, and its last ID is never
        // issued.
        let mut walks = crate::walk::Walks::default();
        let mut start = |registry: &mut Registry| {
            walks.start(
                registry,
                crate::walk::WalkKind::index_backfill(),
                crate::walk::Source::EntryStatus,
                bytes::Bytes::new(),
            )
        };
        assert_eq!(start(&mut reopened), Ok(1));
        assert_eq!(
            Registry::decode(&reopened.encode()).unwrap().next_walk_id(),
            2
        );
        reopened.next_walk_id = u64::MAX;
        assert_eq!(start(&mut reopened), Err(crate::walk::WalkError::Exhausted));
        assert_eq!(reopened.next_walk_id(), u64::MAX);
    }

    #[test]
    fn transitions_and_persisted_invariants_are_checked() {
        let mut registry = Registry::default();
        let id = registry.register("a/1", IndexKind::Secondary).unwrap();
        let missing = Err(RegistryError::UnknownId(99));
        assert_eq!(registry.set_ready(99), missing);
        assert_eq!(registry.drop_index(99), missing);
        assert_eq!(registry.link(99, 1), missing);
        registry.set_ready(id).unwrap();
        let ready_again = Err(RegistryError::InvalidTransition {
            id,
            from: IndexStatus::Ready,
            to: IndexStatus::Ready,
        });
        assert_eq!(registry.set_ready(id), ready_again);
        assert!(
            registry.link(id, 1).is_err(),
            "ready_indexes_are_not_backfilled"
        );
        registry.drop_index(id).unwrap();
        assert!(
            registry.drop_index(id).is_err(),
            "disabled_instances_stay_disabled"
        );

        // Migrations take IDs from the same counter, register once, and complete
        // for good. A state or policy number a later release adds is kept.
        let by = || ReString::from_static("1.8.2");
        let keep = MigrationOnUnknown::Keep;
        let migration = registry.register_migration("m/1", keep, 1, by()).unwrap();
        assert_eq!(migration, 65);
        assert_eq!(registry.register_migration("m/1", keep, 2, by()), Ok(65));
        registry.set_migration_walk(migration, Some(3)).unwrap();
        assert_eq!(registry.walk_of(migration), Some(3));
        registry.complete(migration, 4, by()).unwrap();
        let complete = registry.get_migration(migration).unwrap();
        assert_eq!(
            (
                complete.state(),
                complete.walk,
                complete.timeline.completed_at
            ),
            (Some(MigrationState::Complete), None, Some(4))
        );
        assert_eq!(
            registry.complete(migration, 5, by()),
            Err(RegistryError::NotInProgress(migration))
        );
        registry.migrations[0].state = 9;
        registry.migrations[0].on_unknown = 7;
        let later = Registry::decode(&registry.encode()).unwrap();
        let entry = &later.migrations()[0];
        assert_eq!((entry.state, entry.state(), entry.on_unknown), (9, None, 7));
        assert_eq!(entry.on_unknown(), MigrationOnUnknown::Refuse, "fails safe");

        // Each persisted state that breaks an invariant is rejected on load.
        let valid = {
            let mut valid = Registry::default();
            valid.register("a/1", IndexKind::Secondary).unwrap();
            valid.register("b/1", IndexKind::Aggregate).unwrap();
            let id = valid.register_migration("m/1", keep, 1, by()).unwrap();
            valid.set_migration_walk(id, Some(2)).unwrap();
            valid.link(64, 1).unwrap();
            valid.next_walk_id = 3;
            valid
        };
        type Corruption = (&'static str, fn(&mut Registry));
        let broken: [Corruption; 13] = [
            ("counter below the floor", |r| r.next_id = 10),
            ("walk counter at zero", |r| r.next_walk_id = 0),
            ("ID not issued", |r| r.next_id = 65),
            ("ID below the floor", |r| r.indexes[0].id = 3),
            ("duplicate ID", |r| r.indexes[1].id = 64),
            ("unknown kind", |r| r.indexes[0].kind = IndexKind::Unknown),
            ("two maintained indexes", |r| {
                r.indexes[1].definition = "a/1".into()
            }),
            ("walk on a Ready index", |r| {
                r.indexes[0].state = IndexStatus::Ready;
                r.indexes[0].walk = Some(1);
            }),
            ("migration ID of an index", |r| r.migrations[0].id = 64),
            ("migration ID not issued", |r| r.migrations[0].id = 70),
            ("migration without a state", |r| r.migrations[0].state = 0),
            ("walk on a Complete migration", |r| {
                r.migrations[0].state = MigrationState::Complete as u32
            }),
            ("migration registered twice", |r| {
                let twice = Migration {
                    id: 67,
                    walk: None,
                    ..r.migrations[0].clone()
                };
                r.migrations.push(twice);
                r.next_id = 68;
            }),
        ];
        assert!(Registry::decode(&valid.encode()).is_ok());
        // Entries sharing a walk load: a later release's walk may run a
        // migration and the builds that require it.
        let mut shared = valid.clone();
        shared.migrations[0].walk = Some(1);
        assert!(Registry::decode(&shared.encode()).is_ok());
        for (case, corrupt) in broken {
            let mut registry = valid.clone();
            corrupt(&mut registry);
            assert!(
                matches!(
                    Registry::decode(&registry.encode()),
                    Err(RegistryError::Invalid(_))
                ),
                "{case}"
            );
        }
    }
}
