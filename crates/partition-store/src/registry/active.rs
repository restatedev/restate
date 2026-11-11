// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The instances maintained on every entry write, and how a write reaches them.
//!
//! **Tracking.** An entry is *tracked* by an instance when the instance reflects
//! it: its key or count is present exactly when the entry is a member. A Ready
//! instance tracks every entry; a Maintaining one tracks the entries whose stored
//! list names it. An untracked entry has no contribution yet, so a write treats
//! its before-image as absent (*adopt on write*): an index puts the new key without
//! deleting an old one, a statistic counts the new bucket without uncounting.
//! After any write the entry is tracked by every active instance, so the list it
//! stores is always the same: the Maintaining instances' IDs.
//!
//! For example, with instance 64 Ready and 70 Maintaining, an entry stored with no
//! list is tracked by 64 only. Updating it removes its old key from 64, adds its
//! new keys to both, and stores the list `[70]`. Once 70 is Ready too, its next
//! write stores no list at all.
//!
//! A Maintaining instance whose walk has a committed cursor also tracks every
//! entry whose entry-status key is at or before that cursor: the walk reflected
//! them without listing them (see [`crate::walk`]). Inserts have no before-image,
//! whatever their key.

use bytes::Bytes;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryChange, EntryContext, EntryStateRef, TrackedIds};

use crate::PartitionStoreTransaction;
use crate::walk::Walks;

use super::{RegistrationKind, Registry};

/// Applies an entry change to one instance `id` of a definition. `before` is
/// `None` for insertions and for entries the instance does not track yet.
pub(crate) type ApplyEntryChange = fn(
    &mut PartitionStoreTransaction<'_>,
    u32,
    &EntryContext<'_>,
    Option<&EntryStateRef<'_>>,
    Option<&EntryStateRef<'_>>,
);

/// An entry-scoped index or statistic known to this binary.
pub(crate) struct Definition {
    /// The stable name, including its version.
    pub(crate) name: &'static str,
    pub(crate) kind: RegistrationKind,
    pub(crate) apply: ApplyEntryChange,
}

/// The definitions this binary maintains. Opening a store retires the instances of
/// any other definition.
pub(crate) const DEFINITIONS: &[&Definition] = &[
    &crate::vqueue_table::SERVICE_STATS,
    &crate::vqueue_table::DEPLOYMENT_STATS,
    &crate::vqueue_table::VIRTUAL_OBJECT_STATS,
    &crate::vqueue_table::ENTRY_BY_STAGE_SERVICE,
    &crate::vqueue_table::ENTRY_BY_STAGE,
    &crate::vqueue_table::ENTRY_NEXT_AT_BY_STAGE,
    &crate::vqueue_table::ENTRY_NEXT_AT_BY_STAGE_SERVICE,
    &crate::vqueue_table::ENTRY_BY_VIRTUAL_OBJECT_STAGE,
    &crate::vqueue_table::ENTRY_NEXT_AT_BY_VIRTUAL_OBJECT_STAGE,
];

struct Active {
    id: u32,
    ready: bool,
    definition: &'static Definition,
    /// The walk backfilling this Maintaining instance, once started.
    walk: Option<u64>,
    /// The committed cursor of the walk backfilling this Maintaining instance:
    /// every entry at or before it is reflected, listed or not.
    covered: Option<Bytes>,
}

/// The Maintaining and Ready instances of known definitions, built from the
/// committed registry and walks whenever a commit changes them, and shared by
/// every transaction.
#[derive(Default)]
pub(crate) struct ActiveSet {
    /// Sorted by ID.
    entries: Vec<Active>,
    /// The list every written entry stores: the Maintaining IDs as sorted
    /// big-endian `u32`s. Empty when every instance is Ready.
    maintaining: Vec<u8>,
    /// The committed records this set was built from.
    registry: Registry,
    walks: Walks,
}

impl std::fmt::Debug for ActiveSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.entries
                    .iter()
                    .map(|entry| (entry.id, entry.definition.name, entry.ready)),
            )
            .finish()
    }
}

impl ActiveSet {
    /// Active registrations whose definition is in `known`, with a matching kind,
    /// and the cursors of their walks. Others are not maintained; opening a store
    /// retires them before writes.
    pub(crate) fn new(registry: &Registry, walks: &Walks, known: &[&'static Definition]) -> Self {
        Self::build(registry.clone(), walks.clone(), known)
    }

    /// The set of the same definitions after a commit changed the registry or the
    /// walks to `registry` and `walks`.
    pub(crate) fn successor(&self, registry: Registry, walks: Walks) -> Self {
        let known: Vec<_> = self.entries.iter().map(|entry| entry.definition).collect();
        Self::build(registry, walks, &known)
    }

    fn build(registry: Registry, walks: Walks, known: &[&'static Definition]) -> Self {
        let mut entries: Vec<_> = registry
            .entries()
            .iter()
            .filter(|registration| registration.is_active())
            .filter_map(|registration| {
                known
                    .iter()
                    .find(|definition| {
                        registration.definition == definition.name
                            && definition.kind == registration.kind
                    })
                    .map(|definition| Active {
                        id: registration.id,
                        ready: registration.state == super::RegistrationState::Ready,
                        definition,
                        walk: registration.walk,
                        covered: registration
                            .walk
                            .and_then(|walk| walks.get(walk))
                            .and_then(|walk| walk.cursor.clone()),
                    })
            })
            .collect();
        entries.sort_unstable_by_key(|entry| entry.id);
        let maintaining = entries
            .iter()
            .filter(|entry| !entry.ready)
            .flat_map(|entry| entry.id.to_be_bytes())
            .collect();
        Self {
            entries,
            maintaining,
            registry,
            walks,
        }
    }

    /// The committed registry.
    pub(crate) fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The committed walks.
    pub(crate) fn walks(&self) -> &Walks {
        &self.walks
    }

    /// The instances walk `walk` backfills, or an error if a member's definition
    /// is unknown here.
    pub(crate) fn members(
        &self,
        walk: u64,
    ) -> Result<Vec<(&'static Definition, u32)>, StorageError> {
        let members: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.walk == Some(walk))
            .map(|entry| (entry.definition, entry.id))
            .collect();
        if members.len() != self.registry.members(walk).count() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(members)
    }

    /// The physical ID of the Ready instance of `definition`, or why there is none.
    pub(crate) fn ready_instance(
        &self,
        definition: &str,
        kind: RegistrationKind,
    ) -> Result<u32, String> {
        let entry = self
            .entries
            .iter()
            .find(|entry| entry.definition.name == definition && entry.definition.kind == kind)
            .ok_or_else(|| "no active instance".to_owned())?;
        if !entry.ready {
            return Err(format!("instance {} is still being completed", entry.id));
        }
        Ok(entry.id)
    }

    /// The tracking list every entry written now stores.
    pub(crate) fn tracked_after(&self) -> &[u8] {
        &self.maintaining
    }

    /// Applies `change`, of the entry whose entry-status key is `key`, to every
    /// active instance. The stored list and walk cursors are only consulted while
    /// some instance is Maintaining.
    pub(crate) fn apply(
        &self,
        storage: &mut PartitionStoreTransaction<'_>,
        context: &EntryContext<'_>,
        change: &EntryChange<'_>,
        key: &[u8],
    ) {
        let tracked = change
            .stored_before()
            .map_or(TrackedIds::EMPTY, |before| before.tracked());
        for entry in &self.entries {
            let before = change.before().filter(|_| {
                entry.ready
                    || tracked.contains(entry.id)
                    || entry.covered.as_deref().is_some_and(|cursor| key <= cursor)
            });
            (entry.definition.apply)(storage, entry.id, context, before, change.after());
        }
    }
}
