// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The indexes maintained on every entry write, and how a write reaches them.
//!
//! **Coverage.** An index reflects an entry when its keys or counts match that
//! entry's contribution, including an empty contribution outside its membership.
//! A Ready index reflects every entry. For a Backfilling index, coverage is proved
//! by either the entry's token for its build job (which has the index's ID) or the
//! committed cursor of its walk: every entry-status key at or before the cursor
//! is covered even without a token (see [`crate::walk`]).
//!
//! A write treats an uncovered entry's before-image as absent (*adopt on write*):
//! a secondary index adds its new key without deleting an old one; an aggregate
//! counts its new bucket without uncounting. After any write the entry holds a
//! token for every Backfilling index, so the list it stores is always the same:
//! the Backfilling indexes' IDs.
//!
//! For example, with index 64 Ready and 70 Backfilling, an entry stored with no
//! list and beyond 70's cursor is reflected by 64 only. Updating it removes its
//! old key from 64, adds its new keys to both, and stores the list `[70]`. Once 70
//! is Ready too, its next write stores no list at all.
//!
//! Inserts have no before-image, whatever their key. Backfill slices establish
//! coverage through their cursor without rewriting entry headers.

use bytes::Bytes;

use restate_storage_api::StorageError;
use restate_storage_api::migration_job::MigrationJobIds;
use restate_storage_api::vqueue_table::{EntryChange, EntryContext, EntryStateRef};

use restate_types::config::Configuration;

use crate::PartitionStoreTransaction;
use crate::walk::Walks;

use super::{IndexKind, IndexRegistry};

/// Applies an entry change to one index `id` of a definition. `before` is
/// `None` for insertions and for entries the index does not reflect yet.
pub(crate) type ApplyEntryChange = fn(
    &mut PartitionStoreTransaction<'_>,
    u32,
    &EntryContext<'_>,
    Option<&EntryStateRef<'_>>,
    Option<&EntryStateRef<'_>>,
);

/// Definitions released together, switched on by the same configuration options.
#[allow(dead_code)] // Consumed when the registry is reconciled at open.
pub(crate) struct Family {
    pub(crate) definitions: &'static [&'static IndexDefinition],
    /// Register and maintain the definitions.
    pub(crate) maintain: fn(&Configuration) -> bool,
    /// Disable their indexes, overriding the other requests.
    pub(crate) reset: fn(&Configuration) -> bool,
}

/// An entry-scoped index or statistic known to this binary.
pub(crate) struct IndexDefinition {
    /// The stable name, including its version.
    pub(crate) name: &'static str,
    pub(crate) kind: IndexKind,
    pub(crate) apply: ApplyEntryChange,
}

/// The definitions this binary maintains. Opening a store retires the indexes of
/// any other definition.
pub(crate) const DEFINITIONS: &[&IndexDefinition] = &[
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

#[allow(dead_code)] // Consumed when the registry is reconciled at open.
struct Active {
    id: u32,
    ready: bool,
    definition: &'static IndexDefinition,
    /// The walk backfilling this Backfilling index, once started.
    walk: Option<u64>,
    /// The committed cursor of the walk backfilling this Backfilling index:
    /// every entry at or before it is reflected, listed or not.
    covered: Option<Bytes>,
}

/// The Backfilling and Ready indexes of known definitions, built from the
/// committed registry and walks whenever a commit changes them, and shared by
/// every transaction.
#[derive(Default)]
#[allow(dead_code)] // Consumed when the registry is reconciled at open.
pub(crate) struct MaintainedIndexes {
    /// Sorted by ID.
    entries: Vec<Active>,
    /// The list every written entry stores: the Backfilling IDs as sorted
    /// big-endian `u32`s. Empty when every index is Ready.
    maintaining: Vec<u8>,
    /// The committed records this set was built from.
    registry: IndexRegistry,
    walks: Walks,
}

impl std::fmt::Debug for MaintainedIndexes {
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

impl MaintainedIndexes {
    /// Maintained indexes whose definition is in `known`, with a matching kind,
    /// and the cursors of their walks. Others are not maintained; opening a store
    /// retires them before writes.
    pub(crate) fn new(
        registry: &IndexRegistry,
        walks: &Walks,
        known: &[&'static IndexDefinition],
    ) -> Self {
        Self::build(registry.clone(), walks.clone(), known)
    }

    /// The set of the same definitions after a commit changed the registry or the
    /// walks to `registry` and `walks`.
    #[allow(dead_code)] // Consumed when the registry is reconciled at open.
    pub(crate) fn successor(&self, registry: IndexRegistry, walks: Walks) -> Self {
        let known: Vec<_> = self.entries.iter().map(|entry| entry.definition).collect();
        Self::build(registry, walks, &known)
    }

    fn build(registry: IndexRegistry, walks: Walks, known: &[&'static IndexDefinition]) -> Self {
        let mut entries: Vec<_> = registry
            .entries()
            .iter()
            .filter(|index| index.is_maintained())
            .filter_map(|index| {
                known
                    .iter()
                    .find(|definition| {
                        index.definition == definition.name && definition.kind == index.kind
                    })
                    .map(|definition| Active {
                        id: index.id,
                        ready: index.state == super::IndexStatus::Ready,
                        definition,
                        walk: index.walk,
                        covered: index
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
    #[allow(dead_code)] // Consumed when the registry is reconciled at open.
    pub(crate) fn registry(&self) -> &IndexRegistry {
        &self.registry
    }

    /// The committed walks.
    #[allow(dead_code)] // Consumed when the registry is reconciled at open.
    pub(crate) fn walks(&self) -> &Walks {
        &self.walks
    }

    /// The indexes walk `walk` backfills, or an error if a member's definition
    /// is unknown here.
    #[allow(dead_code)] // Consumed when the registry is reconciled at open.
    pub(crate) fn members(
        &self,
        walk: u64,
    ) -> Result<Vec<(&'static IndexDefinition, u32)>, StorageError> {
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

    /// The index ID of the Ready index of `definition`, or why there is none.
    pub(crate) fn ready_index(&self, definition: &str, kind: IndexKind) -> Result<u32, String> {
        let entry = self
            .entries
            .iter()
            .find(|entry| entry.definition.name == definition && entry.definition.kind == kind)
            .ok_or_else(|| "no maintained index".to_owned())?;
        if !entry.ready {
            return Err(format!("index {} is still being completed", entry.id));
        }
        Ok(entry.id)
    }

    /// The token list every entry written now stores.
    pub(crate) fn touched_by_on_write(&self) -> &[u8] {
        &self.maintaining
    }

    /// Applies `change`, of the entry whose entry-status key is `key`, to every
    /// maintained index. The stored list and walk cursors are only consulted while
    /// some index is Backfilling.
    pub(crate) fn apply(
        &self,
        storage: &mut PartitionStoreTransaction<'_>,
        context: &EntryContext<'_>,
        change: &EntryChange<'_>,
        key: &[u8],
    ) {
        let touched_by = change
            .stored_before()
            .map_or(MigrationJobIds::EMPTY, |before| before.touched_by());
        for entry in &self.entries {
            let before = change.before().filter(|_| {
                entry.ready
                    || touched_by.contains(entry.id)
                    || entry.covered.as_deref().is_some_and(|cursor| key <= cursor)
            });
            (entry.definition.apply)(storage, entry.id, context, before, change.after());
        }
    }
}
