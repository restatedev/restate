// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The index backfill job (`index_backfill/1`): makes Backfilling indexes
//! reflect the entries that existed before they were registered, with a walk of
//! the entry statuses (see [`crate::walk`]), then makes them Ready.
//!
//! **Filling.** An entry reflected by an index lists the index's ID, its token
//! (see [`super::active`]). Entries written since an index was registered already
//! list it, so a walk fills every other entry, in entry-status key order: it
//! applies the entry to each of its indexes, its members, that the entry does not
//! list yet, and rewrites the entry with their tokens added, keeping the tokens it
//! already lists. Members share the traversal: each entry's status is read and its
//! target resolved once. The contributions, the rewritten entries and the walk's
//! cursor commit together, a slice at a time, so a write after the commit finds
//! the tokens and replaces the filled contributions instead of adding them again.
//! The cursor only records how far the walk got.
//!
//! **Completion.** Once no entry remains up to the walk's end, the same commit
//! makes the members Ready, unlinks them and removes the walk. That is sound
//! because the walk is a full pass with no gap: its members are linked only when
//! it starts, at the beginning of the source, and every member can fill, so each
//! entry it passes either listed a member's token already or was filled before
//! the cursor moved past it. A slice that fails to fill an entry commits nothing.
//! Entries written behind the cursor or after the end are covered by their write.
//!
//! For example, statistic 70 counts entries `A..E`. A slice fills `A` and `B`:
//! it counts them and rewrites both with token 70. A command then moves `B` to
//! another stage: `B` lists 70, so the write moves its count. The next slice finds
//! `C` written since (it lists 70) and skips it, fills `D` and `E`, and makes 70
//! Ready: every entry was counted once.
//!
//! Opening a partition starts the walk ([`start`]), and the walk engine runs it
//! a slice at a time (see [`crate::walk::engine`]).

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef, EntryStatusHeader};

use crate::stats::aggregated::backfill::with_entry_target;
use crate::vqueue_table::EntryStatusKey;
use crate::walk::engine::{self, SourceEntry, WalkJob, walk_error};
use crate::walk::{Source, WalkKind, Walks};
use crate::{PartitionDb, PartitionStoreTransaction};

use super::{IndexDefinition, IndexRegistry, MaintainedIndexes, RegistryError};

/// A Backfilling index: its definition and index ID.
type Member = (&'static IndexDefinition, u32);

/// The members of one walk.
pub(crate) struct IndexBackfill {
    members: Vec<Member>,
}

impl IndexBackfill {
    pub(crate) fn new(active: &MaintainedIndexes, walk: u64) -> Result<Self, StorageError> {
        Ok(Self {
            members: active.members(walk)?,
        })
    }
}

impl WalkJob for IndexBackfill {
    fn stage(
        &self,
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        (key, entry, header): &SourceEntry,
    ) -> Result<(), StorageError> {
        let touched_by = header.touched_by();
        // Members are sorted by ID, so the tokens they add are too.
        let unfilled: Vec<_> = self
            .members
            .iter()
            .filter(|(_, id)| !touched_by.contains(*id))
            .collect();
        if unfilled.is_empty() {
            return Ok(());
        }
        let key: [u8; EntryStatusKey::serialized_length_fixed()] = key
            .as_slice()
            .try_into()
            .map_err(|_| StorageError::DataIntegrityError)?;
        let state = EntryStateRef::from_header(header);
        with_entry_target(db, entry, header.vqueue_id(), header.seq(), |target| {
            let context = EntryContext {
                qid: header.vqueue_id(),
                target,
            };
            for (definition, id) in &unfilled {
                (definition.apply)(tx, *id, &context, None, Some(&state));
            }
            Ok(())
        })
        .map_err(|err| StorageError::Generic(err.context(format!("entry {entry}"))))?;
        let touched_by = touched_by.with(unfilled.iter().map(|(_, id)| *id));
        tx.put_entry_status(key, header.vqueue_id(), &state, &touched_by);
        Ok(())
    }

    fn finish(&self, registry: &mut IndexRegistry) -> Result<(), StorageError> {
        for (_, id) in &self.members {
            registry
                .set_ready(*id)
                .map_err(|err| StorageError::Conversion(err.into()))?;
        }
        Ok(())
    }
}

/// Starts one walk for the Backfilling `ids`, ending at the last entry now, and
/// links them to it. A partition without entries has nothing to walk: the
/// indexes become Ready instead, and no walk starts. The change is staged on
/// `registry` and `walks`; the caller commits both.
pub(crate) fn start(
    db: &PartitionDb,
    registry: &mut IndexRegistry,
    walks: &mut Walks,
    ids: &[u32],
) -> Result<Option<u64>, StorageError> {
    let registry_error = |err: RegistryError| StorageError::Conversion(err.into());
    match engine::last_key(db)? {
        None => {
            for id in ids {
                registry.set_ready(*id).map_err(registry_error)?;
            }
            Ok(None)
        }
        Some(end) => {
            let walk = walks
                .start(WalkKind::index_backfill(), Source::EntryStatus, end)
                .map_err(walk_error)?;
            for id in ids {
                registry.link(*id, walk).map_err(registry_error)?;
            }
            Ok(Some(walk))
        }
    }
}

/// Up to `limit` entry statuses after the exclusive `resume` key, in key order.
#[cfg(test)]
pub(super) fn read_chunk(
    store: &crate::PartitionStore,
    resume: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<SourceEntry>, StorageError> {
    engine::read_source(store.partition_db(), resume, None, limit, usize::MAX)
        .map(|(chunk, _)| chunk)
}
