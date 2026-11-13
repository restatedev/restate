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
//! Entries written since an index was registered already list its ID, so a walk
//! adds the contribution of every other entry, in entry-status key order. The
//! indexes of one walk, its members, share its traversal: each entry's status is
//! read and its target resolved once, then applied to every member that does
//! not reflect it yet. Each slice commits its contributions with the walk's
//! cursor, and writes treat every entry up to that cursor as touched, so no
//! entry is contributed twice. Once no entry remains up to the walk's end, the
//! same commit makes its members Ready, unlinks them and removes the walk.
//! Entry headers are never rewritten: once Ready, an index reflects every entry
//! without listing it.
//!
//! Opening a partition starts the walk ([`start`]), and the walk engine runs it
//! a slice at a time (see [`crate::walk::engine`]).

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef, EntryStatusHeader};

use crate::stats::aggregated::backfill::with_entry_target;
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
        (_, entry, header): &SourceEntry,
    ) -> Result<(), StorageError> {
        let touched_by = header.touched_by();
        if self.members.iter().all(|(_, id)| touched_by.contains(*id)) {
            return Ok(());
        }
        with_entry_target(db, entry, header.vqueue_id(), header.seq(), |target| {
            let context = EntryContext {
                qid: header.vqueue_id(),
                target,
            };
            let state = EntryStateRef::from_header(header);
            for (definition, id) in &self.members {
                if !touched_by.contains(*id) {
                    (definition.apply)(tx, *id, &context, None, Some(&state));
                }
            }
            Ok(())
        })
        .map_err(|err| StorageError::Generic(err.context(format!("entry {entry}"))))
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
