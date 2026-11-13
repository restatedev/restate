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
//! **Records.** Each member has a build job with the member's ID (see
//! [`crate::job`]). Starting the walk creates the jobs; each slice adds the rows
//! it took to every member's job and the rows it filled for that member, in the
//! slice's commit; completion moves the jobs to the history.
//!
//! Opening a partition starts the walk ([`start`]), and the walk engine runs it
//! a slice at a time (see [`crate::walk::engine`]).

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef, EntryStatusHeader};

use crate::job::{MigrationJob, MigrationJobRecords, MigrationJobStatus, Stamp};
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
        let members = active.members(walk)?;
        if members.len() > 64 {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "walk {walk} has more than 64 indexes"
            )));
        }
        Ok(Self { members })
    }

    /// The members whose position is set in `filled_by`.
    fn filled(&self, filled_by: u64) -> impl Iterator<Item = &Member> {
        self.members
            .iter()
            .enumerate()
            .filter(move |(position, _)| (filled_by >> position) & 1 == 1)
            .map(|(_, member)| member)
    }
}

impl WalkJob for IndexBackfill {
    fn job_ids(&self) -> impl Iterator<Item = u32> {
        self.members.iter().map(|(_, id)| *id)
    }

    fn stage(
        &self,
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        (key, entry, header): &SourceEntry,
    ) -> Result<u64, StorageError> {
        let touched_by = header.touched_by();
        let filled_by = self
            .members
            .iter()
            .enumerate()
            .filter(|(_, (_, id))| !touched_by.contains(*id))
            .fold(0_u64, |set, (position, _)| set | (1 << position));
        if filled_by == 0 {
            return Ok(0);
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
            for (definition, id) in self.filled(filled_by) {
                (definition.apply)(tx, *id, &context, None, Some(&state));
            }
            Ok(())
        })
        .map_err(|err| StorageError::Generic(err.context(format!("entry {entry}"))))?;
        // Members are sorted by ID, so the tokens they add are too.
        let touched_by = touched_by.with(self.filled(filled_by).map(|(_, id)| *id));
        tx.put_entry_status(key, header.vqueue_id(), &state, &touched_by);
        Ok(filled_by)
    }

    fn finish(&self, registry: &mut IndexRegistry, at: u64) -> Result<(), StorageError> {
        for (_, id) in &self.members {
            set_ready(registry, *id, at)?;
        }
        Ok(())
    }
}

/// Makes the Backfilling index `id` Ready at `at`.
fn set_ready(registry: &mut IndexRegistry, id: u32, at: u64) -> Result<(), StorageError> {
    registry
        .set_ready(id)
        .map_err(|err| StorageError::Conversion(err.into()))?;
    if let Some(timeline) = registry.timeline_mut(id) {
        timeline.ready_at = Some(at);
    }
    Ok(())
}

/// Starts one walk for the Backfilling `ids`, ending at the last entry now, and
/// links them to it, with a `Pending` build job each. A partition without
/// entries has nothing to walk: the indexes become Ready at once, no walk
/// starts, and their jobs go straight to the history as completed without a
/// pass. The change is staged on `registry`, `walks` and `jobs`; the caller
/// commits them.
pub(crate) fn start(
    db: &PartitionDb,
    registry: &mut IndexRegistry,
    walks: &mut Walks,
    jobs: &mut MigrationJobRecords,
    ids: &[u32],
    stamp: &Stamp,
    history_limit: usize,
) -> Result<Option<u64>, StorageError> {
    let registry_error = |err: RegistryError| StorageError::Conversion(err.into());
    let walk = match engine::last_key(db)? {
        None => None,
        Some(end) => Some(
            walks
                .start(WalkKind::index_backfill(), Source::EntryStatus, end)
                .map_err(walk_error)?,
        ),
    };
    for id in ids {
        let name = registry
            .get(*id)
            .map(|index| index.definition.clone())
            .ok_or(StorageError::DataIntegrityError)?;
        let mut job =
            MigrationJob::index_backfill(*id, name, walk, Some(stamp.at), Some(stamp.by.clone()));
        match walk {
            Some(walk) => {
                registry.link(*id, walk).map_err(registry_error)?;
                jobs.unfinished.insert(job);
            }
            None => {
                set_ready(registry, *id, stamp.at)?;
                job.status = MigrationJobStatus::Completed;
                job.started_at = Some(stamp.at);
                job.finished_at = Some(stamp.at);
                std::sync::Arc::make_mut(&mut jobs.history).push_finished(job, history_limit);
            }
        }
    }
    Ok(walk)
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
