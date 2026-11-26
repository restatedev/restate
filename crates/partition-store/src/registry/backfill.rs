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
//! the entry statuses (see [`crate::walk`]), then makes them Complete.
//!
//! **Only after the vqueues migration.** The entry statuses hold every
//! invocation only once the partition has fully migrated to vqueues, so the
//! families over them register, and their walk starts, only then (see
//! [`super::Family::requires_vqueues`]): at open, or at the walk driver's next
//! step after the processor applied the migration and committed its batch. A
//! walk never starts over a partition still migrating, and the entries written
//! before it starts, the migrated ones included, lie within it.
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
//! settles each member, unlinks it and removes the walk. A member that joined at
//! the start of the source and found no gap becomes Complete. That is sound because
//! the walk was a full pass for it with no gap: each entry it passed either listed
//! the member's token already or was filled before the cursor moved past it. A
//! slice that fails to fill an entry commits nothing, and entries written behind
//! the cursor or after the end are covered by their write. Any other member stays
//! Backfilling and waits for another pass (see "Passive members" below).
//!
//! For example, statistic 70 counts entries `A..E`. A slice fills `A` and `B`:
//! it counts them and rewrites both with token 70. A command then moves `B` to
//! another stage: `B` lists 70, so the write moves its count. The next slice finds
//! `C` written since (it lists 70) and skips it, fills `D` and `E`, and makes 70
//! Complete: every entry was counted once.
//!
//! **Passive members.** A member whose definition can't fill
//! ([`IndexDefinition::can_fill`]) leaves an entry without its token alone, and
//! the walk counts the entry as uncovered for it, in the slice's commit. At the
//! walk's end each member settles on its own: one that was a member from the
//! pass's start and found no gap is made Complete; any other stays Backfilling,
//! unlinked, and its job waits for another pass (see [`crate::job`]).
//!
//! **Records.** Each member has a build job with the member's ID (see
//! [`crate::job`]). Registering an index creates its job ([`register_job`]);
//! starting a walk links the jobs, keeping what earlier passes recorded; each
//! slice adds the rows it took to every member's job and the rows it filled for
//! that member, in the slice's commit; completion finishes the jobs, whose
//! records then expire after the configured retention.
//!
//! Opening a partition starts the walk ([`start`]), and the walk engine runs it
//! a slice at a time (see [`crate::walk::engine`]).

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryStateRef, EntryStatusHeader, OwnedEntryStatusHeader,
};
use restate_types::identifiers::BaseEntryId;

use crate::job::{MigrationJob, MigrationJobId, MigrationJobRecords, MigrationJobStatus, Stamp};
use crate::keys::DecodeTableKey;
use crate::stats::aggregated::backfill::with_entry_target;
use crate::vqueue_table::{EntryStatusKey, decode_entry_status};
#[cfg(test)]
use crate::walk::engine::SourceEntry;
use crate::walk::engine::{self, Settlement, Staged, WalkJob, walk_error};
use crate::walk::{Source, WalkKind, Walks};
use crate::{PartitionDb, PartitionStoreTransaction};

use super::{IndexDefinition, PublishedRegistry, Registry, RegistryError};

/// A Backfilling index: its definition and index ID.
type Member = (&'static IndexDefinition, u32);

/// The members of one walk.
pub(crate) struct IndexBackfill {
    members: Vec<Member>,
}

impl IndexBackfill {
    pub(crate) fn new(active: &PublishedRegistry, walk: u64) -> Result<Self, StorageError> {
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
    /// An entry status: the entry's ID and its decoded header.
    type Entry = (BaseEntryId, OwnedEntryStatusHeader);

    fn decode(raw: &[u8], value: &[u8]) -> Result<Self::Entry, StorageError> {
        let entry = EntryStatusKey::deserialize_from(&mut &raw[..])?.id;
        Ok((entry, decode_entry_status(&entry, value)?))
    }

    fn job_ids(&self) -> impl Iterator<Item = u32> {
        self.members.iter().map(|(_, id)| *id)
    }

    fn stage(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        key: &[u8],
        (entry, header): &Self::Entry,
    ) -> Result<Staged, StorageError> {
        let touched_by = header.touched_by();
        let mut staged = Staged::default();
        for (position, (definition, id)) in self.members.iter().enumerate() {
            if !touched_by.contains(*id) {
                if definition.can_fill {
                    staged.changed |= 1 << position;
                } else {
                    staged.uncovered |= 1 << position;
                }
            }
        }
        let filled_by = staged.changed;
        if filled_by == 0 {
            return Ok(staged);
        }
        let key: [u8; EntryStatusKey::serialized_length_fixed()] = key
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
        Ok(staged)
    }

    fn finish(
        &self,
        registry: &mut Registry,
        settled: &[(u32, Settlement)],
        at: u64,
    ) -> Result<(), StorageError> {
        for (id, settlement) in settled {
            match settlement {
                Settlement::Certified => set_complete(registry, *id, at)?,
                Settlement::Unfinished => registry
                    .unlink(*id)
                    .map_err(|err| StorageError::Conversion(err.into()))?,
            }
        }
        Ok(())
    }
}

/// Makes the Backfilling index `id` Complete at `at`.
fn set_complete(registry: &mut Registry, id: u32, at: u64) -> Result<(), StorageError> {
    registry
        .set_complete(id)
        .map_err(|err| StorageError::Conversion(err.into()))?;
    if let Some(timeline) = registry.timeline_mut(id) {
        timeline.completed_at = Some(at);
    }
    Ok(())
}

/// Creates the `Pending` build job of index `id`, registered at `stamp`.
pub(crate) fn register_job(
    registry: &Registry,
    jobs: &mut MigrationJobRecords,
    id: u32,
    stamp: &Stamp,
) -> Result<(), StorageError> {
    let name = registry
        .get(id)
        .map(|index| index.definition.clone())
        .ok_or(StorageError::DataIntegrityError)?;
    jobs.unfinished.insert(MigrationJob::index_backfill(
        id,
        name,
        None,
        Some(stamp.at),
        Some(stamp.by.clone()),
    ));
    Ok(())
}

/// Starts a pass for the Backfilling `ids`: one walk, ending at the last entry
/// now, linked to them and to their build jobs, which must exist (see
/// [`register_job`]). A job keeps what earlier passes recorded; one that waited
/// runs again. A partition without entries has nothing to walk: the indexes
/// become Complete at once and their jobs finish, completed. For a new
/// registration that takes no pass; for a job that waited, it is a full pass
/// that found nothing. The change is staged on `registry` (which issues the
/// walk's ID), `walks` and `jobs`; the caller commits them.
pub(crate) fn start(
    db: &PartitionDb,
    registry: &mut Registry,
    walks: &mut Walks,
    jobs: &mut MigrationJobRecords,
    ids: &[u32],
    at: u64,
) -> Result<Option<u64>, StorageError> {
    let registry_error = |err: RegistryError| StorageError::Conversion(err.into());
    let walk = match engine::last_key(db, Source::EntryStatus)? {
        None => None,
        Some(end) => Some(
            walks
                .start(
                    registry,
                    WalkKind::index_backfill(),
                    Source::EntryStatus,
                    end,
                )
                .map_err(walk_error)?,
        ),
    };
    for id in ids {
        let job_id = MigrationJobId::for_entry(*id);
        let job = jobs
            .unfinished
            .get_mut(job_id)
            .ok_or(StorageError::DataIntegrityError)?;
        match walk {
            Some(walk) => {
                registry.link(*id, walk).map_err(registry_error)?;
                job.walk = Some(walk);
                job.next_eligible_at = None;
                if job.status == MigrationJobStatus::Waiting {
                    job.status = MigrationJobStatus::Running;
                }
            }
            None => {
                set_complete(registry, *id, at)?;
                if job.status == MigrationJobStatus::Pending {
                    job.started_at = Some(at);
                } else {
                    job.pass_started_at = Some(at);
                    job.record_full_pass(at, 0);
                }
                jobs.finish(job_id, MigrationJobStatus::Completed, Some(at), at);
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
