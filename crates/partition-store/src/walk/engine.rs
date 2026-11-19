// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The walk engine: runs a walk one slice at a time through a supplied
//! transaction, the processor's own, between the commands it applies.
//!
//! A **slice** reads source keys after the walk's cursor through a fresh
//! iterator, so no iterator or snapshot outlives it. Neither that iterator nor
//! the lookups of the job's work fill the block cache: a walk reads each key
//! once, and caching it would push out the data the partition's commands read.
//! The walk's job ([`WalkJob`]: an index backfill, or a one-time migration)
//! decodes them and stages its work on them one at a time; a key without its
//! source's length fails the slice before the job decodes it
//! ([`Source::check_key`]). The engine stages the walk advanced over the keys
//! whose work it took or, once no key remains up to the walk's end, the job's
//! finish and the walk's removal. A
//! [`SliceBudget`] bounds the keys read, their bytes and those of the job's own
//! lookups, and the bytes of the
//! staged batch with the records it commits with: the slice's walk and jobs,
//! and the registry a finishing slice changes, each under its own key (see
//! [`crate::registry::records`]); no other walk or job is rewritten. One commit
//! makes the work and the progress visible together. Only after it does the
//! engine publish the committed records (the [`PublishedRegistry`]) to the
//! transaction and the partition, so the next slice resumes after the new
//! cursor, the next write sees indexes the slice made Complete, and a failed commit
//! never publishes.
//! Recording a failure commits and publishes the same way. Starting a walk does
//! not go through the engine: the open stages it in its registration batch and
//! flushes that before publishing (see `registry::backfill::start`).
//!
//! For example, a walk over `A..E` with cursor `B` and a limit of two keys reads
//! `C` and `D`, commits their work with cursor `D`, then publishes cursor `D`.
//! The next slice reads `E`, finds nothing after it, and commits its work with
//! the job's finish. Had the work on `D` taken the staged batch past its byte
//! limit, or its lookups the reads past theirs, the first slice would have
//! dropped it and committed cursor `C`: the next slice starts at `D`, and a slice
//! stopped by a limit never finishes the walk, even when it read up to its end.
//!
//! ```text
//! source    A   B   C   D   E            end = E, cursor = B, budget: two keys
//! slice 1   read C, D  ─▶ stage work(C), work(D) ─▶ commit {work, walk: cursor = D, its jobs}
//!                                                    ─▶ publish
//! slice 2   read E, none after ─▶ stage work(E), finish ─▶ commit {work, registry: Complete,
//!                                                          walk deleted, its jobs finished}
//! ```
//!
//! **Members that can't fill.** A slice leaves an entry alone for a member whose
//! definition can't fill it, and counts the entry as uncovered for that member on
//! the walk itself ([`super::WalkMember`]). The count commits with the slice and
//! rolls back with a candidate dropped at the byte limit, like the entry's work.
//! When the walk is exhausted, each member settles on its own: one that was a
//! member from the start and found no gap is complete; any other waits for
//! another pass (see [`crate::job`]).
//!
//! A slice always takes the work on its first key, so a key whose work alone
//! exceeds a byte limit still makes progress, in a slice of its own: the slice
//! stops before staging anything for the next key. Likewise, a slice that finds
//! no key left always commits the job's finish and the walk's removal, whatever
//! their size.
//!
//! A slice needs a transaction without pending writes. A failure before the
//! commit clears what the slice staged, so the committed records still describe
//! the store and the walk can retry ([`SliceError::Staging`]); it is recorded on
//! the jobs whose records link the walk. A failed commit has
//! an unknown outcome, so the caller stops writing and reopens the partition,
//! which reloads the records ([`SliceError::Commit`]). The background driver
//! (see [`super::WalkDriver`]) chooses which walk runs, retries, and paces; tests
//! drive walks with `run_all`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use bytes::Bytes;
use rocksdb::DBRawIteratorWithThreadMode;
use tokio::time::Instant;

use restate_clock::WallClock;
use restate_storage_api::{StorageError, Transaction};

use crate::job::{MigrationJobId, MigrationJobRecords, MigrationJobStatus, MigrationJobs};
use crate::registry::records::{self, View};
use crate::registry::{IndexBackfill, PublishedRegistry, Registry};
use crate::{PartitionDb, PartitionStoreTransaction};

use super::{Source, Walk, WalkError, Walks};

/// An entry status a slice read: its key, ID and decoded header. Tests only.
#[cfg(test)]
pub(crate) type SourceEntry = (
    Vec<u8>,
    restate_types::identifiers::BaseEntryId,
    restate_storage_api::vqueue_table::OwnedEntryStatusHeader,
);

/// The bounds of one slice; see the module documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SliceBudget {
    /// Source keys read.
    pub(crate) keys: usize,
    /// Bytes of the source keys and values read, and of the lookups the job
    /// reports ([`Staged::read_bytes`]). A lookup's size is known only once it
    /// is read: a key whose lookups go over the limit rolls back, and is the
    /// next slice's first key.
    pub(crate) read_bytes: usize,
    /// Bytes of the staged batch, including the registry records it commits
    /// with.
    pub(crate) write_bytes: usize,
}

impl SliceBudget {
    /// The source bytes a slice reads at most: a guard against a slice that reads
    /// many large keys but writes little.
    pub(crate) const READ_BYTES: usize = 4 << 20;

    /// At most `keys` source keys and `write_bytes` of staged batch, with the
    /// read limit.
    pub(crate) fn new(keys: usize, write_bytes: usize) -> Self {
        Self {
            keys,
            read_bytes: Self::READ_BYTES,
            write_bytes,
        }
    }
}

/// A key count alone, with the default write budget of command batches
/// (`worker.max-command-batch-bytes`). Tests only.
#[cfg(test)]
impl From<usize> for SliceBudget {
    fn from(keys: usize) -> Self {
        let write_bytes = restate_types::config::WorkerOptions::default()
            .max_command_batch_bytes
            .as_usize();
        Self::new(keys, write_bytes)
    }
}

/// What a committed slice did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Slice {
    /// Committed work and the advanced cursor; the walk continues.
    Progressed,
    /// Committed the last work and the job's finish; the walk is removed.
    Exhausted,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SliceError {
    /// Nothing was committed, and nothing the call staged remains; the walk can
    /// retry.
    #[error(transparent)]
    Staging(StorageError),
    /// The commit failed with an unknown outcome: stop writing until the partition
    /// reopens and reloads its records.
    #[error(transparent)]
    Commit(StorageError),
}

impl From<SliceError> for StorageError {
    fn from(err: SliceError) -> Self {
        match err {
            SliceError::Staging(err) | SliceError::Commit(err) => err,
        }
    }
}

/// What a job did with one source entry, as bit sets over its jobs by position
/// in [`WalkJob::job_ids`], and what it read besides the entry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Staged {
    /// The jobs that changed the entry: an index backfill's members that gave
    /// it their token, or the migration.
    pub(crate) changed: u64,
    /// The jobs that lacked the entry but can't fill it (passive ones).
    pub(crate) uncovered: u64,
    /// Bytes of keys and values the job's lookups read for the entry, counted
    /// against the slice's read limit.
    pub(crate) read_bytes: usize,
}

/// How a job ends the pass of an exhausted walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settlement {
    /// A full pass with no gap: the job is complete.
    Certified,
    /// Gaps, or a late join: the job waits for another pass.
    Unfinished,
}

/// A job's side of its walks. The engine reads the source, keeps the walk's
/// progress, its members' evidence and its jobs' records, and commits; a job
/// decodes the source's rows and stages its own work on them.
pub(crate) trait WalkJob {
    /// A decoded source row.
    type Entry;

    /// Decodes the source row stored under `raw` with `value`. `raw` has the
    /// length of its source's keys ([`Source::check_key`]).
    fn decode(raw: &[u8], value: &[u8]) -> Result<Self::Entry, StorageError>;

    /// The migration jobs the walk runs, at most 64. A slice reports what it did
    /// for each by position in this list.
    fn job_ids(&self) -> impl Iterator<Item = u32>;

    /// Stages the work on `entry`, the next source row of the walk, stored under
    /// `raw_key`.
    fn stage(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        raw_key: &[u8],
        entry: &Self::Entry,
    ) -> Result<Staged, StorageError>;

    /// Applies to `registry` how each job ended the walk's pass at `at`; it
    /// commits with the walk's last work and removal.
    fn finish(
        &self,
        registry: &mut Registry,
        settled: &[(u32, Settlement)],
        at: u64,
    ) -> Result<(), StorageError>;
}

/// What a slice runs on: its walk as published, its bounds, and when its work
/// started.
pub(crate) struct SliceInput<'a> {
    pub(crate) db: &'a PartitionDb,
    pub(crate) active: &'a PublishedRegistry,
    pub(crate) walk: &'a Walk,
    pub(crate) budget: SliceBudget,
    pub(crate) started: Instant,
}

/// What a slice staged, to commit and then publish.
pub(crate) struct StagedSlice {
    /// The registry, if the slice finished the walk and so changed it.
    registry: Option<Registry>,
    walks: Walks,
    jobs: MigrationJobs,
    slice: Slice,
}

/// Stages a slice of a migration's walk as the job of the migration's entry ID,
/// counting the rows it examines: [`stage_slice`] monomorphized for one
/// migration (see `MigrationDefinition::run`), so dispatch needs no trait
/// object.
pub(crate) type SliceRunner = fn(
    &mut PartitionStoreTransaction<'_>,
    &SliceInput<'_>,
    u32,
    &mut usize,
) -> Result<StagedSlice, StorageError>;

/// Runs one slice of walk `id` within `budget`; see the module documentation.
#[cfg(test)]
pub(crate) async fn run_slice(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    id: u64,
    budget: impl Into<SliceBudget>,
) -> Result<Slice, SliceError> {
    run_counted_slice(tx, db, id, budget.into())
        .await
        .map(|(slice, _)| slice)
        .map_err(|(err, _)| err)
}

/// Runs one slice of walk `id` within `budget` (see the module documentation),
/// also returning the source entries the slice examined, whether it succeeded or
/// not: every entry it decoded or tried to, including those its job skips and
/// one whose work it dropped at the write limit. A key the slice stops at for
/// its size, before decoding it, is not examined.
///
/// The slice's commit also updates the records of the walk's jobs: the rows it
/// took (not counting one rolled back at the write limit), the rows it changed
/// for each job, and its work time up to the commit.
pub(crate) async fn run_counted_slice(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    id: u64,
    budget: SliceBudget,
) -> Result<(Slice, usize), (SliceError, usize)> {
    let started = Instant::now();
    let active = tx.published_registry();
    let mut examined = 0;
    let staged = stage(tx, |tx| {
        let walk = active
            .walks()
            .get(id)
            .ok_or(WalkError::UnknownId(id))
            .map_err(walk_error)?;
        let input = SliceInput {
            db,
            active: &active,
            walk,
            budget,
            started,
        };
        // Index backfills, then the migrations this binary knows.
        if walk.is_index_backfill() {
            let job = IndexBackfill::new(&active, id)?;
            return stage_slice(tx, &input, job, &mut examined);
        }
        let (migration, entry) = active
            .migration_of(walk)
            .ok_or_else(|| walk_error(WalkError::UnsupportedKind(walk.kind.clone())))?;
        (migration.run)(tx, &input, entry, &mut examined)
    });
    let staged = staged.map_err(|err| (err, examined))?;
    let fault = db.faults().take(crate::faults::FaultPoint::SliceStaged);
    if let Err(err) = crate::faults::inject(fault).await {
        tx.clear();
        let err = SliceError::Staging(StorageError::Generic(err.into()));
        return Err((err, examined));
    }
    let StagedSlice {
        registry,
        walks,
        jobs,
        slice,
    } = staged;
    commit(tx, db, &active, registry, walks, jobs)
        .await
        .map_err(|err| (err, examined))?;
    Ok((slice, examined))
}

/// Stages one slice of `input`'s walk for `job` (see the module documentation),
/// adding to `examined` every source row it decodes or tries to.
pub(crate) fn stage_slice<J: WalkJob>(
    tx: &mut PartitionStoreTransaction<'_>,
    input: &SliceInput<'_>,
    mut job: J,
    examined: &mut usize,
) -> Result<StagedSlice, StorageError> {
    let SliceInput {
        db,
        active,
        walk,
        budget,
        started,
    } = *input;
    let id = walk.id;
    let job_ids: Vec<_> = job.job_ids().collect();
    let mut changed = vec![0_u64; job_ids.len()];
    let mut uncovered = vec![0_u64; job_ids.len()];
    // The first slice of a walk starts a pass.
    let pass_start = walk.cursor.is_none();
    let records = records_estimate(active, walk, &job_ids);
    let source = walk.source().ok_or(StorageError::DataIntegrityError)?;
    let mut rows = SourceCursor::open(db, source, walk.cursor.as_deref(), Some(&walk.end))?;
    let (mut taken, mut read) = (0, 0);
    // One buffer for the last key taken, the cursor's next position.
    let mut last = Vec::new();
    let exhausted = loop {
        let Some(size) = rows.peek()? else {
            break true;
        };
        if taken > 0
            && (taken >= budget.keys
                || read + size > budget.read_bytes
                || tx.estimated_size_in_bytes() + records > budget.write_bytes)
        {
            break false;
        }
        read += size;
        // Decoding examines the row, also when it fails.
        *examined += 1;
        let (raw_key, value) = rows.current()?;
        let entry = J::decode(raw_key, value)?;
        if taken > 0 {
            tx.set_save_point();
        }
        let staged = job.stage(tx, db, raw_key, &entry)?;
        read = read.saturating_add(staged.read_bytes);
        if taken > 0
            && (read > budget.read_bytes
                || tx.estimated_size_in_bytes() + records > budget.write_bytes)
        {
            // The row's work, tokens and evidence roll back together.
            tx.rollback_to_save_point()?;
            break false;
        }
        taken += 1;
        for position in 0..job_ids.len() {
            changed[position] += (staged.changed >> position) & 1;
            uncovered[position] += (staged.uncovered >> position) & 1;
        }
        last.clear();
        last.extend_from_slice(raw_key);
        rows.next();
    };
    let now = WallClock::now_ms().as_u64();
    let mut walks = active.walks().clone();
    for (job_id, uncovered) in job_ids.iter().zip(uncovered) {
        walks
            .add_uncovered(id, *job_id, uncovered)
            .map_err(walk_error)?;
    }
    // Only the finishing slice changes the registry.
    let (registry, slice, ends) = if exhausted {
        let evidence = walks
            .get(id)
            .ok_or(WalkError::UnknownId(id))
            .map_err(walk_error)?;
        // One outcome per job, for both the registry and the job record.
        let ends: Vec<_> = job_ids
            .iter()
            .map(|job_id| (*job_id, PassEnd::of(evidence, *job_id)))
            .collect();
        let settled: Vec<_> = ends
            .iter()
            .map(|(job_id, end)| (*job_id, end.settlement()))
            .collect();
        let mut finished = active.registry().clone();
        job.finish(&mut finished, &settled, now)?;
        walks.remove(id).map_err(walk_error)?;
        (Some(finished), Slice::Exhausted, ends)
    } else {
        debug_assert!(taken > 0, "a slice that stops early took its first row");
        walks.advance(id, Bytes::from(last)).map_err(walk_error)?;
        (None, Slice::Progressed, Vec::new())
    };
    // The slice's work time includes the job's finish.
    let work_us = micros(started.elapsed());
    let mut jobs = active.job_records();
    for (job_id, changed) in job_ids.iter().zip(changed) {
        if let Some(record) = jobs.unfinished.get_mut(MigrationJobId::for_entry(*job_id)) {
            let pass_start = pass_start && !walk.joined_late(*job_id);
            record.record_slice(now, taken as u64, changed, work_us, pass_start);
        }
    }
    for (job_id, end) in ends {
        settle(&mut jobs, job_id, end, now);
    }
    records::stage_changes(
        tx,
        db.partition().id(),
        committed(active),
        View {
            registry: Some(registry.as_ref().unwrap_or_else(|| active.registry())),
            walks: &walks,
            jobs: &jobs.unfinished,
        },
        &jobs.finished,
    )?;
    Ok(StagedSlice {
        registry,
        walks,
        jobs: jobs.unfinished,
        slice,
    })
}

/// The committed records `active` publishes, to stage changes against.
fn committed(active: &PublishedRegistry) -> View<'_> {
    View {
        registry: Some(active.registry()),
        walks: active.walks(),
        jobs: active.jobs(),
    }
}

/// The bytes of the records a slice of `walk` running `job_ids` commits with,
/// generously: the registry, which a finishing slice writes (whether a slice
/// finishes is only known at its end, so every slice counts it), the walk as
/// large as the slice can make it (every job with the largest evidence), and
/// the walk's jobs, grown by what one slice can add. No other walk or job, and
/// no finished job, is written.
pub(crate) fn records_estimate(active: &PublishedRegistry, walk: &Walk, job_ids: &[u32]) -> usize {
    let jobs: usize = job_ids
        .iter()
        .filter_map(|id| active.jobs().get(MigrationJobId::for_entry(*id)))
        .map(|job| bilrost::Message::encoded_len(job) + JOB_GROWTH)
        .sum();
    (2 + job_ids.len()) * RECORD_OVERHEAD
        + bilrost::Message::encoded_len(active.registry())
        + bilrost::Message::encoded_len(walk)
        + MEMBER_GROWTH * job_ids.len()
        + jobs
}

/// How a job ended the pass of an exhausted walk, from the walk's evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassEnd {
    /// It joined after the pass started, so it saw only a suffix.
    Suffix,
    /// A full pass that found this many rows the job couldn't cover; none
    /// completes the job.
    Full(u64),
}

impl PassEnd {
    fn of(evidence: &Walk, job_id: u32) -> Self {
        if evidence.joined_late(job_id) {
            Self::Suffix
        } else {
            Self::Full(evidence.uncovered(job_id))
        }
    }

    fn settlement(self) -> Settlement {
        match self {
            Self::Full(0) => Settlement::Certified,
            Self::Suffix | Self::Full(_) => Settlement::Unfinished,
        }
    }
}

/// Records how job `job_id` ended its pass at `now`. A suffix is not a full
/// pass: the job waits, eligible again at once. A full pass is recorded, gaps or
/// not; a gap-free job completes and one with gaps waits as its schedule says.
fn settle(jobs: &mut MigrationJobRecords, job_id: u32, end: PassEnd, now: u64) {
    let id = MigrationJobId::for_entry(job_id);
    let Some(record) = jobs.unfinished.get_mut(id) else {
        return;
    };
    match end {
        PassEnd::Suffix => {
            let interval = record.recheck_interval_ms;
            record.wait(interval, None);
        }
        PassEnd::Full(gaps) => {
            record.record_full_pass(now, gaps);
            if gaps > 0 {
                record.wait_after_gaps(now);
            } else {
                jobs.finish(id, MigrationJobStatus::Completed, Some(now), now);
            }
        }
    }
}

/// Whole microseconds of `duration`, saturating.
fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Bytes a staged record adds besides its encoded value: its key and the batch's
/// own framing, with room to spare.
const RECORD_OVERHEAD: usize = 256;

/// Bytes an unfinished job's record can grow by in one slice. A slice can set or
/// grow about seventeen fields, the times, the pass fields, the counters and a
/// finished job's expiry, by at most 12 bytes each in their largest encoding
/// (about 200 bytes in all); this leaves room to spare.
const JOB_GROWTH: usize = 256;

/// Bytes a walk can grow by per job in one slice: a job's evidence entry, its
/// index and the largest count of uncovered rows, takes at most 19 bytes.
const MEMBER_GROWTH: usize = 32;

/// Runs every walk of `store` to its end, in ID order, in slices within
/// `budget`, as the background driver does over time. A failed slice is recorded
/// on its walk and returned. Tests only.
#[cfg(test)]
pub(crate) async fn run_all(
    store: &mut crate::PartitionStore,
    budget: impl Into<SliceBudget>,
) -> Result<(), StorageError> {
    let budget = budget.into();
    let db = store.partition_db().clone();
    let mut ids: Vec<_> = store
        .published_registry()
        .walks()
        .entries()
        .iter()
        .map(|walk| walk.id)
        .collect();
    ids.sort_unstable();
    let mut tx = store.transaction();
    for id in ids {
        loop {
            match run_slice(&mut tx, &db, id, budget).await {
                Ok(Slice::Progressed) => {}
                Ok(Slice::Exhausted) => break,
                Err(SliceError::Staging(err)) => {
                    let error = describe(&err);
                    let failed = record_failure(&mut tx, &db, id, &error, Duration::ZERO);
                    if let Err(record) = failed.await {
                        tracing::warn!(walk = id, "Failed to record a failed walk slice: {record}");
                    }
                    return Err(err);
                }
                Err(SliceError::Commit(err)) => return Err(err),
            }
        }
    }
    Ok(())
}

/// Records a failed attempt at walk `id`, which worked for `work` before it
/// failed, on the records of the jobs that link the walk, once the failure
/// cleared what it staged. A failure to record it is returned, not recorded.
pub(crate) async fn record_failure(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    id: u64,
    error: &str,
    work: Duration,
) -> Result<(), SliceError> {
    let active = tx.published_registry();
    if active.walks().get(id).is_none() {
        return Err(SliceError::Staging(walk_error(WalkError::UnknownId(id))));
    }
    let jobs: Vec<_> = active
        .jobs()
        .entries()
        .iter()
        .filter(|job| job.walk == Some(id))
        .map(|job| job.id().as_u32())
        .collect();
    record_job_failures(tx, db, &jobs, error, work).await
}

/// Records a failed attempt, at a slice of their walk or at starting their next
/// pass, on the jobs `ids`, with `error` and the attempt's `work`.
/// Commits and publishes like a slice.
pub(crate) async fn record_job_failures(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    ids: &[u32],
    error: &str,
    work: Duration,
) -> Result<(), SliceError> {
    let active = tx.published_registry();
    let now = WallClock::now_ms().as_u64();
    let jobs = stage(tx, |tx| {
        let mut jobs = active.jobs().clone();
        for id in ids {
            if let Some(record) = jobs.get_mut(MigrationJobId::for_entry(*id)) {
                record.record_failure(now, error, micros(work));
            }
        }
        let after = View {
            jobs: &jobs,
            ..committed(&active)
        };
        records::stage_changes(tx, db.partition().id(), committed(&active), after, &[])?;
        Ok(jobs)
    })?;
    commit(tx, db, &active, None, active.walks().clone(), jobs).await
}

/// Starts a pass for the waiting Backfilling indexes `ids` in one commit, as
/// opening does for eligible ones (see `registry::backfill::start`): one walk over
/// the entries that exist now, or completion at once without any. Commits and
/// publishes like a slice, and returns the new walk, if any.
pub(crate) async fn start_passes(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    ids: &[u32],
) -> Result<Option<u64>, SliceError> {
    let fault = db.faults().take(crate::faults::FaultPoint::PassStart);
    crate::faults::inject(fault)
        .await
        .map_err(|err| SliceError::Staging(StorageError::Generic(err.into())))?;
    let active = tx.published_registry();
    let now = WallClock::now_ms().as_u64();
    let (registry, walks, jobs, walk) = stage(tx, |tx| {
        let mut registry = active.registry().clone();
        let mut walks = active.walks().clone();
        let mut jobs = active.job_records();
        let walk =
            crate::registry::start_backfill(db, &mut registry, &mut walks, &mut jobs, ids, now)?;
        let after = View {
            registry: Some(&registry),
            walks: &walks,
            jobs: &jobs.unfinished,
        };
        records::stage_changes(
            tx,
            db.partition().id(),
            committed(&active),
            after,
            &jobs.finished,
        )?;
        Ok((registry, walks, jobs.unfinished, walk))
    })?;
    commit(tx, db, &active, Some(registry), walks, jobs).await?;
    Ok(walk)
}

/// Runs `f` to stage writes into `tx`, which must have none pending, and clears
/// them if it fails.
pub(crate) fn stage<'a, T>(
    tx: &mut PartitionStoreTransaction<'a>,
    f: impl FnOnce(&mut PartitionStoreTransaction<'a>) -> Result<T, StorageError>,
) -> Result<T, SliceError> {
    if !tx.is_empty() {
        return Err(SliceError::Staging(StorageError::Generic(anyhow!(
            "a walk needs a transaction without pending writes"
        ))));
    }
    f(tx).map_err(|err| {
        tx.clear();
        SliceError::Staging(err)
    })
}

/// Commits what `tx` staged on top of `active`, then publishes `walks`, the
/// unfinished `jobs` and, if the commit changed it, `registry` as committed.
/// Commit and publication are work of the partition's session: a close waits
/// for them, and a closed session commits nothing, so a slice of a closed
/// partition can never publish into a later session.
pub(crate) async fn commit(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    active: &PublishedRegistry,
    registry: Option<Registry>,
    walks: Walks,
    jobs: MigrationJobs,
) -> Result<(), SliceError> {
    let Some(_session) = db.gate().try_acquire() else {
        tx.clear();
        return Err(SliceError::Staging(StorageError::OperationalError));
    };
    tx.commit().await.map_err(SliceError::Commit)?;
    db.faults()
        .reach(crate::faults::FaultPoint::WalkPublication)
        .await;
    let next = Arc::new(active.successor(registry, walks, jobs));
    db.install_published_registry(Arc::clone(&next));
    tx.set_published_registry(next);
    Ok(())
}

/// The last key of `source` in the partition: the end of a walk starting now,
/// or `None` if the source is empty.
pub(crate) fn last_key(db: &PartitionDb, source: Source) -> Result<Option<Bytes>, StorageError> {
    let mut options = rocksdb::ReadOptions::default();
    options.fill_cache(false);
    let mut iterator = db.scan(source.scan(db.partition().key_range), options)?;
    iterator.seek_to_last();
    let last = iterator.key().map(Bytes::copy_from_slice);
    iterator
        .status()
        .map_err(|err| StorageError::Generic(err.into()))?;
    Ok(last)
}

/// A fresh iterator over the rows of a source in `(resume, end]`, in key order.
struct SourceCursor<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, rocksdb::DB>,
    source: Source,
    end: Option<&'a [u8]>,
}

impl<'a> SourceCursor<'a> {
    fn open(
        db: &'a PartitionDb,
        source: Source,
        resume: Option<&[u8]>,
        end: Option<&'a [u8]>,
    ) -> Result<Self, StorageError> {
        let mut options = rocksdb::ReadOptions::default();
        options.fill_cache(false);
        let mut iterator = db.scan(source.scan(db.partition().key_range), options)?;
        if let Some(resume) = resume {
            iterator.seek(resume);
            if iterator.key() == Some(resume) {
                iterator.next();
            }
        }
        Ok(Self {
            iterator,
            source,
            end,
        })
    }

    /// The bytes of the next row's key and value, or `None` once none is left.
    fn peek(&self) -> Result<Option<usize>, StorageError> {
        let end = self.end;
        match self
            .iterator
            .key()
            .filter(|key| end.is_none_or(|end| *key <= end))
        {
            Some(key) => {
                let value = self
                    .iterator
                    .value()
                    .ok_or(StorageError::DataIntegrityError)?;
                Ok(Some(key.len() + value.len()))
            }
            None => {
                self.iterator
                    .status()
                    .map_err(|err| StorageError::Generic(err.into()))?;
                Ok(None)
            }
        }
    }

    /// The key and value of the next row, after [`Self::peek`] found one: a
    /// `DataIntegrityError` if the key doesn't fit the source
    /// ([`Source::check_key`]).
    fn current(&self) -> Result<(&[u8], &[u8]), StorageError> {
        let (key, value) = self
            .iterator
            .key()
            .zip(self.iterator.value())
            .ok_or(StorageError::DataIntegrityError)?;
        self.source.check_key(key)?;
        Ok((key, value))
    }

    /// Moves past the current row.
    fn next(&mut self) {
        self.iterator.next();
    }
}

/// Entry statuses in `(resume, end]`, in key order, through a fresh iterator: at
/// most `keys` of them, and no more than `bytes` of keys and values unless the
/// first alone exceeds them. Also whether none remains after them: an exhausted
/// range, rather than a stop at a limit.
#[cfg(test)]
pub(crate) fn read_source(
    db: &PartitionDb,
    resume: Option<&[u8]>,
    end: Option<&[u8]>,
    keys: usize,
    bytes: usize,
) -> Result<(Vec<SourceEntry>, bool), StorageError> {
    let mut rows = SourceCursor::open(db, Source::EntryStatus, resume, end)?;
    let mut entries = Vec::new();
    let mut read = 0;
    let exhausted = loop {
        let Some(size) = rows.peek()? else {
            break true;
        };
        if entries.len() >= keys || (!entries.is_empty() && read + size > bytes) {
            break false;
        }
        read += size;
        let (raw, value) = rows.current()?;
        let (entry, header) = <IndexBackfill as WalkJob>::decode(raw, value)?;
        entries.push((raw.to_vec(), entry, header));
        rows.next();
    };
    Ok((entries, exhausted))
}

/// `err` with its causes, as a walk records it.
pub(crate) fn describe(err: &StorageError) -> String {
    match err {
        StorageError::Generic(err) | StorageError::Conversion(err) => format!("{err:#}"),
        err => err.to_string(),
    }
}

pub(crate) fn walk_error(err: WalkError) -> StorageError {
    StorageError::Conversion(err.into())
}
