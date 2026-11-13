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
//! iterator, so no iterator or snapshot outlives it. The walk's job stages its
//! work on them one at a time, and the engine stages the walk advanced over the
//! keys whose work it took or, once no key remains up to the walk's end, the
//! job's finish and the walk's removal. A [`SliceBudget`] bounds the keys read,
//! their bytes, and the bytes of the staged batch with the records it commits
//! with. One commit makes the work and the progress visible together. Only
//! after it does the engine publish the committed records (the
//! [`MaintainedIndexes`]) to the transaction and the partition, so the next slice
//! resumes after the new cursor, the next write sees indexes the slice made
//! Ready, and a failed commit never publishes. Recording a failure commits and publishes the
//! same way. Starting a walk does not go through the engine: the open stages it
//! in its registration batch and flushes that before publishing (see
//! `registry::backfill::start`).
//!
//! For example, a walk over `A..E` with cursor `B` and a limit of two keys reads
//! `C` and `D`, commits their work with cursor `D`, then publishes cursor `D`.
//! The next slice reads `E`, finds nothing after it, and commits its work with
//! the job's finish. Had the work on `D` taken the staged batch past its byte
//! limit, the first slice would have dropped it and committed cursor `C`: the
//! next slice starts at `D`, and a slice stopped by a limit never finishes the
//! walk, even when it read up to its end.
//!
//! ```text
//! source    A   B   C   D   E            end = E, cursor = B, budget: two keys
//! slice 1   read C, D  ─▶ stage work(C), work(D) ─▶ commit {work, cursor = D} ─▶ publish
//! slice 2   read E, none after ─▶ stage work(E), finish ─▶ commit {work, Ready, walk removed}
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
//! the store and the walk can retry ([`SliceError::Staging`]). A failed commit has
//! an unknown outcome, so the caller stops writing and reopens the partition,
//! which reloads the records ([`SliceError::Commit`]). The background driver
//! (see [`super::WalkDriver`]) chooses which walk runs, retries, and paces; tests
//! drive walks with `run_all`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use bytes::{Bytes, BytesMut};
use rocksdb::DBRawIteratorWithThreadMode;
use tokio::time::Instant;

use restate_clock::WallClock;
use restate_storage_api::vqueue_table::OwnedEntryStatusHeader;
use restate_storage_api::{StorageError, Transaction};
use restate_types::identifiers::BaseEntryId;

use crate::fsm_table::{fsm_variable, put_migration_jobs, put_registry, put_walks};
use crate::job::{MigrationJobId, MigrationJobRecords, MigrationJobStatus};
use crate::keys::DecodeTableKey;
use crate::registry::{IndexBackfill, IndexRegistry, MaintainedIndexes};
use crate::scan::TableScan;
use crate::vqueue_table::{EntryStatusKey, decode_entry_status};
use crate::{PartitionDb, PartitionStoreTransaction};

use super::{Walk, WalkError, WalkKind, Walks};

/// An entry status a slice read: its key, ID and decoded header.
pub(crate) type SourceEntry = (Vec<u8>, BaseEntryId, OwnedEntryStatusHeader);

/// The bounds of one slice; see the module documentation. A key count alone
/// keeps the default byte limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SliceBudget {
    /// Source keys read.
    pub(crate) keys: usize,
    /// Bytes of the source keys and values read.
    pub(crate) read_bytes: usize,
    /// Bytes of the staged batch, including the walk and registry records it
    /// commits with.
    pub(crate) write_bytes: usize,
}

impl SliceBudget {
    pub(crate) const READ_BYTES: usize = 4 << 20;
    pub(crate) const WRITE_BYTES: usize = 8 << 20;
}

impl From<usize> for SliceBudget {
    fn from(keys: usize) -> Self {
        Self {
            keys,
            read_bytes: Self::READ_BYTES,
            write_bytes: Self::WRITE_BYTES,
        }
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
/// in [`WalkJob::job_ids`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Staged {
    /// The jobs that filled the entry.
    pub(crate) filled: u64,
    /// The jobs that lacked the entry but can't fill it (passive ones).
    pub(crate) uncovered: u64,
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
/// progress, its members' evidence and its jobs' records, and commits; a job only
/// stages its own work.
pub(crate) trait WalkJob {
    /// The migration jobs the walk runs, at most 64. A slice reports what it did
    /// for each by position in this list.
    fn job_ids(&self) -> impl Iterator<Item = u32>;

    /// Stages the work on `entry`, the next source entry of the walk.
    fn stage(
        &self,
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        entry: &SourceEntry,
    ) -> Result<Staged, StorageError>;

    /// Applies to `registry` how each job ended the walk's pass at `at`; it
    /// commits with the walk's last work and removal.
    fn finish(
        &self,
        registry: &mut IndexRegistry,
        settled: &[(u32, Settlement)],
        at: u64,
    ) -> Result<(), StorageError>;
}

/// The job running `walk`.
fn job(active: &MaintainedIndexes, walk: &Walk) -> Result<impl WalkJob, StorageError> {
    if walk.kind != WalkKind::index_backfill() {
        return Err(walk_error(WalkError::UnsupportedKind(walk.kind.clone())));
    }
    IndexBackfill::new(active, walk.id)
}

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
/// took (not counting one rolled back at the write limit), the rows it filled
/// for each job, and its work time up to the commit.
pub(crate) async fn run_counted_slice(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    id: u64,
    budget: SliceBudget,
) -> Result<(Slice, usize), (SliceError, usize)> {
    let started = Instant::now();
    let active = tx.maintained_indexes();
    let mut examined = 0;
    let staged = stage(tx, |tx| {
        let walk = active
            .walks()
            .get(id)
            .ok_or(WalkError::UnknownId(id))
            .map_err(walk_error)?;
        let job = job(&active, walk)?;
        let job_ids: Vec<_> = job.job_ids().collect();
        let mut filled = vec![0_u64; job_ids.len()];
        let mut uncovered = vec![0_u64; job_ids.len()];
        // The first slice of a walk starts a pass.
        let pass_start = walk.cursor.is_none();
        // The records this slice commits with, generously: the walks as large as
        // this slice can make them (every member with the largest evidence), the
        // registry and the unfinished jobs, which grow by at most a few fields,
        // and the history, which a finishing slice also writes. Whether a slice
        // finishes is only known at its end, so every slice counts the history.
        let mut largest_walks = active.walks().clone();
        for job_id in &job_ids {
            largest_walks
                .add_uncovered(id, *job_id, u64::MAX)
                .map_err(walk_error)?;
        }
        let jobs = active.jobs();
        let records = 4 * RECORD_OVERHEAD
            + bilrost::Message::encoded_len(&largest_walks)
            + bilrost::Message::encoded_len(active.registry())
            + jobs.unfinished.encoded_len()
            + jobs.history.encoded_len()
            + JOB_GROWTH * jobs.unfinished.entries().len();
        let mut source = SourceCursor::open(db, walk.cursor.as_deref(), Some(&walk.end))?;
        let (mut taken, mut read) = (0, 0);
        let mut last = None;
        let exhausted = loop {
            let Some(size) = source.peek()? else {
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
            // Decoding examines the entry, also when it fails.
            examined += 1;
            let entry = source.take()?;
            if taken > 0 {
                tx.set_save_point();
            }
            let staged = job.stage(tx, db, &entry)?;
            if taken > 0 && tx.estimated_size_in_bytes() + records > budget.write_bytes {
                // The entry's work, tokens and evidence roll back together.
                tx.rollback_to_save_point()?;
                break false;
            }
            taken += 1;
            for position in 0..job_ids.len() {
                filled[position] += (staged.filled >> position) & 1;
                uncovered[position] += (staged.uncovered >> position) & 1;
            }
            last = Some(entry.0);
        };
        let now = WallClock::now_ms().as_u64();
        let work_us = micros(started.elapsed());
        let partition = db.partition().id();
        let mut registry = active.registry().clone();
        let mut walks = active.walks().clone();
        // Shares the history; only a finishing slice copies it.
        let mut jobs = active.jobs().clone();
        for ((job_id, filled), uncovered) in job_ids.iter().zip(filled).zip(uncovered) {
            walks
                .add_uncovered(id, *job_id, uncovered)
                .map_err(walk_error)?;
            if let Some(record) = jobs.unfinished.get_mut(MigrationJobId::for_index(*job_id)) {
                let pass_start = pass_start && !walk.joined_late(*job_id);
                record.record_slice(now, taken as u64, filled, work_us, pass_start);
            }
        }
        let slice = match last {
            _ if exhausted => {
                let evidence = walks
                    .get(id)
                    .ok_or(WalkError::UnknownId(id))
                    .map_err(walk_error)?
                    .clone();
                let settled: Vec<_> = job_ids
                    .iter()
                    .map(|job_id| {
                        let certified =
                            !evidence.joined_late(*job_id) && evidence.uncovered(*job_id) == 0;
                        let settlement = if certified {
                            Settlement::Certified
                        } else {
                            Settlement::Unfinished
                        };
                        (*job_id, settlement)
                    })
                    .collect();
                job.finish(&mut registry, &settled, now)?;
                walks.remove(id).map_err(walk_error)?;
                for job_id in &job_ids {
                    settle(&mut jobs, &evidence, *job_id, now, active.history_limit());
                }
                put_registry(tx, partition, &registry)?;
                put_migration_jobs(
                    tx,
                    partition,
                    fsm_variable::MIGRATION_JOB_HISTORY,
                    &jobs.history,
                )?;
                Slice::Exhausted
            }
            Some(last) => {
                walks.advance(id, Bytes::from(last)).map_err(walk_error)?;
                Slice::Progressed
            }
            None => unreachable!("a slice that stops early took its first entry"),
        };
        put_walks(tx, partition, &walks)?;
        put_migration_jobs(
            tx,
            partition,
            fsm_variable::MIGRATION_JOBS,
            &jobs.unfinished,
        )?;
        Ok((registry, walks, jobs, slice))
    });
    let (registry, walks, jobs, slice) = staged.map_err(|err| (err, examined))?;
    commit(tx, db, &active, registry, walks, jobs)
        .await
        .map_err(|err| (err, examined))?;
    Ok((slice, examined))
}

/// Records how job `job_id` ended the pass of the exhausted walk `evidence` at
/// `now`. A late joiner saw only a suffix: no full pass, eligible again at once.
/// Otherwise the full pass is recorded, gaps or not; a gap-free job completes and
/// one with gaps waits as its schedule says.
fn settle(
    jobs: &mut MigrationJobRecords,
    evidence: &Walk,
    job_id: u32,
    now: u64,
    history_limit: usize,
) {
    let id = MigrationJobId::for_index(job_id);
    let Some(record) = jobs.unfinished.get_mut(id) else {
        return;
    };
    if evidence.joined_late(job_id) {
        let interval = record.recheck_interval_ms;
        record.wait(interval, None);
        return;
    }
    let gaps = evidence.uncovered(job_id);
    record.record_full_pass(now, gaps);
    if gaps > 0 {
        record.wait_after_gaps(now);
    } else {
        jobs.finish(id, MigrationJobStatus::Completed, Some(now), history_limit);
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
/// grow about fifteen fields, the times, the pass fields and the counters, by at
/// most 12 bytes each in their largest encoding (about 180 bytes in all); this
/// leaves room to spare.
const JOB_GROWTH: usize = 256;

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
        .maintained_indexes()
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
/// failed, on the records of the walk's jobs, once the failure cleared what it
/// staged. A failure to record it is returned, not recorded.
pub(crate) async fn record_failure(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    id: u64,
    error: &str,
    work: Duration,
) -> Result<(), SliceError> {
    let active = tx.maintained_indexes();
    let now = WallClock::now_ms().as_u64();
    let jobs = stage(tx, |tx| {
        active
            .walks()
            .get(id)
            .ok_or(WalkError::UnknownId(id))
            .map_err(walk_error)?;
        let mut jobs = active.jobs().clone();
        for member in active.registry().members(id) {
            if let Some(record) = jobs
                .unfinished
                .get_mut(MigrationJobId::for_index(member.id))
            {
                record.record_failure(now, error, micros(work));
            }
        }
        put_migration_jobs(
            tx,
            db.partition().id(),
            fsm_variable::MIGRATION_JOBS,
            &jobs.unfinished,
        )?;
        Ok(jobs)
    })?;
    let (registry, walks) = (active.registry().clone(), active.walks().clone());
    commit(tx, db, &active, registry, walks, jobs).await
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

/// Commits what `tx` staged on top of `active`, then publishes `registry`,
/// `walks` and `jobs` as committed. Commit and publication are work of the
/// partition's session: a close waits for them, and a closed session commits
/// nothing, so a slice of a closed partition can never publish into a later
/// session.
pub(crate) async fn commit(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    active: &MaintainedIndexes,
    registry: IndexRegistry,
    walks: Walks,
    jobs: MigrationJobRecords,
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
    db.install_maintained_indexes(Arc::clone(&next));
    tx.set_maintained_indexes(next);
    Ok(())
}

/// The last entry-status key of the partition: the end of a walk starting now.
pub(crate) fn last_key(db: &PartitionDb) -> Result<Option<Bytes>, StorageError> {
    let mut arena = BytesMut::new();
    let mut options = rocksdb::ReadOptions::default();
    options.fill_cache(false);
    let mut iterator = db.scan(
        TableScan::ScanPartitionKeyRange::<EntryStatusKey>(db.partition().key_range)
            .encode(&mut arena),
        options,
    )?;
    iterator.seek_to_last();
    let last = iterator.key().map(Bytes::copy_from_slice);
    iterator
        .status()
        .map_err(|err| StorageError::Generic(err.into()))?;
    Ok(last)
}

/// A fresh iterator over the entry statuses in `(resume, end]`, in key order.
struct SourceCursor<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, rocksdb::DB>,
    end: Option<&'a [u8]>,
}

impl<'a> SourceCursor<'a> {
    fn open(
        db: &'a PartitionDb,
        resume: Option<&[u8]>,
        end: Option<&'a [u8]>,
    ) -> Result<Self, StorageError> {
        let mut options = rocksdb::ReadOptions::default();
        options.fill_cache(false);
        let mut iterator = db.scan(
            TableScan::ScanPartitionKeyRange::<EntryStatusKey>(db.partition().key_range)
                .encode(&mut BytesMut::new()),
            options,
        )?;
        if let Some(resume) = resume {
            iterator.seek(resume);
            if iterator.key() == Some(resume) {
                iterator.next();
            }
        }
        Ok(Self { iterator, end })
    }

    /// The bytes of the next entry's key and value, or `None` once none is left.
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

    /// Decodes the next entry, after [`Self::peek`] found one, and moves past it.
    fn take(&mut self) -> Result<SourceEntry, StorageError> {
        let (Some(raw), Some(value)) = (self.iterator.key(), self.iterator.value()) else {
            return Err(StorageError::DataIntegrityError);
        };
        let mut key = raw;
        let entry = EntryStatusKey::deserialize_from(&mut key)?.id;
        let decoded = (raw.to_vec(), entry, decode_entry_status(&entry, value)?);
        self.iterator.next();
        Ok(decoded)
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
    let mut source = SourceCursor::open(db, resume, end)?;
    let mut entries = Vec::new();
    let mut read = 0;
    let exhausted = loop {
        let Some(size) = source.peek()? else {
            break true;
        };
        if entries.len() >= keys || (!entries.is_empty() && read + size > bytes) {
            break false;
        }
        read += size;
        entries.push(source.take()?);
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
