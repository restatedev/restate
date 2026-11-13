// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Migration-job records: what each job building an index did, kept on the
//! replica for operators.
//!
//! A **migration job** is background work that builds derived data, such as an
//! index, from a table written at the same time. Every index has one **build
//! job**, whose [`MigrationJobId`] is the index's ID, and a walk runs it a slice
//! at a time (see [`crate::walk`]). The registry decides whether an index is
//! Ready, and the walk holds where the work resumes and the evidence for
//! completing it. A job record reports how the work went: when it started and
//! finished, the rows and time its slices took, and its failures. It also says
//! when a waiting job may start its next pass, and keeps the counts its schedule
//! compares. Nothing in it decides readiness or coverage, so it can be rebuilt
//! from the registry: a missing or partly unknown record is reported as such,
//! never repaired by guessing, and a rebuilt waiting job is eligible at once.
//!
//! Two records hold the jobs of a partition store, each an FSM variable:
//! - **unfinished jobs** ([`MigrationJobs`] under `MIGRATION_JOBS`), rewritten by
//!   every slice of their walk;
//! - **history**, the finished jobs in the order they finished (also a
//!   [`MigrationJobs`], under `MIGRATION_JOB_HISTORY`), written only when a job
//!   finishes or the store opens, and trimmed to a configured number of jobs.
//!
//! For example, statistic 70 registers at 10:00: job 70 is `Pending`. Its first
//! slice at 10:01 makes it `Running` and sets its start. Each slice adds the rows
//! it examined and filled, and its work time. A failed attempt counts a failure
//! and keeps its error until the next slice commits. The last slice completes
//! the job and moves it to history.
//!
//! **Passes.** A walk from the start of its source to its captured end is a full
//! pass. A job whose definition can't fill rows (a *passive* one) may end a full
//! pass with rows it couldn't cover: it then `Waiting`s for another pass, which
//! starts once it is eligible ([`MigrationJob::next_eligible_at`]). Every full
//! pass records how many rows it found uncovered, the certifying zero included,
//! and keeps the previous count for comparison. A job that joined a pass late saw
//! only a suffix; that is not a full pass and records nothing.
//!
//! Times are wall-clock milliseconds since the epoch on this replica, `None` when
//! unknown, for example for a job rebuilt from the registry because its records
//! couldn't be read. Work times are microseconds of a slice's work before its commit: a
//! slice can't record a duration that includes its own commit.

use std::num::NonZeroU32;
use std::sync::Arc;

use restate_util_string::ReString;

/// The persisted format this binary reads and writes.
const FORMAT: u32 = 1;

/// The longest error a job records, in bytes.
pub const MAX_ERROR_LEN: usize = 512;

/// How many finished jobs the history keeps unless configured otherwise
/// (`worker.storage.migration-job-history-limit`).
pub(crate) const DEFAULT_HISTORY_LIMIT: usize = 100;

/// The shortest wait between two passes of a job that found uncovered rows.
///
/// A constant while no definition is passive. The release that adds the first
/// passive definition must make it, and [`RECHECK_CAP_MS`], configuration
/// options (decided 2026-11-13; see `docs/dev/passive-ready-design.md`).
pub(crate) const RECHECK_FLOOR_MS: u64 = 10 * 60 * 1000;

/// The longest wait between two passes of a job that found uncovered rows; see
/// [`RECHECK_FLOOR_MS`].
pub(crate) const RECHECK_CAP_MS: u64 = 24 * 60 * 60 * 1000;

/// The ID of a migration job, never 0. A build job has the ID of the index it
/// builds, and the same 4 bytes are its token in the rows it reflects. Records
/// store it as a plain `u32`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MigrationJobId(NonZeroU32);

impl MigrationJobId {
    /// The build job of index `index_id`.
    ///
    /// # Panics
    /// If `index_id` is 0, which the registry never issues: index IDs start at
    /// [`crate::registry::FIRST_INDEX_ID`].
    pub const fn for_index(index_id: u32) -> Self {
        match NonZeroU32::new(index_id) {
            Some(id) => Self(id),
            None => panic!("index IDs are never 0"),
        }
    }

    pub const fn as_u32(self) -> u32 {
        self.0.get()
    }
}

impl std::fmt::Display for MigrationJobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// What a migration job does.
///
/// **Compatibility.** A kind this binary doesn't know makes the whole job list
/// unreadable (bilrost rejects unknown enumeration values); opening the store
/// then rebuilds the list from the registry, losing that job's record. Records
/// are diagnostics, so a release may add a kind without a version floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum MigrationJobKind {
    #[bilrost(0)]
    Unknown,
    /// Builds an index; the job has the index's ID.
    #[bilrost(1)]
    IndexBackfill,
}

/// Where a migration job is in its life.
///
/// **Compatibility.** As for [`MigrationJobKind`]: a status this binary doesn't
/// know makes the job list unreadable, and opening rebuilds it from the registry.
/// A release may add a status without a version floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum MigrationJobStatus {
    #[bilrost(0)]
    Unknown,
    /// Registered; no slice of it has ever committed.
    #[bilrost(1)]
    Pending,
    /// A walk runs it: its first slice ever has committed, or it started another
    /// pass after waiting.
    #[bilrost(2)]
    Running,
    /// Done: its index is Ready.
    #[bilrost(3)]
    Completed,
    /// Stopped before completion: its index was dropped.
    #[bilrost(4)]
    Cancelled,
    /// Between passes: its last full pass found rows it couldn't cover, or it saw
    /// only a suffix of one. Unfinished; its index stays maintained.
    #[bilrost(5)]
    Waiting,
}

impl MigrationJobStatus {
    pub fn is_finished(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }
}

/// One job and what it did. Every member of a walk is charged the walk's full
/// committed rows and work time, so those counters are per-job views of a shared
/// pass, not additive across the jobs of one walk; `rows_filled` is per job.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct MigrationJob {
    /// A [`MigrationJobId`]; [`MigrationJobs::decode`] rejects 0.
    #[bilrost(1)]
    id: u32,
    #[bilrost(2)]
    pub kind: MigrationJobKind,
    /// The index definition it builds.
    #[bilrost(3)]
    pub name: ReString,
    #[bilrost(4)]
    pub status: MigrationJobStatus,
    /// The walk running it, while unfinished.
    #[bilrost(5)]
    pub walk: Option<u64>,
    #[bilrost(6)]
    pub created_at: Option<u64>,
    /// The Restate version that created it.
    #[bilrost(7)]
    pub created_by: Option<ReString>,
    /// When its first slice committed; `None` while pending, or if the job was
    /// already running when this record was rebuilt.
    #[bilrost(8)]
    pub started_at: Option<u64>,
    #[bilrost(9)]
    pub finished_at: Option<u64>,
    /// Full passes over its source.
    #[bilrost(10)]
    pub passes: u32,
    /// Source rows committed slices examined.
    #[bilrost(11)]
    pub rows_examined: u64,
    /// Rows its slices filled, as opposed to rows that already listed its token.
    #[bilrost(12)]
    pub rows_filled: u64,
    /// Committed slices.
    #[bilrost(13)]
    pub slices: u64,
    /// Work time before commit, in microseconds, of committed slices and failed
    /// attempts.
    #[bilrost(14)]
    pub work_us: u64,
    #[bilrost(15)]
    pub last_slice_at: Option<u64>,
    /// The last committed slice's work time before commit, in microseconds.
    #[bilrost(16)]
    pub last_slice_work_us: Option<u64>,
    /// Failed attempts at a slice.
    #[bilrost(17)]
    pub failures: u32,
    /// Failed attempts since the last committed slice.
    #[bilrost(18)]
    pub consecutive_failures: u32,
    /// Why the latest attempt failed, until the next slice commits.
    #[bilrost(19)]
    pub last_error: Option<ReString>,
    /// When the latest attempt failed. Unlike the error, it stays after the next
    /// slice commits, so `failures` keeps the time of its most recent one.
    #[bilrost(20)]
    pub last_error_at: Option<u64>,
    /// When the current or last full pass committed its first slice.
    #[bilrost(21)]
    pub pass_started_at: Option<u64>,
    #[bilrost(22)]
    pub last_pass_finished_at: Option<u64>,
    /// Rows the last full pass found it couldn't cover; `None` before one ended.
    #[bilrost(23)]
    pub rows_uncovered: Option<u64>,
    /// The same count for the full pass before, for comparison.
    #[bilrost(24)]
    pub rows_uncovered_previous: Option<u64>,
    /// The wait before the next pass, while waiting after a pass with gaps.
    #[bilrost(25)]
    pub recheck_interval_ms: Option<u64>,
    /// When a `Waiting` job may start its next pass; `None` means now.
    #[bilrost(26)]
    pub next_eligible_at: Option<u64>,
}

impl MigrationJob {
    /// The `Pending` build job of index `index_id`, created at `at` by `by`.
    pub(crate) fn index_backfill(
        index_id: u32,
        name: ReString,
        walk: Option<u64>,
        at: Option<u64>,
        by: Option<ReString>,
    ) -> Self {
        Self {
            id: MigrationJobId::for_index(index_id).as_u32(),
            kind: MigrationJobKind::IndexBackfill,
            name,
            status: MigrationJobStatus::Pending,
            walk,
            created_at: at,
            created_by: by,
            started_at: None,
            finished_at: None,
            passes: 0,
            rows_examined: 0,
            rows_filled: 0,
            slices: 0,
            work_us: 0,
            last_slice_at: None,
            last_slice_work_us: None,
            failures: 0,
            consecutive_failures: 0,
            last_error: None,
            last_error_at: None,
            pass_started_at: None,
            last_pass_finished_at: None,
            rows_uncovered: None,
            rows_uncovered_previous: None,
            recheck_interval_ms: None,
            next_eligible_at: None,
        }
    }

    pub fn id(&self) -> MigrationJobId {
        MigrationJobId::for_index(self.id)
    }

    /// Records a slice that committed at `at`, examining `examined` rows and
    /// filling `filled` of them for this job in `work_us`; `pass_start` when it is
    /// the first slice of a full pass. Only a pending job's first slice sets its
    /// start: a job rebuilt while running keeps its start unknown.
    pub(crate) fn record_slice(
        &mut self,
        at: u64,
        examined: u64,
        filled: u64,
        work_us: u64,
        pass_start: bool,
    ) {
        if self.status == MigrationJobStatus::Pending {
            self.started_at = Some(at);
        }
        if pass_start {
            self.pass_started_at = Some(at);
        }
        self.status = MigrationJobStatus::Running;
        self.rows_examined = self.rows_examined.saturating_add(examined);
        self.rows_filled = self.rows_filled.saturating_add(filled);
        self.slices = self.slices.saturating_add(1);
        self.work_us = self.work_us.saturating_add(work_us);
        self.last_slice_at = Some(at);
        self.last_slice_work_us = Some(work_us);
        self.consecutive_failures = 0;
        self.last_error = None;
    }

    /// Records an attempt that failed at `at` with `error` after `work_us`.
    pub(crate) fn record_failure(&mut self, at: u64, error: &str, work_us: u64) {
        let mut len = error.len().min(MAX_ERROR_LEN);
        while !error.is_char_boundary(len) {
            len -= 1;
        }
        self.failures = self.failures.saturating_add(1);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.work_us = self.work_us.saturating_add(work_us);
        self.last_error = Some(ReString::from(&error[..len]));
        self.last_error_at = Some(at);
    }

    /// Records a full pass that ended at `at` having found `uncovered` rows it
    /// couldn't cover, zero included: the count before it becomes the previous one.
    pub(crate) fn record_full_pass(&mut self, at: u64, uncovered: u64) {
        self.passes = self.passes.saturating_add(1);
        self.last_pass_finished_at = Some(at);
        self.rows_uncovered_previous = self.rows_uncovered;
        self.rows_uncovered = Some(uncovered);
    }

    /// Waits after a full pass with gaps that ended at `at` (see
    /// [`Self::record_full_pass`]). The first such wait is the floor; afterwards
    /// the wait stays if the gaps at least halved since the previous pass, and
    /// doubles otherwise, within [`RECHECK_FLOOR_MS`] and [`RECHECK_CAP_MS`].
    pub(crate) fn wait_after_gaps(&mut self, at: u64) {
        let interval = match (
            self.recheck_interval_ms,
            self.rows_uncovered_previous,
            self.rows_uncovered,
        ) {
            (Some(interval), Some(previous), Some(now)) if now <= previous / 2 => interval,
            (Some(interval), Some(_), Some(_)) => interval.saturating_mul(2),
            _ => RECHECK_FLOOR_MS,
        }
        .clamp(RECHECK_FLOOR_MS, RECHECK_CAP_MS);
        self.wait(Some(interval), at.checked_add(interval));
    }

    /// Waits for another pass, eligible at `next_eligible_at` (`None`: now),
    /// with the schedule's `interval`, if any.
    pub(crate) fn wait(&mut self, interval: Option<u64>, next_eligible_at: Option<u64>) {
        self.status = MigrationJobStatus::Waiting;
        self.walk = None;
        self.recheck_interval_ms = interval;
        self.next_eligible_at = next_eligible_at;
    }

    /// Whether a `Waiting` job may start its next pass at `now`.
    pub(crate) fn is_eligible(&self, now: u64) -> bool {
        self.next_eligible_at.is_none_or(|at| at <= now)
    }

    /// Finishes the job with `status`, at `at` if known, and drops its schedule.
    pub(crate) fn finish(&mut self, status: MigrationJobStatus, at: Option<u64>) {
        debug_assert!(status.is_finished());
        self.status = status;
        self.walk = None;
        self.finished_at = at;
        self.recheck_interval_ms = None;
        self.next_eligible_at = None;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationJobError {
    #[error("unsupported job record format {0}; this binary supports format {FORMAT}")]
    UnsupportedFormat(u32),
    #[error("malformed job record: {0}")]
    Malformed(#[from] bilrost::DecodeError),
    #[error("invalid job record: a job has ID 0")]
    ZeroId,
}

/// A list of jobs: the unfinished jobs of a store, or its history of finished
/// jobs in the order they finished.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct MigrationJobs {
    #[bilrost(1)]
    format: u32,
    #[bilrost(2)]
    jobs: Vec<MigrationJob>,
}

impl Default for MigrationJobs {
    fn default() -> Self {
        Self {
            format: FORMAT,
            jobs: Vec::new(),
        }
    }
}

impl MigrationJobs {
    pub fn decode(bytes: &[u8]) -> Result<Self, MigrationJobError> {
        let jobs = <Self as bilrost::OwnedMessage>::decode(bytes)?;
        if jobs.format != FORMAT {
            return Err(MigrationJobError::UnsupportedFormat(jobs.format));
        }
        if jobs.jobs.iter().any(|job| job.id == 0) {
            return Err(MigrationJobError::ZeroId);
        }
        Ok(jobs)
    }

    pub fn encode(&self) -> Vec<u8> {
        bilrost::Message::encode_to_vec(self)
    }

    /// The size of [`Self::encode`]'s output.
    pub(crate) fn encoded_len(&self) -> usize {
        bilrost::Message::encoded_len(self)
    }

    pub fn entries(&self) -> &[MigrationJob] {
        &self.jobs
    }

    pub fn get(&self, id: MigrationJobId) -> Option<&MigrationJob> {
        self.jobs.iter().find(|job| job.id == id.as_u32())
    }

    pub(crate) fn get_mut(&mut self, id: MigrationJobId) -> Option<&mut MigrationJob> {
        self.jobs.iter_mut().find(|job| job.id == id.as_u32())
    }

    /// Adds `job`, replacing the job with the same ID if there is one.
    pub(crate) fn insert(&mut self, job: MigrationJob) {
        match self.get_mut(job.id()) {
            Some(existing) => *existing = job,
            None => self.jobs.push(job),
        }
    }

    pub(crate) fn remove(&mut self, id: MigrationJobId) -> Option<MigrationJob> {
        let index = self.jobs.iter().position(|job| job.id == id.as_u32())?;
        Some(self.jobs.remove(index))
    }

    /// Appends a finished `job` to this history, then drops the oldest finished
    /// jobs beyond `limit`. Finish order, not wall-clock time, decides which are
    /// oldest.
    pub(crate) fn push_finished(&mut self, job: MigrationJob, limit: usize) {
        self.jobs.push(job);
        self.trim(limit);
    }

    /// Drops the oldest jobs beyond `limit`.
    pub(crate) fn trim(&mut self, limit: usize) {
        let excess = self.jobs.len().saturating_sub(limit);
        self.jobs.drain(..excess);
    }
}

/// The unfinished jobs and the history of a store, as committed. The history is
/// shared: slices that finish no job publish it without copying.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MigrationJobRecords {
    pub unfinished: MigrationJobs,
    pub history: Arc<MigrationJobs>,
}

impl MigrationJobRecords {
    /// Finishes the unfinished job `id` with `status` at `at`, moving it to the
    /// history, which keeps at most `limit` jobs. A job that isn't unfinished is
    /// left alone.
    pub(crate) fn finish(
        &mut self,
        id: MigrationJobId,
        status: MigrationJobStatus,
        at: Option<u64>,
        limit: usize,
    ) {
        if let Some(mut job) = self.unfinished.remove(id) {
            job.finish(status, at);
            Arc::make_mut(&mut self.history).push_finished(job, limit);
        }
    }
}

/// When and by which Restate version something happened, for the records.
#[derive(Debug, Clone)]
pub(crate) struct Stamp {
    /// Wall-clock milliseconds since the epoch.
    pub(crate) at: u64,
    pub(crate) by: ReString,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jobs_record_slices_failures_and_finish_into_a_bounded_history() {
        let mut job = MigrationJob::index_backfill(
            70,
            ReString::from_static("service_stats/1"),
            Some(1),
            Some(1_000),
            Some(ReString::from_static("1.8.2")),
        );
        // Sub-millisecond work adds up instead of rounding away.
        job.record_slice(1_100, 128, 100, 900, true);
        job.record_failure(1_200, &"é".repeat(MAX_ERROR_LEN), 900);
        assert_eq!(
            (job.status, job.started_at, job.failures, job.work_us),
            (MigrationJobStatus::Running, Some(1_100), 1, 1_800)
        );
        assert!(job.last_error.as_ref().unwrap().len() <= MAX_ERROR_LEN);
        // A committed slice clears the error but keeps the failure count, and
        // only the first slice sets the start.
        job.record_slice(1_300, 28, 28, 900, false);
        assert_eq!(
            (job.rows_examined, job.rows_filled, job.slices, job.work_us),
            (156, 128, 2, 2_700)
        );
        assert_eq!(
            (
                job.started_at,
                job.pass_started_at,
                job.consecutive_failures,
                job.failures,
                job.last_error.as_ref()
            ),
            (Some(1_100), Some(1_100), 0, 1, None)
        );
        // A job rebuilt while running keeps its start unknown.
        let mut rebuilt = MigrationJob {
            status: MigrationJobStatus::Running,
            started_at: None,
            ..job.clone()
        };
        rebuilt.record_slice(1_400, 1, 1, 900, false);
        assert_eq!(rebuilt.started_at, None);

        // A passive job's full passes: the first wait is the floor; it stays when
        // the gaps at least halve, doubles otherwise, never past the cap; the
        // certifying pass records its zero, and finishing drops the schedule.
        let mut passive = job.clone();
        let wait = |job: &mut MigrationJob, at, gaps| {
            job.record_full_pass(at, gaps);
            job.wait_after_gaps(at);
            (
                job.status,
                job.recheck_interval_ms.unwrap(),
                job.next_eligible_at,
            )
        };
        let (floor, at) = (RECHECK_FLOOR_MS, 10_000);
        assert_eq!(
            wait(&mut passive, at, 10),
            (MigrationJobStatus::Waiting, floor, Some(at + floor))
        );
        assert!(!passive.is_eligible(at) && passive.is_eligible(at + floor));
        assert_eq!(wait(&mut passive, at, 5).1, floor, "halved");
        assert_eq!(wait(&mut passive, at, 3).1, 2 * floor, "not halved");
        for _ in 0..20 {
            wait(&mut passive, at, 3);
        }
        assert_eq!(passive.recheck_interval_ms, Some(RECHECK_CAP_MS));
        passive.record_full_pass(at, 0);
        passive.finish(MigrationJobStatus::Completed, Some(at));
        assert_eq!(
            (
                passive.passes,
                passive.rows_uncovered,
                passive.rows_uncovered_previous,
                passive.recheck_interval_ms,
                passive.next_eligible_at,
            ),
            (24, Some(0), Some(3), None, None)
        );

        // Finishing moves a job to the history, which keeps the latest ones.
        let mut records = MigrationJobRecords::default();
        let id = MigrationJobId::for_index;
        for index in [70, 71, 72] {
            records.unfinished.insert(MigrationJob {
                id: index,
                ..job.clone()
            });
        }
        records.finish(id(70), MigrationJobStatus::Completed, Some(2_000), 2);
        records.finish(id(71), MigrationJobStatus::Cancelled, None, 2);
        records.finish(id(72), MigrationJobStatus::Completed, Some(1_500), 2);
        let ids: Vec<_> = records
            .history
            .entries()
            .iter()
            .map(MigrationJob::id)
            .collect();
        assert_eq!(
            ids,
            [id(71), id(72)],
            "finish order, not time, decides what is trimmed"
        );
        let completed = records.history.get(id(72)).unwrap();
        assert_eq!(
            (completed.passes, completed.walk),
            (0, None),
            "finishing records no pass; the walk's end does"
        );
        assert!(records.unfinished.entries().is_empty());
        Arc::make_mut(&mut records.history).trim(0);
        assert!(records.history.entries().is_empty());
        assert_eq!(
            DEFAULT_HISTORY_LIMIT,
            restate_types::config::StorageOptions::default().migration_job_history_limit as usize,
            "tools and tests keep what the option keeps by default"
        );

        // Round trip, the ID encodes as the index's `u32`, and an unsupported
        // format is refused.
        let encoded = records.unfinished.encode();
        assert_eq!(MigrationJobs::decode(&encoded).unwrap(), records.unfinished);
        #[derive(bilrost::Message)]
        struct PlainId {
            #[bilrost(1)]
            id: u32,
        }
        let plain = <PlainId as bilrost::OwnedMessage>::decode(
            bilrost::Message::encode_to_vec(&job).as_slice(),
        )
        .unwrap();
        assert_eq!(plain.id, 70);
        let newer = MigrationJobs {
            format: 2,
            jobs: Vec::new(),
        };
        assert!(matches!(
            MigrationJobs::decode(&newer.encode()),
            Err(MigrationJobError::UnsupportedFormat(2))
        ));
        let zero = MigrationJobs {
            format: FORMAT,
            jobs: vec![MigrationJob { id: 0, ..job }],
        };
        assert!(matches!(
            MigrationJobs::decode(&zero.encode()),
            Err(MigrationJobError::ZeroId)
        ));
        assert_eq!(
            size_of::<Option<MigrationJobId>>(),
            size_of::<u32>(),
            "the niche keeps an optional ID at 4 bytes"
        );
    }
}
