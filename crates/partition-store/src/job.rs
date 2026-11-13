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
//! Ready, and the walk where the work resumes; a job record only reports how the
//! work went: when it started and finished, the rows and time its slices took,
//! and its failures. Nothing reads a job record to decide anything, so a record
//! that is missing or partly unknown is reported as such, never repaired by
//! guessing.
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
//! Times are wall-clock milliseconds since the epoch on this replica, `None` when
//! unknown, for example for a job reconstructed for a store from before these
//! records. Work times are microseconds of a slice's work before its commit: a
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum MigrationJobKind {
    #[bilrost(0)]
    Unknown,
    /// Builds an index; the job has the index's ID.
    #[bilrost(1)]
    IndexBackfill,
}

/// Where a migration job is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum MigrationJobStatus {
    #[bilrost(0)]
    Unknown,
    /// Registered; no slice has committed yet.
    #[bilrost(1)]
    Pending,
    /// At least one slice has committed.
    #[bilrost(2)]
    Running,
    /// Done: its index is Ready.
    #[bilrost(3)]
    Completed,
    /// Stopped before completion: its index was dropped.
    #[bilrost(4)]
    Cancelled,
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
    #[bilrost(20)]
    pub last_error_at: Option<u64>,
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
        }
    }

    pub fn id(&self) -> MigrationJobId {
        MigrationJobId::for_index(self.id)
    }

    /// Records a slice that committed at `at`, examining `examined` rows and
    /// filling `filled` of them for this job in `work_us`. Only a pending job's
    /// first slice sets its start: a job rebuilt while running keeps its start
    /// unknown.
    pub(crate) fn record_slice(&mut self, at: u64, examined: u64, filled: u64, work_us: u64) {
        if self.status == MigrationJobStatus::Pending {
            self.started_at = Some(at);
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

    /// Finishes the job with `status`, at `at` if known. A completed job made a
    /// full pass.
    pub(crate) fn finish(&mut self, status: MigrationJobStatus, at: Option<u64>) {
        debug_assert!(status.is_finished());
        if status == MigrationJobStatus::Completed {
            self.passes = self.passes.saturating_add(1);
        }
        self.status = status;
        self.walk = None;
        self.finished_at = at;
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
        job.record_slice(1_100, 128, 100, 900);
        job.record_failure(1_200, &"é".repeat(MAX_ERROR_LEN), 900);
        assert_eq!(
            (job.status, job.started_at, job.failures, job.work_us),
            (MigrationJobStatus::Running, Some(1_100), 1, 1_800)
        );
        assert!(job.last_error.as_ref().unwrap().len() <= MAX_ERROR_LEN);
        // A committed slice clears the error but keeps the failure count, and
        // only the first slice sets the start.
        job.record_slice(1_300, 28, 28, 900);
        assert_eq!(
            (job.rows_examined, job.rows_filled, job.slices, job.work_us),
            (156, 128, 2, 2_700)
        );
        assert_eq!(
            (
                job.started_at,
                job.consecutive_failures,
                job.failures,
                job.last_error.as_ref()
            ),
            (Some(1_100), 0, 1, None)
        );
        // A job rebuilt while running keeps its start unknown.
        let mut rebuilt = MigrationJob {
            status: MigrationJobStatus::Running,
            started_at: None,
            ..job.clone()
        };
        rebuilt.record_slice(1_400, 1, 1, 900);
        assert_eq!(rebuilt.started_at, None);

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
        assert_eq!((completed.passes, completed.walk), (1, None));
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
