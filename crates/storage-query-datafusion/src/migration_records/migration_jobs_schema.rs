// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;

use crate::table_macro::*;

define_table!(
    /// The background migration jobs of each partition replica on the worker
    /// nodes, such as the build of an index: unfinished ones, and finished ones
    /// up to `worker.storage.migration-job-history-limit`. Each replica keeps its
    /// own; a replica's rows come from one snapshot of it, separate from the one
    /// `indexes` reads.
    ///
    /// Jobs that share a walk are each charged the walk's rows examined, slices
    /// and work time, so those columns don't add up across jobs; `rows_filled`
    /// is per job. Rows and slices count committed slices only; work time and
    /// failures also count failed attempts.
    migration_jobs(
        /// The node holding the replica.
        plain_node_id: DataType::Utf8,

        /// The node's current generation.
        gen_node_id: DataType::Utf8,

        /// The partition the replica belongs to.
        partition_id: DataType::UInt32,

        /// The job's ID; a build job has its index's ID.
        job_id: DataType::UInt32,

        /// What the job does: `index_backfill`.
        kind: DataType::Utf8,

        /// What it builds: for an index backfill, the definition, for example
        /// `service_stats/1`.
        name: DataType::Utf8,

        /// `pending` (no slice yet), `running`, `waiting` (between passes),
        /// `completed` or `cancelled`.
        status: DataType::Utf8,

        /// `active` when this binary's definition fills rows itself, `passive`
        /// when only writes and deletions cover them; NULL for a definition this
        /// binary doesn't know.
        mode: DataType::Utf8,

        /// How far the current pass is through the replica's key range, in percent.
        /// Approximate, never evidence of completion; NULL between passes, before
        /// the first slice, or over a single partition key.
        progress_percent: DataType::UInt32,

        /// When the current pass should end if it keeps its rate so far: the time
        /// since `pass_started_at`, stretched by how much of the key range is still
        /// ahead. Approximate like `progress_percent`, but from the exact cursor
        /// position, so it can show while that still rounds to 0. NULL while
        /// progress or `pass_started_at` is unknown, or the cursor hasn't moved
        /// yet.
        eta: TimestampMillisecond,

        /// When the job was created; NULL if unknown.
        created_at: TimestampMillisecond,

        /// The Restate version that created it; NULL if unknown.
        created_by_version: DataType::Utf8,

        /// When its first slice committed; NULL while pending, or if unknown
        /// (for a job rebuilt while running).
        started_at: TimestampMillisecond,

        /// When the current or last full pass started its first slice; NULL if
        /// unknown, for a job moved onto a pass already under way.
        pass_started_at: TimestampMillisecond,

        /// When the last full pass ended.
        last_pass_finished_at: TimestampMillisecond,

        /// When it finished; NULL until then, or if unknown.
        finished_at: TimestampMillisecond,

        /// When a waiting job may start its next pass; NULL means now.
        next_eligible_at: TimestampMillisecond,

        /// Full passes that ended.
        passes: DataType::UInt32,

        /// Source rows its committed slices examined.
        rows_examined: DataType::UInt64,

        /// Rows its slices filled for this job.
        rows_filled: DataType::UInt64,

        /// Rows the last full pass found it couldn't cover; NULL before one ended.
        rows_uncovered: DataType::UInt64,

        /// The same count for the full pass before, for the trend.
        rows_uncovered_previous: DataType::UInt64,

        /// Committed slices.
        slices: DataType::UInt64,

        /// Work time of its slices and failed attempts, before their commits.
        work_ms: DataType::UInt64,

        /// When the last slice committed.
        last_slice_at: TimestampMillisecond,

        /// The last slice's work time before its commit.
        last_slice_work_ms: DataType::UInt64,

        /// Failed attempts: at a slice, or at starting another pass.
        failures: DataType::UInt32,

        /// Failed attempts since the last committed slice.
        consecutive_failures: DataType::UInt32,

        /// Why the latest attempt failed; NULL after the next committed slice.
        last_error: DataType::Utf8,

        /// When the latest attempt failed; kept after the error clears.
        last_error_at: TimestampMillisecond,
    )
);
