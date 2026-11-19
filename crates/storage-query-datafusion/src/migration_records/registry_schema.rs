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
    /// The registry of each partition replica on the worker nodes: its indexes and
    /// statistics, and its one-time migrations, with their status. Each replica
    /// keeps its own: two replicas of a partition can be at different points. A replica's rows come from one snapshot
    /// of it, separate from the one `migration_jobs` reads, except
    /// `approx_size_bytes`, estimated over the replica's current data.
    registry(
        /// The node holding the replica.
        plain_node_id: DataType::Utf8,

        /// The node's current generation.
        gen_node_id: DataType::Utf8,

        /// The partition the replica belongs to.
        partition_id: DataType::UInt32,

        /// The definition the index builds or the migration runs, with its
        /// version, for example `service_stats/1`.
        definition: DataType::Utf8,

        /// `secondary` (one key per entry) or `aggregate` (counters, a statistic)
        /// for an index; `migration` for a one-time migration.
        kind: DataType::Utf8,

        /// The entry's ID in this replica; for an index, the prefix of its keys.
        id: DataType::UInt32,

        /// For an index, `backfilling` (maintained but incomplete: queries don't
        /// read it), `ready` or `dropped`; for a migration, `in_progress` or
        /// `complete`.
        status: DataType::Utf8,

        /// The job, while the index is backfilling or the migration in progress;
        /// see `migration_jobs`.
        job_id: DataType::UInt32,

        /// When the entry was registered; NULL if unknown.
        registered_at: TimestampMillisecond,

        /// The Restate version that registered it; NULL if unknown.
        registered_by_version: DataType::Utf8,

        /// When the index became Ready; NULL until then, if unknown, and for a
        /// migration.
        ready_at: TimestampMillisecond,

        /// When the migration completed; NULL until then, if unknown, and for an
        /// index.
        completed_at: TimestampMillisecond,

        /// When it was dropped; NULL unless dropped, or if unknown.
        dropped_at: TimestampMillisecond,

        /// Why it was dropped: `reset`, `unknown-definition`, or `unknown(N)` for a
        /// reason this binary doesn't know. NULL unless dropped.
        drop_reason: DataType::Utf8,

        /// RocksDB's estimate of the storage the index's keys take on this replica,
        /// including writes not flushed yet, over the replica's current data rather
        /// than the rows' snapshot. Approximate: small indexes can show 0, and a
        /// dropped index may still show bytes until compaction removes its keys.
        /// NULL for a migration, an index of unknown kind, or when RocksDB's
        /// estimate returns an error.
        approx_size_bytes: DataType::UInt64,
    )
);
