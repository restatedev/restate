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
    /// The indexes and statistics of each partition replica on the worker nodes,
    /// with their status. Each replica keeps its own: two replicas of a partition
    /// can be at different points. A replica's rows come from one snapshot of it,
    /// separate from the one `migration_jobs` reads.
    indexes(
        /// The node holding the replica.
        plain_node_id: DataType::Utf8,

        /// The node's current generation.
        gen_node_id: DataType::Utf8,

        /// The partition the replica belongs to.
        partition_id: DataType::UInt32,

        /// The definition the index builds, with its version, for example
        /// `service_stats/1`.
        definition: DataType::Utf8,

        /// `secondary` (one key per entry) or `aggregate` (counters, a statistic).
        kind: DataType::Utf8,

        /// The index's ID in this replica, the prefix of its keys.
        index_id: DataType::UInt32,

        /// `backfilling` (maintained but incomplete: queries don't read it),
        /// `ready` or `dropped`.
        status: DataType::Utf8,

        /// The build job, while the index is backfilling; see `migration_jobs`.
        job_id: DataType::UInt32,

        /// When the index was registered; NULL if unknown.
        registered_at: TimestampMillisecond,

        /// The Restate version that registered it; NULL if unknown.
        registered_by_version: DataType::Utf8,

        /// When it became Ready; NULL until then, or if unknown.
        ready_at: TimestampMillisecond,

        /// When it was dropped; NULL unless dropped, or if unknown.
        dropped_at: TimestampMillisecond,

        /// Why it was dropped: `reset`, `unknown-definition`, or `unknown(N)` for a
        /// reason this binary doesn't know. NULL unless dropped.
        drop_reason: DataType::Utf8,
    )
);
