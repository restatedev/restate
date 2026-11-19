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

use restate_clock::time::MillisSinceEpoch;
use restate_types::nodes_config::Role;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// The registry of each partition replica on the worker nodes: its indexes and
    /// statistics, and its one-time migrations, with their status. Each replica
    /// keeps its own: two replicas of a partition can be at different points. A replica's rows come from one snapshot
    /// of it, separate from the one `migration_jobs` reads, except
    /// `approx_size_bytes`, estimated over the replica's current data.
    registry {
        name: "registry",
        sharding: ShardingScheme::ByNode(Some(Role::Worker)),
        sharding_columns { plain_node_id: PlainNodeId, gen_node_id: GenerationalNodeId },
        columns {
            /// The node holding the replica.
            plain_node_id: ReString => DataType::Utf8,
            /// The node's current generation.
            gen_node_id: ReString => DataType::Utf8,
            /// The partition the replica belongs to.
            partition_id: u32 => DataType::UInt32,
            /// The definition the index builds or the migration runs, with its
            /// version, for example `service_stats/1`.
            definition: ReString => DataType::Utf8,
            /// `secondary` (one key per entry) or `aggregate` (counters, a
            /// statistic) for an index; `migration` for a one-time migration.
            kind: ReString => DataType::Utf8,
            /// The entry's ID in this replica; for an index, the prefix of its keys.
            id: u32 => DataType::UInt32,
            /// For an index, `backfilling` (maintained but incomplete: queries
            /// don't read it), `complete` (queries read it) or `dropped`; for a
            /// migration, `in_progress` or `complete`.
            status: ReString => DataType::Utf8,
            /// The job, while the index is backfilling or the migration in
            /// progress; see `migration_jobs`.
            job_id: Option<u32> => DataType::UInt32,
            /// When the entry was registered; NULL if unknown.
            registered_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// The Restate version that registered it; NULL if unknown.
            registered_by_version: Option<ReString> => DataType::Utf8,
            /// When the index or the migration completed; NULL until then, or if
            /// unknown.
            completed_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// When it was dropped; NULL unless dropped, or if unknown.
            dropped_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Why it was dropped: `reset`, `unknown-definition`, or `unknown(N)` for a
            /// reason this binary doesn't know. NULL unless dropped.
            drop_reason: Option<ReString> => DataType::Utf8,
            /// RocksDB's estimate of the storage the index's keys take on this
            /// replica, including writes not flushed yet, over the replica's current
            /// data rather than the rows' snapshot. Approximate: small indexes can
            /// show 0, and a dropped index may still show bytes until compaction
            /// removes its keys. NULL for a migration, an index of unknown kind, or
            /// when RocksDB's estimate returns an error.
            approx_size_bytes: Option<u64> => DataType::UInt64,
        }
    }
}
