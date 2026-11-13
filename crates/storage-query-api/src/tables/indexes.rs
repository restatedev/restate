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
    /// The indexes and statistics of each partition replica on the worker nodes,
    /// with their status. Each replica keeps its own: two replicas of a partition
    /// can be at different points. A replica's rows come from one snapshot of it,
    /// separate from the one `migration_jobs` reads.
    indexes {
        name: "indexes",
        sharding: ShardingScheme::ByNode(Some(Role::Worker)),
        sharding_columns { plain_node_id: PlainNodeId, gen_node_id: GenerationalNodeId },
        columns {
            /// The node holding the replica.
            plain_node_id: ReString => DataType::Utf8,
            /// The node's current generation.
            gen_node_id: ReString => DataType::Utf8,
            /// The partition the replica belongs to.
            partition_id: u32 => DataType::UInt32,
            /// The definition the index builds, with its version, for example
            /// `service_stats/1`.
            definition: ReString => DataType::Utf8,
            /// `secondary` (one key per entry) or `aggregate` (counters, a statistic).
            kind: ReString => DataType::Utf8,
            /// The index's ID in this replica, the prefix of its keys.
            index_id: u32 => DataType::UInt32,
            /// `backfilling` (maintained but incomplete: queries don't read it),
            /// `ready` or `dropped`.
            status: ReString => DataType::Utf8,
            /// The build job, while the index is backfilling; see `migration_jobs`.
            job_id: Option<u32> => DataType::UInt32,
            /// When the index was registered; NULL if unknown.
            registered_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// The Restate version that registered it; NULL if unknown.
            registered_by_version: Option<ReString> => DataType::Utf8,
            /// When it became Ready; NULL until then, or if unknown.
            ready_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// When it was dropped; NULL unless dropped, or if unknown.
            dropped_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Why it was dropped: `reset`, `unknown-definition`, or `unknown(N)` for a
            /// reason this binary doesn't know. NULL unless dropped.
            drop_reason: Option<ReString> => DataType::Utf8,
        }
    }
}
