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

use restate_types::identifiers::PartitionKey;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Counters of the concurrency limits set by rules.
    ///
    /// One row per counter, with its current usage, its limit, the rule defining it and how many vqueues are waiting on it.
    user_limits {
        name: "sys_user_limits",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: true },
        sharding_columns { partition_key: PartitionKey, scope: Scope },
        columns {
            /// Internal column that is used for partitioning. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// The scope this counter belongs to.
            scope: ReString => DataType::LargeUtf8,
            /// The level-1 key component (null for scope-level counters).
            l1: Option<ReString> => DataType::LargeUtf8,
            /// The level-2 key component (null for scope-level and L1-level counters).
            l2: Option<ReString> => DataType::LargeUtf8,
            /// The hierarchy level: "Scope", "Level1", or "Level2".
            level: ReString => DataType::LargeUtf8,
            /// Current concurrency usage at this counter.
            usage: u32 => DataType::UInt32,
            /// The configured concurrency limit (null if unlimited).
            concurrency_limit: Option<u32> => DataType::UInt32,
            /// The rule pattern that defines the limit (null if unlimited).
            /// Resolved from the rule handle; shows "[removed]" if the rule was
            /// deleted since the counter was created.
            rule_pattern: Option<ReString> => DataType::LargeUtf8,
            /// Available capacity (limit - usage). Null if unlimited.
            available: Option<u32> => DataType::UInt32,
            /// Number of vqueues currently waiting behind this counter.
            num_waiters: u64 => DataType::UInt64,
        }
    }
}
