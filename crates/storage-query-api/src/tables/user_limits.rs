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
    user_limits {
        name: "sys_user_limits",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: true },
        sharding_columns { partition_key: PartitionKey, scope: Scope },
        columns {
            /// Partition key derived from the counter's scope.
            partition_key: PartitionKey => DataType::UInt64,
            /// Scope owning this counter.
            scope: ReString => DataType::LargeUtf8,
            /// Level-1 key; null for scope-level counters.
            l1: Option<ReString> => DataType::LargeUtf8,
            /// Level-2 key; null for scope-level and level-1 counters.
            l2: Option<ReString> => DataType::LargeUtf8,
            /// Counter level: Scope, Level1, or Level2.
            level: ReString => DataType::LargeUtf8,
            /// Current concurrency usage.
            usage: u32 => DataType::UInt32,
            /// Configured concurrency limit; null if unlimited.
            concurrency_limit: Option<u32> => DataType::UInt32,
            /// Rule defining this limit; null if unlimited.
            rule_pattern: Option<ReString> => DataType::LargeUtf8,
            /// Remaining capacity; null if unlimited.
            available: Option<u32> => DataType::UInt32,
            /// Number of waiting vqueues.
            num_waiters: u64 => DataType::UInt64,
        }
    }
}
