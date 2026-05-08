// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::table_macro::*;

use datafusion::arrow::datatypes::DataType;

define_sort_order!(sys_user_limits(partition_key, scope, l1, l2));

define_table!(sys_user_limits(
    /// Internal column that is used for partitioning. Can be ignored.
    partition_key: DataType::UInt64,

    /// The scope this counter belongs to.
    scope: DataType::Utf8,

    /// The level-1 key component (null for scope-level counters).
    l1: DataType::Utf8,

    /// The level-2 key component (null for scope-level and L1-level counters).
    l2: DataType::Utf8,

    /// The hierarchy level: "Scope", "Level1", or "Level2".
    level: DataType::Utf8,

    /// Current concurrency usage at this counter.
    usage: DataType::UInt32,

    /// The configured concurrency limit (null if unlimited).
    concurrency_limit: DataType::UInt32,

    /// The rule pattern that defines the limit (null if unlimited).
    /// Resolved from the rule handle; shows "[removed]" if the rule was
    /// deleted since the counter was created.
    rule_pattern: DataType::Utf8,

    /// Available capacity (limit - usage). Null if unlimited.
    available: DataType::UInt32,

    /// Number of vqueues currently waiting behind this counter.
    num_waiters: DataType::UInt64,
));
