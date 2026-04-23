// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

define_sort_order!(sys_scheduler(partition_key, id));

define_table!(sys_scheduler(
    /// Internal column that is used for partitioning. Can be ignored.
    partition_key: DataType::UInt64,

    /// Identifier of the scheduled vqueue (vq_...).
    id: DataType::Utf8,

    /// Number of entries currently waiting in the inbox stage.
    num_inbox: DataType::UInt64,

    /// High-level scheduler state (Dormant, Ready, BlockedOn, ...).
    status: DataType::Utf8,

    /// Identifier of the head entry if the scheduler has already advanced to it.
    /// Null when the head has not yet been materialized
    head_entry_id: DataType::Utf8,

    /// Earliest time when the head entry becomes runnable.
    ///
    /// Set only when status is `Scheduled`.
    scheduled_at: TimestampMillisecond,

    /// Detailed blocking resource when status is `BlockedOn`.
    blocked_on: DataType::Utf8,

    /// Time the head entry spent waiting on global invoker concurrency.
    invoker_concurrency_block_duration: DataType::Duration,

    /// Time the head entry spent waiting on user-defined per-vqueue throttling rules.
    throttling_rules_block_duration: DataType::Duration,

    /// Time the head entry spent waiting on node-level invoker throttling.
    invoker_throttling_block_duration: DataType::Duration,

    /// Time the head entry spent waiting on invoker memory pool capacity.
    invoker_memory_block_duration: DataType::Duration,

    /// Time the head entry spent waiting on user-defined concurrency rules.
    concurrency_rules_block_duration: DataType::Duration,

    /// Time the head entry spent waiting on virtual-object locks.
    lock_block_duration: DataType::Duration,

    /// Time the head entry spent waiting on deployment-level concurrency capacity.
    deployment_concurrency_block_duration: DataType::Duration,
));
