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

define_sort_order!(sys_vqueue_entry_status(partition_key));

define_table!(sys_vqueue_entry_status(
    /// Internal column that is used for partitioning. Can be ignored.
    partition_key: DataType::UInt64,

    /// Identifier of the entry.
    ///
    /// Due to quirks in DataFusion, this should remain `LargeUtf8` to match
    /// `id` in `sys_invocation_status` for dynamic filter pushdown.
    entry_id: DataType::LargeUtf8,

    /// The VQueue Identifier (vq_...).
    vqueue_id: DataType::LargeUtf8,

    /// The stage this entry currently belongs to. Choices are 'inbox', 'running', 'paused',
    /// 'suspended', and 'finished'.
    stage: DataType::LargeUtf8,

    /// The entry processing status. Examples are `new`, `scheduled`, `started`,
    /// `backing-off`, `yielded`, `killed`, `cancelled`, `failed`, and `succeeded`.
    status: DataType::LargeUtf8,

    /// Whether this entry currently holds a lock.
    has_lock: DataType::Boolean,

    /// The next timestamp at which this entry is scheduled for the next transition.
    next_at: TimestampMillisecond,

    /// Sequence number encoded in the entry ordering key.
    sequence_number: DataType::UInt64,

    /// Entry kind (`invocation` or `state-mutation`).
    entry_kind: DataType::LargeUtf8,

    /// Creation timestamp of the entry.
    created_at: TimestampMillisecond,

    /// Timestamp of the latest stage transition.
    transitioned_at: TimestampMillisecond,

    /// Number of times this entry has been moved to the run queue.
    num_attempts: DataType::UInt32,

    /// Number of times this entry has yielded execution due to transient errors.
    num_errors: DataType::UInt32,

    /// Number of times this entry has been moved to the paused stage.
    num_pauses: DataType::UInt32,

    /// Number of times this entry has been moved to the suspended stage.
    num_suspensions: DataType::UInt32,

    /// Number of times this entry has yielded execution.
    num_yields: DataType::UInt32,

    /// Timestamp of the first attempt to run this entry.
    first_attempt_at: TimestampMillisecond,

    /// Timestamp of the latest attempt to run this entry.
    latest_attempt_at: TimestampMillisecond,

    /// The realistic earliest time at which this entry can run its first attempt.
    first_runnable_at: TimestampMillisecond,

    /// If set, the entry's pinned deployment identifier.
    deployment: DataType::LargeUtf8,

    /// If set, the amount of memory in bytes the invocation seems to require on the invoker side.
    needed_memory: DataType::UInt64,

    /// Number of retry attempts recorded in the entry metadata.
    retry_attempts: DataType::UInt32,

    /// Number of retries since the latest stored command.
    retry_count_since_last_stored_command: DataType::UInt32,

    /// Time the latest attempt spent waiting on global invoker capacity.
    latest_attempt_blocked_on_invoker_concurrency: DataType::Duration,

    /// Time the latest attempt spent blocked on user-defined per-vqueue throttling rules.
    latest_attempt_blocked_on_throttling_rules: DataType::Duration,

    /// Time the latest attempt spent blocked on node-level invoker throttling.
    latest_attempt_blocked_on_invoker_throttling: DataType::Duration,

    /// Time the latest attempt spent waiting on the invoker memory pool.
    latest_attempt_blocked_on_invoker_memory: DataType::Duration,

    /// Time the latest attempt spent waiting on user-defined concurrency limits.
    latest_attempt_blocked_on_concurrency_rules: DataType::Duration,

    /// Time the latest attempt spent waiting to acquire a virtual object lock.
    latest_attempt_blocked_on_lock: DataType::Duration,

    /// Time the latest attempt spent blocked on deployment concurrency capacity.
    latest_attempt_blocked_on_deployment_concurrency: DataType::Duration,

    /// Total time spent waiting on global invoker capacity across all attempts.
    total_blocked_on_invoker_concurrency: DataType::Duration,

    /// Total time spent blocked on user-defined per-vqueue throttling rules across all attempts.
    total_blocked_on_throttling_rules: DataType::Duration,

    /// Total time spent blocked on node-level invoker throttling across all attempts.
    total_blocked_on_invoker_throttling: DataType::Duration,

    /// Total time spent waiting on the invoker memory pool across all attempts.
    total_blocked_on_invoker_memory: DataType::Duration,

    /// Total time spent waiting on user-defined concurrency limits across all attempts.
    total_blocked_on_concurrency_rules: DataType::Duration,

    /// Total time spent waiting to acquire a virtual object lock across all attempts.
    total_blocked_on_lock: DataType::Duration,

    /// Total time spent blocked on deployment concurrency capacity across all attempts.
    total_blocked_on_deployment_concurrency: DataType::Duration,
));
