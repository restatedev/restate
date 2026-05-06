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

// Stage scans run concurrently and merge into a shared builder, so only
// `partition_key` is monotone within a single partition's output stream.
define_sort_order!(sys_vqueues(partition_key));

define_table!(sys_vqueues(
    /// Internal column that is used for partitioning. Can be ignored.
    partition_key: DataType::UInt64,

    /// The VQueue Identifier (vq_...).
    id: DataType::Utf8,

    /// The stage this entry currently belongs to. Choices are 'inbox', 'running', 'paused',
    /// 'suspended', and 'finished'.
    stage: DataType::Utf8,

    /// The entry processing status. Examples are `new`, `scheduled`, `running`,
    /// `backing_off`, `yielded`, `suspended`, `failed`, and `succeeded`.
    status: DataType::Utf8,

    /// Whether this entry currently holds a lock.
    has_lock: DataType::Boolean,

    /// The entry will be eligible to run after this timestamp. Only present for entries
    /// that are in the waiting inbox.
    run_at: TimestampMillisecond,

    /// Sequence number encoded in the queue ordering key.
    sequence_number: DataType::UInt64,

    /// Identifier of the entry.
    entry_id: DataType::Utf8,

    /// Entry kind (`invocation` or `state-mutation`).
    entry_kind: DataType::Utf8,

    /// Creation timestamp of the entry.
    created_at: TimestampMillisecond,

    /// Timestamp of the latest stage transition.
    transitioned_at: TimestampMillisecond,

    /// Number of times this entry has been moved to the run queue.
    num_attempts: DataType::UInt32,

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
    deployment: DataType::Utf8,
));
