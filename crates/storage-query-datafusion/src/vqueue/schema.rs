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

define_sort_order!(sys_vqueues(
    partition_key,
    id,
    stage,
    run_at,
    sequence_number
));

define_table!(sys_vqueues(
    /// Internal column that is used for partitioning. Can be ignored.
    partition_key: DataType::UInt64,

    /// The VQueue Identifier (vq_...).
    id: DataType::Utf8,

    /// The stage this entry currently belongs to. Choices are 'inbox', 'run', 'paused',
    /// 'suspended', and 'finished'.
    stage: DataType::Utf8,

    /// Whether this entry currently holds a lock.
    has_lock: DataType::Boolean,

    /// The entry will be eligible to run after this timestamp. Only present for entries
    /// that are in the waiting inbox.
    run_after: TimestampMillisecond,

    /// Sequence number encoded in the queue ordering key.
    sequence_number: DataType::UInt64,

    /// Identifier of the entry.
    entry_id: DataType::Utf8,

    /// Entry kind (`invocation` or `state-mutation`).
    entry_kind: DataType::Utf8,

    /// The realistic earliest time at which this entry can run its first attempt (aka. start)
    first_runnable_at: TimestampMillisecond,

    /// Timestamp of the first attempt to run this entry.
    first_started_at: TimestampMillisecond,

    /// If set, the entry's pinned deployment identifier.
    deployment: DataType::Utf8,
));
