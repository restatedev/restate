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

define_sort_order!(sys_vqueue_meta(partition_key, id));

define_table!(sys_vqueue_meta(
    /// Internal column that is used for partitioning. Can be ignored.
    partition_key: DataType::UInt64,

    /// The VQueue Identifier (vq_...)
    id: DataType::Utf8,

    /// Whether this vqueue is active or not. An active vqueue
    /// is a vqueue that is not paused, and has non-finished
    /// items.
    is_active: DataType::Boolean,

    /// Whether this vqueue is paused.
    queue_is_paused: DataType::Boolean,

    /// Service name linked to this vqueue
    service_name: DataType::Utf8,

    /// The scope of this vqueue.
    scope: DataType::Utf8,

    /// The name of the limit-key assigned to this vqueue
    limit_key: DataType::Utf8,

    /// The name of the lock (in the format of `service/key`)
    ///
    /// This is only set if this is a vqueue for a virtual object.
    lock_name: DataType::Utf8,

    /// When was this vqueue first created
    created_at: TimestampMillisecond,

    /// Last timestamp an entry moved into Inbox.
    ///
    /// This covers items enqueued for the first time only.
    last_enqueued_at: TimestampMillisecond,

    /// Last timestamp an entry first transitioned to Run.
    last_start_at: TimestampMillisecond,

    /// Last timestamp an entry transitioned to Run.
    last_attempt_at: TimestampMillisecond,

    /// Last timestamp an entry transitioned to Finished.
    last_finish_at: TimestampMillisecond,

    /// Exponential moving average (EMA) of first-attempt queue wait time.
    avg_queue_duration: DataType::Duration,

    /// Exponential moving average (EMA) of how long entries stayed in inbox.
    avg_inbox_duration: DataType::Duration,

    /// Exponential moving average (EMA) of how long entries stayed running.
    avg_run_duration: DataType::Duration,

    /// Exponential moving average (EMA) of how long entries stayed suspended.
    avg_suspension_duration: DataType::Duration,

    /// Exponential moving average (EMA) of end-to-end entry lifetime from first-runnable time to completion.
    /// Note that this only tracks entries that were not killed/cancelled or failed/paused.
    avg_end_to_end_duration: DataType::Duration,

    /// Exponential moving average (EMA) of time the head item spent blocked on
    /// user-defined concurrency rules before entering Running. Sampled on every
    /// Inbox → Running transition (every run attempt, including retries).
    avg_blocked_on_concurrency_rules: DataType::Duration,

    /// Exponential moving average (EMA) of time the head item spent blocked on
    /// node-level invoker throttling before entering Running. Sampled on every
    /// Inbox → Running transition (every run attempt, including retries).
    avg_blocked_on_invoker_throttling: DataType::Duration,

    /// The number of entries that are in the inbox. The inbox is the priority
    /// queue that the scheduler uses to choose which entries to run next.
    num_inbox: DataType::UInt64,

    /// The number of entries that are currently running.
    num_running: DataType::UInt64,

    /// The number of entries that are suspended.
    num_suspended: DataType::UInt64,

    /// The number of entries that are paused.
    num_paused: DataType::UInt64,

    /// The number of entries that have finished processing and are pending
    /// deletion or archival.
    num_finished: DataType::UInt64,

));
