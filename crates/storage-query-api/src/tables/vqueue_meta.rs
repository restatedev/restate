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

use restate_storage_api::vqueue_table::metadata::VQueueMetaRef;
use restate_types::identifiers::PartitionKey;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Metadata and statistics of each vqueue.
    ///
    /// Number of entries per stage, latest activity timestamps, and moving averages of wait, run and blocking times.
    vqueue_meta {
        name: "sys_vqueue_meta",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        // A scoped vqueue's partition key is its scope's hash.
        sharding_columns { partition_key: PartitionKey, id: VQueueId, scope: Scope },
        row: for<'a> VQueueMetaView<'a>, append: append_vqueue_meta,
        columns {
            /// Internal column that is used for partitioning. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// The VQueue Identifier (vq_...)
            id: VQueueId => DataType::LargeUtf8,
            /// Whether this vqueue is active or not. An active vqueue
            /// is a vqueue that is not paused, and has non-finished
            /// items.
            is_active: bool => DataType::Boolean,
            /// Whether this vqueue is paused.
            queue_is_paused: bool => DataType::Boolean,
            /// Service name linked to this vqueue
            service_name: Option<ReString> => DataType::LargeUtf8,
            /// The scope of this vqueue.
            scope: Option<ReString> => DataType::LargeUtf8,
            /// The name of the limit-key assigned to this vqueue
            limit_key: Option<ReString> => DataType::LargeUtf8,
            /// The name of the lock (in the format of `service/key`)
            ///
            /// This is only set if this is a vqueue for a virtual object.
            lock_name: Option<ReString> => DataType::LargeUtf8,
            /// When was this vqueue first created
            created_at: MillisSinceEpoch => TimestampMillisecond,
            /// Last timestamp an entry moved into Inbox.
            ///
            /// This covers items enqueued for the first time only.
            last_enqueued_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Last timestamp an entry first transitioned to Run.
            last_start_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Last timestamp an entry transitioned to Run.
            last_attempt_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Last timestamp an entry transitioned to Finished.
            last_finish_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Exponential moving average (EMA) of first-attempt queue wait time.
            avg_queue_duration: i64 => DataType::Duration,
            /// Exponential moving average (EMA) of how long entries stayed in inbox.
            avg_inbox_duration: i64 => DataType::Duration,
            /// Exponential moving average (EMA) of how long entries stayed running.
            avg_run_duration: i64 => DataType::Duration,
            /// Exponential moving average (EMA) of how long entries stayed suspended.
            avg_suspension_duration: i64 => DataType::Duration,
            /// Exponential moving average (EMA) of end-to-end entry lifetime from first-runnable time to completion.
            /// Note that this only tracks entries that were not killed/cancelled or failed/paused.
            avg_end_to_end_duration: i64 => DataType::Duration,
            /// Exponential moving average (EMA) of time the head item spent blocked on
            /// user-defined concurrency rules before entering Running. Sampled on every
            /// Inbox → Running transition (every run attempt, including retries).
            avg_blocked_on_concurrency_rules: i64 => DataType::Duration,
            /// Exponential moving average (EMA) of time the head item spent blocked on
            /// node-level invoker concurrency tokens before entering Running.
            avg_blocked_on_invoker_concurrency: i64 => DataType::Duration,
            /// Exponential moving average (EMA) of time the head item spent blocked on
            /// node-level invoker throttling before entering Running. Sampled on every
            /// Inbox → Running transition (every run attempt, including retries).
            avg_blocked_on_invoker_throttling: i64 => DataType::Duration,
            /// Exponential moving average (EMA) of time the head item spent blocked on
            /// a virtual object lock before entering Running.
            avg_blocked_on_lock: i64 => DataType::Duration,
            /// The number of entries that are in the inbox. The inbox is the priority
            /// queue that the scheduler uses to choose which entries to run next.
            num_inbox: u64 => DataType::UInt64,
            /// The number of entries that are currently running.
            num_running: u64 => DataType::UInt64,
            /// The number of entries that are suspended.
            num_suspended: u64 => DataType::UInt64,
            /// The number of entries that are paused.
            num_paused: u64 => DataType::UInt64,
            /// The number of entries that have finished processing and are pending
            /// deletion or archival.
            num_finished: u64 => DataType::UInt64,
        }
    }
}

/// A borrowed vqueue metadata row.
pub struct VQueueMetaView<'a> {
    pub id: &'a VQueueId,
    pub meta: &'a VQueueMetaRef<'a>,
}

fn append_vqueue_meta(builder: &mut VqueueMetaBatchBuilder, view: VQueueMetaView<'_>) {
    let (id, meta) = (view.id, view.meta);
    let stats = &meta.stats;
    let millis = |time: MillisSinceEpoch| time.as_u64() as i64;
    let mut row = builder.row();
    row.partition_key(id.partition_key());
    if row.is_id_defined() {
        row.fmt_id(id);
    }
    if row.is_scope_defined()
        && let Some(scope) = meta.scope
    {
        row.scope(scope);
    }
    if row.is_service_name_defined()
        && let Some(service_name) = meta.service_name()
    {
        row.service_name(service_name);
    }
    if row.is_is_active_defined() {
        row.is_active(meta.is_active());
    }
    if row.is_queue_is_paused_defined() {
        row.queue_is_paused(meta.queue_is_paused);
    }
    if row.is_limit_key_defined() && !meta.limit_key.is_none() {
        row.fmt_limit_key(&meta.limit_key);
    }
    if row.is_lock_name_defined()
        && let Some(lock_name) = meta.lock_name()
    {
        row.fmt_lock_name(lock_name);
    }
    if row.is_created_at_defined() {
        row.created_at(millis(stats.created_at().to_unix_millis()));
    }
    if let Some(time) = stats.last_enqueued_at() {
        row.last_enqueued_at(millis(time.to_unix_millis()));
    }
    if let Some(time) = stats.last_start_at() {
        row.last_start_at(millis(time.to_unix_millis()));
    }
    if let Some(time) = stats.last_attempt_at() {
        row.last_attempt_at(millis(time.to_unix_millis()));
    }
    if let Some(time) = stats.last_finish_at() {
        row.last_finish_at(millis(time.to_unix_millis()));
    }
    row.avg_queue_duration(stats.avg_queue_duration_ms() as i64);
    row.avg_inbox_duration(stats.avg_inbox_duration_ms() as i64);
    row.avg_run_duration(stats.avg_run_duration_ms() as i64);
    row.avg_suspension_duration(stats.avg_suspension_duration_ms() as i64);
    row.avg_end_to_end_duration(stats.avg_end_to_end_duration_ms() as i64);
    let wait = stats.avg_wait_stats();
    row.avg_blocked_on_lock(wait.blocked_on_lock_ms as i64);
    row.avg_blocked_on_concurrency_rules(wait.blocked_on_concurrency_rules_ms as i64);
    row.avg_blocked_on_invoker_concurrency(wait.blocked_on_invoker_concurrency_ms as i64);
    row.avg_blocked_on_invoker_throttling(wait.blocked_on_invoker_throttling_ms as i64);
    row.num_inbox(stats.num_inbox());
    row.num_running(stats.num_running());
    row.num_suspended(stats.num_suspended());
    row.num_paused(stats.num_paused());
    row.num_finished(stats.num_finished());
}
