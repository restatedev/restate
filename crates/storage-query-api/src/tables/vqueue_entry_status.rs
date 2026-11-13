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
use strum::VariantArray;

use restate_storage_api::vqueue_table::{Stage, Status};
use restate_types::identifiers::{BaseEntryId, CanonicalEntryId, PartitionKey};
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryKind, VQueueId};
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Status of every vqueue entry.
    ///
    /// One row per entry (an invocation or a state mutation), with its stage, its attempts and how long it was blocked on each resource.
    vqueue_entry_status {
        name: "sys_vqueue_entry_status",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        sharding_columns {
            partition_key: PartitionKey,
            entry_id: BaseEntryId,
            vqueue_id: VQueueId,
            canonical_id: CanonicalEntryId,
        },
        primary_key: entry_id,
        columns {
            /// Internal column that is used for partitioning. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// Resource identifier of the entry: an invocation ID or a state mutation
            /// ID, without its sequence number. Use `canonical_id` to distinguish
            /// incarnations of the same resource.
            ///
            /// Due to quirks in DataFusion, this should remain `LargeUtf8` to match
            /// `id` in `sys_invocation_status` for dynamic filter pushdown.
            entry_id: BaseEntryId => DataType::LargeUtf8,
            /// The VQueue Identifier (vq_...).
            vqueue_id: VQueueId => DataType::LargeUtf8,
            /// The stage this entry currently belongs to. Choices are 'inbox', 'running', 'paused',
            /// 'suspended', and 'finished'.
            #[values(Stage::VARIANTS.iter().copied())]
            #[starts_with]
            stage: Stage => DataType::LargeUtf8,
            /// The entry processing status. Examples are `new`, `scheduled`, `started`,
            /// `backing-off`, `yielded`, `killed`, `cancelled`, `failed`, and `succeeded`.
            #[values(Status::VARIANTS.iter().copied())]
            status: Status => DataType::LargeUtf8,
            /// Whether this entry currently holds a lock.
            has_lock: bool => DataType::Boolean,
            /// The next timestamp at which this entry is scheduled for the next transition.
            next_at: MillisSinceEpoch => TimestampMillisecond,
            /// Sequence number encoded in the entry ordering key.
            sequence_number: u64 => DataType::UInt64,
            /// Entry kind (`invocation` or `state-mutation`).
            #[values(EntryKind::VARIANTS.iter().copied())]
            entry_kind: EntryKind => DataType::LargeUtf8,
            /// Creation timestamp of the entry.
            created_at: MillisSinceEpoch => TimestampMillisecond,
            /// Timestamp of the latest stage transition.
            transitioned_at: MillisSinceEpoch => TimestampMillisecond,
            /// Number of times this entry has been moved to the run queue.
            num_attempts: u32 => DataType::UInt32,
            /// Number of times this entry has yielded execution due to transient errors.
            num_errors: u32 => DataType::UInt32,
            /// Number of times this entry has been moved to the paused stage.
            num_pauses: u32 => DataType::UInt32,
            /// Number of times this entry has been moved to the suspended stage.
            num_suspensions: u32 => DataType::UInt32,
            /// Number of times this entry has yielded execution.
            num_yields: u32 => DataType::UInt32,
            /// Timestamp of the first attempt to run this entry.
            first_attempt_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Timestamp of the latest attempt to run this entry.
            latest_attempt_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// The realistic earliest time at which this entry can run its first attempt.
            first_runnable_at: MillisSinceEpoch => TimestampMillisecond,
            /// If set, the entry's pinned deployment identifier.
            deployment: Option<ReString> => DataType::LargeUtf8,
            /// If set, the amount of memory in bytes the invocation seems to require on the invoker side.
            needed_memory: Option<u64> => DataType::UInt64,
            /// Number of retry attempts recorded in the entry metadata.
            retry_attempts: u32 => DataType::UInt32,
            /// Number of retries since the latest stored command.
            retry_count_since_last_stored_command: u32 => DataType::UInt32,
            /// Time the latest attempt spent waiting on global invoker capacity.
            latest_attempt_blocked_on_invoker_concurrency: i64 => DataType::Duration,
            /// Time the latest attempt spent blocked on user-defined per-vqueue throttling rules.
            latest_attempt_blocked_on_throttling_rules: i64 => DataType::Duration,
            /// Time the latest attempt spent blocked on node-level invoker throttling.
            latest_attempt_blocked_on_invoker_throttling: i64 => DataType::Duration,
            /// Time the latest attempt spent waiting on the invoker memory pool.
            latest_attempt_blocked_on_invoker_memory: i64 => DataType::Duration,
            /// Time the latest attempt spent waiting on user-defined concurrency limits.
            latest_attempt_blocked_on_concurrency_rules: i64 => DataType::Duration,
            /// Time the latest attempt spent waiting to acquire a virtual object lock.
            latest_attempt_blocked_on_lock: i64 => DataType::Duration,
            /// Time the latest attempt spent blocked on deployment concurrency capacity.
            latest_attempt_blocked_on_deployment_concurrency: i64 => DataType::Duration,
            /// Total time spent waiting on global invoker capacity across all attempts.
            total_blocked_on_invoker_concurrency: i64 => DataType::Duration,
            /// Total time spent blocked on user-defined per-vqueue throttling rules across all attempts.
            total_blocked_on_throttling_rules: i64 => DataType::Duration,
            /// Total time spent blocked on node-level invoker throttling across all attempts.
            total_blocked_on_invoker_throttling: i64 => DataType::Duration,
            /// Total time spent waiting on the invoker memory pool across all attempts.
            total_blocked_on_invoker_memory: i64 => DataType::Duration,
            /// Total time spent waiting on user-defined concurrency limits across all attempts.
            total_blocked_on_concurrency_rules: i64 => DataType::Duration,
            /// Total time spent waiting to acquire a virtual object lock across all attempts.
            total_blocked_on_lock: i64 => DataType::Duration,
            /// Total time spent blocked on deployment concurrency capacity across all attempts.
            total_blocked_on_deployment_concurrency: i64 => DataType::Duration,
            /// Canonical identifier of the entry: its resource ID followed by `_` and
            /// its sequence number.
            canonical_id: CanonicalEntryId => DataType::LargeUtf8,
        }
    }
}
