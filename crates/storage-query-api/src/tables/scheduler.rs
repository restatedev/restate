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

use restate_types::identifiers::{BaseEntryId, PartitionKey};
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Scheduler state of each vqueue.
    ///
    /// Shows whether a vqueue is dormant, blocked or scheduled, what its head entry is waiting on, and how long it waited on each resource.
    scheduler {
        name: "sys_scheduler",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: true },
        sharding_columns { partition_key: PartitionKey, id: VQueueId, head_entry_id: BaseEntryId },
        columns {
            /// Internal column that is used for partitioning. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// Identifier of the scheduled vqueue (vq_...).
            id: VQueueId => DataType::LargeUtf8,
            /// Number of entries currently waiting in the inbox stage.
            num_inbox: u64 => DataType::UInt64,
            /// High-level scheduler state (dormant, blocked, scheduled, ...).
            status: ReString => DataType::LargeUtf8,
            /// Identifier of the head entry if the scheduler has already advanced to it.
            /// Null when the head has not yet been materialized
            head_entry_id: Option<BaseEntryId> => DataType::LargeUtf8,
            /// Earliest time when the head entry becomes runnable.
            ///
            /// Set only when status is `Scheduled`.
            scheduled_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// The current resource blocking the queue from dequeuing.
            blocked_on: Option<ReString> => DataType::LargeUtf8,
            /// Detailed blocking resource when status is `blocked`.
            blocked_on_json: Option<ReString> => DataType::LargeUtf8,
            /// Time the head entry spent waiting on global invoker concurrency.
            invoker_concurrency_block_duration: i64 => DataType::Duration,
            /// Time the head entry spent waiting on user-defined per-vqueue throttling rules.
            throttling_rules_block_duration: i64 => DataType::Duration,
            /// Time the head entry spent waiting on node-level invoker throttling.
            invoker_throttling_block_duration: i64 => DataType::Duration,
            /// Time the head entry spent waiting on invoker memory pool capacity.
            invoker_memory_block_duration: i64 => DataType::Duration,
            /// Time the head entry spent waiting on user-defined concurrency rules.
            concurrency_rules_block_duration: i64 => DataType::Duration,
            /// Time the head entry spent waiting on virtual-object locks.
            lock_block_duration: i64 => DataType::Duration,
            /// Time the head entry spent waiting on deployment-level concurrency capacity.
            deployment_concurrency_block_duration: i64 => DataType::Duration,
        }
    }
}
