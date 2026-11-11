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

use restate_types::identifiers::{InvocationId, PartitionKey};
use restate_types::time::MillisSinceEpoch;
use restate_util_string::ReString;

use crate::selection::domain::Domain;
use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Persisted invocation status and metadata.
    invocation_status {
        name: "sys_invocation_status",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        sharding_columns { partition_key: PartitionKey, id: InvocationId, scope: Scope },
        // Unscoped invocations are keyed by their service key. That rule is
        // conditional on a NULL scope, so it prunes shards but is not a sharding column.
        shard_keys: |domains: InvocationStatusDomains| {
            super::state::partition_keys(
                Domain::all(),
                domains.scope,
                domains.target_service_key.try_map_values(Clone::clone),
            )
        },
        columns {
            /// Internal partitioning key.
            partition_key: PartitionKey => DataType::UInt64,
            /// Invocation ID.
            id: InvocationId => DataType::LargeUtf8,
            /// Invocation ID and creation sequence; null when the sequence is unknown.
            canonical_id: Option<ReString> => DataType::LargeUtf8,
            /// Assigned virtual queue; null before migration to vqueues.
            vqueue_id: Option<ReString> => DataType::LargeUtf8,
            /// Persisted state: inboxed, scheduled, invoked, suspended, paused, or completed.
            status: ReString => DataType::LargeUtf8,
            /// Success or failure for completed invocations.
            completion_result: Option<ReString> => DataType::LargeUtf8,
            /// Completed invocation failure cause.
            completion_failure: Option<ReString> => DataType::LargeUtf8,
            /// Invocation target, including service, optional key, and handler.
            target: ReString => DataType::LargeUtf8,
            /// Invoked service name.
            target_service_name: ReString => DataType::LargeUtf8,
            /// Virtual-object key or workflow ID; null for regular services.
            target_service_key: Option<ReString> => DataType::LargeUtf8,
            /// Invoked handler name.
            target_handler_name: ReString => DataType::LargeUtf8,
            /// Service type: service, virtual_object, or workflow.
            target_service_ty: ReString => DataType::LargeUtf8,
            /// Invocation scope, when scoped.
            scope: Option<ReString> => DataType::LargeUtf8,
            /// Invocation limit key, when set.
            limit_key: Option<ReString> => DataType::LargeUtf8,
            /// Idempotency key, when set.
            idempotency_key: Option<ReString> => DataType::LargeUtf8,
            /// Source of the invocation.
            invoked_by: ReString => DataType::LargeUtf8,
            /// Caller invocation ID for service calls.
            invoked_by_id: Option<ReString> => DataType::LargeUtf8,
            /// Subscription ID for subscription-triggered calls.
            invoked_by_subscription_id: Option<ReString> => DataType::LargeUtf8,
            /// Caller service name for service calls.
            invoked_by_service_name: Option<ReString> => DataType::LargeUtf8,
            /// Caller invocation target for service calls.
            invoked_by_target: Option<ReString> => DataType::LargeUtf8,
            /// Original invocation ID for restart-as-new calls.
            restarted_from: Option<ReString> => DataType::LargeUtf8,
            /// Deployment pinned after the first journal entry is stored.
            pinned_deployment_id: Option<ReString> => DataType::LargeUtf8,
            /// Negotiated service protocol version.
            pinned_service_protocol_version: Option<u32> => DataType::UInt32,
            /// Invocation trace ID, when tracing is enabled.
            trace_id: Option<ReString> => DataType::LargeUtf8,
            /// Number of durable journal entries.
            journal_size: Option<u32> => DataType::UInt32,
            /// Number of journal commands for protocol version 4 and later.
            journal_commands_size: Option<u32> => DataType::UInt32,
            /// Invocation creation time.
            created_at: MillisSinceEpoch => TimestampMillisecond,
            /// Restate version used to create the invocation.
            created_using_restate_version: ReString => DataType::LargeUtf8,
            /// Most recent persisted status transition time.
            modified_at: MillisSinceEpoch => TimestampMillisecond,
            /// Time of the inboxed transition, if any.
            inboxed_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Time of the scheduled transition, if any.
            scheduled_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Requested execution time for a scheduled invocation.
            scheduled_start_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Time of the first running transition, if any.
            running_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Completion time, if any.
            completed_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Retention of invocation metadata and results after completion.
            completion_retention: i64 => DataType::Duration,
            /// Retention of the journal after completion.
            journal_retention: i64 => DataType::Duration,
            /// Deprecated: awaited completion IDs; use suspended_waiting_future_json.
            suspended_waiting_for_completions: Option<Vec<u32>> => UInt32List,
            /// Deprecated: awaited signal indexes; use suspended_waiting_future_json.
            suspended_waiting_for_signals: Option<Vec<u32>> => UInt32List,
            /// Future tree on which the invocation is suspended.
            suspended_waiting_future_json: Option<ReString> => DataType::LargeUtf8,
        }
    }
}
