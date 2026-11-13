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
            /// Internal column that is used for partitioning the services invocations. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// [Invocation ID](/services/invocation/managing-invocations#invocation-id).
            id: InvocationId => DataType::LargeUtf8,
            /// Canonical invocation ID: the invocation ID followed by the sequence number of the
            /// record that created the invocation, e.g. `inv_..._42`. NULL if the sequence number
            /// is unknown.
            /// Since v1.8.1.
            canonical_id: Option<ReString> => DataType::LargeUtf8,
            /// The VQueue assigned to the the invocation. NULL if invocation was not migrated to vqueues.
            /// Since v1.7.0.
            vqueue_id: Option<ReString> => DataType::LargeUtf8,
            /// Either `inboxed` or `scheduled` or `invoked` or `suspended` or `paused` or `completed`
            status: ReString => DataType::LargeUtf8,
            /// If `status = 'completed'`, this contains either `success` or `failure`
            completion_result: Option<ReString> => DataType::LargeUtf8,
            /// If `status = 'completed' AND completion_result = 'failure'`, this contains the error cause
            completion_failure: Option<ReString> => DataType::LargeUtf8,
            /// Invocation Target. Format for plain services: `ServiceName/HandlerName`, e.g.
            /// `Greeter/greet`. Format for virtual objects/workflows: `VirtualObjectName/Key/HandlerName`,
            /// e.g. `Greeter/Francesco/greet`.
            target: ReString => DataType::LargeUtf8,
            /// The name of the invoked service.
            target_service_name: ReString => DataType::LargeUtf8,
            /// The key of the virtual object or the workflow ID. Null for regular services.
            target_service_key: Option<ReString> => DataType::LargeUtf8,
            /// The invoked handler.
            target_handler_name: ReString => DataType::LargeUtf8,
            /// The service type. Either `service` or `virtual_object` or `workflow`.
            target_service_ty: ReString => DataType::LargeUtf8,
            /// The scope of the invocation for vqueue partitioning, if scoped. NULL for unscoped invocations.
            /// Since v1.7.0.
            scope: Option<ReString> => DataType::LargeUtf8,
            /// The limit key that was used for the invocation. NULL if no limit key was set.
            /// Since v1.7.0.
            limit_key: Option<ReString> => DataType::LargeUtf8,
            /// Idempotency key, if any.
            idempotency_key: Option<ReString> => DataType::LargeUtf8,
            /// Either:
            /// * `ingress` if the invocation was created externally.
            /// * `service` if the invocation was created by another Restate service.
            /// * `subscription` if the invocation was created by a subscription (e.g. Kafka).
            /// * `restart_as_new` if the invocation was created by restarting an old invocation as new.
            /// * `ingress_ingestion` if the invocation was created by the ingestion API.
            invoked_by: ReString => DataType::LargeUtf8,
            /// The caller [Invocation ID](/services/invocation/managing-invocations#invocation-id) if `invoked_by = 'service'`.
            invoked_by_id: Option<ReString> => DataType::LargeUtf8,
            /// The subscription id if `invoked_by = 'subscription'`.
            invoked_by_subscription_id: Option<ReString> => DataType::LargeUtf8,
            /// The name of caller service if `invoked_by = 'service'`.
            invoked_by_service_name: Option<ReString> => DataType::LargeUtf8,
            /// The caller invocation target if `invoked_by = 'service'`.
            invoked_by_target: Option<ReString> => DataType::LargeUtf8,
            /// The original invocation id if `invoked_by = 'restart_as_new'`.
            restarted_from: Option<ReString> => DataType::LargeUtf8,
            /// The ID of the service deployment that started processing this invocation, and will continue
            /// to do so (e.g. for retries). This gets set after the first journal entry has been stored for
            /// this invocation.
            pinned_deployment_id: Option<ReString> => DataType::LargeUtf8,
            /// The negotiated protocol version used for this invocation.
            /// This gets set after the first journal entry has been stored for this invocation.
            pinned_service_protocol_version: Option<u32> => DataType::UInt32,
            /// The ID of the trace that is assigned to this invocation. Only relevant when tracing is
            /// enabled.
            trace_id: Option<ReString> => DataType::LargeUtf8,
            /// The number of journal entries durably logged for this invocation.
            journal_size: Option<u32> => DataType::UInt32,
            /// The number of commands generated by this invocation, stored in the journal.
            /// Only relevant when pinned_service_protocol_version >= 4.
            journal_commands_size: Option<u32> => DataType::UInt32,
            /// Timestamp indicating the start of this invocation.
            created_at: MillisSinceEpoch => TimestampMillisecond,
            /// restate-server version in use when this invocation was created.
            created_using_restate_version: ReString => DataType::LargeUtf8,
            /// Timestamp indicating the last invocation status transition. For example, last time the
            /// status changed from `invoked` to `suspended`.
            modified_at: MillisSinceEpoch => TimestampMillisecond,
            /// Timestamp indicating when the invocation was inboxed, if ever.
            inboxed_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Timestamp indicating when the invocation was scheduled, if ever.
            scheduled_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// If the invocation was scheduled, indicates the timestamp when the invocation should start.
            scheduled_start_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Timestamp indicating when the invocation first transitioned to running, if ever.
            running_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Timestamp indicating when the invocation was completed, if ever.
            completed_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// For how long the metadata of this invocation, including its result, is retained after completion.
            completion_retention: i64 => DataType::Duration,
            /// For how long the journal is retained after completion.
            journal_retention: i64 => DataType::Duration,
            /// List of completion ids the invocation is awaiting on, if `status = suspended`.
            /// DEPRECATED: use `suspended_waiting_future_json` instead.
            suspended_waiting_for_completions: Option<Vec<u32>> => UInt32List,
            /// List of signals the invocation is awaiting on, if `status = suspended`.
            /// DEPRECATED: use `suspended_waiting_future_json` instead.
            suspended_waiting_for_signals: Option<Vec<u32>> => UInt32List,
            /// Future tree the invocation is suspended on, if `status = suspended`.
            suspended_waiting_future_json: Option<ReString> => DataType::LargeUtf8,
        }
    }
}
