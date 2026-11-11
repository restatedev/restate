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

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Live invocation attempt state reported by the partition leader's invoker.
    invocation_state {
        name: "sys_invocation_state",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: true },
        sharding_columns { partition_key: PartitionKey, id: InvocationId },
        columns {
            /// Internal partitioning key.
            partition_key: PartitionKey => DataType::UInt64,
            /// Invocation ID.
            id: InvocationId => DataType::LargeUtf8,
            /// Whether the invocation is currently in flight.
            in_flight: bool => DataType::Boolean,
            /// Number of attempts since this leader started executing the invocation.
            retry_count: u64 => DataType::UInt64,
            /// Start time of the most recent attempt.
            last_start_at: MillisSinceEpoch => TimestampMillisecond,
            /// Deployment selected for the most recent attempt.
            last_attempt_deployment_id: Option<ReString> => DataType::LargeUtf8,
            /// Server or SDK version used for the most recent attempt.
            last_attempt_server: Option<ReString> => DataType::LargeUtf8,
            /// Start time of the next attempt.
            next_retry_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Most recent failure message.
            last_failure: Option<ReString> => DataType::LargeUtf8,
            /// Most recent failure's documentation error code.
            last_failure_error_code: Option<ReString> => DataType::LargeUtf8,
            /// Index of the command related to the failure.
            last_failure_related_command_index: Option<u64> => DataType::UInt64,
            /// Name of the command related to the failure.
            last_failure_related_command_name: Option<ReString> => DataType::LargeUtf8,
            /// Type of the command related to the failure.
            last_failure_related_command_type: Option<ReString> => DataType::LargeUtf8,
            /// Deprecated: index of the entry related to the failure.
            last_failure_related_entry_index: Option<u64> => DataType::UInt64,
            /// Deprecated: name of the entry related to the failure.
            last_failure_related_entry_name: Option<ReString> => DataType::LargeUtf8,
            /// Deprecated: type of the entry related to the failure.
            last_failure_related_entry_type: Option<ReString> => DataType::LargeUtf8,
            /// Last known future the SDK was awaiting.
            last_awaiting_on_future_json: Option<ReString> => DataType::LargeUtf8,
        }
    }
}
