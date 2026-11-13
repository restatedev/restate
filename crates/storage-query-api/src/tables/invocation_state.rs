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
            /// Internal column that is used for partitioning the services invocations. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// [Invocation ID](/services/invocation/managing-invocations#invocation-id).
            id: InvocationId => DataType::LargeUtf8,
            /// If true, the invocation is currently in-flight
            in_flight: bool => DataType::Boolean,
            /// The number of invocation attempts since the current leader started executing it. Increments
            /// on start, so a value greater than 1 means a failure occurred. Note: the value is not a
            /// global attempt counter across invocation suspensions and leadership changes.
            retry_count: u64 => DataType::UInt64,
            /// Timestamp indicating the start of the most recent attempt of this invocation.
            last_start_at: MillisSinceEpoch => TimestampMillisecond,
            /// The ID of the service deployment that executed the most recent attempt of this invocation;
            /// this is set before a journal entry is stored, but can change later.
            last_attempt_deployment_id: Option<ReString> => DataType::LargeUtf8,
            /// Server/SDK version, e.g. `restate-sdk-java/1.0.1`
            last_attempt_server: Option<ReString> => DataType::LargeUtf8,
            /// Timestamp indicating the start of the next attempt of this invocation.
            next_retry_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// An error message describing the most recent failed attempt of this invocation, if any.
            last_failure: Option<ReString> => DataType::LargeUtf8,
            /// The error code of the most recent failed attempt of this invocation, if any.
            last_failure_error_code: Option<ReString> => DataType::LargeUtf8,
            /// The index of the command in the journal that caused the failure, if any. It may be out-of-bound
            /// of the currently stored commands in `sys_journal`.
            last_failure_related_command_index: Option<u64> => DataType::UInt64,
            /// The name of the command that caused the failure, if any.
            last_failure_related_command_name: Option<ReString> => DataType::LargeUtf8,
            /// The type of the command that caused the failure, if any. You can check all the
            /// available command types in [`entries.rs`](https://github.com/restatedev/restate/blob/main/crates/types/src/journal_v2/command.rs).
            last_failure_related_command_type: Option<ReString> => DataType::LargeUtf8,
            /// The index of the journal entry that caused the failure, if any. It may be out-of-bound
            /// of the currently stored entries in `sys_journal`.
            /// DEPRECATED: you should not use this field anymore, but last_failure_related_command_index instead.
            last_failure_related_entry_index: Option<u64> => DataType::UInt64,
            /// The name of the journal entry that caused the failure, if any.
            /// DEPRECATED: you should not use this field anymore, but last_failure_related_command_name instead.
            last_failure_related_entry_name: Option<ReString> => DataType::LargeUtf8,
            /// The type of the journal entry that caused the failure, if any. You can check all the
            /// available entry types in [`entries.rs`](https://github.com/restatedev/restate/blob/main/crates/types/src/journal/entries.rs).
            /// DEPRECATED: you should not use this field anymore, but last_failure_related_command_type instead.
            last_failure_related_entry_type: Option<ReString> => DataType::LargeUtf8,
            /// Last known future the SDK was awaiting on, if `in_flight = true`.
            last_awaiting_on_future_json: Option<ReString> => DataType::LargeUtf8,
        }
    }
}
