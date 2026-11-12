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

use restate_storage_api::promise_table::{Promise as StoredPromise, PromiseResult, PromiseState};
use restate_types::errors::InvocationError;
use restate_types::identifiers::PartitionKey;
use restate_util_string::ReString;

use crate::selection::domain::Domain;
use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Durable promises of Workflows.
    ///
    /// One row per promise of a workflow instance, with its completion value or failure once completed.
    promise {
        name: "sys_promise",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        sharding_columns { partition_key: PartitionKey, scope: Scope },
        // Unscoped workflows are keyed by their workflow ID. That rule is conditional
        // on a NULL scope, so it prunes shards but is not a sharding column.
        shard_keys: |domains: PromiseDomains| {
            super::state::partition_keys(Domain::all(), domains.scope, domains.service_key)
        },
        row: for<'a> PromiseView<'a>, append: append_promise,
        columns {
            /// Internal column that is used for partitioning the services invocations. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// The scope of the workflow instance, if scoped. NULL for unscoped entries.
            /// Since v1.7.0
            scope: Option<ReString> => DataType::LargeUtf8,
            /// The name of the workflow service.
            service_name: ReString => DataType::LargeUtf8,
            /// The workflow ID.
            service_key: ReString => DataType::LargeUtf8,
            /// The promise key.
            key: ReString => DataType::LargeUtf8,
            /// True if the promise was completed.
            completed: bool => DataType::Boolean,
            /// The completion success, if any.
            completion_success_value: Option<Vec<u8>> => DataType::LargeBinary,
            /// The completion success as UTF-8 string, if any.
            completion_success_value_utf8: Option<ReString> => DataType::LargeUtf8,
            /// The completion failure, if any.
            completion_failure: Option<ReString> => DataType::LargeUtf8,
        }
    }
}

/// A borrowed promise row.
pub struct PromiseView<'a> {
    pub partition_key: PartitionKey,
    pub scope: Option<&'a str>,
    pub service_name: &'a str,
    pub service_key: &'a str,
    pub key: &'a str,
    pub promise: &'a StoredPromise,
}

fn append_promise(builder: &mut PromiseBatchBuilder, promise: PromiseView<'_>) {
    let mut row = builder.row();
    row.partition_key(promise.partition_key);
    if let Some(scope) = promise.scope {
        row.scope(scope);
    }
    row.service_name(promise.service_name);
    row.service_key(promise.service_key);
    row.key(promise.key);
    match &promise.promise.state {
        PromiseState::Completed(result) => {
            row.completed(true);
            match result {
                PromiseResult::Success(value) => {
                    row.completion_success_value(value.as_ref());
                    if row.is_completion_success_value_utf8_defined()
                        && let Ok(value) = std::str::from_utf8(value)
                    {
                        row.completion_success_value_utf8(value);
                    }
                }
                PromiseResult::Failure(code, message, _) => {
                    if row.is_completion_failure_defined() {
                        row.fmt_completion_failure(InvocationError::new(*code, message.clone()))
                    }
                }
            }
        }
        PromiseState::NotCompleted(_) => row.completed(false),
    }
}
