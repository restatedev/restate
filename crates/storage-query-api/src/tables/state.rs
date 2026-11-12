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

use restate_types::identifiers::{PartitionKey, partitioner::HashPartitioner};
use restate_util_string::ReString;

use crate::selection::domain::Domain;
use crate::{ShardingScheme, define_schema};

define_schema! {
    /// User state of virtual objects and workflows.
    state {
        name: "state",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        sharding_columns { partition_key: PartitionKey, scope: Scope },
        // Unscoped objects are keyed by their service key. That rule is conditional
        // on a NULL scope, so it prunes shards but is not a sharding column.
        shard_keys: |domains: StateDomains| partition_keys(Domain::all(), domains.scope, domains.service_key),
        row: for<'a> StateView<'a>, append: append_state,
        columns {
            /// Internal routing key.
            partition_key: PartitionKey => DataType::UInt64,
            /// Scope of the object, or NULL for unscoped state.
            #[starts_with]
            scope: Option<ReString> => DataType::LargeUtf8,
            /// Service name.
            #[starts_with]
            service_name: ReString => DataType::LargeUtf8,
            /// Virtual-object or workflow key.
            #[starts_with]
            service_key: ReString => DataType::LargeUtf8,
            /// UTF-8 state key, or NULL for a legacy non-UTF-8 key.
            #[starts_with]
            key: Option<ReString> => DataType::LargeUtf8,
            /// State-key byte length.
            key_length: u64 => DataType::UInt64,
            /// UTF-8 state value, or NULL for non-UTF-8 data.
            #[starts_with]
            value_utf8: Option<ReString> => DataType::LargeUtf8,
            /// Uninterpreted value bytes.
            value: Vec<u8> => DataType::LargeBinary,
            /// Value byte length.
            value_length: u64 => DataType::UInt64,
        }
    }
}

/// Candidate shard keys for both physical layouts, preserving NULL-scope semantics:
/// a scoped row hashes its scope, and an unscoped row hashes its service key.
pub fn partition_keys(
    keys: Domain<PartitionKey>,
    scope: Domain<Option<ReString>>,
    service_key: Domain<ReString>,
) -> Domain<PartitionKey> {
    let unscoped = if scope.contains(&None) {
        service_key.map_values(|key| HashPartitioner::compute_partition_key(key))
    } else {
        Domain::empty()
    };
    let non_null = scope.intersect(Domain::excluding_values([None]));
    let scoped =
        non_null.try_map_values(|scope| scope.as_ref().map(HashPartitioner::compute_partition_key));
    keys.intersect(scoped.union(unscoped))
}

/// A borrowed logical row; binary values need no intermediate allocation.
pub struct StateView<'a> {
    pub partition_key: PartitionKey,
    pub scope: Option<&'a str>,
    pub service_name: &'a str,
    pub service_key: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

fn append_state(builder: &mut StateBatchBuilder, value: StateView<'_>) {
    let mut row = builder.row();
    row.partition_key(value.partition_key);
    if let Some(scope) = value.scope {
        row.scope(scope);
    }
    if row.is_service_name_defined() {
        row.service_name(value.service_name);
    }
    if row.is_service_key_defined() {
        row.service_key(value.service_key);
    }
    if row.is_key_defined()
        && let Ok(key) = std::str::from_utf8(value.key)
    {
        row.key(key);
    }
    if row.is_value_utf8_defined()
        && let Ok(value) = std::str::from_utf8(value.value)
    {
        row.value_utf8(value);
    }
    // note: this value can be quite large, we don't want to place it in the batch
    // if it's not asked for in projections.
    if row.is_value_defined() {
        row.value(value.value);
    }

    row.key_length(value.key.len() as u64);
    row.value_length(value.value.len() as u64);
}
