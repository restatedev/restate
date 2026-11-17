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
    /// State of Virtual Objects and Workflows.
    ///
    /// One row per state key of each Virtual Object or Workflow instance.
    state {
        name: "state",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        sharding_columns { partition_key: PartitionKey, scope: Scope },
        // Unscoped objects are keyed by their service key. That rule is conditional
        // on a NULL scope, so it prunes shards but is not a sharding column.
        shard_keys: |domains: StateDomains| partition_keys(Domain::all(), domains.scope, domains.service_key),
        row: for<'a> StateView<'a>, append: append_state,
        columns {
            /// Internal column that is used for partitioning the services invocations. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// The scope of the Virtual Object instance, if scoped. NULL for unscoped entries.
            /// Since v1.7.0
            #[starts_with]
            scope: Option<ReString> => DataType::LargeUtf8,
            /// The name of the invoked service.
            #[starts_with]
            service_name: ReString => DataType::LargeUtf8,
            /// The key of the Virtual Object.
            #[starts_with]
            service_key: ReString => DataType::LargeUtf8,
            /// The `utf8` state key.
            #[starts_with]
            key: Option<ReString> => DataType::LargeUtf8,
            /// The byte length of the state key. If you are writing a query that only needs to know the length,
            /// reading this field will be much more efficient than reading octet_length(key).
            key_length: u64 => DataType::UInt64,
            /// Only contains meaningful values when a service stores state as `utf8`. This is the case for
            /// services that serialize state using JSON (default for Typescript SDK, Java/Kotlin SDK if
            /// using JsonSerdes).
            #[starts_with]
            value_utf8: Option<ReString> => DataType::LargeUtf8,
            /// A binary, uninterpreted representation of the value. You can use the more specific column
            /// `value_utf8` if the value is a string.
            value: Vec<u8> => DataType::LargeBinary,
            /// The byte length of the value. If you are writing a query that only needs to know the length,
            /// reading this field will be much more efficient than reading length(value).
            value_length: u64 => DataType::UInt64,
        }
    }
}

/// The most state keys one alternative of a query may name for V2 to look state
/// up by key instead of reading its objects. Pinning every key column names
/// their combinations: `service_key IN (…300…) AND key IN (…50…)` names 15,000
/// keys. Above this many, that alternative reads its objects instead, with the
/// same rows.
pub const MAX_LOOKUP_KEYS: usize = 10_000;

/// A state key as a query names it: its scope (NULL when unscoped), service
/// name, service key and UTF-8 state key.
pub type StateKeyId = (Option<ReString>, ReString, ReString, ReString);

/// The state keys one alternative of a query names, restricted to `scope`: every
/// combination of its scopes, service names, service keys and state keys, up to
/// [`MAX_LOOKUP_KEYS`] of them. Above that, or if some column isn't pinned to
/// finitely many values, it names every key, so the alternative is read instead.
/// So is one that allows a NULL state key: such a key isn't UTF-8, and no
/// literal names its bytes.
pub fn named_state_keys(
    scope: Domain<Option<ReString>>,
    domains: &StateDomains,
) -> Domain<StateKeyId> {
    let Some(keys) = domains
        .key
        .clone()
        .into_values()
        .and_then(|keys| keys.into_iter().collect::<Option<Vec<_>>>())
    else {
        return Domain::all();
    };
    scope
        .bounded_product(&domains.service_name, MAX_LOOKUP_KEYS)
        .bounded_product(&domains.service_key, MAX_LOOKUP_KEYS)
        .bounded_product(&Domain::values(keys), MAX_LOOKUP_KEYS)
        .map_values(|(((scope, service_name), service_key), key)| {
            (
                scope.clone(),
                service_name.clone(),
                service_key.clone(),
                key.clone(),
            )
        })
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
