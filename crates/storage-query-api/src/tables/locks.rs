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

use restate_storage_api::lock_table::{AcquiredBy, LockState};
use restate_types::identifiers::{BaseEntryId, PartitionKey};
use restate_types::time::MillisSinceEpoch;
use restate_types::{LockName, Scope};
use restate_util_string::ReString;

use crate::selection::domain::Domain;
use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Locks currently held on Virtual Object keys.
    ///
    /// One row per lock, with when it was acquired and the invocation (or other operation) holding it.
    locks {
        name: "sys_locks",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        sharding_columns { partition_key: PartitionKey, scope: Scope },
        // An unscoped lock is keyed by its lock name's key, and a holder's entry ID
        // carries the lock's partition key. Both depend on the value's form, so
        // they prune shards but are not sharding columns.
        shard_keys: |domains: LocksDomains| {
            partition_keys(Domain::all(), domains.scope, domains.lock_name, domains.acquired_by)
        },
        row: for<'a> LockView<'a>, append: append_lock,
        columns {
            /// Internal column that is used for partitioning. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// The scope of this lock. Only present if this is a scoped lock (i.e. a lock of a scoped virtual
            /// object).
            scope: Option<ReString> => DataType::LargeUtf8,
            /// The name of the lock (in the format of `service/key`)
            lock_name: ReString => DataType::LargeUtf8,
            /// Timestamp of lock acquisition
            acquired_at: MillisSinceEpoch => TimestampMillisecond,
            /// The invocation (or other operation) that acquired this lock.
            acquired_by: Option<ReString> => DataType::LargeUtf8,
        }
    }
}

/// Candidate partition keys of locks. A scoped lock hashes its scope and an
/// unscoped lock hashes its lock name's key, as `CanonicalLockId` does; a holder
/// given by entry ID (an invocation or state mutation) has the lock's partition key.
/// Values of an unexpected form leave the candidates unrestricted.
pub fn partition_keys(
    keys: Domain<PartitionKey>,
    scope: Domain<Option<ReString>>,
    lock_name: Domain<ReString>,
    acquired_by: Domain<Option<ReString>>,
) -> Domain<PartitionKey> {
    let lock_keys = lock_name.try_map_values(|name| {
        LockName::parse_unchecked(name)
            .ok()
            .map(|name| name.key().clone())
    });
    let holders = acquired_by.try_map_values(|holder| {
        holder
            .as_deref()?
            .parse::<BaseEntryId>()
            .ok()
            .map(|id| id.partition_key())
    });
    super::state::partition_keys(keys, scope, lock_keys).intersect(holders)
}

/// A borrowed lock row.
pub struct LockView<'a> {
    pub partition_key: PartitionKey,
    pub scope: Option<&'a Scope>,
    pub lock_name: &'a LockName,
    pub state: &'a LockState,
}

fn append_lock(builder: &mut LocksBatchBuilder, lock: LockView<'_>) {
    let mut row = builder.row();
    row.partition_key(lock.partition_key);
    if row.is_scope_defined()
        && let Some(scope) = lock.scope
    {
        row.scope(scope);
    }
    if row.is_lock_name_defined() {
        row.fmt_lock_name(lock.lock_name);
    }
    if row.is_acquired_at_defined() {
        row.acquired_at(lock.state.acquired_at.to_unix_millis().as_u64() as i64);
    }
    if row.is_acquired_by_defined() {
        match &lock.state.acquired_by {
            AcquiredBy::Empty => {}
            AcquiredBy::Other(other) => row.fmt_acquired_by(other),
            AcquiredBy::InvocationId(invocation_id) => row.fmt_acquired_by(invocation_id),
            AcquiredBy::StateMutation(mutation_id) => row.fmt_acquired_by(mutation_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{IsNullExpr, binary, col, lit};
    use datafusion::physical_plan::PhysicalExpr;

    use restate_types::identifiers::partitioner::HashPartitioner;
    use restate_types::identifiers::{InvocationId, InvocationUuid, WithPartitionKey};

    use crate::LogicalSchema;

    use super::*;

    #[test]
    fn scopes_unscoped_lock_names_and_holders_route_to_partition_keys() {
        let schema = Locks::arrow_schema();
        let eq = |column: &str, value: &str| {
            binary(
                col(column, &schema).unwrap(),
                Operator::Eq,
                lit(value),
                &schema,
            )
            .unwrap()
        };
        let unscoped: Arc<dyn PhysicalExpr> =
            Arc::new(IsNullExpr::new(col("scope", &schema).unwrap()));
        let keys = |filters: &[Arc<dyn PhysicalExpr>]| {
            Locks::candidate_shard_keys(filters).unwrap().into_values()
        };
        let hash = HashPartitioner::compute_partition_key;
        let holder = InvocationId::from_parts(42, InvocationUuid::from_u128(7));

        assert_eq!(keys(&[eq("scope", "tenant")]), Some(vec![hash("tenant")]));
        assert_eq!(
            keys(&[unscoped, eq("lock_name", "Cart/alice")]),
            Some(vec![hash("alice")]),
            "an_unscoped_lock_hashes_its_key"
        );
        assert_eq!(
            keys(&[eq("lock_name", "Cart/alice")]),
            None,
            "a_scoped_lock_of_that_name_may_be_anywhere"
        );
        assert_eq!(
            keys(&[eq("acquired_by", &holder.to_string())]),
            Some(vec![holder.partition_key()])
        );
        assert_eq!(
            keys(&[eq("acquired_by", "admin")]),
            None,
            "other_holders_are_unrouted"
        );
    }
}
