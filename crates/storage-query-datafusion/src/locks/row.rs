// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::lock_table::{AcquiredBy, LockState};
use restate_types::identifiers::PartitionKey;
use restate_types::{LockName, Scope};

use crate::locks::schema::SysLocksBuilder;

#[inline]
pub(crate) fn append_lock_row(
    builder: &mut SysLocksBuilder,
    partition_key: PartitionKey,
    scope: Option<Scope>,
    lock_name: LockName,
    state: LockState,
) {
    let mut row = builder.row();

    row.partition_key(partition_key);
    if row.is_scope_defined()
        && let Some(scope) = scope
    {
        row.scope(scope);
    }

    if row.is_lock_name_defined() {
        row.fmt_lock_name(lock_name);
    }

    if row.is_acquired_at_defined() {
        row.acquired_at(state.acquired_at.to_unix_millis().as_u64() as i64);
    }

    if row.is_acquired_by_defined() {
        match state.acquired_by {
            AcquiredBy::Empty => { /* do not set */ }
            AcquiredBy::Other(other) => row.fmt_acquired_by(other),
            AcquiredBy::InvocationId(invocation_id) => row.fmt_acquired_by(invocation_id),
            AcquiredBy::StateMutation(mutation_id) => row.fmt_acquired_by(mutation_id),
        }
    }
}
