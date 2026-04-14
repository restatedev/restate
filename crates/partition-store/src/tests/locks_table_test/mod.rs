// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use restate_clock::UniqueTimestamp;
use restate_storage_api::Transaction;
use restate_storage_api::lock_table::{AcquiredBy, LoadLocks, LockState, WriteLockTable};
use restate_types::identifiers::WithPartitionKey;
use restate_types::{LockName, Scope};

use crate::PartitionStore;

fn lock_name(raw: &str) -> LockName {
    LockName::parse(raw).expect("lock name should be valid")
}

fn lock_state(ts: u64) -> LockState {
    LockState {
        acquired_at: UniqueTimestamp::try_from(ts).expect("timestamp should be valid"),
        acquired_by: AcquiredBy::Empty,
    }
}

fn distinct_scopes(count: usize) -> Vec<Scope> {
    let mut scopes = Vec::with_capacity(count);
    let mut partition_keys = HashSet::with_capacity(count);

    for idx in 0..10_000 {
        let scope_name = format!("locks-scan-scope-{idx}");
        let scope = Scope::new(&scope_name);
        if partition_keys.insert(scope.partition_key()) {
            scopes.push(scope);
            if scopes.len() == count {
                return scopes;
            }
        }
    }

    panic!("failed to find distinct scope partition keys");
}

async fn scan_all_locked_observes_active_locks_across_partition_keys(rocksdb: &mut PartitionStore) {
    let mut scopes = distinct_scopes(3).into_iter();
    let scope_a = Some(scopes.next().unwrap());
    let scope_b = Some(scopes.next().unwrap());
    let scope_c = Some(scopes.next().unwrap());

    let pkey_a = scope_a.as_ref().unwrap().partition_key();
    let pkey_b = scope_b.as_ref().unwrap().partition_key();
    let pkey_c = scope_c.as_ref().unwrap().partition_key();

    assert_ne!(
        pkey_a, pkey_b,
        "scope_a and scope_b must use different partition keys"
    );
    assert_ne!(
        pkey_b, pkey_c,
        "scope_b and scope_c must use different partition keys"
    );
    assert_ne!(
        pkey_a, pkey_c,
        "scope_a and scope_c must use different partition keys"
    );

    let lock_a = lock_name("lock-svc/shared-key-a");
    let lock_b = lock_name("lock-svc/shared-key-b");
    let lock_c = lock_name("lock-svc/shared-key-c");
    let released_lock = lock_name("lock-svc/released-key");
    let no_scope = None;

    let mut txn = rocksdb.transaction();
    txn.acquire_lock(&scope_a, &lock_a, &lock_state(1_000));
    txn.acquire_lock(&scope_b, &lock_b, &lock_state(2_000));
    txn.acquire_lock(&scope_c, &lock_c, &lock_state(3_000));
    txn.acquire_lock(&no_scope, &released_lock, &lock_state(4_000));
    txn.release_lock(&no_scope, &released_lock);
    txn.commit().await.expect("commit should succeed");

    let db = rocksdb.partition_db();

    let mut observed = HashSet::new();
    db.scan_all_locked(|scope, lock_name| {
        observed.insert((scope, lock_name));
    })
    .expect("scan all locked should succeed");

    let mut expected = HashSet::new();
    expected.insert((scope_a, lock_a));
    expected.insert((scope_b, lock_b));
    expected.insert((scope_c, lock_c));

    assert_eq!(observed, expected);
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    scan_all_locked_observes_active_locks_across_partition_keys(&mut rocksdb).await;
}
