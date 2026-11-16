// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::UniqueTimestamp;
use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::lock_table::{AcquiredBy, LockState, WriteLockTable};
use restate_types::identifiers::partitioner::HashPartitioner;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionId};
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::{LockName, Scope};

use crate::mocks::{differential_sessions, rows};

#[restate_core::test]
async fn locks_match_v1_across_scopes_and_holders() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    // A holder runs on its lock's partition key: the scope hash, or the key hash
    // when unscoped.
    let hash = HashPartitioner::compute_partition_key;
    let alice_holder = InvocationId::from_parts(hash("alice"), InvocationUuid::from_u128(1));
    let tenant_holder = InvocationId::from_parts(hash("tenant-2"), InvocationUuid::from_u128(2));
    let locks = [
        (None, "Cart/alice", AcquiredBy::InvocationId(alice_holder)),
        (None, "Cart/bob", AcquiredBy::Other("admin".into())),
        (Some("tenant-1"), "Cart/alice", AcquiredBy::Empty),
        (
            Some("tenant-2"),
            "Profile/carol",
            AcquiredBy::InvocationId(tenant_holder),
        ),
    ];
    let mut tx = store.transaction();
    for (index, (scope, name, acquired_by)) in locks.into_iter().enumerate() {
        let acquired_at = MillisSinceEpoch::new(1_700_000_000_000 + index as u64);
        tx.acquire_lock(
            &scope.map(|scope| Scope::try_non_interned(scope).unwrap()),
            &LockName::parse(name).unwrap(),
            &LockState {
                acquired_at: UniqueTimestamp::try_from_unix_millis(acquired_at).unwrap(),
                acquired_by,
            },
        );
    }
    tx.commit().await.unwrap();
    drop(tx);

    let [old, new] = differential_sessions(&manager).await;
    for sql in [
        "SELECT * FROM sys_locks ORDER BY scope, lock_name".to_owned(),
        "SELECT lock_name, acquired_by FROM sys_locks WHERE scope IS NULL ORDER BY lock_name"
            .into(),
        "SELECT lock_name FROM sys_locks WHERE scope IS NULL AND lock_name = 'Cart/alice'".into(),
        "SELECT scope, lock_name FROM sys_locks WHERE scope = 'tenant-2'".into(),
        "SELECT lock_name FROM sys_locks WHERE acquired_by IS NULL".into(),
        "SELECT lock_name FROM sys_locks ORDER BY acquired_at DESC LIMIT 2 OFFSET 1".into(),
        "SELECT COUNT(*) FROM sys_locks".into(),
    ] {
        assert_eq!(
            rows(new.as_ref(), &sql).await,
            rows(old.as_ref(), &sql).await,
            "{sql}"
        );
    }
    for (holder, expected) in [
        (alice_holder, ["", "Cart/alice"]),
        (tenant_holder, ["tenant-2", "Profile/carol"]),
    ] {
        let sql = format!("SELECT scope, lock_name FROM sys_locks WHERE acquired_by = '{holder}'");
        let actual = rows(new.as_ref(), &sql).await;
        assert_eq!(actual, rows(old.as_ref(), &sql).await, "{sql}");
        assert_eq!(actual, [expected], "{sql}");
    }
    assert_eq!(
        rows(new.as_ref(), "SELECT COUNT(*) FROM sys_locks").await,
        [["4"]]
    );
}
