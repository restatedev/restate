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

/// Finite scope and lock-name pairs are looked up by key, with V1's rows: past
/// the analysis budget, over a thousand keys, empty keys and names that extend
/// one another, and with residual predicates applied to the fetched rows. A
/// branch that can't name keys, such as an invalid scope or a LIKE, scans the
/// whole source.
#[restate_core::test]
async fn lock_lookups_match_v1() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let unscoped = (0..60)
        .map(|n| format!("Svc/k{n}"))
        .chain(["Svc/", "Svc/a", "Cart/alice", "Cart/bob"].map(str::to_owned))
        .map(|name| (None, name));
    let scoped = [("tenant-1", "Cart/alice"), ("tenant-2", "Profile/carol")]
        .map(|(scope, name)| (Some(scope), name.to_owned()));
    let mut tx = store.transaction();
    for (scope, name) in unscoped.chain(scoped) {
        let acquired_by = match name.as_str() {
            "Cart/bob" => AcquiredBy::Other("admin".into()),
            _ => AcquiredBy::Empty,
        };
        tx.acquire_lock(
            &scope.map(|scope| Scope::try_non_interned(scope).unwrap()),
            &LockName::parse_unchecked(&name).unwrap(),
            &LockState {
                acquired_at: UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(
                    1_700_000_000_000,
                ))
                .unwrap(),
                acquired_by,
            },
        );
    }
    tx.commit().await.unwrap();
    drop(tx);

    let [old, new] = differential_sessions(&manager).await;
    let pairs = (0..50)
        .map(|n| format!("(scope IS NULL AND lock_name = 'Svc/k{n}')"))
        .collect::<Vec<_>>()
        .join(" OR ");
    // 101 scopes by 100 names is past the pair limit, so it scans.
    let many_scopes = std::iter::once("'tenant-1'".to_owned())
        .chain((0..100).map(|n| format!("'t{n}'")))
        .collect::<Vec<_>>()
        .join(", ");
    let some_names = std::iter::once("'Cart/alice'".to_owned())
        .chain((0..99).map(|n| format!("'Svc/x{n}'")))
        .collect::<Vec<_>>()
        .join(", ");
    let names = (0..1100)
        .map(|n| format!("'Svc/k{n}'"))
        .collect::<Vec<_>>()
        .join(", ");
    // (query, keys looked up or None for a scan, rows)
    let cases = [
        (
            format!("SELECT lock_name FROM sys_locks WHERE {pairs} ORDER BY lock_name"),
            Some(50),
            50,
        ),
        (
            "SELECT scope, lock_name FROM sys_locks \
             WHERE (scope = 'tenant-1' AND lock_name = 'Cart/alice') \
             OR (scope IS NULL AND lock_name IN ('Cart/alice', 'Cart/bob', 'Cart/missing')) \
             ORDER BY scope, lock_name"
                .to_owned(),
            Some(4),
            3,
        ),
        (
            "SELECT scope, lock_name FROM sys_locks \
             WHERE scope IN ('tenant-1', 'tenant-2') AND lock_name IN ('Cart/alice', 'Profile/carol') \
             ORDER BY scope"
                .to_owned(),
            Some(4),
            2,
        ),
        (
            format!(
                "SELECT lock_name FROM sys_locks \
                 WHERE scope IS NULL AND lock_name IN ({names}) ORDER BY lock_name"
            ),
            Some(1100),
            60,
        ),
        (
            "SELECT lock_name FROM sys_locks \
             WHERE scope IS NULL AND lock_name IN ('Svc/', 'Svc/a/b', 'Svc/ab') ORDER BY lock_name"
                .to_owned(),
            Some(3),
            1,
        ),
        (
            "SELECT lock_name FROM sys_locks \
             WHERE scope IS NULL AND lock_name IN ('Cart/alice', 'Cart/bob') \
             AND acquired_by LIKE 'adm%'"
                .to_owned(),
            Some(2),
            1,
        ),
        (
            "SELECT lock_name FROM sys_locks \
             WHERE (scope IS NULL AND lock_name = 'Cart/alice') \
             OR (scope IS NULL AND lock_name = 'noslash')"
                .to_owned(),
            None,
            1,
        ),
        (
            "SELECT lock_name FROM sys_locks \
             WHERE (scope IS NULL AND lock_name = 'Cart/alice') \
             OR (scope = 'bad scope' AND lock_name = 'Cart/bob')"
                .to_owned(),
            None,
            1,
        ),
        (
            "SELECT lock_name FROM sys_locks \
             WHERE (scope IS NULL AND lock_name = 'Cart/alice') OR lock_name LIKE 'Profile/%' \
             ORDER BY lock_name"
                .to_owned(),
            None,
            2,
        ),
        (
            "SELECT scope FROM sys_locks WHERE lock_name = 'Cart/alice' ORDER BY scope".to_owned(),
            None,
            2,
        ),
        (
            format!(
                "SELECT scope, lock_name FROM sys_locks \
                 WHERE scope IN ({many_scopes}) AND lock_name IN ({some_names})"
            ),
            None,
            1,
        ),
    ];
    for (sql, keys, count) in cases {
        let actual = rows(new.as_ref(), &sql).await;
        assert_eq!(actual, rows(old.as_ref(), &sql).await, "{sql}");
        assert_eq!(actual.len(), count, "{sql}");
        let plan = rows(new.as_ref(), &format!("EXPLAIN {sql}"))
            .await
            .concat()
            .join("\n");
        match keys {
            Some(keys) => assert!(plan.contains(&format!("keys={keys}")), "{sql}\n{plan}"),
            None => assert!(!plan.contains("MultiGetExec"), "{sql}\n{plan}"),
        }
    }
}
