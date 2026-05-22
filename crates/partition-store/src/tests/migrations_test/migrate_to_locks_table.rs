// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU16;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::thread;

use restate_clock::ClockUpkeep;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::lock_table::{AcquiredBy, LoadLocks, ScanLocksTable, WriteLockTable};
use restate_storage_api::service_status_table::{
    ReadVirtualObjectStatusTable, VirtualObjectStatus, WriteVirtualObjectStatusTable,
};
use restate_types::LockName;
use restate_types::config::Configuration;
use restate_types::identifiers::{
    InvocationId, InvocationUuid, PartitionId, PartitionKey, ServiceId, WithPartitionKey,
};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;

use crate::migrations::MigrationContext;
use crate::migrations::tests::distinct_service_ids;
use crate::{PartitionStore, PartitionStoreManager};

async fn storage_test_environment() -> PartitionStore {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true)
        .await
        .expect("DB storage creation succeeds");
    manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .expect("DB storage creation succeeds")
}

fn lock_name(service_id: &ServiceId) -> LockName {
    LockName::parse(&format!("{}/{}", service_id.service_name, service_id.key))
        .expect("lock name should be valid")
}

async fn collect_locks(rocksdb: &PartitionStore) -> HashMap<String, (PartitionKey, InvocationId)> {
    let observed = Arc::new(Mutex::new(HashMap::new()));
    let observed_for_scan = Arc::clone(&observed);

    rocksdb
        .for_each_lock(
            rocksdb.partition_key_range(),
            move |(partition_key, scope, lock_name, state)| {
                assert_eq!(scope, None);
                let invocation_id = match state.acquired_by {
                    AcquiredBy::InvocationId(invocation_id) => invocation_id,
                    other => panic!("unexpected lock owner: {other:?}"),
                };
                observed_for_scan
                    .lock()
                    .expect("lock should not be poisoned")
                    .insert(lock_name.to_string(), (partition_key, invocation_id));
                ControlFlow::Continue(())
            },
        )
        .expect("lock scan should start")
        .await
        .expect("lock scan should succeed");

    // The background iterator thread may still be tearing down its closure
    // (which holds another Arc clone) when this future resolves, so we drain
    // the mutex instead of relying on unique Arc ownership.
    std::mem::take(&mut *observed.lock().expect("lock should not be poisoned"))
}

#[restate_core::test]
async fn migrate_to_locks_table_moves_service_status_locks_to_lock_table() {
    let _clock_guard = ClockUpkeep::start().expect("clock upkeep should start");
    let mut rocksdb = storage_test_environment().await;
    let service_ids = distinct_service_ids(3);
    let lock_names = service_ids.iter().map(lock_name).collect::<Vec<_>>();
    let invocation_ids = service_ids
        .iter()
        .enumerate()
        .map(|(idx, service_id)| {
            InvocationId::from_parts(
                service_id.partition_key(),
                InvocationUuid::from_u128(12345678900001 + idx as u128),
            )
        })
        .collect::<Vec<_>>();

    let mut txn = rocksdb.transaction();
    for (service_id, invocation_id) in service_ids.iter().zip(invocation_ids.iter()) {
        txn.put_virtual_object_status(service_id, &VirtualObjectStatus::Locked(*invocation_id))
            .expect("service status write should succeed");
    }
    txn.commit().await.expect("commit should succeed");
    drop(txn);

    let config = Configuration::default();
    let mut ctx = MigrationContext::new(
        &config,
        rocksdb.partition_db(),
        rocksdb.partition_key_range(),
    );
    crate::migrations::migrate_to_locks_table::migrate_to_locks_table(&mut ctx)
        .expect("migration should succeed");
    crate::migrations::migrate_to_locks_table::delete_service_status_data(&mut ctx)
        .expect("service status cleanup should succeed");

    for service_id in &service_ids {
        assert_eq!(
            rocksdb
                .get_virtual_object_status(service_id)
                .await
                .expect("service status read should succeed"),
            VirtualObjectStatus::Unlocked
        );
    }

    let observed = collect_locks(&rocksdb).await;
    assert_eq!(observed.len(), service_ids.len());
    for ((service_id, lock_name), invocation_id) in service_ids
        .iter()
        .zip(lock_names.iter())
        .zip(invocation_ids.iter())
    {
        assert_eq!(
            observed.get(&lock_name.to_string()),
            Some(&(service_id.partition_key(), *invocation_id))
        );
    }

    let mut txn = rocksdb.transaction();
    txn.release_lock(&None, &lock_names[0]);
    txn.commit().await.expect("commit should succeed");
    drop(txn);

    let mut still_locked = HashSet::new();
    rocksdb
        .partition_db()
        .scan_all_locked(|scope, lock_name| {
            still_locked.insert((scope, lock_name.to_string()));
        })
        .expect("lock scan should succeed");

    assert!(!still_locked.contains(&(None, lock_names[0].to_string())));
    assert!(still_locked.contains(&(None, lock_names[1].to_string())));
    assert!(still_locked.contains(&(None, lock_names[2].to_string())));

    RocksDbManager::get().shutdown().await;
}

#[restate_core::test]
async fn migrate_to_locks_table_runs_split_ranges_concurrently_with_shared_context() {
    let _clock_guard = ClockUpkeep::start().expect("clock upkeep should start");
    let mut rocksdb = storage_test_environment().await;
    let key_range = KeyRange::new(1, 100);
    let service_ids = key_range
        .iter()
        .map(|partition_key| {
            ServiceId::with_partition_key(
                partition_key,
                "migration-svc",
                format!("migration-key-{partition_key}"),
            )
        })
        .collect::<Vec<_>>();
    let lock_names = service_ids.iter().map(lock_name).collect::<Vec<_>>();
    let invocation_ids = service_ids
        .iter()
        .enumerate()
        .map(|(idx, service_id)| {
            InvocationId::from_parts(
                service_id.partition_key(),
                InvocationUuid::from_u128(12345678900001 + idx as u128),
            )
        })
        .collect::<Vec<_>>();

    let mut txn = rocksdb.transaction();
    for (service_id, invocation_id) in service_ids.iter().zip(invocation_ids.iter()) {
        txn.put_virtual_object_status(service_id, &VirtualObjectStatus::Locked(*invocation_id))
            .expect("service status write should succeed");
    }
    txn.commit().await.expect("commit should succeed");
    drop(txn);

    let config = Configuration::default();
    let ctx = MigrationContext::new(&config, rocksdb.partition_db(), key_range);
    let contexts = ctx
        .split(NonZeroU16::new(2).expect("two is non-zero"))
        .collect::<Vec<_>>();
    assert_eq!(
        contexts.iter().map(|ctx| ctx.key_range).collect::<Vec<_>>(),
        vec![KeyRange::new(1, 50), KeyRange::new(51, 100)]
    );

    thread::scope(|scope| {
        let mut migrations = Vec::with_capacity(contexts.len());
        for mut ctx in contexts {
            migrations.push(scope.spawn(move || {
                crate::migrations::migrate_to_locks_table::migrate_to_locks_table(&mut ctx)?;
                crate::migrations::migrate_to_locks_table::delete_service_status_data(&mut ctx)
            }));
        }

        for migration in migrations {
            migration
                .join()
                .expect("migration thread should not panic")
                .expect("migration should succeed");
        }
    });

    for service_id in &service_ids {
        assert_eq!(
            rocksdb
                .get_virtual_object_status(service_id)
                .await
                .expect("service status read should succeed"),
            VirtualObjectStatus::Unlocked
        );
    }

    let observed = collect_locks(&rocksdb).await;
    assert_eq!(observed.len(), service_ids.len());
    for ((service_id, lock_name), invocation_id) in service_ids
        .iter()
        .zip(lock_names.iter())
        .zip(invocation_ids.iter())
    {
        assert_eq!(
            observed.get(&lock_name.to_string()),
            Some(&(service_id.partition_key(), *invocation_id))
        );
    }

    RocksDbManager::get().shutdown().await;
}
