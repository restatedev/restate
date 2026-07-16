// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! End-to-end tests for the V1_5 → ScopedStateAndPromise migration. Exercises
//! `verify_and_run_migrations` with and without the experimental opt-in.

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use tokio_util::sync::CancellationToken;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::promise_table::{
    Promise, PromiseState, ReadPromiseTable, WritePromiseTable,
};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_types::config::{Configuration, set_current_config};
use restate_types::identifiers::{PartitionId, PartitionKey, ServiceId};
use restate_types::logs::Lsn;
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;

use crate::PartitionStoreManager;
use crate::fsm_table::put_storage_version;
use crate::keys::DecodeTableKey;
use crate::promise_table::{PromiseKey, ScopedPromiseKey};
use crate::scan::{PhysicalScan, TableScan};
use crate::state_table::{ScopedStateKey, StateKey};
use restate_types::partitions::StorageVersion;

fn with_migrate_scoped_tables(enabled: bool) {
    let mut config = Configuration::default();
    config
        .common
        .experimental
        .set_migrate_scoped_tables(enabled);
    set_current_config(config);
}

async fn seed_unscoped_data(
    store: &mut crate::PartitionStore,
) -> (Vec<(ServiceId, Bytes)>, Vec<ServiceId>) {
    let service_ids = crate::migrations::tests::distinct_service_ids(3);
    let state_entries: Vec<(ServiceId, Bytes)> = service_ids
        .iter()
        .map(|sid| (sid.clone(), Bytes::from_static(b"k1")))
        .collect();

    let mut txn = store.transaction();
    for (service_id, state_key) in &state_entries {
        txn.put_user_state(service_id, state_key, Bytes::from_static(b"v1"))
            .expect("state write should succeed");
    }
    for service_id in &service_ids {
        txn.put_promise(
            service_id,
            &ByteString::from_static("p"),
            &Promise {
                state: PromiseState::NotCompleted(vec![]),
            },
        )
        .expect("promise write should succeed");
    }
    txn.put_applied_lsn(Lsn::from(1)).expect("lsn write");
    txn.commit().await.expect("commit should succeed");

    (state_entries, service_ids)
}

fn count_legacy_state(store: &crate::PartitionStore) -> usize {
    let mut arena = BytesMut::new();
    let mut it = store
        .partition_db()
        .scan(
            PhysicalScan::from(
                TableScan::ScanPartitionKeyRange::<StateKey>(store.partition_key_range()),
                &mut arena,
            ),
            rocksdb::ReadOptions::default(),
        )
        .expect("scan legacy state");
    it.seek_to_first();
    let mut n = 0;
    while it.valid() {
        n += 1;
        it.next();
    }
    n
}

fn count_legacy_promise(store: &crate::PartitionStore) -> usize {
    let mut arena = BytesMut::new();
    let mut it = store
        .partition_db()
        .scan(
            PhysicalScan::from(
                TableScan::ScanPartitionKeyRange::<PromiseKey>(store.partition_key_range()),
                &mut arena,
            ),
            rocksdb::ReadOptions::default(),
        )
        .expect("scan legacy promise");
    it.seek_to_first();
    let mut n = 0;
    while it.valid() {
        n += 1;
        it.next();
    }
    n
}

fn count_scoped_state(store: &crate::PartitionStore) -> usize {
    let mut arena = BytesMut::new();
    let mut it = store
        .partition_db()
        .scan(
            PhysicalScan::from(
                TableScan::ScanPartitionKeyRange::<ScopedStateKey>(store.partition_key_range()),
                &mut arena,
            ),
            rocksdb::ReadOptions::default(),
        )
        .expect("scan scoped state");
    it.seek_to_first();
    let mut n = 0;
    while it.valid() {
        let (mut key, _) = it.item().unwrap();
        let scoped = ScopedStateKey::deserialize_from(&mut key).unwrap();
        let (_pk, scope, _name, _key, _state_key) = scoped.split();
        assert_eq!(scope, None, "migrated state must have scope = None");
        n += 1;
        it.next();
    }
    n
}

fn count_scoped_promise(store: &crate::PartitionStore) -> usize {
    let mut arena = BytesMut::new();
    let mut it = store
        .partition_db()
        .scan(
            PhysicalScan::from(
                TableScan::ScanPartitionKeyRange::<ScopedPromiseKey>(store.partition_key_range()),
                &mut arena,
            ),
            rocksdb::ReadOptions::default(),
        )
        .expect("scan scoped promise");
    it.seek_to_first();
    let mut n = 0;
    while it.valid() {
        let (mut key, _) = it.item().unwrap();
        let scoped = ScopedPromiseKey::deserialize_from(&mut key).unwrap();
        let (_pk, scope, _name, _key, _pkey) = scoped.split();
        assert_eq!(scope, None, "migrated promise must have scope = None");
        n += 1;
        it.next();
    }
    n
}

#[restate_core::test]
async fn flag_off_keeps_partition_at_v1_5() {
    with_migrate_scoped_tables(false);
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true)
        .await
        .expect("manager create");
    let mut store = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .expect("open");

    let (_state_entries, _service_ids) = seed_unscoped_data(&mut store).await;
    // Stamp the FSM as V1_5 so verify_and_run_migrations takes the migration path
    // rather than the empty-partition fast path.
    let partition_id = store.partition_id();
    put_storage_version(&mut store, partition_id, StorageVersion::V1_5 as u16)
        .await
        .expect("seed V1_5");

    let legacy_state_before = count_legacy_state(&store);
    let legacy_promise_before = count_legacy_promise(&store);
    assert!(legacy_state_before > 0);
    assert!(legacy_promise_before > 0);

    let cancel = CancellationToken::new();
    store
        .verify_and_run_migrations(cancel, &Configuration::pinned())
        .await
        .expect("verify with flag off");

    assert_eq!(store.storage_version(), StorageVersion::V1_5);
    assert_eq!(count_legacy_state(&store), legacy_state_before);
    assert_eq!(count_legacy_promise(&store), legacy_promise_before);
    assert_eq!(count_scoped_state(&store), 0);
    assert_eq!(count_scoped_promise(&store), 0);

    RocksDbManager::get().shutdown().await;
}

#[restate_core::test]
async fn flag_on_migrates_and_bumps_version() {
    with_migrate_scoped_tables(true);
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true)
        .await
        .expect("manager create");
    let mut store = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .expect("open");

    // Switch the flag back off so that seeding via WriteStateTable hits the legacy table.
    with_migrate_scoped_tables(false);
    let (state_entries, service_ids) = seed_unscoped_data(&mut store).await;
    let partition_id = store.partition_id();
    put_storage_version(&mut store, partition_id, StorageVersion::V1_5 as u16)
        .await
        .expect("seed V1_5");
    // Sanity: data really landed in the legacy tables.
    assert_eq!(count_legacy_state(&store), state_entries.len());
    assert_eq!(count_legacy_promise(&store), service_ids.len());

    // Now turn the flag back on and run migrations.
    with_migrate_scoped_tables(true);
    let cancel = CancellationToken::new();
    store
        .verify_and_run_migrations(cancel, &Configuration::pinned())
        .await
        .expect("verify with flag on");

    assert_eq!(
        store.storage_version(),
        StorageVersion::ScopedStateAndPromise
    );
    assert_eq!(count_legacy_state(&store), 0);
    assert_eq!(count_legacy_promise(&store), 0);
    assert_eq!(count_scoped_state(&store), state_entries.len());
    assert_eq!(count_scoped_promise(&store), service_ids.len());

    // Reads via the public API (scope = None) hit the scoped table now.
    for (service_id, state_key) in &state_entries {
        let v = store
            .get_user_state(service_id, state_key)
            .await
            .expect("get state");
        assert_eq!(v, Some(Bytes::from_static(b"v1")));
    }
    for service_id in &service_ids {
        let p = store
            .get_promise(service_id, &ByteString::from_static("p"))
            .await
            .expect("get promise");
        assert!(p.is_some());
    }

    // Idempotency: a second verify is a no-op.
    let cancel = CancellationToken::new();
    store
        .verify_and_run_migrations(cancel, &Configuration::pinned())
        .await
        .expect("second verify");
    assert_eq!(
        store.storage_version(),
        StorageVersion::ScopedStateAndPromise
    );
    assert_eq!(count_legacy_state(&store), 0);
    assert_eq!(count_scoped_state(&store), state_entries.len());

    RocksDbManager::get().shutdown().await;
}

#[restate_core::test]
async fn flag_on_writes_post_migration_land_in_scoped() {
    with_migrate_scoped_tables(true);
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true)
        .await
        .expect("manager create");
    let mut store = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .expect("open");

    // Empty partition + flag on → fast path initializes at ScopedStateAndPromise.
    let cancel = CancellationToken::new();
    store
        .verify_and_run_migrations(cancel, &Configuration::pinned())
        .await
        .expect("verify fresh");
    assert_eq!(
        store.storage_version(),
        StorageVersion::ScopedStateAndPromise
    );

    let service_id = ServiceId::new(None, "svc", "k");
    {
        let mut txn = store.transaction();
        txn.put_user_state(&service_id, &Bytes::from_static(b"key"), b"value")
            .expect("write state");
        txn.commit().await.expect("commit");
    }

    assert_eq!(count_legacy_state(&store), 0);
    assert_eq!(count_scoped_state(&store), 1);

    RocksDbManager::get().shutdown().await;
}
