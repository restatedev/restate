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

use restate_limiter::LimitKey;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::{ReadFsmTable, WriteFsmTable};
use restate_storage_api::promise_table::{
    Promise, PromiseState, ReadPromiseTable, WritePromiseTable,
};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::vqueue_table::metadata::{
    Action, MoveMetrics, Update, VQueueLink, VQueueMeta,
};
use restate_storage_api::vqueue_table::{ReadVQueueTable, Stage, WriteVQueueTable};
use restate_types::clock::UniqueTimestamp;
use restate_types::config::{Configuration, set_current_config};
use restate_types::identifiers::{PartitionId, PartitionKey, ServiceId};
use restate_types::logs::Lsn;
use restate_types::partitions::{Partition, StorageVersion};
use restate_types::sharding::KeyRange;
use restate_types::vqueues::VQueueId;
use restate_types::{RESTATE_VERSION_1_7_9, RESTATE_VERSION_1_7_10, SemanticRestateVersion};

use crate::PartitionStoreManager;
use crate::fsm_table::{
    delete_storage_features, get_min_restate_version_from_partition_db,
    get_storage_features_from_partition_db, get_storage_version_from_partition_db,
    put_storage_features_json, put_storage_version,
};
use crate::keys::DecodeTableKey;
use crate::migrations::MigrationError;
use crate::promise_table::{PromiseKey, ScopedPromiseKey};
use crate::scan::{PhysicalScan, TableScan};
use crate::state_table::{ScopedStateKey, StateKey};

fn with_migrate_scoped_tables(enabled: bool) {
    with_scoped_table_migrations(enabled, enabled);
}

fn with_scoped_table_migrations(state: bool, promise: bool) {
    let mut config = Configuration::default();
    config
        .common
        .experimental
        .set_scoped_state_table_migration(state);
    config
        .common
        .experimental
        .set_scoped_promise_table_migration(promise);
    set_current_config(config);
}

fn storage_version(store: &crate::PartitionStore) -> StorageVersion {
    get_storage_version_from_partition_db(store.partition_db()).expect("read storage version")
}

fn min_restate_version(store: &crate::PartitionStore) -> SemanticRestateVersion {
    get_min_restate_version_from_partition_db(store.partition_db()).expect(
        "read min restate
    version",
    )
}

fn scoped_tables_migrated(store: &crate::PartitionStore) -> bool {
    let features = store.storage_features();
    features.is_migrated_to_scoped_state_table && features.is_migrated_to_scoped_promise_table
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

    assert_eq!(storage_version(&store), StorageVersion::V1_5);
    assert!(!scoped_tables_migrated(&store));
    assert!(
        get_storage_features_from_partition_db(store.partition_db())
            .expect("read storage features")
            .is_some()
    );
    assert_eq!(count_legacy_state(&store), legacy_state_before);
    assert_eq!(count_legacy_promise(&store), legacy_promise_before);
    assert_eq!(count_scoped_state(&store), 0);
    assert_eq!(count_scoped_promise(&store), 0);

    RocksDbManager::get().shutdown().await;
}

#[restate_core::test]
async fn scoped_tables_migrate_independently_and_bump_version() {
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

    let (state_entries, service_ids) = seed_unscoped_data(&mut store).await;
    let partition_id = store.partition_id();
    put_storage_version(&mut store, partition_id, StorageVersion::V1_5 as u16)
        .await
        .expect("seed V1_5");
    // Sanity: data really landed in the legacy tables.
    assert_eq!(count_legacy_state(&store), state_entries.len());
    assert_eq!(count_legacy_promise(&store), service_ids.len());

    // Migrate state independently and persist a valid intermediate layout.
    with_scoped_table_migrations(true, false);
    let config = Configuration::pinned().clone();
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .expect("migrate state table");

    assert_eq!(storage_version(&store), StorageVersion::V1_5);
    assert_eq!(min_restate_version(&store), *RESTATE_VERSION_1_7_9);
    let features = store.storage_features();
    assert!(features.is_migrated_to_scoped_state_table);
    assert!(!features.is_migrated_to_scoped_promise_table);
    assert_eq!(count_legacy_state(&store), 0);
    assert_eq!(count_scoped_state(&store), state_entries.len());
    assert_eq!(count_legacy_promise(&store), service_ids.len());
    assert_eq!(count_scoped_promise(&store), 0);

    // Reopen with both options disabled to prove the intermediate marker is authoritative.
    let mut store = crate::PartitionStore::from(store.into_inner());
    with_migrate_scoped_tables(false);
    let config = Configuration::pinned().clone();
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .expect("verify intermediate layout");
    assert!(store.storage_features().is_migrated_to_scoped_state_table);
    let (service_id, state_key) = &state_entries[0];
    assert_eq!(
        store.get_user_state(service_id, state_key).await.unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert!(
        store
            .get_promise(&service_ids[0], &ByteString::from_static("p"))
            .await
            .unwrap()
            .is_some()
    );

    // Enabling the second feature immediately advances the compatibility version.
    with_scoped_table_migrations(false, true);
    let config = Configuration::pinned().clone();
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .expect("migrate promise table");

    assert_eq!(
        storage_version(&store),
        StorageVersion::ScopedStateAndPromise
    );
    assert!(scoped_tables_migrated(&store));
    assert_eq!(
        store.get_min_restate_version().await.unwrap(),
        (*RESTATE_VERSION_1_7_9).clone()
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
        storage_version(&store),
        StorageVersion::ScopedStateAndPromise
    );
    assert!(scoped_tables_migrated(&store));
    assert_eq!(count_legacy_state(&store), 0);
    assert_eq!(count_scoped_state(&store), state_entries.len());

    // A store migrated before key 12 existed derives the feature from the mirrored StorageVersion
    // and persists the new JSON ledger without requiring the experimental option again.
    let partition_id = store.partition_id();
    delete_storage_features(&mut store, partition_id)
        .await
        .expect("remove storage features");
    let mut store = crate::PartitionStore::from(store.into_inner());
    with_migrate_scoped_tables(false);
    let config = Configuration::pinned().clone();
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .expect("bootstrap storage features from StorageVersion");
    assert!(scoped_tables_migrated(&store));
    assert!(
        get_storage_features_from_partition_db(store.partition_db())
            .expect("read bootstrapped storage features")
            .is_some()
    );

    RocksDbManager::get().shutdown().await;
}

#[restate_core::test]
async fn does_not_automatically_migrate_scoped_tables_at_1_8() {
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

    let (state_entries, service_ids) = seed_unscoped_data(&mut store).await;
    let partition_id = store.partition_id();
    put_storage_version(&mut store, partition_id, StorageVersion::V1_5 as u16)
        .await
        .expect("seed V1_5");

    let config = Configuration::pinned().clone();
    store
        .verify_and_run_migrations_at_version(
            &SemanticRestateVersion::parse("1.8.0-dev").unwrap(),
            CancellationToken::new(),
            &config,
        )
        .await
        .expect("verify without automatic migration");

    assert_eq!(storage_version(&store), StorageVersion::V1_5);
    assert!(!scoped_tables_migrated(&store));
    assert_eq!(count_legacy_state(&store), state_entries.len());
    assert_eq!(count_legacy_promise(&store), service_ids.len());
    assert_eq!(count_scoped_state(&store), 0);
    assert_eq!(count_scoped_promise(&store), 0);

    RocksDbManager::get().shutdown().await;
}

#[restate_core::test]
async fn future_unknown_features_report_the_names_raising_the_barrier() {
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
    let partition_id = store.partition_id();

    put_storage_features_json(
        &mut store,
        partition_id,
        br#"{
            "features": {
                "future-a": { "min_required_version": "2.0.0" },
                "future-b": { "min_required_version": "2.0.0" },
                "older-feature": { "min_required_version": "1.9.0" }
            }
        }"#,
    )
    .await
    .expect("seed future storage features");

    let config = Configuration::pinned().clone();
    let error = store
        .verify_and_run_migrations_at_version(
            &SemanticRestateVersion::new(1, 9, 0),
            CancellationToken::new(),
            &config,
        )
        .await
        .unwrap_err();

    match error {
        MigrationError::StorageFeatureVersionBarrier {
            required_min_version,
            storage_features,
        } => {
            assert_eq!(required_min_version, SemanticRestateVersion::new(2, 0, 0));
            assert_eq!(storage_features, ["future-a", "future-b"]);
        }
        other => panic!("unexpected error: {other}"),
    }
    assert_eq!(storage_version(&store), StorageVersion::None);

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
        storage_version(&store),
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

#[restate_core::test]
async fn obsolete_vqueue_metadata_cleanup_is_version_gated_and_idempotent() {
    with_migrate_scoped_tables(false);
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true)
        .await
        .expect("manager create");
    let mut store = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(10, 20)),
            None,
        )
        .await
        .expect("open");

    let at = UniqueTimestamp::try_from(1_744_000_000_000u64).unwrap();
    let below_range_qid = VQueueId::custom(9, "below-range");
    let obsolete_qid = VQueueId::custom(10, "obsolete");
    let paused_qid = VQueueId::custom(11, "paused");
    let busy_qid = VQueueId::custom(12, "busy");
    let finished_qid = VQueueId::custom(13, "finished");
    let upper_bound_qid = VQueueId::custom(20, "upper-bound");
    let above_range_qid = VQueueId::custom(21, "above-range");
    let new_meta = || VQueueMeta::new(at, None, LimitKey::None, VQueueLink::None);

    let mut paused_meta = new_meta();
    paused_meta.apply_update(&Update::new(at, Action::PauseVQueue {}));

    let move_to_stage = |stage| {
        Update::new(
            at,
            Action::Move {
                prev_stage: None,
                next_stage: stage,
                metrics: MoveMetrics {
                    last_transition_at: at,
                    has_started: false,
                    first_runnable_at: at.to_unix_millis(),
                    scheduler_wait_stats: None,
                },
            },
        )
    };
    let mut busy_meta = new_meta();
    busy_meta.apply_update(&move_to_stage(Stage::Inbox));
    let mut finished_meta = new_meta();
    finished_meta.apply_update(&move_to_stage(Stage::Finished));

    {
        let mut txn = store.transaction();
        txn.create_vqueue(&below_range_qid, &new_meta());
        txn.create_vqueue(&obsolete_qid, &new_meta());
        txn.create_vqueue(&paused_qid, &paused_meta);
        txn.create_vqueue(&busy_qid, &busy_meta);
        txn.create_vqueue(&finished_qid, &finished_meta);
        txn.create_vqueue(&upper_bound_qid, &new_meta());
        txn.create_vqueue(&above_range_qid, &new_meta());
        txn.put_applied_lsn(Lsn::from(1)).expect("lsn write");
        txn.commit().await.expect("seed vqueue metadata");
    }
    let partition_id = store.partition_id();
    put_storage_version(&mut store, partition_id, StorageVersion::V1_5 as u16)
        .await
        .expect("seed V1_5");

    let mut config = Configuration::pinned().clone();
    config.common.experimental.set_vqueue_obsolete_cleanup(true);
    store
        .verify_and_run_migrations_at_version(
            &SemanticRestateVersion::new(1, 7, 9),
            CancellationToken::new(),
            &config,
        )
        .await
        .expect("1.7.9 must not enable cleanup");
    assert!(!store.storage_features().is_vqueue_metadata_cleanup_v1);
    {
        let txn = store.transaction();
        assert!(txn.get_vqueue(&obsolete_qid).await.unwrap().is_some());
    }

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        store
            .verify_and_run_migrations_at_version(&RESTATE_VERSION_1_7_10, cancelled, &config)
            .await,
        Err(MigrationError::MigrationCancelled)
    ));
    assert!(!store.storage_features().is_vqueue_metadata_cleanup_v1);

    store
        .verify_and_run_migrations_at_version(
            &RESTATE_VERSION_1_7_10,
            CancellationToken::new(),
            &config,
        )
        .await
        .expect("1.7.10 enables cleanup");

    assert!(store.storage_features().is_vqueue_metadata_cleanup_v1);
    assert_eq!(min_restate_version(&store), *RESTATE_VERSION_1_7_10);
    {
        let txn = store.transaction();
        assert!(txn.get_vqueue(&obsolete_qid).await.unwrap().is_none());
        assert!(txn.get_vqueue(&paused_qid).await.unwrap().is_some());
        assert!(txn.get_vqueue(&busy_qid).await.unwrap().is_some());
        assert!(txn.get_vqueue(&finished_qid).await.unwrap().is_some());
        assert!(txn.get_vqueue(&upper_bound_qid).await.unwrap().is_none());
        assert!(txn.get_vqueue(&below_range_qid).await.unwrap().is_some());
        assert!(txn.get_vqueue(&above_range_qid).await.unwrap().is_some());
    }

    // The marker records one cleanup pass and avoids repeated startup scans.
    let after_cleanup_qid = VQueueId::custom(14, "after-cleanup");
    {
        let mut txn = store.transaction();
        txn.create_vqueue(&after_cleanup_qid, &new_meta());
        txn.commit().await.expect("seed after cleanup");
    }
    store
        .verify_and_run_migrations_at_version(
            &RESTATE_VERSION_1_7_10,
            CancellationToken::new(),
            &config,
        )
        .await
        .expect("completed cleanup is not repeated");
    {
        let txn = store.transaction();
        assert!(txn.get_vqueue(&after_cleanup_qid).await.unwrap().is_some());
    }

    RocksDbManager::get().shutdown().await;
}
