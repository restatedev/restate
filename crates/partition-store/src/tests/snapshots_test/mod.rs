// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::{FsmPosition, ReadFsmTable, WriteFsmTable};
use restate_test_util::tempdir;
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::partitions::Partition;
use restate_types::partitions::features::PersistedFeatures;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, SemanticRestateVersion};

use crate::fsm_table::{put_fsm_position, put_storage_features_json};
use crate::migrations::MigrationError;
use crate::snapshots::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotDir, SnapshotFormatVersion,
};
use crate::{PartitionStore, PartitionStoreManager};

pub(crate) async fn run_tests(
    manager: Arc<PartitionStoreManager>,
    mut partition_store: PartitionStore,
) {
    insert_test_data(&mut partition_store).await;

    let snapshots_dir = tempdir().unwrap();

    let partition_id = partition_store.partition_id();

    let snapshot = partition_store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            SnapshotId::new(),
        )
        .await
        .unwrap();

    let key_range = partition_store.partition_key_range();
    let snapshot_meta = PartitionSnapshotMetadata {
        version: SnapshotFormatVersion::V1,
        cluster_name: "cluster_name".to_string(),
        cluster_fingerprint: None,
        partition_id,
        node_name: "node".to_string(),
        node_id: Some(GenerationalNodeId::new(1, 1)),
        created_at: MillisSinceEpoch::new(0).into_timestamp(),
        snapshot_id: SnapshotId::from_parts(0, 0),
        key_range,
        log_id: LogId::from(partition_id),
        min_applied_lsn: snapshot.min_applied_lsn,
        db_comparator_name: snapshot.db_comparator_name.clone(),
        compatibility: snapshot.compatibility.clone(),
        byte_offsets_base_lsn: snapshot.byte_offsets_base_lsn,
        log_byte_offset: snapshot.log_byte_offset,
        partition_store_byte_offset: snapshot.partition_store_byte_offset,
        files: snapshot.files.clone(),
    };
    assert!(snapshot.compatibility.is_some());
    assert_eq!(
        (
            snapshot.byte_offsets_base_lsn,
            snapshot.log_byte_offset,
            snapshot.partition_store_byte_offset
        ),
        (
            Some(POSITION.byte_offsets_base_lsn),
            Some(POSITION.log_byte_offset),
            Some(POSITION.partition_store_byte_offset)
        )
    );
    let metadata_json = serde_json::to_string_pretty(&snapshot_meta).unwrap();

    drop(partition_store);
    // Disarm the exported snapshot's directory guard so the on-disk SST files survive past this
    // drop; we restore from them below. `snapshots_dir` (a TempDir) still owns the parent dir.
    let _exported_dir = snapshot.base_dir.into_path();

    manager
        .drop_partition_store(&Partition::new(partition_id, key_range))
        .await
        .unwrap();

    let snapshot_meta: PartitionSnapshotMetadata = serde_json::from_str(&metadata_json).unwrap();
    assert_eq!(snapshot_meta.compatibility, snapshot.compatibility);
    assert_eq!(snapshot_meta.node_id, Some(GenerationalNodeId::new(1, 1)));
    assert_eq!(snapshot_meta.log_byte_offset, snapshot.log_byte_offset);

    let snapshot = LocalPartitionSnapshot {
        base_dir: SnapshotDir::new(snapshots_dir.path().into()),
        log_id: LogId::from(partition_id),
        min_applied_lsn: snapshot_meta.min_applied_lsn,
        db_comparator_name: snapshot_meta.db_comparator_name.clone(),
        files: snapshot_meta.files.clone(),
        key_range,
        compatibility: snapshot_meta.compatibility.clone(),
        byte_offsets_base_lsn: snapshot_meta.byte_offsets_base_lsn,
        log_byte_offset: snapshot_meta.log_byte_offset,
        partition_store_byte_offset: snapshot_meta.partition_store_byte_offset,
    };

    let mut new_partition_store = manager
        .open_from_snapshot(
            &Partition::new(partition_id, KeyRange::new(0, PartitionKey::MAX - 1)),
            snapshot,
        )
        .await
        .unwrap();

    verify_restored_data(&mut new_partition_store).await;
}

/// The position of the test data.
const POSITION: FsmPosition = FsmPosition {
    applied_lsn: Lsn::new(100),
    byte_offsets_base_lsn: Lsn::new(50),
    log_byte_offset: 1000,
    partition_store_byte_offset: 2000,
};

async fn insert_test_data(partition: &mut PartitionStore) {
    let mut txn = partition.transaction();
    txn.put_applied_lsn(Lsn::new(100)).unwrap();
    put_fsm_position(&mut txn, &POSITION).unwrap();
    txn.commit().await.expect("commit succeeds");
}

async fn verify_restored_data(partition: &mut PartitionStore) {
    assert_eq!(
        Lsn::new(100),
        partition.get_applied_lsn().await.unwrap().unwrap()
    );
    assert_eq!(partition.get_fsm_position().await.unwrap(), Some(POSITION));
}

/// While a fence holds, a transaction buffers writes but commits none of them, and refuses a
/// second fence. Commits wait even with nothing to commit, and neither cancelling a waiting commit
/// nor clearing the transaction lifts the fence. Dropping the fence lets the commit through.
#[restate_core::test]
async fn commit_fence_holds_back_commits() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .unwrap();
    let mut reader = store.clone();
    let mut txn = store.transaction();

    let fence = txn.fence_commits().unwrap();
    assert!(txn.fence_commits().is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), txn.commit())
            .await
            .is_err()
    );
    txn.clear();
    txn.put_applied_lsn(Lsn::new(100)).unwrap();
    {
        let mut commit = std::pin::pin!(txn.commit());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut commit)
                .await
                .is_err()
        );
        assert_eq!(reader.get_applied_lsn().await.unwrap(), None);

        drop(fence);
        commit.await.unwrap();
    }
    assert_eq!(reader.get_applied_lsn().await.unwrap(), Some(Lsn::new(100)));
    assert!(txn.fence_commits().is_some());
}

/// A snapshot records the version that the barrier check requires of its data, counting storage
/// features this server doesn't know, and whichever of those and the version barrier is newer. It
/// also records the partition's storage and partition features.
#[restate_core::test]
async fn snapshot_records_what_the_barrier_check_requires() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .unwrap();
    let config = Configuration::pinned().clone();
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    insert_test_data(&mut store).await;
    {
        let mut txn = store.transaction();
        txn.put_state_machine_features(&PersistedFeatures {
            journal_v2: true,
            vqueues: true,
            ..PersistedFeatures::default()
        })
        .unwrap();
        txn.commit().await.unwrap();
    }
    let partition_id = store.partition_id();
    put_storage_features_json(
        &mut store,
        partition_id,
        br#"{ "features": { "future-feature": { "min_required_version": "9.0.0" } } }"#,
    )
    .await
    .unwrap();

    let snapshots_dir = tempdir().unwrap();
    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            SnapshotId::new(),
        )
        .await
        .unwrap();
    let compatibility = snapshot.compatibility.clone().unwrap();
    assert_eq!(
        compatibility.min_restate_version,
        SemanticRestateVersion::new(9, 0, 0)
    );
    assert_eq!(compatibility.storage_features, ["future-feature"]);
    assert_eq!(compatibility.partition_features, ["journal_v2", "vqueues"]);
    let error = store
        .verify_and_run_migrations_at_version(
            &SemanticRestateVersion::new(8, 9, 0),
            CancellationToken::new(),
            &config,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MigrationError::StorageFeatureVersionBarrier { required_min_version, .. }
            if required_min_version == compatibility.min_restate_version
    ));

    {
        let mut txn = store.transaction();
        txn.put_min_restate_version(&SemanticRestateVersion::new(9, 1, 0))
            .unwrap();
        txn.commit().await.unwrap();
    }

    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            SnapshotId::new(),
        )
        .await
        .unwrap();
    let compatibility = snapshot.compatibility.clone().unwrap();
    assert_eq!(
        compatibility.min_restate_version,
        SemanticRestateVersion::new(9, 1, 0)
    );
    let error = store
        .verify_and_run_migrations_at_version(
            &SemanticRestateVersion::new(9, 0, 0),
            CancellationToken::new(),
            &config,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MigrationError::VersionBarrier { required_min_version }
            if required_min_version == compatibility.min_restate_version
    ));
    store
        .verify_and_run_migrations_at_version(
            &compatibility.min_restate_version,
            CancellationToken::new(),
            &config,
        )
        .await
        .unwrap();
}
