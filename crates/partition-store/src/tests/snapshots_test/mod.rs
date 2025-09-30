// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::SystemTime;

use tempfile::tempdir;

use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::{ReadFsmTable, WriteFsmTable};
use restate_types::identifiers::{PartitionKey, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::partitions::Partition;
use restate_types::time::MillisSinceEpoch;

use crate::snapshots::{LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotFormatVersion};
use crate::{PartitionStore, PartitionStoreManager};

pub(crate) async fn run_tests(
    manager: Arc<PartitionStoreManager>,
    mut partition_store: PartitionStore,
) {
    insert_test_data(&mut partition_store).await;

    let snapshots_dir = tempdir().unwrap();

    let partition_id = partition_store.partition_id();

    let snapshot = partition_store
        .create_local_snapshot(snapshots_dir.path(), None, SnapshotId::new())
        .await
        .unwrap();

    let key_range = partition_store.partition_key_range().clone();
    let snapshot_meta = PartitionSnapshotMetadata {
        version: SnapshotFormatVersion::V1,
        cluster_name: "cluster_name".to_string(),
        cluster_fingerprint: None,
        partition_id,
        node_name: "node".to_string(),
        created_at: humantime::Timestamp::from(SystemTime::from(MillisSinceEpoch::new(0))),
        snapshot_id: SnapshotId::from_parts(0, 0),
        key_range: key_range.clone(),
        log_id: LogId::from(partition_id),
        min_applied_lsn: snapshot.min_applied_lsn,
        db_comparator_name: snapshot.db_comparator_name.clone(),
        files: snapshot.files.clone(),
    };
    let metadata_json = serde_json::to_string_pretty(&snapshot_meta).unwrap();

    drop(partition_store);
    drop(snapshot);

    manager.drop_partition(partition_id).await.unwrap();

    let snapshot_meta: PartitionSnapshotMetadata = serde_json::from_str(&metadata_json).unwrap();

    let snapshot = LocalPartitionSnapshot {
        base_dir: snapshots_dir.path().into(),
        log_id: LogId::from(partition_id),
        min_applied_lsn: snapshot_meta.min_applied_lsn,
        db_comparator_name: snapshot_meta.db_comparator_name.clone(),
        files: snapshot_meta.files.clone(),
        key_range,
    };

    let mut new_partition_store = manager
        .open_from_snapshot(
            &Partition::new(partition_id, RangeInclusive::new(0, PartitionKey::MAX - 1)),
            snapshot,
        )
        .await
        .unwrap();

    verify_restored_data(&mut new_partition_store).await;
}

async fn insert_test_data(partition: &mut PartitionStore) {
    let mut txn = partition.transaction();
    txn.put_applied_lsn(Lsn::new(100)).unwrap();
    txn.commit().await.expect("commit succeeds");
}

async fn verify_restored_data(partition: &mut PartitionStore) {
    assert_eq!(
        Lsn::new(100),
        partition.get_applied_lsn().await.unwrap().unwrap()
    );
}
