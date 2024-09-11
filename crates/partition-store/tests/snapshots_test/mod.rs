use restate_partition_store::snapshots::{LocalPartitionSnapshot, PartitionSnapshotMetadata};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::fsm_table::{FsmTable, ReadOnlyFsmTable};
use restate_storage_api::Transaction;
use restate_types::config::WorkerOptions;
use restate_types::identifiers::PartitionKey;
use restate_types::live::Live;
use restate_types::logs::Lsn;
use std::ops::RangeInclusive;
use tempfile::tempdir;
use tracing::instrument;

#[instrument(skip_all, level = "warn")]
pub(crate) async fn run_tests(manager: PartitionStoreManager, mut partition_store: PartitionStore) {
    insert_test_data(&mut partition_store).await;

    let snapshots_dir = tempdir().unwrap();

    let partition_id = partition_store.partition_id();
    let path_buf = snapshots_dir.path().to_path_buf().join("sn1");

    let snapshot = partition_store.create_snapshot(path_buf).await.unwrap();

    let snapshot_meta = PartitionSnapshotMetadata {
        cluster_name: "cluster_name".to_string(),
        partition_id,
        key_range: partition_store.partition_key_range().clone(),
        min_applied_lsn: snapshot.min_applied_lsn,
        files: snapshot.files.clone(),
    };
    let metadata_json = serde_json::to_string_pretty(&snapshot_meta).unwrap();
    let db_comparator_name = snapshot.db_comparator_name.clone();

    drop(partition_store);
    drop(snapshot);
    manager.drop_partition(partition_id).await;

    let snapshot_meta: PartitionSnapshotMetadata =
        serde_json::from_str(metadata_json.as_str()).unwrap();
    let snapshot = LocalPartitionSnapshot {
        base_dir: snapshots_dir.path().into(),
        db_comparator_name,
        min_applied_lsn: snapshot_meta.min_applied_lsn,
        files: snapshot_meta.files.clone(),
    };

    let worker_options = Live::from_value(WorkerOptions::default());

    let mut new_partition_store = manager
        .create_partition_store_from_snapshot(
            partition_id,
            RangeInclusive::new(0, PartitionKey::MAX - 1),
            snapshot,
            &worker_options.pinned().storage.rocksdb,
        )
        .await
        .unwrap();

    verify_restored_data(&mut new_partition_store).await;
}

async fn insert_test_data(partition: &mut PartitionStore) {
    let mut txn = partition.transaction();
    txn.put_applied_lsn(Lsn::new(100)).await;
    txn.commit().await.expect("commit succeeds");
}

async fn verify_restored_data(partition: &mut PartitionStore) {
    assert_eq!(
        Lsn::new(100),
        partition.get_applied_lsn().await.unwrap().unwrap()
    );
}
