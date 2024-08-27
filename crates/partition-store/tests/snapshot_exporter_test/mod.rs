use restate_partition_store::snapshots::SnapshotMetadata;
use restate_partition_store::PartitionStore;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::Transaction;
use restate_types::logs::Lsn;
use std::path::Path;
use tracing::instrument;

#[instrument(skip_all, level = "warn")]
pub(crate) async fn run_tests(mut partition: PartitionStore) {
    std::fs::remove_dir_all("/tmp/snapshot").ok();

    let mut txn = partition.transaction();
    txn.put_applied_lsn(Lsn::new(100)).await;
    txn.commit().await.expect("commit succeeds");

    let snap = partition
        .export_snapshot(Path::new("/tmp/snapshot"))
        .await
        .unwrap();

    let snapshot_meta = SnapshotMetadata {
        cluster_name: "cluster_name".to_string(),
        partition_id: partition.partition_id(),
        key_range: partition.partition_key_range().clone(),
        minimum_lsn: snap.minimum_lsn,
        files: snap.files,
    };
    println!("{}", serde_json::to_string_pretty(&snapshot_meta).unwrap());
}
