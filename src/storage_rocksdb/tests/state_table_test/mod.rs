use crate::assert_stream_eq;
use bytes::Bytes;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::{Storage, Transaction};
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::ServiceId;

async fn populate_data<T: StateTable>(table: &mut T) {
    table
        .put_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k1"),
            &Bytes::from_static(b"v1"),
        )
        .await;

    table
        .put_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k2"),
            &Bytes::from_static(b"v2"),
        )
        .await;

    table
        .put_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
            &Bytes::from_static(b"k2"),
            &Bytes::from_static(b"v2"),
        )
        .await;
}

async fn point_lookup<T: StateTable>(table: &mut T) {
    let result = table
        .get_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k1"),
        )
        .await
        .expect("should not fail");

    assert_eq!(result, Some(Bytes::from_static(b"v1")));
}

async fn prefix_scans<T: StateTable>(table: &mut T) {
    let result = table.get_all_user_states(&ServiceId::with_partition_key(1337, "svc-1", "key-1"));

    let expected = vec![
        (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
        (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
    ];

    assert_stream_eq(result, expected).await;
}

async fn deletes<T: StateTable>(table: &mut T) {
    table
        .delete_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k2"),
        )
        .await;
}

async fn verify_delete<T: StateTable>(table: &mut T) {
    let result = table
        .get_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k2"),
        )
        .await
        .expect("should not fail");

    assert!(result.is_none());
}

async fn verify_prefix_scan_after_delete<T: StateTable>(table: &mut T) {
    let result = table.get_all_user_states(&ServiceId::with_partition_key(1337, "svc-1", "key-1"));

    let expected = vec![(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))];

    assert_stream_eq(result, expected).await;
}

pub(crate) async fn run_tests(rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    point_lookup(&mut txn).await;
    prefix_scans(&mut txn).await;

    let mut txn = rocksdb.transaction();
    deletes(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_delete(&mut txn).await;
    verify_prefix_scan_after_delete(&mut txn).await;
}
