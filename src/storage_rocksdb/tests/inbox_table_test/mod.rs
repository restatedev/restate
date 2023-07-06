use crate::{assert_stream_eq, mock_service_invocation};
use restate_storage_api::inbox_table::{InboxEntry, InboxTable};
use restate_storage_api::{Storage, Transaction};
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::ServiceId;

async fn populate_data<T: InboxTable>(table: &mut T) {
    table
        .put_invocation(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            InboxEntry::new(7, mock_service_invocation()),
        )
        .await;

    table
        .put_invocation(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            InboxEntry::new(8, mock_service_invocation()),
        )
        .await;

    table
        .put_invocation(
            &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
            InboxEntry::new(9, mock_service_invocation()),
        )
        .await;
}

async fn find_the_next_message_in_an_inbox<T: InboxTable>(table: &mut T) {
    let result = table
        .peek_inbox(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
        .await;

    assert_eq!(
        result.unwrap(),
        Some(InboxEntry::new(7, mock_service_invocation()))
    );
}

async fn get_svc_inbox<T: InboxTable>(table: &mut T) {
    let stream = table.inbox(&ServiceId::with_partition_key(1337, "svc-1", "key-1"));

    let vec = vec![
        InboxEntry::new(7, mock_service_invocation()),
        InboxEntry::new(8, mock_service_invocation()),
    ];

    assert_stream_eq(stream, vec).await;
}

async fn delete_entry<T: InboxTable>(table: &mut T) {
    table
        .delete_invocation(&ServiceId::with_partition_key(1337, "svc-1", "key-1"), 7)
        .await;
}

async fn peek_after_delete<T: InboxTable>(table: &mut T) {
    let result = table
        .peek_inbox(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
        .await;

    assert_eq!(
        result.unwrap(),
        Some(InboxEntry::new(8, mock_service_invocation()))
    );
}

pub(crate) async fn run_tests(rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    find_the_next_message_in_an_inbox(&mut txn).await;
    get_svc_inbox(&mut txn).await;

    let mut txn = rocksdb.transaction();
    delete_entry(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    peek_after_delete(&mut txn).await;
}
