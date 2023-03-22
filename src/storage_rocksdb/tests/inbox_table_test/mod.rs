use crate::assert_stream_eq;
use common::types::ServiceId;
use storage_api::inbox_table::InboxTable;
use storage_api::{Storage, Transaction};
use storage_proto::storage::v1::InboxEntry;
use storage_rocksdb::RocksDBStorage;

async fn populate_data<T: InboxTable>(table: &mut T) {
    table
        .put_invocation(
            1337,
            &ServiceId::new("svc-1", "key-1"),
            7,
            InboxEntry::default(),
        )
        .await;

    table
        .put_invocation(
            1337,
            &ServiceId::new("svc-1", "key-1"),
            8,
            InboxEntry::default(),
        )
        .await;

    table
        .put_invocation(
            1337,
            &ServiceId::new("svc-1", "key-2"),
            9,
            InboxEntry::default(),
        )
        .await;
}

async fn find_the_next_message_in_an_inbox<T: InboxTable>(table: &mut T) {
    let result = table
        .peek_inbox(1337, &ServiceId::new("svc-1", "key-1"))
        .await;

    assert_eq!(result.unwrap(), Some((7, InboxEntry::default())));
}

async fn get_svc_inbox<T: InboxTable>(table: &mut T) {
    let stream = table.inbox(1337, &ServiceId::new("svc-1", "key-1"));

    let vec = vec![(7, InboxEntry::default()), (8, InboxEntry::default())];

    assert_stream_eq(stream, vec).await;
}

async fn delete_entry<T: InboxTable>(table: &mut T) {
    table
        .delete_invocation(1337, &ServiceId::new("svc-1", "key-1"), 7)
        .await;
}

async fn peek_after_delete<T: InboxTable>(table: &mut T) {
    let result = table
        .peek_inbox(1337, &ServiceId::new("svc-1", "key-1"))
        .await;

    assert_eq!(result.unwrap(), Some((8, InboxEntry::default())));
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
