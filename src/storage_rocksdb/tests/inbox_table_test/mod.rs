use crate::{assert_stream_eq, uuid_str};
use bytes::Bytes;
use bytestring::ByteString;
use common::types::{InboxEntry, ServiceId, ServiceInvocation, ServiceInvocationId, SpanRelation};
use storage_api::inbox_table::InboxTable;
use storage_api::{Storage, Transaction};
use storage_rocksdb::RocksDBStorage;

fn mock_service_invocation() -> ServiceInvocation {
    let (service_invocation, _) = ServiceInvocation::new(
        ServiceInvocationId::new(
            ByteString::from_static("service"),
            Bytes::new(),
            uuid_str("018756fa-3f7f-7854-a76b-42c59a3d7f2d"),
        ),
        ByteString::from_static("service"),
        Bytes::new(),
        None,
        SpanRelation::None,
    );

    service_invocation
}

async fn populate_data<T: InboxTable>(table: &mut T) {
    table
        .put_invocation(
            1337,
            &ServiceId::new("svc-1", "key-1"),
            InboxEntry::new(7, mock_service_invocation()),
        )
        .await;

    table
        .put_invocation(
            1337,
            &ServiceId::new("svc-1", "key-1"),
            InboxEntry::new(8, mock_service_invocation()),
        )
        .await;

    table
        .put_invocation(
            1337,
            &ServiceId::new("svc-1", "key-2"),
            InboxEntry::new(9, mock_service_invocation()),
        )
        .await;
}

async fn find_the_next_message_in_an_inbox<T: InboxTable>(table: &mut T) {
    let result = table
        .peek_inbox(1337, &ServiceId::new("svc-1", "key-1"))
        .await;

    assert_eq!(
        result.unwrap(),
        Some(InboxEntry::new(7, mock_service_invocation()))
    );
}

async fn get_svc_inbox<T: InboxTable>(table: &mut T) {
    let stream = table.inbox(1337, &ServiceId::new("svc-1", "key-1"));

    let vec = vec![
        InboxEntry::new(7, mock_service_invocation()),
        InboxEntry::new(8, mock_service_invocation()),
    ];

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
