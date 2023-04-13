use crate::mock_service_invocation;
use common::types::OutboxMessage;
use storage_api::outbox_table::OutboxTable;
use storage_api::{Storage, Transaction};
use storage_rocksdb::RocksDBStorage;

fn mock_outbox_message() -> OutboxMessage {
    OutboxMessage::ServiceInvocation(mock_service_invocation())
}

pub(crate) async fn populate_data<T: OutboxTable>(txn: &mut T) {
    txn.add_message(1337, 0, mock_outbox_message()).await;
    txn.add_message(1337, 1, mock_outbox_message()).await;
    txn.add_message(1337, 2, mock_outbox_message()).await;
    txn.add_message(1337, 3, mock_outbox_message()).await;
}

pub(crate) async fn consume_message_and_truncate<T: OutboxTable>(txn: &mut T) {
    let mut sequence = 0;
    while let Ok(Some((seq, _))) = txn.get_next_outbox_message(1337, sequence).await {
        sequence = seq + 1;
    }
    assert_eq!(sequence, 4);

    txn.truncate_outbox(1337, 0..sequence).await;
}

pub(crate) async fn verify_outbox_is_empty_after_truncation<T: OutboxTable>(txn: &mut T) {
    let result = txn
        .get_next_outbox_message(1337, 0)
        .await
        .expect("should not fail");

    assert_eq!(result, None);
}

pub(crate) async fn run_tests(rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    consume_message_and_truncate(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_outbox_is_empty_after_truncation(&mut txn).await;
}
