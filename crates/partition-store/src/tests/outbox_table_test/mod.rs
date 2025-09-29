// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::mock_random_service_invocation;

use crate::PartitionStore;
use restate_storage_api::Transaction;
use restate_storage_api::outbox_table::{OutboxMessage, ReadOutboxTable, WriteOutboxTable};

fn mock_outbox_message() -> OutboxMessage {
    OutboxMessage::ServiceInvocation(mock_random_service_invocation())
}

pub(crate) async fn populate_data<T: WriteOutboxTable>(txn: &mut T, initial: Vec<u64>) {
    for seq_no in initial {
        txn.put_outbox_message(seq_no, &mock_outbox_message())
            .unwrap();
    }
}

pub(crate) async fn verify_outbox_head_seq_number<T: ReadOutboxTable>(
    txn: &mut T,
    expected: Option<u64>,
) {
    let head = txn
        .get_outbox_head_seq_number()
        .await
        .expect("should not fail");
    assert_eq!(expected, head);
}

pub(crate) async fn consume_messages_and_truncate_range<T: ReadOutboxTable + WriteOutboxTable>(
    txn: &mut T,
    expected: Vec<u64>,
) {
    for seq_no in expected.clone() {
        let msg = txn
            .get_next_outbox_message(seq_no)
            .await
            .expect("should not fail");
        assert!(msg.is_some(), "message should be present in outbox");
    }

    txn.truncate_outbox(*expected.first().unwrap()..=*expected.last().unwrap())
        .unwrap();
}

pub(crate) async fn verify_outbox_is_empty_after_truncation<T: ReadOutboxTable>(txn: &mut T) {
    let result = txn
        .get_next_outbox_message(0)
        .await
        .expect("should not fail");

    assert_eq!(result, None);
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();
    verify_outbox_head_seq_number(&mut txn, None).await;
    populate_data(&mut txn, vec![0, 1, 2, 3]).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_outbox_head_seq_number(&mut txn, Some(0)).await;
    consume_messages_and_truncate_range(&mut txn, vec![0, 1, 2]).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_outbox_head_seq_number(&mut txn, Some(3)).await;
    consume_messages_and_truncate_range(&mut txn, vec![3]).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_outbox_is_empty_after_truncation(&mut txn).await;
}
