// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_partition_store::PartitionStore;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::Transaction;
use restate_types::identifiers::PartitionId;

use crate::mock_random_service_invocation;

fn mock_outbox_message() -> OutboxMessage {
    OutboxMessage::ServiceInvocation(mock_random_service_invocation())
}

pub(crate) async fn populate_data<T: OutboxTable>(txn: &mut T, initial: Vec<u64>) {
    txn.add_message(PartitionId::from(1336), 0, mock_outbox_message())
        .await;

    let partition1337 = PartitionId::from(1337);
    for seq_no in initial {
        txn.add_message(partition1337, seq_no, mock_outbox_message())
            .await;
    }

    txn.add_message(PartitionId::from(1338), 0, mock_outbox_message())
        .await;
}

pub(crate) async fn verify_outbox_head_seq_number<T: OutboxTable>(
    txn: &mut T,
    expected: Option<u64>,
) {
    let head = txn
        .get_outbox_head_seq_number(PartitionId::from(1337))
        .await
        .expect("should not fail");
    assert_eq!(expected, head);
}

pub(crate) async fn consume_messages_and_truncate_range<T: OutboxTable>(
    txn: &mut T,
    expected: Vec<u64>,
) {
    let partition1337 = PartitionId::from(1337);

    for seq_no in expected.clone() {
        let msg = txn
            .get_next_outbox_message(partition1337, seq_no)
            .await
            .expect("should not fail");
        assert!(msg.is_some(), "message should be present in outbox");
    }

    txn.truncate_outbox(
        partition1337,
        *expected.first().unwrap()..=*expected.last().unwrap(),
    )
    .await;
}

pub(crate) async fn verify_outbox_is_empty_after_truncation<T: OutboxTable>(txn: &mut T) {
    let partition1337 = PartitionId::from(1337);
    let result = txn
        .get_next_outbox_message(partition1337, 0)
        .await
        .expect("should not fail");

    assert_eq!(result, None);
}

pub(crate) async fn verify_neighbor_partitions_unchanged<T: OutboxTable>(txn: &mut T) {
    let partition1336 = PartitionId::from(1336);
    let predecessor_partition = txn
        .get_next_outbox_message(partition1336, 0)
        .await
        .expect("should not fail");

    let partition1338 = PartitionId::from(1338);
    let successor_partition = txn
        .get_next_outbox_message(partition1338, 0)
        .await
        .expect("should not fail");

    assert!(successor_partition.is_some());
    assert!(predecessor_partition.is_some());
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();
    verify_outbox_head_seq_number(&mut txn, None).await;
    populate_data(&mut txn, vec![0, 1, 2, 3]).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_outbox_head_seq_number(&mut txn, Some(0)).await;
    consume_messages_and_truncate_range(&mut txn, vec![0, 1, 2]).await;
    verify_neighbor_partitions_unchanged(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_outbox_head_seq_number(&mut txn, Some(3)).await;
    consume_messages_and_truncate_range(&mut txn, vec![3]).await;
    verify_neighbor_partitions_unchanged(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_outbox_is_empty_after_truncation(&mut txn).await;
    verify_neighbor_partitions_unchanged(&mut txn).await;
}
