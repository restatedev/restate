// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{assert_stream_eq, storage_test_environment};
use bytes::Bytes;
use restate_partition_store::PartitionStore;
use restate_storage_api::state_table::{ReadOnlyStateTable, StateTable};
use restate_storage_api::Transaction;
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
    let service_id = &ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let result = table.get_all_user_states(service_id);

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
    let service_id = &ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let result = table.get_all_user_states(service_id);

    let expected = vec![(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))];

    assert_stream_eq(result, expected).await;
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();

    populate_data(&mut txn).await;
    point_lookup(&mut txn).await;
    prefix_scans(&mut txn).await;
    deletes(&mut txn).await;

    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_delete(&mut txn).await;
    verify_prefix_scan_after_delete(&mut txn).await;
}

#[tokio::test]
async fn test_delete_all() {
    let mut rocksdb = storage_test_environment().await;

    let mut txn = rocksdb.transaction();

    populate_data(&mut txn).await;
    txn.commit().await.expect("should not fail");

    // Do delete all
    let mut txn = rocksdb.transaction();
    txn.delete_all_user_state(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
        .await
        .unwrap();
    txn.commit().await.expect("should not fail");

    // No more state for key-1
    let mut txn = rocksdb.transaction();
    assert_stream_eq(
        txn.get_all_user_states(&ServiceId::with_partition_key(1337, "svc-1", "key-1")),
        vec![],
    )
    .await;

    // key-2 should be untouched
    assert!(txn
        .get_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
            &Bytes::from_static(b"k2"),
        )
        .await
        .expect("should not fail")
        .is_some());
}
