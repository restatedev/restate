// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{assert_stream_eq, storage_test_environment};

use crate::PartitionStore;
use bytes::Bytes;
use futures::StreamExt;
use restate_memory::LocalMemoryPool;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_types::identifiers::ServiceId;

fn populate_data<T: WriteStateTable>(table: &mut T) {
    table
        .put_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k1"),
            Bytes::from_static(b"v1"),
        )
        .expect("");

    table
        .put_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k2"),
            Bytes::from_static(b"v2"),
        )
        .unwrap();

    table
        .put_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
            &Bytes::from_static(b"k2"),
            Bytes::from_static(b"v2"),
        )
        .unwrap();
}

async fn point_lookup<T: ReadStateTable>(table: &mut T) {
    let result = table
        .get_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k1"),
        )
        .await
        .expect("should not fail");

    assert_eq!(result, Some(Bytes::from_static(b"v1")));
}

async fn prefix_scans<T: ReadStateTable>(table: &T) {
    let service_id = &ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let result = table.get_all_user_states_for_service(service_id).unwrap();

    let expected = vec![
        (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
        (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
    ];

    assert_stream_eq(result, expected).await;
}

async fn point_reads_budgeted<T: ReadStateTable>(table: &T) {
    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let mut budget = LocalMemoryPool::unlimited();

    // Whitelist interleaves present keys with an absent one: absent keys are
    // omitted, present ones are returned with their leases.
    let keys = vec![
        Bytes::from_static(b"k1"),
        Bytes::from_static(b"absent"),
        Bytes::from_static(b"k2"),
    ];
    let stream = table
        .get_user_states_budgeted(&service_id, keys, &mut budget)
        .unwrap();
    let mut got: Vec<(Bytes, Bytes)> = stream
        .map(|entry| {
            let (key, value, _lease) = entry.expect("point read should not fail");
            (key, value)
        })
        .collect()
        .await;
    got.sort();

    assert_eq!(
        got,
        vec![
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
        ]
    );
}

fn deletes<T: WriteStateTable>(table: &mut T) {
    table
        .delete_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k2"),
        )
        .unwrap();
}

async fn verify_delete<T: ReadStateTable>(table: &mut T) {
    let result = table
        .get_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k2"),
        )
        .await
        .expect("should not fail");

    assert!(result.is_none());
}

async fn verify_prefix_scan_after_delete<T: ReadStateTable>(table: &T) {
    let service_id = &ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let result = table.get_all_user_states_for_service(service_id).unwrap();

    let expected = vec![(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))];

    assert_stream_eq(result, expected).await;
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();

    populate_data(&mut txn);
    point_lookup(&mut txn).await;
    prefix_scans(&txn).await;
    point_reads_budgeted(&txn).await;
    deletes(&mut txn);

    txn.commit().await.expect("should not fail");
    drop(txn);

    let mut txn = rocksdb.transaction();
    verify_delete(&mut txn).await;
    verify_prefix_scan_after_delete(&txn).await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_all() {
    let mut rocksdb = storage_test_environment().await;

    let mut txn = rocksdb.transaction();

    populate_data(&mut txn);
    txn.commit().await.expect("should not fail");
    drop(txn);

    // Do delete all
    let mut txn = rocksdb.transaction();
    txn.delete_all_user_state(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
        .unwrap();
    txn.commit().await.expect("should not fail");
    drop(txn);

    // No more state for key-1
    let mut txn = rocksdb.transaction();
    assert_stream_eq(
        txn.get_all_user_states_for_service(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
            .unwrap(),
        vec![],
    )
    .await;

    // key-2 should be untouched
    assert!(
        txn.get_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
            &Bytes::from_static(b"k2"),
        )
        .await
        .expect("should not fail")
        .is_some()
    );

    RocksDbManager::get().shutdown().await;
}
