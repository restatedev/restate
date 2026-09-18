// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake};

use bytes::Bytes;
use bytestring::ByteString;
use futures::{Stream, StreamExt};

use restate_memory::{
    ByteCount, LocalMemoryPool, MemoryPool, NonZeroByteCount, OutOfMemoryKind, PinnableMemoryStream,
};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::{BudgetedReadError, IsolationLevel, Transaction};
use restate_types::Scope;
use restate_types::identifiers::ServiceId;
use restate_util_string::RestateString;

use super::{assert_stream_eq, storage_test_environment};
use crate::PartitionStore;

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

    assert_stream_eq(result, expected.clone()).await;

    let mut budget = LocalMemoryPool::unlimited();
    let stream = table
        .get_all_user_states_budgeted(service_id, &mut budget)
        .unwrap();
    let got: Vec<_> = stream
        .map(|entry| {
            let (key, value, lease) = entry.unwrap();
            assert!(lease.size() >= key.len() + value.len());
            (key, value)
        })
        .collect()
        .await;
    assert_eq!(got, expected);
}

async fn point_reads_budgeted<T: ReadStateTable>(table: &mut T) {
    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let mut budget = LocalMemoryPool::unlimited();

    // Whitelist interleaves present keys with an absent one: absent keys are
    // omitted, present ones are returned with their leases.
    let keys = vec![
        ByteString::from_static("k2"),
        ByteString::from_static("absent"),
        ByteString::from_static("k1"),
    ];
    let stream = table
        .get_user_states_budgeted(&service_id, keys.as_slice(), &mut budget)
        .unwrap();
    let got: Vec<(Bytes, Bytes)> = stream
        .map(|entry| {
            let (key, value, lease) = entry.expect("point read should not fail");
            assert_eq!(lease.size(), key.len() + value.len());
            (key, value)
        })
        .collect()
        .await;

    assert_eq!(
        got,
        vec![
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
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
    point_reads_budgeted(&mut txn).await;
    deletes(&mut txn);

    txn.commit().await.expect("should not fail");
    drop(txn);

    let mut txn = rocksdb.transaction();
    verify_delete(&mut txn).await;
    verify_prefix_scan_after_delete(&txn).await;
}

fn bounded_budget(capacity: usize, upper_bound: usize) -> (MemoryPool, LocalMemoryPool) {
    let pool =
        MemoryPool::with_capacity(NonZeroByteCount::new(NonZeroUsize::new(capacity).unwrap()));
    let budget = LocalMemoryPool::new(
        pool.clone(),
        pool.empty_lease(),
        ByteCount::ZERO,
        NonZeroByteCount::new(NonZeroUsize::new(upper_bound).unwrap()),
    );
    (pool, budget)
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[restate_core::test]
async fn budgeted_point_reads_are_lazy_and_snapshot_consistent() {
    let mut store = storage_test_environment().await;
    for scope in [None, Some(Scope::try_from_static("scope").unwrap())] {
        let service_id = ServiceId::new(scope, "svc", "key");
        let k1 = Bytes::from_static(b"k1");
        let k2 = Bytes::from_static(b"k2");
        let mut writer = store.clone();
        let mut txn = writer.transaction();
        txn.put_user_state(&service_id, &k1, b"old").unwrap();
        txn.put_user_state(&service_id, &k2, b"old").unwrap();
        txn.commit().await.unwrap();
        drop(txn);

        let mut snapshot_store = store.clone();
        let mut snapshot =
            snapshot_store.transaction_with_isolation(IsolationLevel::RepeatableReads);
        snapshot
            .put_user_state(&service_id, &k2, b"buffered")
            .unwrap();
        let keys = [
            ByteString::from_static("k2"),
            ByteString::from_static("absent"),
            ByteString::from_static("k1"),
        ];
        let (pool, mut budget) = bounded_budget(1024, 1024);
        let mut live = Box::pin(
            store
                .get_user_states_budgeted(&service_id, &keys, &mut budget)
                .unwrap(),
        );
        assert_eq!(pool.used(), ByteCount::ZERO);
        let mut snapshot_budget = LocalMemoryPool::unlimited();
        let mut frozen = Box::pin(
            snapshot
                .get_user_states_budgeted(&service_id, &keys, &mut snapshot_budget)
                .unwrap(),
        );

        // Writes after stream construction must be visible to the live reader,
        // but not to the snapshot (which must also preserve buffered writes).
        let mut txn = writer.transaction();
        txn.put_user_state(&service_id, &k2, b"new").unwrap();
        txn.commit().await.unwrap();
        drop(txn);
        let (key, value, lease) = live.next().await.unwrap().unwrap();
        assert_eq!((key, value), (k2.clone(), Bytes::from_static(b"new")));
        assert_eq!(lease.size(), 5);
        assert_eq!(pool.used(), ByteCount::from(5_usize));
        drop(lease);
        let (key, value, _) = frozen.next().await.unwrap().unwrap();
        assert_eq!((key, value), (k2, Bytes::from_static(b"buffered")));

        // The next key is not prefetched when yielding the first entry.
        let mut txn = writer.transaction();
        txn.put_user_state(&service_id, &k1, b"latest").unwrap();
        txn.commit().await.unwrap();
        drop(txn);
        let (key, value, _) = live.next().await.unwrap().unwrap();
        assert_eq!((key, value), (k1.clone(), Bytes::from_static(b"latest")));
        let (key, value, _) = frozen.next().await.unwrap().unwrap();
        assert_eq!((key, value), (k1, Bytes::from_static(b"old")));
        assert!(live.next().await.is_none());
        assert!(frozen.next().await.is_none());
    }
    RocksDbManager::get().shutdown().await;
}

#[restate_core::test]
async fn budgeted_point_reads_wait_wake_and_cancel() {
    let mut store = storage_test_environment().await;
    let mut txn = store.transaction();
    populate_data(&mut txn);
    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let keys = [ByteString::from_static("k1")];
    let (_pool, mut budget) = bounded_budget(4, 4);
    let held = budget.try_reserve(4).unwrap();
    let counter = Arc::new(WakeCounter::default());
    let waker = counter.clone().into();
    let mut cx = Context::from_waker(&waker);

    // Cancellation while waiting must not leak a reservation.
    let mut stream = Box::pin(
        txn.get_user_states_budgeted(&service_id, &keys, &mut budget)
            .unwrap(),
    );
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    drop(stream);
    assert_eq!(budget.in_flight(), 4);

    let mut stream = Box::pin(
        txn.get_user_states_budgeted(&service_id, &keys, &mut budget)
            .unwrap(),
    );
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    drop(held);
    assert!(counter.0.load(Ordering::Relaxed) > 0);
    let entry = stream.as_mut().poll_next(&mut cx);
    let std::task::Poll::Ready(Some(Ok((key, value, lease)))) = entry else {
        panic!("expected an entry after releasing memory: {entry:?}");
    };
    assert_eq!(
        (key, value),
        (Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
    );
    assert_eq!(lease.size(), 4);
    assert!(stream.next().await.is_none());
    drop(stream);
    drop(lease);
    assert_eq!(budget.in_flight(), 0);
    txn.commit().await.unwrap();
    drop(txn);

    // A non-snapshot point read must recheck a value that grows while waiting.
    // Cancelling the second wait must also release the already acquired portion.
    let mut writer = store.clone();
    let (_pool, mut budget) = bounded_budget(8, 8);
    let first_held = budget.try_reserve(4).unwrap();
    let second_held = budget.try_reserve(4).unwrap();
    let mut stream = Box::pin(
        store
            .get_user_states_budgeted(&service_id, &keys, &mut budget)
            .unwrap(),
    );
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    let mut txn = writer.transaction();
    txn.put_user_state(&service_id, &Bytes::from_static(b"k1"), b"longer")
        .unwrap();
    txn.commit().await.unwrap();
    drop(txn);
    drop(first_held);
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    drop(stream);
    assert_eq!(budget.in_flight(), 4);
    drop(second_held);
    assert_eq!(budget.in_flight(), 0);
    RocksDbManager::get().shutdown().await;
}

#[restate_core::test]
async fn budgeted_point_reads_reject_infeasible_reservations() {
    let mut store = storage_test_environment().await;
    let mut txn = store.transaction();
    populate_data(&mut txn);
    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let keys = [ByteString::from_static("k1"), ByteString::from_static("k2")];

    for (capacity, upper_bound, pin_first, expected_kind) in [
        (4, 3, false, OutOfMemoryKind::UpperBoundExceeded),
        (8, 4, true, OutOfMemoryKind::UpperBoundExceeded),
        (4, 8, true, OutOfMemoryKind::PoolExhausted),
    ] {
        let (_pool, mut budget) = bounded_budget(capacity, upper_bound);
        let mut stream = Box::pin(
            txn.get_user_states_budgeted(&service_id, &keys, &mut budget)
                .unwrap(),
        );
        let retained = if pin_first {
            let entry = stream.next().await.unwrap().unwrap();
            stream.as_mut().pin_memory(entry.2.size());
            Some(entry)
        } else {
            None
        };
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        let result = stream.as_mut().poll_next(&mut cx);
        assert!(
            matches!(result, std::task::Poll::Ready(Some(Err(
                BudgetedReadError::OutOfMemory { kind, .. }
            ))) if kind == expected_kind),
            "expected {expected_kind:?}, got {result:?}"
        );
        drop(stream);
        drop(retained);
        assert_eq!(budget.in_flight(), 0);
    }
    drop(txn);
    RocksDbManager::get().shutdown().await;
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
