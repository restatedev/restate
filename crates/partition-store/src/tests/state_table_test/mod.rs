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
use std::task::{Context, Wake, Waker};

use bytes::Bytes;
use bytestring::ByteString;
use futures::{Stream, StreamExt};

use restate_memory::{
    ByteCount, LendableMemoryPool, LocalMemoryPool, MemoryPool, NonZeroByteCount, OutOfMemoryKind,
    PinnableMemoryStream,
};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::{BudgetedReadError, Transaction};
use restate_types::Scope;
use restate_types::identifiers::ServiceId;
use restate_util_string::RestateString;

use super::{assert_stream_eq, low_pri_executor, storage_test_environment};
use crate::PartitionStore;
use crate::budgeted_reads::MAX_INLINE_ROWS;

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

async fn verify_budgeted_point_reads_after_delete(store: &mut PartitionStore) {
    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let mut budget = LocalMemoryPool::unlimited();

    // A deleted or absent key reads as `None`, a present one comes with its lease.
    for (key, expected) in [("k2", None), ("absent", None), ("k1", Some("v1"))] {
        let got = store
            .get_user_state_budgeted(&service_id, &ByteString::from_static(key), &mut budget)
            .await
            .expect("point read should not fail")
            .map(|(value, lease)| {
                assert_eq!(lease.size(), key.len() + value.len());
                value
            });
        assert_eq!(
            got,
            expected.map(|value| Bytes::from_static(value.as_bytes()))
        );
    }
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

async fn verify_budgeted_prefix_scan_after_delete(store: &mut PartitionStore) {
    let executor = low_pri_executor(store);
    let service_id = &ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let snapshot = store.snapshot().unwrap();
    let mut budget = LendableMemoryPool::new(LocalMemoryPool::unlimited());
    let stream = snapshot
        .get_all_user_states_budgeted(service_id, budget.claim().await, &executor)
        .unwrap();
    let got: Vec<_> = stream
        .map(|entry| {
            let (key, value, lease) = entry.unwrap();
            assert!(lease.size() >= key.len() + value.len());
            (key, value)
        })
        .collect()
        .await;
    assert_eq!(
        got,
        vec![(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))]
    );
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();

    populate_data(&mut txn);
    point_lookup(&mut txn).await;
    prefix_scans(&txn).await;
    deletes(&mut txn);

    txn.commit().await.expect("should not fail");
    drop(txn);

    let mut txn = rocksdb.transaction();
    verify_delete(&mut txn).await;
    verify_prefix_scan_after_delete(&txn).await;
    drop(txn);

    verify_budgeted_point_reads_after_delete(&mut rocksdb).await;
    verify_budgeted_prefix_scan_after_delete(&mut rocksdb).await;
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

        let mut txn = writer.transaction();
        txn.put_user_state(&service_id, &k2, b"buffered").unwrap();
        txn.commit().await.unwrap();
        drop(txn);

        let mut snapshot_store = store.clone();
        let snapshot = snapshot_store.snapshot().unwrap();
        let keys = [
            ByteString::from_static("k2"),
            ByteString::from_static("absent"),
            ByteString::from_static("k1"),
        ];
        let mut live_budget = LocalMemoryPool::unlimited();
        let (pool, budget) = bounded_budget(1024, 1024);
        let mut budget = LendableMemoryPool::new(budget);
        let mut frozen = Box::pin(
            snapshot
                .get_user_states_budgeted(&service_id, &keys, budget.claim().await)
                .unwrap(),
        );
        assert_eq!(pool.used(), ByteCount::ZERO);

        // Writes after the snapshot must be visible to the live reader,
        // but not to the snapshot.
        let mut txn = writer.transaction();
        txn.put_user_state(&service_id, &k2, b"new").unwrap();
        txn.commit().await.unwrap();
        drop(txn);
        let (value, lease) = store
            .get_user_state_budgeted(&service_id, &keys[0], &mut live_budget)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value, Bytes::from_static(b"new"));
        assert_eq!(lease.size(), 5);
        let (key, value, lease) = frozen.next().await.unwrap().unwrap();
        assert_eq!((key, value), (k2, Bytes::from_static(b"buffered")));
        assert_eq!(lease.size(), 10);
        // The next key is not read when yielding the first entry.
        assert_eq!(pool.used(), ByteCount::from(10_usize));

        let (key, value, _) = frozen.next().await.unwrap().unwrap();
        assert_eq!((key, value), (k1, Bytes::from_static(b"old")));
        assert!(frozen.next().await.is_none());
    }
}

#[restate_core::test]
async fn budgeted_point_reads_wait_wake_and_cancel() {
    let mut store = storage_test_environment().await;
    let mut txn = store.transaction();
    populate_data(&mut txn);
    txn.commit().await.unwrap();
    drop(txn);
    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let keys = [ByteString::from_static("k1")];
    let mut snapshot_store = store.clone();
    let snapshot = snapshot_store.snapshot().unwrap();
    let (_pool, mut budget) = bounded_budget(4, 4);
    let held = budget.try_reserve(4).unwrap();
    let mut budget = LendableMemoryPool::new(budget);
    let counter = Arc::new(WakeCounter::default());
    let waker = counter.clone().into();
    let mut cx = Context::from_waker(&waker);

    // Cancellation while waiting must not leak a reservation.
    let mut stream = Box::pin(
        snapshot
            .get_user_states_budgeted(&service_id, &keys, budget.claim().await)
            .unwrap(),
    );
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    drop(stream);
    assert_eq!(budget.claim().await.in_flight(), 4);

    let mut stream = Box::pin(
        snapshot
            .get_user_states_budgeted(&service_id, &keys, budget.claim().await)
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
    assert_eq!(budget.claim().await.in_flight(), 0);

    // A non-snapshot point read must recheck a value that grows while waiting.
    // Cancelling the second wait must also release the already acquired portion.
    let mut writer = store.clone();
    let (_pool, mut budget) = bounded_budget(8, 8);
    let first_held = budget.try_reserve(4).unwrap();
    let second_held = budget.try_reserve(4).unwrap();
    let mut read = Box::pin(store.get_user_state_budgeted(&service_id, &keys[0], &mut budget));
    assert!(read.as_mut().poll(&mut cx).is_pending());
    let mut txn = writer.transaction();
    txn.put_user_state(&service_id, &Bytes::from_static(b"k1"), b"longer")
        .unwrap();
    txn.commit().await.unwrap();
    drop(txn);
    drop(first_held);
    assert!(read.as_mut().poll(&mut cx).is_pending());
    drop(read);
    assert_eq!(budget.in_flight(), 4);
    drop(second_held);
    assert_eq!(budget.in_flight(), 0);
}

/// Scans the states of `service_id`, releasing each row's budget once it's read.
async fn budgeted_scan_keys(
    store: &mut PartitionStore,
    service_id: &ServiceId,
    budget: LocalMemoryPool,
) -> Vec<Bytes> {
    let executor = low_pri_executor(store);
    let snapshot = store.snapshot().unwrap();
    let mut budget = LendableMemoryPool::new(budget);
    let keys = snapshot
        .get_all_user_states_budgeted(service_id, budget.claim().await, &executor)
        .unwrap()
        .map(|entry| entry.unwrap().0)
        .collect()
        .await;
    // The scan gave the budget back, with nothing reserved.
    assert_eq!(budget.claim().await.in_flight(), 0);
    keys
}

#[restate_core::test]
async fn budgeted_scans_continue_on_a_storage_worker() {
    let mut store = storage_test_environment().await;
    let executor = low_pri_executor(&store);
    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");
    // More rows than a scan reads on its caller's task.
    let keys: Vec<_> = (0..2 * MAX_INLINE_ROWS + 1)
        .map(|i| Bytes::from(format!("k{i:05}")))
        .collect();
    let mut txn = store.transaction();
    for key in &keys {
        txn.put_user_state(&service_id, key, b"value").unwrap();
    }
    // A neighbour, which the scans must not reach.
    let neighbour = ServiceId::with_partition_key(1337, "svc-1", "key-2");
    txn.put_user_state(&neighbour, &keys[0], b"value").unwrap();
    txn.commit().await.unwrap();
    drop(txn);
    // What a row costs: the size of its stored key and value. The rows are all
    // the same size.
    let row_cost = {
        let snapshot = store.snapshot().unwrap();
        let mut budget = LendableMemoryPool::new(LocalMemoryPool::unlimited());
        let mut stream = Box::pin(
            snapshot
                .get_all_user_states_budgeted(&service_id, budget.claim().await, &executor)
                .unwrap(),
        );
        stream.next().await.unwrap().unwrap().2.size()
    };

    // The storage worker takes over after the rows read inline, at the first
    // row that has to wait for budget, and at the first row that isn't in
    // memory. Either way, every row arrives once, in order.
    let unlimited = LocalMemoryPool::unlimited();
    assert_eq!(
        budgeted_scan_keys(&mut store, &service_id, unlimited).await,
        keys
    );
    let (_pool, three_rows) = bounded_budget(3 * row_cost, 3 * row_cost);
    assert_eq!(
        budgeted_scan_keys(&mut store, &service_id, three_rows).await,
        keys
    );
    store.partition_db().flush_memtables(true).await.unwrap();
    let unlimited = LocalMemoryPool::unlimited();
    assert_eq!(
        budgeted_scan_keys(&mut store, &service_id, unlimited).await,
        keys
    );

    // Dropping the stream while the storage worker waits for budget cancels
    // the scan, and the budget comes back.
    let snapshot = store.snapshot().unwrap();
    let (_pool, mut budget) = bounded_budget(row_cost, row_cost);
    let held = budget.try_reserve(row_cost).unwrap();
    let mut budget = LendableMemoryPool::new(budget);
    let mut cx = Context::from_waker(Waker::noop());
    let mut stream = Box::pin(
        snapshot
            .get_all_user_states_budgeted(&service_id, budget.claim().await, &executor)
            .unwrap(),
    );
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    drop(stream);
    assert_eq!(budget.claim().await.in_flight(), row_cost);

    // Once the budget is free, the storage worker goes on.
    let mut stream = Box::pin(
        snapshot
            .get_all_user_states_budgeted(&service_id, budget.claim().await, &executor)
            .unwrap(),
    );
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    drop(held);
    let (key, _value, lease) = stream.next().await.unwrap().unwrap();
    assert_eq!(key, keys[0]);
    drop((stream, lease));
    assert_eq!(budget.claim().await.in_flight(), 0);

    // A row that can never fit fails the scan.
    let (_pool, too_small) = bounded_budget(row_cost - 1, row_cost - 1);
    let mut budget = LendableMemoryPool::new(too_small);
    let mut stream = Box::pin(
        snapshot
            .get_all_user_states_budgeted(&service_id, budget.claim().await, &executor)
            .unwrap(),
    );
    let failed = stream.next().await;
    assert!(
        matches!(failed, Some(Err(BudgetedReadError::OutOfMemory { .. }))),
        "{failed:?}"
    );
    assert!(stream.next().await.is_none());
}

#[restate_core::test]
async fn budgeted_point_reads_reject_infeasible_reservations() {
    let mut store = storage_test_environment().await;
    let mut txn = store.transaction();
    populate_data(&mut txn);
    txn.commit().await.unwrap();
    drop(txn);
    let snapshot = store.snapshot().unwrap();
    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let keys = [ByteString::from_static("k1"), ByteString::from_static("k2")];

    for (capacity, upper_bound, pin_first, expected_kind) in [
        (4, 3, false, OutOfMemoryKind::UpperBoundExceeded),
        (8, 4, true, OutOfMemoryKind::UpperBoundExceeded),
        (4, 8, true, OutOfMemoryKind::PoolExhausted),
    ] {
        let (_pool, budget) = bounded_budget(capacity, upper_bound);
        let mut budget = LendableMemoryPool::new(budget);
        let mut stream = Box::pin(
            snapshot
                .get_user_states_budgeted(&service_id, &keys, budget.claim().await)
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
        assert_eq!(budget.claim().await.in_flight(), 0);
    }
}

#[restate_core::test]
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
}
