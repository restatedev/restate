use common::types::{ServiceId, ServiceInvocationId};
use futures_util::StreamExt;
use storage_api::timer_table::{TimerKey, TimerTable};
use storage_api::{Storage, Transaction};
use storage_proto::storage::v1::Timer;
use storage_rocksdb::RocksDBStorage;

async fn populate_data<T: TimerTable>(txn: &mut T) {
    txn.add_timer(
        1337,
        &TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("svc-1", "key-1"),
                invocation_id: Default::default(),
            },
            journal_index: 0,
            timestamp: 0,
        },
        Timer::default(),
    )
    .await;

    txn.add_timer(
        1337,
        &TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("svc-1", "key-1"),
                invocation_id: Default::default(),
            },
            journal_index: 1,
            timestamp: 0,
        },
        Timer::default(),
    )
    .await;

    txn.add_timer(
        1337,
        &TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("svc-1", "key-1"),
                invocation_id: Default::default(),
            },
            journal_index: 2,
            timestamp: 1,
        },
        Timer::default(),
    )
    .await;
}

async fn demo_how_to_find_first_timers_in_a_partition<T: TimerTable>(txn: &mut T) {
    // a key that sorts before every other legal key
    let zero = TimerKey {
        service_invocation_id: ServiceInvocationId {
            service_id: ServiceId {
                service_name: Default::default(),
                key: Default::default(),
            },
            invocation_id: Default::default(),
        },
        journal_index: 0,
        timestamp: 0,
    };

    let mut stream = txn.next_timers_greater_than(1337, &zero, usize::MAX);

    let mut count = 0;
    while (stream.next().await).is_some() {
        count += 1;
    }

    assert_eq!(count, 3);
}

async fn find_timers_greater_than<T: TimerTable>(txn: &mut T) {
    let mut stream = txn.next_timers_greater_than(
        1337,
        &TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("svc-1", "key-1"),
                invocation_id: Default::default(),
            },
            journal_index: 0,
            timestamp: 0,
        },
        usize::MAX,
    );

    if let Some(Ok((key, _))) = stream.next().await {
        // make sure that we skip the first timer that has a journal_index of 0
        // take a look at populate_data once again.
        assert_eq!(key.journal_index, 1);
    } else {
        panic!("test failure");
    }

    if let Some(Ok((key, _))) = stream.next().await {
        assert_eq!(key.journal_index, 2);
    } else {
        panic!("test failure");
    }
}

async fn delete_the_first_timer<T: TimerTable>(txn: &mut T) {
    txn.delete_timer(
        1337,
        &TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("svc-1", "key-1"),
                invocation_id: Default::default(),
            },
            journal_index: 0,
            timestamp: 0,
        },
    )
    .await;
}

async fn verify_next_timer_after_deletion<T: TimerTable>(txn: &mut T) {
    let mut stream = txn.next_timers_greater_than(
        1337,
        &TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("", ""),
                invocation_id: Default::default(),
            },
            journal_index: 0,
            timestamp: 0,
        },
        usize::MAX,
    );

    if let Some(Ok((key, _))) = stream.next().await {
        // make sure that we skip the first timer
        assert_eq!(key.journal_index, 1);
    } else {
        panic!("test failure");
    }
}

pub(crate) async fn run_tests(rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    demo_how_to_find_first_timers_in_a_partition(&mut txn).await;
    find_timers_greater_than(&mut txn).await;

    let mut txn = rocksdb.transaction();
    delete_the_first_timer(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_next_timer_after_deletion(&mut txn).await;
}
