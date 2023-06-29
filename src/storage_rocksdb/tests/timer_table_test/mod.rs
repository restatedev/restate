use futures_util::StreamExt;
use restate_storage_api::timer_table::{Timer, TimerKey, TimerTable};
use restate_storage_api::{Storage, Transaction};
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::ServiceId;
use restate_types::invocation::{ServiceInvocation, ServiceInvocationId, SpanRelation};

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
        Timer::CompleteSleepEntry,
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
        Timer::CompleteSleepEntry,
    )
    .await;

    let (service_invocation, _) = ServiceInvocation::new(
        ServiceInvocationId {
            service_id: ServiceId::new("svc-2", "key-2"),
            invocation_id: Default::default(),
        },
        "mymethod".to_string().into(),
        Default::default(),
        None,
        SpanRelation::None,
    );
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
        Timer::Invoke(service_invocation),
    )
    .await;

    //
    // add a successor and a predecessor partitions
    //
    txn.add_timer(
        1336,
        &TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("", ""),
                invocation_id: Default::default(),
            },
            journal_index: 0,
            timestamp: 0,
        },
        Timer::CompleteSleepEntry,
    )
    .await;

    txn.add_timer(
        1338,
        &TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("", ""),
                invocation_id: Default::default(),
            },
            journal_index: 0,
            timestamp: 0,
        },
        Timer::CompleteSleepEntry,
    )
    .await;
}

async fn demo_how_to_find_first_timers_in_a_partition<T: TimerTable>(txn: &mut T) {
    let mut stream = txn.next_timers_greater_than(1337, None, usize::MAX);

    let mut count = 0;
    while (stream.next().await).is_some() {
        count += 1;
    }

    assert_eq!(count, 3);
}

async fn find_timers_greater_than<T: TimerTable>(txn: &mut T) {
    let mut stream = txn.next_timers_greater_than(
        1337,
        Some(&TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("svc-1", "key-1"),
                invocation_id: Default::default(),
            },
            journal_index: 0,
            timestamp: 0,
        }),
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
        Some(&TimerKey {
            service_invocation_id: ServiceInvocationId {
                service_id: ServiceId::new("", ""),
                invocation_id: Default::default(),
            },
            journal_index: 0,
            timestamp: 0,
        }),
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
