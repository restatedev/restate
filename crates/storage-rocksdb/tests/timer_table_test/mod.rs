// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mock_service_invocation;
use futures_util::StreamExt;
use restate_storage_api::timer_table::{Timer, TimerKey, TimerTable};
use restate_storage_api::Transaction;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{InvocationUuid, ServiceId};
use restate_types::invocation::ServiceInvocation;
use std::pin::pin;

const FIXTURE_INVOCATION: InvocationUuid =
    InvocationUuid::from_parts(1706027034946, 12345678900001);

async fn populate_data<T: TimerTable>(txn: &mut T) {
    txn.add_timer(
        1337,
        &TimerKey {
            invocation_uuid: FIXTURE_INVOCATION,
            journal_index: 0,
            timestamp: 0,
        },
        Timer::CompleteSleepEntry(1337),
    )
    .await;

    txn.add_timer(
        1337,
        &TimerKey {
            invocation_uuid: FIXTURE_INVOCATION,
            journal_index: 1,
            timestamp: 0,
        },
        Timer::CompleteSleepEntry(1337),
    )
    .await;

    let service_invocation = ServiceInvocation {
        ..mock_service_invocation(ServiceId::new("svc-2", "key-2"))
    };
    txn.add_timer(
        1337,
        &TimerKey {
            invocation_uuid: FIXTURE_INVOCATION,
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
            invocation_uuid: FIXTURE_INVOCATION,
            journal_index: 0,
            timestamp: 0,
        },
        Timer::CompleteSleepEntry(1336),
    )
    .await;

    txn.add_timer(
        1338,
        &TimerKey {
            invocation_uuid: FIXTURE_INVOCATION,
            journal_index: 0,
            timestamp: 0,
        },
        Timer::CompleteSleepEntry(1338),
    )
    .await;
}

async fn demo_how_to_find_first_timers_in_a_partition<T: TimerTable>(txn: &mut T) {
    let mut stream = pin!(txn.next_timers_greater_than(1337, None, usize::MAX));

    let mut count = 0;
    while stream.next().await.is_some() {
        count += 1;
    }

    assert_eq!(count, 3);
}

async fn find_timers_greater_than<T: TimerTable>(txn: &mut T) {
    let timer_key = &TimerKey {
        invocation_uuid: FIXTURE_INVOCATION,
        journal_index: 0,
        timestamp: 0,
    };
    let mut stream = pin!(txn.next_timers_greater_than(1337, Some(timer_key), usize::MAX));

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
            invocation_uuid: FIXTURE_INVOCATION,
            journal_index: 0,
            timestamp: 0,
        },
    )
    .await;
}

async fn verify_next_timer_after_deletion<T: TimerTable>(txn: &mut T) {
    let timer_key = &TimerKey {
        invocation_uuid: FIXTURE_INVOCATION,
        journal_index: 0,
        timestamp: 0,
    };
    let mut stream = pin!(txn.next_timers_greater_than(1337, Some(timer_key), usize::MAX,));

    if let Some(Ok((key, _))) = stream.next().await {
        // make sure that we skip the first timer
        assert_eq!(key.journal_index, 1);
    } else {
        panic!("test failure");
    }
}

pub(crate) async fn run_tests(mut rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();

    populate_data(&mut txn).await;
    demo_how_to_find_first_timers_in_a_partition(&mut txn).await;
    find_timers_greater_than(&mut txn).await;
    delete_the_first_timer(&mut txn).await;

    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_next_timer_after_deletion(&mut txn).await;
}
