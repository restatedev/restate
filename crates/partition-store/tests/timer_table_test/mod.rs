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
use googletest::matchers::eq;
use googletest::{assert_that, pat};
use restate_partition_store::PartitionStore;
use restate_storage_api::timer_table::{Timer, TimerKey, TimerKind, TimerTable};
use restate_storage_api::Transaction;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionId, ServiceId};
use restate_types::invocation::ServiceInvocation;
use std::pin::pin;

const FIXTURE_INVOCATION_UUID: InvocationUuid =
    InvocationUuid::from_parts(1706027034946, 12345678900001);
const FIXTURE_INVOCATION: InvocationId = InvocationId::from_parts(1337, FIXTURE_INVOCATION_UUID);

const PARTITION1337: PartitionId = PartitionId::new_unchecked(1337);

async fn populate_data<T: TimerTable>(txn: &mut T) {
    txn.add_timer(
        PARTITION1337,
        &TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION.invocation_uuid(),
                journal_index: 0,
            },
            timestamp: 0,
        },
        Timer::CompleteSleepEntry(FIXTURE_INVOCATION, 0),
    )
    .await;

    txn.add_timer(
        PARTITION1337,
        &TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION.invocation_uuid(),
                journal_index: 1,
            },
            timestamp: 0,
        },
        Timer::CompleteSleepEntry(FIXTURE_INVOCATION, 1),
    )
    .await;

    let service_invocation = ServiceInvocation {
        ..mock_service_invocation(ServiceId::new("svc-2", "key-2"))
    };
    txn.add_timer(
        PARTITION1337,
        &TimerKey {
            kind: TimerKind::Invocation {
                invocation_uuid: service_invocation.invocation_id.invocation_uuid(),
            },
            timestamp: 1,
        },
        Timer::Invoke(service_invocation),
    )
    .await;

    //
    // add a successor and a predecessor partitions
    //
    txn.add_timer(
        PARTITION1337,
        &TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION_UUID,
                journal_index: 0,
            },
            timestamp: 0,
        },
        Timer::CompleteSleepEntry(InvocationId::from_parts(1336, FIXTURE_INVOCATION_UUID), 0),
    )
    .await;

    txn.add_timer(
        PartitionId::from(1338),
        &TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION_UUID,
                journal_index: 0,
            },
            timestamp: 0,
        },
        Timer::CompleteSleepEntry(InvocationId::from_parts(1338, FIXTURE_INVOCATION_UUID), 0),
    )
    .await;
}

async fn demo_how_to_find_first_timers_in_a_partition<T: TimerTable>(txn: &mut T) {
    let mut stream = pin!(txn.next_timers_greater_than(PARTITION1337, None, usize::MAX));

    let mut count = 0;
    while stream.next().await.is_some() {
        count += 1;
    }

    assert_eq!(count, 3);
}

async fn find_timers_greater_than<T: TimerTable>(txn: &mut T) {
    let timer_key = &TimerKey {
        kind: TimerKind::Journal {
            invocation_uuid: FIXTURE_INVOCATION_UUID,
            journal_index: 0,
        },
        timestamp: 0,
    };
    let mut stream = pin!(txn.next_timers_greater_than(PARTITION1337, Some(timer_key), usize::MAX));

    if let Some(Ok((key, _))) = stream.next().await {
        // make sure that we skip the first timer that has a journal_index of 0
        // take a look at populate_data once again.
        assert_that!(
            key.kind,
            pat!(TimerKind::Journal {
                journal_index: eq(1),
            })
        );
    } else {
        panic!("test failure");
    }

    if let Some(Ok((key, _))) = stream.next().await {
        assert_that!(key.kind, pat!(TimerKind::Invocation { .. }));
        assert_eq!(key.timestamp, 1);
    } else {
        panic!("test failure");
    }
}

async fn delete_the_first_timer<T: TimerTable>(txn: &mut T) {
    txn.delete_timer(
        PARTITION1337,
        &TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION_UUID,
                journal_index: 0,
            },
            timestamp: 0,
        },
    )
    .await;
}

async fn verify_next_timer_after_deletion<T: TimerTable>(txn: &mut T) {
    let timer_key = &TimerKey {
        kind: TimerKind::Journal {
            invocation_uuid: FIXTURE_INVOCATION_UUID,
            journal_index: 0,
        },
        timestamp: 0,
    };
    let mut stream =
        pin!(txn.next_timers_greater_than(PARTITION1337, Some(timer_key), usize::MAX,));

    if let Some(Ok((key, _))) = stream.next().await {
        assert_that!(
            key.kind,
            pat!(TimerKind::Journal {
                journal_index: eq(1)
            })
        );
    } else {
        panic!("test failure");
    }
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();

    populate_data(&mut txn).await;
    demo_how_to_find_first_timers_in_a_partition(&mut txn).await;
    find_timers_greater_than(&mut txn).await;
    delete_the_first_timer(&mut txn).await;

    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_next_timer_after_deletion(&mut txn).await;
}
