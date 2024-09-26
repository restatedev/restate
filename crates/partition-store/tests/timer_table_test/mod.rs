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
use restate_storage_api::timer_table::{Timer, TimerKey, TimerKeyKind, TimerTable};
use restate_storage_api::Transaction;
use restate_types::identifiers::{InvocationId, InvocationUuid, ServiceId};
use restate_types::invocation::ServiceInvocation;
use std::pin::pin;

const FIXTURE_INVOCATION_UUID: InvocationUuid = InvocationUuid::from_u128(12345678900001);
const FIXTURE_INVOCATION: InvocationId = InvocationId::from_parts(1337, FIXTURE_INVOCATION_UUID);

async fn populate_data<T: TimerTable>(txn: &mut T) {
    txn.put_timer(
        &TimerKey {
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: FIXTURE_INVOCATION.invocation_uuid(),
                journal_index: 0,
            },
            timestamp: 0,
        },
        &Timer::CompleteJournalEntry(FIXTURE_INVOCATION, 0),
    )
    .await;

    txn.put_timer(
        &TimerKey {
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: FIXTURE_INVOCATION.invocation_uuid(),
                journal_index: 1,
            },
            timestamp: 0,
        },
        &Timer::CompleteJournalEntry(FIXTURE_INVOCATION, 1),
    )
    .await;

    let service_invocation = ServiceInvocation {
        ..mock_service_invocation(ServiceId::new("svc-2", "key-2"))
    };
    txn.put_timer(
        &TimerKey {
            kind: TimerKeyKind::Invoke {
                invocation_uuid: service_invocation.invocation_id.invocation_uuid(),
            },
            timestamp: 1,
        },
        &Timer::Invoke(service_invocation),
    )
    .await;

    //
    // add a successor and a predecessor partitions
    //
    txn.put_timer(
        &TimerKey {
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: FIXTURE_INVOCATION_UUID,
                journal_index: 0,
            },
            timestamp: 0,
        },
        &Timer::CompleteJournalEntry(InvocationId::from_parts(1336, FIXTURE_INVOCATION_UUID), 0),
    )
    .await;
}

async fn demo_how_to_find_first_timers_in_a_partition<T: TimerTable>(txn: &mut T) {
    let mut stream = pin!(txn.next_timers_greater_than(None, usize::MAX));

    let mut count = 0;
    while stream.next().await.is_some() {
        count += 1;
    }

    assert_eq!(count, 3);
}

async fn find_timers_greater_than<T: TimerTable>(txn: &mut T) {
    let timer_key = &TimerKey {
        kind: TimerKeyKind::CompleteJournalEntry {
            invocation_uuid: FIXTURE_INVOCATION_UUID,
            journal_index: 0,
        },
        timestamp: 0,
    };
    let mut stream = pin!(txn.next_timers_greater_than(Some(timer_key), usize::MAX));

    if let Some(Ok((key, _))) = stream.next().await {
        // make sure that we skip the first timer that has a journal_index of 0
        // take a look at populate_data once again.
        assert_that!(
            key.kind,
            pat!(TimerKeyKind::CompleteJournalEntry {
                journal_index: eq(1),
            })
        );
    } else {
        panic!("test failure");
    }

    if let Some(Ok((key, _))) = stream.next().await {
        assert_that!(key.kind, pat!(TimerKeyKind::Invoke { .. }));
        assert_eq!(key.timestamp, 1);
    } else {
        panic!("test failure");
    }
}

async fn delete_the_first_timer<T: TimerTable>(txn: &mut T) {
    txn.delete_timer(&TimerKey {
        kind: TimerKeyKind::CompleteJournalEntry {
            invocation_uuid: FIXTURE_INVOCATION_UUID,
            journal_index: 0,
        },
        timestamp: 0,
    })
    .await;
}

async fn verify_next_timer_after_deletion<T: TimerTable>(txn: &mut T) {
    let timer_key = &TimerKey {
        kind: TimerKeyKind::CompleteJournalEntry {
            invocation_uuid: FIXTURE_INVOCATION_UUID,
            journal_index: 0,
        },
        timestamp: 0,
    };
    let mut stream = pin!(txn.next_timers_greater_than(Some(timer_key), usize::MAX,));

    if let Some(Ok((key, _))) = stream.next().await {
        assert_that!(
            key.kind,
            pat!(TimerKeyKind::CompleteJournalEntry {
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
