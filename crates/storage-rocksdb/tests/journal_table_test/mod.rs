// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use futures_util::StreamExt;
use restate_storage_api::journal_table::{JournalEntry, JournalTable};
use restate_storage_api::Transaction;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
use std::pin::pin;

// false positive because of Bytes
#[allow(clippy::declare_interior_mutable_const)]
const MOCK_JOURNAL_ENTRY: JournalEntry = JournalEntry::Entry(EnrichedRawEntry::new(
    EnrichedEntryHeader::ClearState {},
    Bytes::new(),
));

const MOCK_INVOCATION_ID_1: InvocationId =
    InvocationId::from_parts(1, InvocationUuid::from_parts(1706027034946, 12345678900001));

async fn populate_data<T: JournalTable>(txn: &mut T) {
    txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 0, MOCK_JOURNAL_ENTRY)
        .await;
    txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 1, MOCK_JOURNAL_ENTRY)
        .await;
    txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 2, MOCK_JOURNAL_ENTRY)
        .await;
    txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 3, MOCK_JOURNAL_ENTRY)
        .await;
}

async fn get_entire_journal<T: JournalTable>(txn: &mut T) {
    let mut journal = pin!(txn.get_journal(&MOCK_INVOCATION_ID_1, 4));
    let mut count = 0;
    while (journal.next().await).is_some() {
        count += 1;
    }

    assert_eq!(count, 4);
}

async fn get_subset_of_a_journal<T: JournalTable>(txn: &mut T) {
    let mut journal = pin!(txn.get_journal(&MOCK_INVOCATION_ID_1, 2));
    let mut count = 0;
    while (journal.next().await).is_some() {
        count += 1;
    }

    assert_eq!(count, 2);
}

async fn point_lookups<T: JournalTable>(txn: &mut T) {
    let result = txn
        .get_journal_entry(&MOCK_INVOCATION_ID_1, 2)
        .await
        .expect("should not fail");

    assert!(result.is_some());

    let result = txn
        .get_journal_entry(&MOCK_INVOCATION_ID_1, 10000)
        .await
        .expect("should not fail");

    assert!(result.is_none());
}

async fn delete_journal<T: JournalTable>(txn: &mut T) {
    txn.delete_journal(&MOCK_INVOCATION_ID_1, 4).await;
}

async fn verify_journal_deleted<T: JournalTable>(txn: &mut T) {
    for i in 0..4 {
        let result = txn
            .get_journal_entry(&MOCK_INVOCATION_ID_1, i)
            .await
            .expect("should not fail");

        assert!(result.is_none());
    }
}

pub(crate) async fn run_tests(mut rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();

    populate_data(&mut txn).await;
    get_entire_journal(&mut txn).await;
    get_subset_of_a_journal(&mut txn).await;
    point_lookups(&mut txn).await;
    delete_journal(&mut txn).await;

    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_journal_deleted(&mut txn).await;
}
