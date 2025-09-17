// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::pin;
use std::sync::LazyLock;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use futures_util::StreamExt;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::journal_table::{JournalEntry, JournalTable};
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use restate_types::journal::enriched::{
    CallEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
};

use super::storage_test_environment;

// false positive because of Bytes
#[allow(clippy::declare_interior_mutable_const)]
static MOCK_SLEEP_JOURNAL_ENTRY: JournalEntry = const {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::ClearState {},
        Bytes::new(),
    ))
};

static MOCK_INVOKE_JOURNAL_ENTRY: LazyLock<JournalEntry> = LazyLock::new(|| {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::Call {
            is_completed: true,
            enrichment_result: Some(CallEnrichmentResult {
                invocation_id: InvocationId::from_parts(789, InvocationUuid::from_u128(456)),
                invocation_target: InvocationTarget::Service {
                    name: ByteString::from_static("MySvc"),
                    handler: ByteString::from_static("MyHandler"),
                },
                completion_retention_time: Some(Duration::from_secs(10)),
                span_context: ServiceInvocationSpanContext::empty(),
            }),
        },
        Bytes::new(),
    ))
});

const MOCK_INVOCATION_ID_1: InvocationId =
    InvocationId::from_parts(1, InvocationUuid::from_u128(12345678900001));

async fn populate_data<T: JournalTable>(txn: &mut T) {
    txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 0, &MOCK_SLEEP_JOURNAL_ENTRY)
        .await
        .unwrap();
    txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 1, &MOCK_SLEEP_JOURNAL_ENTRY)
        .await
        .unwrap();
    txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 2, &MOCK_SLEEP_JOURNAL_ENTRY)
        .await
        .unwrap();
    txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 3, &MOCK_SLEEP_JOURNAL_ENTRY)
        .await
        .unwrap();
    txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 4, &MOCK_INVOKE_JOURNAL_ENTRY)
        .await
        .unwrap();
}

async fn get_entire_journal<T: JournalTable>(txn: &mut T) {
    let mut journal = pin!(txn.get_journal(&MOCK_INVOCATION_ID_1, 5).unwrap());
    let mut count = 0;
    while (journal.next().await).is_some() {
        count += 1;
    }

    assert_eq!(count, 5);
}

async fn get_subset_of_a_journal<T: JournalTable>(txn: &mut T) {
    let mut journal = pin!(txn.get_journal(&MOCK_INVOCATION_ID_1, 2).unwrap());
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

    // false positive because of Bytes
    #[allow(clippy::borrow_interior_mutable_const)]
    {
        assert_eq!(result.unwrap(), MOCK_SLEEP_JOURNAL_ENTRY);
    }

    let result = txn
        .get_journal_entry(&MOCK_INVOCATION_ID_1, 4)
        .await
        .expect("should not fail");

    assert_eq!(result.unwrap(), MOCK_INVOKE_JOURNAL_ENTRY.clone());

    let result = txn
        .get_journal_entry(&MOCK_INVOCATION_ID_1, 10000)
        .await
        .expect("should not fail");

    assert!(result.is_none());
}

async fn delete_journal<T: JournalTable>(txn: &mut T) {
    txn.delete_journal(&MOCK_INVOCATION_ID_1, 5).await.unwrap();
}

async fn verify_journal_deleted<T: JournalTable>(txn: &mut T) {
    for i in 0..5 {
        let result = txn
            .get_journal_entry(&MOCK_INVOCATION_ID_1, i)
            .await
            .expect("should not fail");

        assert!(result.is_none());
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_tests() {
    let mut rocksdb = storage_test_environment().await;

    let mut txn = rocksdb.transaction();

    populate_data(&mut txn).await;
    get_entire_journal(&mut txn).await;
    get_subset_of_a_journal(&mut txn).await;
    point_lookups(&mut txn).await;
    delete_journal(&mut txn).await;

    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_journal_deleted(&mut txn).await;
    RocksDbManager::get().shutdown().await;
}
