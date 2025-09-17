// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::storage_test_environment;
use futures_util::StreamExt;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::journal_events::EventView;
use restate_storage_api::journal_events::{JournalEventsTable, ReadOnlyJournalEventsTable};
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{Event, TransientErrorEvent};
use restate_types::time::MillisSinceEpoch;

const MOCK_INVOCATION_ID_1: InvocationId =
    InvocationId::from_parts(1, InvocationUuid::from_u128(12345678900001));

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_event() {
    let mut rocksdb = storage_test_environment().await;

    let mut txn = rocksdb.transaction();

    let event: RawEvent = Event::TransientError(TransientErrorEvent {
        error_code: 500u16.into(),
        error_message: "my_error".to_string(),
        error_stacktrace: None,
        restate_doc_error_code: None,
        related_command_index: None,
        related_command_name: Some("Input".to_string()),
        related_command_type: None,
    })
    .into();
    let event_view = EventView::new(MillisSinceEpoch::now(), 0, event);

    // Populate
    txn.put_journal_event(MOCK_INVOCATION_ID_1, event_view.clone(), 0)
        .await
        .unwrap();

    // Get all events
    let mut journal_events = txn.get_journal_events(MOCK_INVOCATION_ID_1).unwrap();
    assert_eq!(journal_events.next().await.unwrap().unwrap(), event_view);

    assert!(journal_events.next().await.is_none());
    drop(journal_events);

    txn.commit().await.expect("should not fail");

    // Verify we can remove events

    let mut txn = rocksdb.transaction();
    txn.delete_journal_events(MOCK_INVOCATION_ID_1)
        .await
        .unwrap();
    txn.commit().await.expect("should not fail");

    // Should be empty
    let mut journal_events = rocksdb.get_journal_events(MOCK_INVOCATION_ID_1).unwrap();
    assert!(journal_events.next().await.is_none());
    drop(journal_events);

    RocksDbManager::get().shutdown().await;
}
