// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{assert_stream_eq, mock_full_invocation_id, mock_state_mutation};
use once_cell::sync::Lazy;
use restate_storage_api::inbox_table::{InboxEntry, InboxTable, SequenceNumberInboxEntry};
use restate_storage_api::Transaction;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::ServiceId;

static INBOX_ENTRIES: Lazy<Vec<SequenceNumberInboxEntry>> = Lazy::new(|| {
    vec![
        SequenceNumberInboxEntry::new(
            7,
            InboxEntry::Invocation(mock_full_invocation_id(ServiceId::new("svc-1", "key-1"))),
        ),
        SequenceNumberInboxEntry::new(
            8,
            InboxEntry::StateMutation(mock_state_mutation(ServiceId::new("svc-1", "key-1"))),
        ),
        SequenceNumberInboxEntry::new(
            9,
            InboxEntry::Invocation(mock_full_invocation_id(ServiceId::new("svc-2", "key-1"))),
        ),
        SequenceNumberInboxEntry::new(
            10,
            InboxEntry::Invocation(mock_full_invocation_id(ServiceId::new("svc-1", "key-1"))),
        ),
    ]
});

async fn populate_data<T: InboxTable>(table: &mut T) {
    for inbox_entry in INBOX_ENTRIES.iter() {
        table
            .put_inbox_entry(inbox_entry.service_id(), inbox_entry.clone())
            .await;
    }
}

async fn find_the_next_message_in_an_inbox<T: InboxTable>(table: &mut T) {
    let result = table.peek_inbox(INBOX_ENTRIES[0].service_id()).await;

    assert_eq!(result.unwrap(), Some(INBOX_ENTRIES[0].clone()));
}

async fn get_svc_inbox<T: InboxTable>(table: &mut T) {
    let stream = table.inbox(INBOX_ENTRIES[0].service_id());

    let vec = vec![
        INBOX_ENTRIES[0].clone(),
        INBOX_ENTRIES[1].clone(),
        INBOX_ENTRIES[3].clone(),
    ];

    assert_stream_eq(stream, vec).await;
}

async fn delete_entry<T: InboxTable>(table: &mut T) {
    table
        .delete_inbox_entry(INBOX_ENTRIES[0].service_id(), 7)
        .await;
}

async fn peek_after_delete<T: InboxTable>(table: &mut T) {
    let result = table.peek_inbox(INBOX_ENTRIES[0].service_id()).await;

    assert_eq!(result.unwrap(), Some(INBOX_ENTRIES[1].clone()));
}

pub(crate) async fn run_tests(mut rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    find_the_next_message_in_an_inbox(&mut txn).await;
    get_svc_inbox(&mut txn).await;
    delete_entry(&mut txn).await;

    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    peek_after_delete(&mut txn).await;
}
