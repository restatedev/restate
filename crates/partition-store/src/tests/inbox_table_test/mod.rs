// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use restate_storage_api::Transaction;
use restate_storage_api::inbox_table::{
    InboxEntry, ReadInboxTable, SequenceNumberInboxEntry, WriteInboxTable,
};
use restate_types::identifiers::{InvocationId, ServiceId};

use super::{assert_stream_eq, mock_state_mutation};
use crate::PartitionStore;

static INBOX_ENTRIES: LazyLock<Vec<SequenceNumberInboxEntry>> = LazyLock::new(|| {
    vec![
        SequenceNumberInboxEntry::new(
            7,
            InboxEntry::Invocation(
                ServiceId::new("svc-1", "key-1"),
                InvocationId::mock_random(),
            ),
        ),
        SequenceNumberInboxEntry::new(
            8,
            InboxEntry::StateMutation(mock_state_mutation(ServiceId::new("svc-1", "key-1"))),
        ),
        SequenceNumberInboxEntry::new(
            9,
            InboxEntry::Invocation(
                ServiceId::new("svc-2", "key-1"),
                InvocationId::mock_random(),
            ),
        ),
        SequenceNumberInboxEntry::new(
            10,
            InboxEntry::Invocation(
                ServiceId::new("svc-1", "key-1"),
                InvocationId::mock_random(),
            ),
        ),
    ]
});

fn populate_data<T: WriteInboxTable>(table: &mut T) {
    for SequenceNumberInboxEntry {
        inbox_sequence_number,
        inbox_entry,
    } in INBOX_ENTRIES.iter()
    {
        table
            .put_inbox_entry(*inbox_sequence_number, inbox_entry)
            .expect("storage to work");
    }
}

async fn find_the_next_message_in_an_inbox<T: WriteInboxTable + ReadInboxTable>(table: &mut T) {
    let result = table.peek_inbox(INBOX_ENTRIES[0].service_id()).await;
    assert_eq!(result.unwrap(), Some(INBOX_ENTRIES[0].clone()));
}

async fn get_svc_inbox<T: WriteInboxTable + ReadInboxTable>(table: &mut T) {
    let stream = table.inbox(INBOX_ENTRIES[0].service_id()).unwrap();

    let vec = vec![
        INBOX_ENTRIES[0].clone(),
        INBOX_ENTRIES[1].clone(),
        INBOX_ENTRIES[3].clone(),
    ];

    assert_stream_eq(stream, vec).await;
}

fn delete_entry<T: WriteInboxTable + ReadInboxTable>(table: &mut T) {
    table
        .delete_inbox_entry(INBOX_ENTRIES[0].service_id(), 7)
        .expect("storage to work");
}

async fn peek_after_delete<T: WriteInboxTable + ReadInboxTable>(table: &mut T) {
    let result = table.peek_inbox(INBOX_ENTRIES[0].service_id()).await;

    assert_eq!(result.unwrap(), Some(INBOX_ENTRIES[1].clone()));
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn);

    find_the_next_message_in_an_inbox(&mut txn).await;
    get_svc_inbox(&mut txn).await;
    delete_entry(&mut txn);

    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    peek_after_delete(&mut txn).await;
}
