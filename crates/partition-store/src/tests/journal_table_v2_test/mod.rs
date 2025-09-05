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
use std::time::Duration;

use super::storage_test_environment;
use bytes::Bytes;
use bytestring::ByteString;
use futures_util::StreamExt;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::Transaction;
use restate_storage_api::journal_table_v2::{JournalTable, ReadOnlyJournalTable};
use restate_test_util::let_assert;
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use restate_types::journal_v2::raw::RawCommandSpecificMetadata;
use restate_types::journal_v2::{
    CallCommand, CallRequest, CommandType, CompletionId, CompletionType, Entry, EntryMetadata,
    EntryType, NotificationId, NotificationType, OneWayCallCommand, SleepCommand, SleepCompletion,
};
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::time::MillisSinceEpoch;

const MOCK_INVOCATION_ID_1: InvocationId =
    InvocationId::from_parts(1, InvocationUuid::from_u128(12345678900001));

fn mock_sleep_command(completion_id: CompletionId) -> Entry {
    Entry::from(SleepCommand {
        wake_up_time: 1.into(),
        completion_id,
        name: Default::default(),
    })
}

fn mock_sleep_completion(completion_id: CompletionId) -> Entry {
    Entry::from(SleepCompletion { completion_id })
}

fn mock_call_command(
    invocation_id_completion_id: CompletionId,
    result_completion_id: CompletionId,
) -> Entry {
    Entry::from(CallCommand {
        request: CallRequest {
            invocation_id: InvocationId::from_parts(789, InvocationUuid::from_u128(456)),
            invocation_target: InvocationTarget::Service {
                name: ByteString::from_static("MySvc"),
                handler: ByteString::from_static("MyHandler"),
            },
            span_context: ServiceInvocationSpanContext::empty(),
            parameter: Bytes::from_static(b"some payload"),
            headers: vec![],
            idempotency_key: Some(ByteString::from_static("my-idempotency-key")),
            completion_retention_duration: Duration::from_secs(10),
            journal_retention_duration: Duration::from_secs(11),
        },
        invocation_id_completion_id,
        name: Default::default(),
        result_completion_id,
    })
}

fn mock_one_way_call_command(invocation_id_completion_id: CompletionId) -> Entry {
    Entry::from(OneWayCallCommand {
        request: CallRequest {
            invocation_id: InvocationId::from_parts(789, InvocationUuid::from_u128(456)),
            invocation_target: InvocationTarget::Service {
                name: ByteString::from_static("MySvc"),
                handler: ByteString::from_static("MyHandler"),
            },
            span_context: ServiceInvocationSpanContext::empty(),
            parameter: Bytes::from_static(b"some payload"),
            headers: vec![],
            idempotency_key: Some(ByteString::from_static("my-idempotency-key")),
            completion_retention_duration: Duration::from_secs(10),
            journal_retention_duration: Duration::from_secs(11),
        },
        invoke_time: 0.into(),
        invocation_id_completion_id,
        name: Default::default(),
    })
}

async fn populate_sleep_journal<T: JournalTable>(txn: &mut T) {
    for i in 0..5 {
        txn.put_journal_entry(
            MOCK_INVOCATION_ID_1,
            i,
            &StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::now()),
                mock_sleep_command(i).encode::<ServiceProtocolV4Codec>(),
            ),
            &[i],
        )
        .await
        .unwrap();
    }
    for i in 5..10 {
        txn.put_journal_entry(
            MOCK_INVOCATION_ID_1,
            i,
            &StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::now()),
                mock_sleep_completion(i - 5).encode::<ServiceProtocolV4Codec>(),
            ),
            &[],
        )
        .await
        .unwrap();
    }
}

async fn get_entire_sleep_journal<T: JournalTable>(txn: &mut T) {
    let mut journal = pin!(txn.get_journal(MOCK_INVOCATION_ID_1, 10).unwrap());
    for _ in 0..5 {
        let entry = journal.next().await.unwrap().unwrap().1;
        assert_eq!(entry.ty(), EntryType::Command(CommandType::Sleep));
    }
    for _ in 0..5 {
        let entry = journal.next().await.unwrap().unwrap().1;
        assert_eq!(
            entry.ty(),
            EntryType::Notification(NotificationType::Completion(CompletionType::Sleep))
        );
    }

    assert!(journal.next().await.is_none());
}

async fn check_sleep_completion_index<T: JournalTable>(txn: &mut T) {
    for i in 0..5 {
        assert_eq!(
            txn.get_command_by_completion_id(MOCK_INVOCATION_ID_1, i)
                .await
                .unwrap()
                .unwrap()
                .1
                .command_type(),
            CommandType::Sleep
        );
    }
}

async fn check_sleep_notification_index<T: JournalTable>(txn: &mut T) {
    assert_eq!(
        txn.get_notifications_index(MOCK_INVOCATION_ID_1)
            .await
            .unwrap(),
        (0..5)
            .map(|i| (NotificationId::for_completion(i), i + 5))
            .collect()
    );
}

async fn get_subset_of_a_journal<T: JournalTable>(txn: &mut T) {
    let mut journal = pin!(txn.get_journal(MOCK_INVOCATION_ID_1, 2).unwrap());
    let mut count = 0;
    while (journal.next().await).is_some() {
        count += 1;
    }

    assert_eq!(count, 2);
}

async fn sleep_point_lookups<T: JournalTable>(txn: &mut T) {
    let result = txn
        .get_journal_entry(MOCK_INVOCATION_ID_1, 2)
        .await
        .expect("should not fail");
    assert_eq!(result.unwrap().ty(), EntryType::Command(CommandType::Sleep));

    let result = txn
        .get_journal_entry(MOCK_INVOCATION_ID_1, 4)
        .await
        .expect("should not fail");
    assert_eq!(result.unwrap().ty(), EntryType::Command(CommandType::Sleep));

    let result = txn
        .get_journal_entry(MOCK_INVOCATION_ID_1, 10000)
        .await
        .expect("should not fail");
    assert!(result.is_none());
}

async fn delete_journal<T: JournalTable>(txn: &mut T) {
    txn.delete_journal(MOCK_INVOCATION_ID_1).await.unwrap();
}

async fn verify_journal_deleted<T: JournalTable>(txn: &mut T, length: usize) {
    for i in 0..length {
        let result = txn
            .get_journal_entry(MOCK_INVOCATION_ID_1, i as u32)
            .await
            .expect("should not fail");

        assert!(result.is_none());
    }
    for i in 0..length {
        let result = txn
            .get_command_by_completion_id(MOCK_INVOCATION_ID_1, i as u32)
            .await
            .expect("should not fail");

        assert!(result.is_none());
    }
    let notifications_index = txn
        .get_notifications_index(MOCK_INVOCATION_ID_1)
        .await
        .expect("should not fail");

    assert!(notifications_index.is_empty());
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sleep_journal() {
    let mut rocksdb = storage_test_environment().await;

    let mut txn = rocksdb.transaction();

    populate_sleep_journal(&mut txn).await;
    get_entire_sleep_journal(&mut txn).await;
    get_subset_of_a_journal(&mut txn).await;
    check_sleep_completion_index(&mut txn).await;
    check_sleep_notification_index(&mut txn).await;

    sleep_point_lookups(&mut txn).await;
    delete_journal(&mut txn).await;

    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_journal_deleted(&mut txn, 10).await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_journal() {
    let mut rocksdb = storage_test_environment().await;

    let mut txn = rocksdb.transaction();

    // Populate
    txn.put_journal_entry(
        MOCK_INVOCATION_ID_1,
        0,
        &StoredRawEntry::new(
            StoredRawEntryHeader::new(MillisSinceEpoch::now()),
            mock_call_command(0, 1).encode::<ServiceProtocolV4Codec>(),
        ),
        &[0, 1],
    )
    .await
    .unwrap();
    txn.put_journal_entry(
        MOCK_INVOCATION_ID_1,
        1,
        &StoredRawEntry::new(
            StoredRawEntryHeader::new(MillisSinceEpoch::now()),
            mock_one_way_call_command(2).encode::<ServiceProtocolV4Codec>(),
        ),
        &[2],
    )
    .await
    .unwrap();

    // Verify the journal is correct
    let mut journal = txn.get_journal(MOCK_INVOCATION_ID_1, 2).unwrap();

    // First entry is call
    let entry = journal.next().await.unwrap().unwrap().1;
    assert_eq!(entry.ty(), EntryType::Command(CommandType::Call));
    let cmd = entry.inner.try_as_command().unwrap();
    let_assert!(RawCommandSpecificMetadata::CallOrSend(_) = cmd.command_specific_metadata());

    // Second entry is one way call
    let entry = journal.next().await.unwrap().unwrap().1;
    assert_eq!(entry.ty(), EntryType::Command(CommandType::OneWayCall));
    let cmd = entry.inner.try_as_command().unwrap();
    let_assert!(RawCommandSpecificMetadata::CallOrSend(_) = cmd.command_specific_metadata());

    // No more entries
    assert!(journal.next().await.is_none());
    drop(journal);

    // Check completion index
    for i in 0..1 {
        assert_eq!(
            txn.get_command_by_completion_id(MOCK_INVOCATION_ID_1, i)
                .await
                .unwrap()
                .unwrap()
                .1
                .command_type(),
            CommandType::Call
        );
    }
    assert_eq!(
        txn.get_command_by_completion_id(MOCK_INVOCATION_ID_1, 2)
            .await
            .unwrap()
            .unwrap()
            .1
            .command_type(),
        CommandType::OneWayCall
    );

    txn.commit().await.expect("should not fail");
}
