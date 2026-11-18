// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_memory::LocalMemoryPool;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::Transaction;
use restate_storage_api::blob_table::{RawParts, ReadBlobTable};
use restate_storage_api::journal_table_v2::{
    NotificationEntryIndex, ReadJournalTable, WriteJournalTable,
};
use restate_types::config::Configuration;
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use restate_types::journal_v2::raw::{RawCommandSpecificMetadata, RawNotificationResultVariant};
use restate_types::journal_v2::{
    CallCommand, CallRequest, Command, CommandType, CompletionId, CompletionType, Entry,
    EntryMetadata, EntryType, InputCommand, NotificationId, NotificationType, OneWayCallCommand,
    SleepCommand, SleepCompletion,
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
                scope: None,
            },
            span_context: ServiceInvocationSpanContext::empty(),
            parameter: Bytes::from_static(b"some payload"),
            headers: vec![],
            idempotency_key: Some(ByteString::from_static("my-idempotency-key")),
            completion_retention_duration: Duration::from_secs(10),
            journal_retention_duration: Duration::from_secs(11),
            limit_key: Default::default(),
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
                scope: None,
            },
            span_context: ServiceInvocationSpanContext::empty(),
            parameter: Bytes::from_static(b"some payload"),
            headers: vec![],
            idempotency_key: Some(ByteString::from_static("my-idempotency-key")),
            completion_retention_duration: Duration::from_secs(10),
            journal_retention_duration: Duration::from_secs(11),
            limit_key: Default::default(),
        },
        invoke_time: 0.into(),
        invocation_id_completion_id,
        name: Default::default(),
    })
}

fn populate_sleep_journal<T: WriteJournalTable>(txn: &mut T) {
    for i in 0..5 {
        txn.put_journal_entry(
            &MOCK_INVOCATION_ID_1,
            i,
            &StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::now()),
                mock_sleep_command(i).encode::<ServiceProtocolV4Codec>(),
            ),
            &[i],
        )
        .unwrap();
    }
    for i in 5..10 {
        txn.put_journal_entry(
            &MOCK_INVOCATION_ID_1,
            i,
            &StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::now()),
                mock_sleep_completion(i - 5).encode::<ServiceProtocolV4Codec>(),
            ),
            &[],
        )
        .unwrap();
    }
}

async fn get_entire_sleep_journal<T: ReadJournalTable>(txn: &T) {
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

async fn check_sleep_completion_index<T: ReadJournalTable>(txn: &mut T) {
    for i in 0..5 {
        assert_eq!(
            txn.get_command_by_completion_id(MOCK_INVOCATION_ID_1, i)
                .await
                .unwrap()
                .unwrap()
                .ty(),
            EntryType::Command(CommandType::Sleep)
        );
    }
}

async fn check_sleep_notification_index<T: ReadJournalTable>(txn: &mut T) {
    assert_eq!(
        txn.get_notifications_index(MOCK_INVOCATION_ID_1)
            .await
            .unwrap(),
        (0..5)
            .map(|i| (
                NotificationId::for_completion(i),
                NotificationEntryIndex::from_entry_index(i + 5, RawNotificationResultVariant::Void)
            ))
            .collect()
    );
}

async fn get_subset_of_a_journal<T: ReadJournalTable>(txn: &T) {
    let mut journal = pin!(txn.get_journal(MOCK_INVOCATION_ID_1, 2).unwrap());
    let mut count = 0;
    while (journal.next().await).is_some() {
        count += 1;
    }

    assert_eq!(count, 2);
}

async fn sleep_point_lookups<T: ReadJournalTable>(txn: &mut T) {
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

fn delete_journal<T: WriteJournalTable>(txn: &mut T, length: usize) {
    txn.delete_journal(&MOCK_INVOCATION_ID_1, length as u32)
        .unwrap();
}

async fn verify_journal_deleted<T: ReadJournalTable>(txn: &mut T, length: usize) {
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

#[restate_core::test]
async fn sleep_journal() {
    let mut rocksdb = storage_test_environment().await;

    let mut txn = rocksdb.transaction();

    populate_sleep_journal(&mut txn);
    get_entire_sleep_journal(&txn).await;
    get_subset_of_a_journal(&txn).await;
    check_sleep_completion_index(&mut txn).await;
    check_sleep_notification_index(&mut txn).await;

    sleep_point_lookups(&mut txn).await;
    delete_journal(&mut txn, 10);

    txn.commit().await.expect("should not fail");
    drop(txn);

    let mut txn = rocksdb.transaction();
    verify_journal_deleted(&mut txn, 10).await;
}

#[restate_core::test]
async fn call_journal() {
    let mut rocksdb = storage_test_environment().await;

    let mut txn = rocksdb.transaction();

    // Populate
    txn.put_journal_entry(
        &MOCK_INVOCATION_ID_1,
        0,
        &StoredRawEntry::new(
            StoredRawEntryHeader::new(MillisSinceEpoch::now()),
            mock_call_command(0, 1).encode::<ServiceProtocolV4Codec>(),
        ),
        &[0, 1],
    )
    .unwrap();
    txn.put_journal_entry(
        &MOCK_INVOCATION_ID_1,
        1,
        &StoredRawEntry::new(
            StoredRawEntryHeader::new(MillisSinceEpoch::now()),
            mock_one_way_call_command(2).encode::<ServiceProtocolV4Codec>(),
        ),
        &[2],
    )
    .unwrap();

    // Verify the journal is correct
    let mut journal = txn.get_journal(MOCK_INVOCATION_ID_1, 2).unwrap();

    // First entry is call
    let entry = journal.next().await.unwrap().unwrap().1;
    assert_eq!(entry.ty(), EntryType::Command(CommandType::Call));
    restate_test_util::assert!(let Some(RawCommandSpecificMetadata::CallOrSend(_)) = entry.command_specific_metadata());

    // Second entry is one way call
    let entry = journal.next().await.unwrap().unwrap().1;
    assert_eq!(entry.ty(), EntryType::Command(CommandType::OneWayCall));
    restate_test_util::assert!(let Some(RawCommandSpecificMetadata::CallOrSend(_)) = entry.command_specific_metadata());

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
                .ty(),
            EntryType::Command(CommandType::Call)
        );
    }
    assert_eq!(
        txn.get_command_by_completion_id(MOCK_INVOCATION_ID_1, 2)
            .await
            .unwrap()
            .unwrap()
            .ty(),
        EntryType::Command(CommandType::OneWayCall)
    );

    txn.commit().await.expect("should not fail");
}

#[restate_core::test]
async fn journal_entry_content_blob() {
    let mut rocksdb = storage_test_environment().await;
    let mut config = Configuration::pinned().as_ref().clone();
    config.common.experimental.set_write_blob_table(true);
    rocksdb
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();

    let threshold = config.worker.inline_journal_entry_threshold.as_usize();
    let header = StoredRawEntryHeader::new(MillisSinceEpoch::now());
    let large = StoredRawEntry::new(
        header.clone(),
        Entry::from(InputCommand {
            headers: vec![],
            payload: Bytes::from(vec![7; threshold + 1]),
            name: Default::default(),
        })
        .encode::<ServiceProtocolV4Codec>(),
    );
    let small = StoredRawEntry::new(
        header,
        mock_sleep_command(1).encode::<ServiceProtocolV4Codec>(),
    );
    let copy_id = InvocationId::from_parts(1, InvocationUuid::from_u128(2));

    let mut txn = rocksdb.transaction();
    let blob_id = txn
        .put_journal_entry(&MOCK_INVOCATION_ID_1, 0, &large, &[])
        .unwrap()
        .expect("stored in a blob");
    assert_eq!(
        txn.put_journal_entry(&MOCK_INVOCATION_ID_1, 1, &small, &[1])
            .unwrap(),
        None
    );

    // The transaction sees the blob it wrote
    let stored = txn
        .get_journal_entry(MOCK_INVOCATION_ID_1, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.content_blob().map(|blob| blob.id), Some(blob_id));
    assert_eq!(stored.content_len(), large.inner.serialized_length());
    let stored = stored.try_into_inline().expect_err("content in a blob");
    assert_eq!(txn.hydrate_journal_entry(stored.clone()).unwrap(), large);
    let inline = txn
        .get_journal_entry(MOCK_INVOCATION_ID_1, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inline.try_into_inline().unwrap(), small);

    // The metadata of a command is available before its content is read from the blob
    let Entry::Command(Command::Call(mut call)) = mock_call_command(0, 1) else {
        unreachable!("a call command");
    };
    call.request.parameter = Bytes::from(vec![7; threshold + 1]);
    let large_call = StoredRawEntry::new(
        StoredRawEntryHeader::new(MillisSinceEpoch::now()),
        Entry::from(call).encode::<ServiceProtocolV4Codec>(),
    );
    let call_id = InvocationId::from_parts(1, InvocationUuid::from_u128(3));
    txn.put_journal_entry(&call_id, 0, &large_call, &[0, 1])
        .unwrap()
        .expect("stored in a blob");
    let stored_call = txn.get_journal_entry(call_id, 0).await.unwrap().unwrap();
    assert_eq!(stored_call.ty(), EntryType::Command(CommandType::Call));
    restate_test_util::assert!(let Some(RawCommandSpecificMetadata::CallOrSend(_)) = stored_call.command_specific_metadata());
    assert_eq!(txn.hydrate_journal_entry(stored_call).unwrap(), large_call);

    // The copy shares the blob
    txn.copy_journal_entry(&copy_id, 0, stored, &[]).unwrap();

    // Budgeted scans of the transaction see the blobs it wrote, and their updated ref counts
    let mut budget = LocalMemoryPool::unlimited();
    let entries: Vec<_> = txn
        .get_journal_budgeted(MOCK_INVOCATION_ID_1, 2, &mut budget)
        .unwrap()
        .map(|item| item.unwrap().1)
        .collect()
        .await;
    assert_eq!(entries, [large.clone(), small.clone()]);
    let entries: Vec<_> = txn
        .get_journal_budgeted(copy_id, 1, &mut budget)
        .unwrap()
        .map(|item| item.unwrap().1)
        .collect()
        .await;
    assert_eq!(entries, std::slice::from_ref(&large));
    txn.commit().await.unwrap();
    drop(txn);

    // Budgeted reads return hydrated entries, the lease covers the blob
    let (entry, lease) = rocksdb
        .get_journal_entry_budgeted(MOCK_INVOCATION_ID_1, 0, &mut budget)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry, large);
    assert!(lease.size() > threshold);
    drop(lease);
    let entries: Vec<_> = rocksdb
        .get_journal_budgeted(MOCK_INVOCATION_ID_1, 2, &mut budget)
        .unwrap()
        .map(|item| item.unwrap().1)
        .collect()
        .await;
    assert_eq!(entries, [large.clone(), small]);
    assert_eq!(
        rocksdb
            .get_blob::<RawParts>(blob_id)
            .await
            .unwrap()
            .unwrap()
            .1,
        2
    );

    // Each journal releases its reference
    let mut txn = rocksdb.transaction();
    txn.delete_journal(&MOCK_INVOCATION_ID_1, 2).unwrap();
    txn.commit().await.unwrap();
    drop(txn);
    verify_journal_deleted(&mut rocksdb.transaction(), 2).await;
    let copied = rocksdb
        .get_journal_entry(copy_id, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rocksdb.hydrate_journal_entry(copied).unwrap(), large);

    let mut txn = rocksdb.transaction();
    txn.delete_journal(&copy_id, 1).unwrap();
    txn.commit().await.unwrap();
    drop(txn);
    assert!(
        rocksdb
            .get_blob::<RawParts>(blob_id)
            .await
            .unwrap()
            .is_none()
    );
}
