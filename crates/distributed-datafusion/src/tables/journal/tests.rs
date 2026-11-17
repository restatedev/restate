// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use futures::TryStreamExt;
use prost::Message;

use restate_partition_store::keys::{EncodeTableKey, KeyKind};
use restate_partition_store::{PartitionStoreManager, journal_table, journal_table_v2};
use restate_rocksdb::RocksDbManager;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::Transaction;
use restate_storage_api::journal_table::{JournalEntry, WriteJournalTable};
use restate_storage_api::journal_table_v2::WriteJournalTable as WriteJournalTableV2;
use restate_storage_query_api::{QueryOptions, QuerySession};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionId};
use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use restate_types::journal::enriched::{
    CallEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
};
use restate_types::journal::{CompletionResult, Entry, InputEntry};
use restate_types::journal_v2::{CallCommand, CallRequest, SleepCommand, SleepCompletion};
use restate_types::partition_table::Partition;
use restate_types::service_protocol;
use restate_types::sharding::KeyRange;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::time::MillisSinceEpoch;

use crate::mocks::{differential_sessions, rows};

fn key(key: impl EncodeTableKey) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(key.serialized_length());
    key.serialize_to(&mut bytes);
    bytes
}

async fn query_error(session: &dyn QuerySession, sql: &str) -> String {
    match session.execute(sql, QueryOptions {}).await {
        Err(error) => error.to_string(),
        Ok(result) => result
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err()
            .to_string(),
    }
}

#[restate_core::test]
async fn journal_matches_v1_and_id_lookups_skip_other_invocations() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    // Invocations of one virtual-object key share a partition key. The neighbor's
    // UUID sorts between the other two.
    let on_key = |uuid| InvocationId::from_parts(7, InvocationUuid::from_u128(uuid));
    let (legacy, neighbor, current) = (on_key(1), on_key(2), on_key(3));
    let elsewhere = InvocationId::from_parts(9, InvocationUuid::from_u128(4));

    let entry = |header, message: Vec<u8>| {
        JournalEntry::Entry(EnrichedRawEntry::new(header, message.into()))
    };
    let legacy_entries = [
        JournalEntry::Entry(ProtobufRawEntryCodec::serialize_enriched(Entry::Input(
            InputEntry {
                headers: vec![],
                value: Default::default(),
            },
        ))),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Call {
                is_completed: false,
                enrichment_result: Some(CallEnrichmentResult {
                    invocation_id: elsewhere,
                    invocation_target: InvocationTarget::mock_virtual_object(),
                    completion_retention_time: None,
                    span_context: Default::default(),
                }),
            },
            Bytes::new(),
        )),
        entry(
            EnrichedEntryHeader::Run {},
            service_protocol::RunEntryMessage {
                name: "side-effect".into(),
                result: None,
            }
            .encode_to_vec(),
        ),
        entry(
            EnrichedEntryHeader::GetPromise {
                is_completed: false,
            },
            service_protocol::GetPromiseEntryMessage {
                key: "approval".into(),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        entry(
            EnrichedEntryHeader::Sleep { is_completed: true },
            service_protocol::SleepEntryMessage {
                wake_up_time: 1_700_000_000_000,
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        JournalEntry::Completion(CompletionResult::Empty),
    ];
    let current_entries = [
        restate_types::journal_v2::Entry::from(SleepCommand {
            wake_up_time: 1.into(),
            completion_id: 0,
            name: "nap".into(),
        }),
        restate_types::journal_v2::Entry::from(CallCommand {
            request: CallRequest {
                invocation_id: legacy,
                invocation_target: InvocationTarget::mock_service(),
                span_context: ServiceInvocationSpanContext::empty(),
                parameter: Bytes::from_static(b"payload"),
                headers: vec![],
                idempotency_key: None,
                completion_retention_duration: Default::default(),
                journal_retention_duration: Default::default(),
                limit_key: Default::default(),
            },
            invocation_id_completion_id: 1,
            name: Default::default(),
            result_completion_id: 2,
        }),
        restate_types::journal_v2::Entry::from(SleepCompletion { completion_id: 0 }),
    ];
    let mut tx = store.transaction();
    for (index, entry) in legacy_entries.iter().enumerate() {
        WriteJournalTable::put_journal_entry(&mut tx, &legacy, index as u32, entry).unwrap();
    }
    WriteJournalTable::put_journal_entry(&mut tx, &neighbor, 0, &legacy_entries[0]).unwrap();
    for id in [current, elsewhere] {
        for (index, entry) in current_entries.iter().enumerate() {
            let entry = StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::new(1_700_000_000_000 + index as u64)),
                None,
                entry.clone().encode::<ServiceProtocolV4Codec>(),
            );
            WriteJournalTableV2::put_journal_entry(&mut tx, &id, index as u32, &entry, &[])
                .unwrap();
        }
    }
    tx.commit().await.unwrap();
    drop(tx);

    let [old, new] = differential_sessions(&manager).await;
    for sql in [
        "SELECT * FROM sys_journal ORDER BY id, index".to_owned(),
        format!(
            "SELECT index, entry_type, name, completed, invoked_id, invoked_target, promise_name, \
             sleep_wakeup_at FROM sys_journal WHERE id = '{legacy}' ORDER BY index"
        ),
        format!(
            "SELECT id, index, version, entry_type, name, raw_length, entry_json, entry_lite_json, \
             appended_at FROM sys_journal WHERE id IN ('{legacy}', '{current}', '{elsewhere}') \
             ORDER BY id, index"
        ),
        format!(
            "SELECT index FROM sys_journal WHERE id = '{legacy}' AND index BETWEEN 1 AND 3 \
             ORDER BY index"
        ),
        "SELECT id, index FROM sys_journal WHERE partition_key = 7 ORDER BY id, index \
         LIMIT 3 OFFSET 2"
            .into(),
        "SELECT id, index FROM sys_journal WHERE completed ORDER BY id, index LIMIT 2".into(),
        "SELECT COUNT(*) FROM sys_journal".into(),
    ] {
        assert_eq!(
            rows(new.as_ref(), &sql).await,
            rows(old.as_ref(), &sql).await,
            "{sql}"
        );
    }
    assert_eq!(
        rows(new.as_ref(), "SELECT COUNT(*) FROM sys_journal").await,
        [["13"]]
    );

    // Malformed entries of the neighbor in both layouts: ID lookups on its partition
    // key must skip them by key, before decoding, while a full scan still fails.
    let mut tx = store.transaction();
    let uuid = neighbor.invocation_uuid();
    tx.raw_put_cf(
        KeyKind::Journal,
        key(journal_table::JournalKey {
            partition_key: 7,
            invocation_uuid: uuid,
            journal_index: 1,
        }),
        b"malformed",
    );
    tx.raw_put_cf(
        KeyKind::JournalV2,
        key(journal_table_v2::JournalKey {
            partition_key: 7,
            invocation_uuid: uuid,
            journal_index: 0,
        }),
        b"malformed",
    );
    tx.commit().await.unwrap();
    drop(tx);
    for (sql, expected) in [
        (
            format!("SELECT COUNT(*) FROM sys_journal WHERE id = '{legacy}'"),
            "6",
        ),
        (
            format!("SELECT COUNT(*) FROM sys_journal WHERE id = '{legacy}' AND index >= 4"),
            "2",
        ),
        (
            format!("SELECT COUNT(*) FROM sys_journal WHERE id IN ('{legacy}', '{current}')"),
            "9",
        ),
    ] {
        assert_eq!(rows(new.as_ref(), &sql).await, [[expected]], "{sql}");
    }
    assert!(
        !query_error(
            new.as_ref(),
            "SELECT COUNT(*) FROM sys_journal WHERE partition_key = 7"
        )
        .await
        .is_empty(),
        "unselected_corruption_is_still_read"
    );
}
