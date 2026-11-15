// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_partition_store::PartitionStoreManager;
use restate_partition_store::journal_events::JournalEventKey;
use restate_partition_store::keys::{EncodeTableKey, KeyKind};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::journal_events::{EventView, WriteJournalEventsTable};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionId};
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{Event, PausedEvent, TransientErrorEvent};
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;

use crate::mocks::{differential_sessions, rows};

#[restate_core::test]
async fn journal_events_match_v1_and_id_lookups_skip_other_invocations() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    // Invocations of one virtual-object key share a partition key. The neighbor's
    // UUID sorts between the other two.
    let on_key = |uuid| InvocationId::from_parts(7, InvocationUuid::from_u128(uuid));
    let (first, neighbor, last) = (on_key(1), on_key(2), on_key(3));
    let elsewhere = InvocationId::from_parts(9, InvocationUuid::from_u128(4));
    let error = TransientErrorEvent {
        error_code: 500u16.into(),
        error_message: "boom".to_owned(),
        error_stacktrace: None,
        restate_doc_error_code: None,
        related_command_index: Some(2),
        related_command_name: Some("call".to_owned()),
        related_command_type: None,
    };
    let events = [
        Event::TransientError(error.clone()),
        Event::Paused(PausedEvent {
            last_failure: Some(error),
        }),
    ];
    let mut tx = store.transaction();
    for (lsn, id) in [first, neighbor, last, elsewhere].into_iter().enumerate() {
        for (index, event) in events.iter().enumerate() {
            let view = EventView::new(
                MillisSinceEpoch::new(1_700_000_000_000 + index as u64),
                index as u32 + 1,
                RawEvent::from(event.clone()),
            );
            tx.put_journal_event(&id, view, (lsn * 10 + index) as u64)
                .unwrap();
        }
    }
    tx.commit().await.unwrap();
    drop(tx);

    let [old, new] = differential_sessions(&manager).await;
    for sql in [
        "SELECT * FROM sys_journal_events ORDER BY id, appended_at".to_owned(),
        format!(
            "SELECT event_type, event_json FROM sys_journal_events WHERE id = '{first}' \
             ORDER BY appended_at"
        ),
        format!(
            "SELECT id, after_journal_entry_index FROM sys_journal_events \
             WHERE id IN ('{first}', '{last}', '{elsewhere}') ORDER BY id, appended_at"
        ),
        "SELECT id FROM sys_journal_events WHERE partition_key = 7 ORDER BY id, appended_at \
         LIMIT 2 OFFSET 1"
            .into(),
        "SELECT COUNT(*) FROM sys_journal_events".into(),
    ] {
        assert_eq!(
            rows(new.as_ref(), &sql).await,
            rows(old.as_ref(), &sql).await,
            "{sql}"
        );
    }
    assert_eq!(
        rows(new.as_ref(), "SELECT COUNT(*) FROM sys_journal_events").await,
        [["8"]]
    );

    // A malformed event of the neighbor: ID lookups on its partition key skip it by
    // key, before decoding, while a scan of that partition key still reads it.
    let mut tx = store.transaction();
    let key = JournalEventKey {
        partition_key: 7,
        invocation_uuid: neighbor.invocation_uuid(),
        event_type: 0,
        timestamp: 0,
        lsn: 0,
    };
    let mut bytes = Vec::with_capacity(key.serialized_length());
    key.serialize_to(&mut bytes);
    tx.raw_put_cf(KeyKind::JournalEvent, bytes, b"malformed");
    tx.commit().await.unwrap();
    drop(tx);
    for (sql, expected) in [
        (
            format!("SELECT COUNT(*) FROM sys_journal_events WHERE id = '{first}'"),
            "2",
        ),
        (
            format!("SELECT COUNT(*) FROM sys_journal_events WHERE id IN ('{first}', '{last}')"),
            "4",
        ),
    ] {
        assert_eq!(rows(new.as_ref(), &sql).await, [[expected]], "{sql}");
    }
    let scan = new
        .execute(
            "SELECT COUNT(*) FROM sys_journal_events WHERE partition_key = 7",
            restate_storage_query_api::QueryOptions {},
        )
        .await;
    let failed = match scan {
        Err(_) => true,
        Ok(result) => futures::TryStreamExt::try_collect::<Vec<_>>(result.stream)
            .await
            .is_err(),
    };
    assert!(failed, "unselected_corruption_is_still_read");
}
