// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Every storage source's key-ordered reads come out in each order it declares
//! ([`PartitionSource::ORDERS`]).

use bytes::Bytes;

use restate_clock::UniqueTimestamp;
use restate_limiter::LimitKey;
use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::{EventView, WriteJournalEventsTable};
use restate_storage_api::journal_table_v2::WriteJournalTable;
use restate_storage_api::lock_table::{AcquiredBy, LockState, WriteLockTable};
use restate_storage_api::promise_table::{Promise, PromiseState, WritePromiseTable};
use restate_storage_api::state_table::WriteStateTable;
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
};
use restate_storage_query_api::ordering::OrderTerm;
use restate_storage_query_api::{LogicalSchema, QuerySession, SelectionSchema};
use restate_types::LockName;
use restate_types::identifiers::{
    BaseEntryId, InvocationId, InvocationUuid, PartitionId, ServiceId, WithPartitionKey,
};
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{Event, PausedEvent};
use restate_types::journal_v2::{Entry, SleepCommand};
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryId, EntryTargetRef, VQueueId};

use crate::mocks::{rows, v2_session};
use crate::sharding::PartitionSource;
use crate::tables::invocation_status::SysInvocationStatusTable;
use crate::tables::journal::JournalV2Source;
use crate::tables::journal_events::SysJournalEventsTable;
use crate::tables::locks::SysLocksTable;
use crate::tables::promise::ScopedPromiseSource;
use crate::tables::state::ScopedStateSource;
use crate::tables::vqueue_entry_status::schema::SysVqueueEntryStatusTable;
use crate::tables::vqueue_meta::SysVqueueMetaTable;

/// A scan of each storage source, and a lookup where it has one, comes out in
/// each order the source declares. Several records share each partition key,
/// and IDs' text sorts unlike their orders' values. `ORDER BY` an ID still sorts
/// its text.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_ordered_reads_come_out_in_their_declared_orders() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let at = UniqueTimestamp::try_from_unix_millis(now).unwrap();
    // Few partition keys, so several records share one and the rest of the key
    // decides.
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        seed
    };
    let ids: Vec<_> = (0..48)
        .map(|_| {
            InvocationId::from_parts(
                next() % 4,
                InvocationUuid::from_u128(u128::from(next()) << 64 | u128::from(next())),
            )
        })
        .collect();
    let mut tx = store.transaction();
    for (n, id) in (1..).zip(&ids) {
        tx.put_invocation_status(
            id,
            &InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
        )
        .unwrap();
        let qid = VQueueId::custom(id.partition_key(), "q");
        let key = EntryKey::new(false, now, n, EntryId::from(id));
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &EntryTargetRef::Service {
                    scope: None,
                    service: "svc",
                    handler: "run",
                },
            },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(at, key.run_at()),
            },
        );
    }
    // Journal entries and events of every other invocation.
    for id in ids.iter().step_by(2) {
        for index in 0..3 {
            let entry = StoredRawEntry::new(
                StoredRawEntryHeader::new(now),
                Entry::from(SleepCommand {
                    wake_up_time: 1.into(),
                    completion_id: index,
                    name: "nap".into(),
                })
                .encode::<ServiceProtocolV4Codec>(),
            );
            WriteJournalTable::put_journal_entry(&mut tx, id, index, &entry, &[]).unwrap();
        }
        for lsn in 0..2 {
            let event = Event::Paused(PausedEvent { last_failure: None });
            let view = EventView::new(now, lsn as u32, RawEvent::from(event));
            tx.put_journal_event(id, view, lsn).unwrap();
        }
    }
    // Unscoped records only, which all live in one of the two layouts.
    for object in 0..24 {
        let object = format!("{object}");
        let service = ServiceId::new(None, "Counter", object.as_str());
        for key in ["count", "limits"] {
            tx.put_user_state(&service, &Bytes::from(key), "1").unwrap();
            let promise = Promise {
                state: PromiseState::NotCompleted(vec![]),
            };
            tx.put_promise(&service, &key.into(), &promise).unwrap();
        }
        tx.acquire_lock(
            &None,
            &LockName::parse(&format!("Counter/{object}")).unwrap(),
            &LockState {
                acquired_at: at,
                acquired_by: AcquiredBy::InvocationId(ids[0]),
            },
        );
    }
    for key in [1, 256, 257] {
        for name in ["a", "b", "c", "d"] {
            let meta = VQueueMeta::new(at, None, LimitKey::None, VQueueLink::None);
            tx.create_vqueue(&VQueueId::custom(key, name), &meta);
        }
    }
    tx.commit().await.unwrap();
    drop(tx);
    // One lane, so that no repartition interleaves the reads' rows: a query
    // without `ORDER BY` returns them in read order.
    let session = v2_session(
        &manager,
        vec![(
            PartitionId::MIN,
            Partition::new(PartitionId::MIN, KeyRange::FULL),
        )],
        1,
    )
    .await;
    let session = session.as_ref();
    let quoted = |values: Vec<String>| {
        values
            .iter()
            .map(|value| format!("'{value}'"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let some_ids = quoted(ids.iter().step_by(3).map(ToString::to_string).collect());
    let some_entries = quoted(
        ids.iter()
            .step_by(3)
            .map(|id| BaseEntryId::from(*id).to_string())
            .collect(),
    );
    let some_objects = quoted((0..24).step_by(3).map(|n| n.to_string()).collect());
    let some_locks = quoted((0..24).step_by(3).map(|n| format!("Counter/{n}")).collect());
    let some_queues = quoted(
        [1, 256, 257]
            .into_iter()
            .flat_map(|key| ["a", "c"].map(|name| VQueueId::custom(key, name).to_string()))
            .collect(),
    );
    assert_orders::<SysInvocationStatusTable>(session, Some(&format!("id IN ({some_ids})"))).await;
    assert_orders::<SysVqueueEntryStatusTable>(
        session,
        Some(&format!("entry_id IN ({some_entries})")),
    )
    .await;
    assert_orders::<JournalV2Source>(
        session,
        Some(&format!("id IN ({some_ids}) AND index IN (0, 2)")),
    )
    .await;
    // Events have no lookup: an ID set scans each ID's range.
    assert_orders::<SysJournalEventsTable>(session, None).await;
    assert_orders::<ScopedStateSource>(
        session,
        Some(&format!(
            "scope IS NULL AND service_name = 'Counter' AND service_key IN ({some_objects}) \
             AND key = 'count'"
        )),
    )
    .await;
    assert_orders::<ScopedPromiseSource>(
        session,
        Some(&format!(
            "scope IS NULL AND service_name = 'Counter' AND service_key IN ({some_objects}) \
             AND key = 'count'"
        )),
    )
    .await;
    assert_orders::<SysLocksTable>(
        session,
        Some(&format!("scope IS NULL AND lock_name IN ({some_locks})")),
    )
    .await;
    assert_orders::<SysVqueueMetaTable>(session, Some(&format!("id IN ({some_queues})"))).await;
    let ids_of = async |sql: &str| {
        rows(session, sql)
            .await
            .into_iter()
            .map(|mut row| row.remove(0))
            .collect::<Vec<_>>()
    };
    for table in ["sys_invocation_status", "sys_vqueue_meta"] {
        let mut text = ids_of(&format!("SELECT id FROM {table}")).await;
        text.sort();
        let sorted = ids_of(&format!("SELECT id FROM {table} ORDER BY id")).await;
        assert_eq!(sorted, text, "order_by_sorts_text: {table}");
    }
}

/// Reads `T`'s table with a scan and, given `lookup`, with a lookup of complete
/// keys, and asserts that both come out in each of `T`'s orders.
async fn assert_orders<T: PartitionSource>(session: &dyn QuerySession, lookup: Option<&str>) {
    let table = T::Schema::NAME;
    let id = T::ID_COLUMN.map(|column| T::Schema::sql_name(column).unwrap());
    let mut columns: Vec<&str> = id.into_iter().collect();
    for term in T::ORDERS.iter().flat_map(|order| order.iter()) {
        let column = match term {
            OrderTerm::Column(column) => column,
            OrderTerm::Function(_) => id.expect("function terms take the ID column"),
        };
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    let at = |column: &str| columns.iter().position(|known| *known == column).unwrap();
    let scan = format!("SELECT {} FROM {table}", columns.join(", "));
    let lookup = lookup.map(|filter| format!("{scan} WHERE {filter}"));
    if let Some(lookup) = &lookup {
        let plan = rows(session, &format!("EXPLAIN {lookup}")).await;
        assert!(
            plan.iter().any(|row| row[1].contains("MultiGetExec")),
            "looks_up: {lookup}"
        );
    }
    for sql in std::iter::once(scan).chain(lookup) {
        let read = rows(session, &sql).await;
        assert!(read.len() > 2, "reads_rows: {sql}");
        if let Some(id) = id {
            let text: Vec<_> = read.iter().map(|row| &row[at(id)]).collect();
            assert!(!text.is_sorted(), "text_order_differs: {sql}");
        }
        for order in T::ORDERS {
            let values: Vec<Vec<Vec<u8>>> = read
                .iter()
                .map(|row| {
                    order
                        .iter()
                        .map(|term| match term {
                            OrderTerm::Column(column) => row[at(column)]
                                .parse::<u64>()
                                .unwrap()
                                .to_be_bytes()
                                .to_vec(),
                            OrderTerm::Function(function) => function
                                .encode(&row[at(id.unwrap())], <[u8]>::to_vec)
                                .unwrap(),
                        })
                        .collect()
                })
                .collect();
            assert!(values.is_sorted(), "{order:?}: {sql}");
        }
    }
}
