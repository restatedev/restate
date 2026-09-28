// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::ops::ControlFlow;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Int64Array, LargeStringArray, StringArray, TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_expr::planner::logical2physical;
use futures::TryStreamExt;

use restate_partition_store::PartitionStoreManager;
use restate_partition_store::index::{
    BusyVQueueKey, EntryByStageKey, EntryByStageServiceKey, EntryByVirtualObjectStageKey,
    EntryNextAtByStageKey, EntryNextAtByStageServiceKey, EntryNextAtByVirtualObjectStageKey,
};
use restate_partition_store::keys::{IndexFieldEncode, KeyKind};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::filter::Filter;
use restate_storage_api::index::{
    BusyVQueue, EntryByStage, EntryByVirtualObject, EntryNextAtByService, EntryNextAtByStage,
    EntryNextAtByVirtualObject,
};
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryStatusHeader, ReadVQueueTable,
    Stage, Status, WriteVQueueTable,
};
use restate_types::clock::{RoughTimestamp, UniqueTimestamp};
use restate_types::config::{Configuration, QueryEngineOptions};
use restate_types::errors::GenericError;
use restate_types::identifiers::CanonicalEntryId;
use restate_types::partition_table::Partition;
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, Seq, VQueueId};

use crate::context::{PartitionTables, QueryContext, SelectPartitions};
use crate::mocks::MockQueryEngine;
use crate::partition_store_scanner::ScanLocalPartitionFilter;
use crate::remote_query_scanner_manager::RemoteScannerManager;

use super::entry_by_stage::schema::IdxEntryByStageBuilder;
use super::entry_next_at_by_stage::schema::IdxEntryNextAtByStageBuilder;

const TABLES: [(&str, &str); 2] = [
    ("_idx_entry_by_stage", "transitioned_at"),
    ("_idx_entry_next_at_by_stage", "next_at"),
];

const NEW_ENTRY_TABLES: [(&str, &str); 3] = [
    ("_idx_entry_next_at_by_service", "next_at"),
    ("_idx_entry_by_virtual_object", "transitioned_at"),
    ("_idx_entry_next_at_by_virtual_object", "next_at"),
];

fn at(offset: u64, logical: u64) -> UniqueTimestamp {
    UniqueTimestamp::try_from_parts(100_000 + offset, logical).unwrap()
}

async fn select(engine: &MockQueryEngine, sql: &str) -> Vec<RecordBatch> {
    engine
        .execute(sql)
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

fn strings(batches: &[RecordBatch], name: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn assert_ids(
    engine: &MockQueryEngine,
    table: &str,
    predicate: &str,
    expected: &[CanonicalEntryId],
) {
    let batches = select(
        engine,
        &format!("SELECT canonical_id FROM {table} WHERE {predicate}"),
    )
    .await;
    let mut actual = strings(&batches, "canonical_id");
    actual.sort();
    let mut expected = expected.iter().map(ToString::to_string).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected, "{table}: {predicate}");
}

async fn populate(engine: &mut MockQueryEngine) -> Vec<CanonicalEntryId> {
    let mut config = Configuration::default();
    config.common.experimental.set_indexes_v1(true);
    engine
        .partition_store()
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();
    let mut tx = engine.partition_store().transaction();
    let mut ids = Vec::new();
    for (i, (stage, status, kind, pk, timestamp, next)) in [
        (
            Stage::Inbox,
            Status::New,
            EntryKind::Invocation,
            100,
            at(0, 1),
            RoughTimestamp::new(100),
        ),
        (
            Stage::Inbox,
            Status::Scheduled,
            EntryKind::StateMutation,
            200,
            at(0, 2),
            RoughTimestamp::new(101),
        ),
        (
            Stage::Inbox,
            Status::BackingOff,
            EntryKind::Invocation,
            100,
            at(1, 0),
            RoughTimestamp::new(103),
        ),
        (
            Stage::Running,
            Status::Started,
            EntryKind::Invocation,
            300,
            at(2, 0),
            RoughTimestamp::new(102),
        ),
        (
            Stage::Finished,
            Status::Succeeded,
            EntryKind::StateMutation,
            200,
            at(3, 0),
            RoughTimestamp::MAX,
        ),
        (
            Stage::Paused,
            Status::Failed,
            EntryKind::Invocation,
            400,
            at(4, 0),
            RoughTimestamp::RESTATE_EPOCH,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let qid = VQueueId::custom(pk, format!("stage-{i}"));
        let target = EntryTargetRef::VirtualObject {
            service: "svc",
            scope: if matches!(i, 1 | 4) {
                Some("tenant")
            } else {
                None
            },
            key: if matches!(i, 1 | 4) { "b" } else { "key" },
            handler: if kind == EntryKind::StateMutation {
                HandlerRef::StateMutation
            } else {
                HandlerRef::UserHandler("handle")
            },
        };
        let key = EntryKey::new(false, next, i as u64, EntryId::new(kind, [i as u8 + 1; 16]));
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &target,
            },
            EntryStateRef {
                stage,
                status,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(timestamp, next),
            },
        );
        ids.push(key.to_canonical_entry_id(pk));
    }
    tx.commit().await.unwrap();
    ids
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn stage_tables_project_filter_and_follow_lifecycle_updates() {
    let mut engine = MockQueryEngine::create().await;
    for (table, _) in TABLES {
        assert_ids(&engine, table, "true", &[]).await;
    }
    let ids = populate(&mut engine).await;
    let ms = at(0, 0).to_unix_millis().as_u64();
    for (table, time) in TABLES {
        assert_ids(&engine, table, "true", &ids).await;
        assert_ids(
            &engine,
            table,
            "stage IN ('inbox', 'inbox', NULL)",
            &ids[..3],
        )
        .await;
        assert_ids(
            &engine,
            table,
            &format!("stage = 'inbox' AND canonical_id = '{}'", ids[1]),
            &ids[1..2],
        )
        .await;
        assert_ids(
            &engine,
            table,
            "stage = 'not-a-stage' OR stage IS NULL",
            &[],
        )
        .await;
        assert_ids(&engine, table, "stage NOT IN ('inbox')", &ids[3..]).await;
        assert_ids(&engine, table, "partition_key = 100", &[ids[0], ids[2]]).await;
        assert_ids(
            &engine,
            table,
            &format!("canonical_id IN ('{}', '{}', '{}')", ids[0], ids[1], ids[0]),
            &ids[..2],
        )
        .await;
        assert_ids(
            &engine,
            table,
            &format!("canonical_id = '{}'", ids[1].with_seq(Seq::MAX)),
            &[],
        )
        .await;
        assert_ids(
            &engine,
            table,
            &format!("entry_id = '{}'", ids[1].to_base_entry_id()),
            &ids[1..2],
        )
        .await;
        let count = select(&engine, &format!("SELECT COUNT(*) FROM {table}")).await;
        assert_eq!(
            count[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            6
        );

        let projected = select(
            &engine,
            &format!("SELECT * FROM {table} WHERE canonical_id = '{}'", ids[1]),
        )
        .await;
        assert_eq!(
            projected[0]
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            if time == "next_at" {
                vec![
                    "stage",
                    time,
                    "seq",
                    "canonical_id",
                    "entry_id",
                    "partition_key",
                ]
            } else {
                vec!["stage", time, "canonical_id", "entry_id", "partition_key"]
            }
        );
        assert_eq!(strings(&projected, "canonical_id"), [ids[1].to_string()]);
        assert_eq!(
            strings(&projected, "entry_id"),
            [ids[1].to_base_entry_id().to_string()]
        );
        assert!(
            engine
                .execute(&format!("SELECT status FROM {table}"))
                .await
                .is_err()
        );
        if time == "next_at" {
            assert_eq!(
                projected[0]
                    .column_by_name("seq")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                1
            );
        }
        let timestamp = projected[0]
            .column_by_name(time)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0);
        assert_eq!(
            timestamp,
            (ms + if time == "next_at" { 1000 } else { 0 }) as i64
        );

        let explain = select(&engine, &format!("EXPLAIN ANALYZE SELECT canonical_id FROM {table} WHERE stage = 'inbox' AND canonical_id = '{}'", ids[1])).await;
        let plan = explain
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("plan")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect::<Vec<_>>()
            .join("\n");
        for metric in [
            "storage_scans_reported=1",
            "storage_scans_completed=1",
            "storage_iterators=1",
        ] {
            assert!(plan.contains(metric), "{table}: missing {metric}: {plan}");
        }
    }
    assert_ids(
        &engine,
        TABLES[0].0,
        &format!("transitioned_at = to_timestamp_millis({ms})"),
        &ids[..2],
    )
    .await;
    for (predicate, expected) in [
        (
            format!("next_at = to_timestamp_millis({})", ms + 1),
            &ids[..0],
        ),
        (
            format!(
                "next_at > to_timestamp_millis({}) AND next_at <= to_timestamp_millis({})",
                ms + 1,
                ms + 1000
            ),
            &ids[1..2],
        ),
        (
            format!(
                "next_at >= to_timestamp_millis({}) AND next_at < to_timestamp_millis({})",
                ms + 1000,
                ms + 2000
            ),
            &ids[1..2],
        ),
        ("next_at = to_timestamp_millis(-1)".into(), &ids[..0]),
    ] {
        assert_ids(&engine, TABLES[1].0, &predicate, expected).await;
    }

    let mut tx = engine.partition_store().transaction();
    let header = tx
        .get_vqueue_entry_status(&ids[0].to_base_entry_id())
        .await
        .unwrap()
        .unwrap();
    let before = EntryStateRef::from_header(&header);
    let key = before.entry_key.set_run_at(Some(RoughTimestamp::MAX));
    let after = EntryStateRef {
        entry_key: &key,
        status: Status::Scheduled,
        ..before
    };
    let qid = VQueueId::custom(100, "stage-0");
    let target = EntryTargetRef::VirtualObject {
        service: "svc",
        scope: None,
        key: "key",
        handler: HandlerRef::UserHandler("handle"),
    };
    let context = EntryContext {
        qid: &qid,
        target: &target,
    };
    tx.update_vqueue_entry_status(&context, before, after);
    tx.commit().await.unwrap();
    drop(tx);
    for (table, _) in TABLES {
        assert_ids(&engine, table, "stage = 'inbox'", &ids[..3]).await;
    }
    assert_ids(
        &engine,
        TABLES[1].0,
        &format!(
            "next_at = to_timestamp_millis({})",
            RoughTimestamp::MAX.as_unix_millis().as_u64()
        ),
        &[ids[0], ids[4]],
    )
    .await;
    let mut tx = engine.partition_store().transaction();
    tx.delete_vqueue_entry_status(&context, after);
    tx.commit().await.unwrap();
    drop(tx);
    for (table, _) in TABLES {
        assert_ids(&engine, table, "true", &ids[1..]).await;
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn stage_index_native_filters_survive_transport_and_skip_timestamp_gaps() {
    let mut engine = MockQueryEngine::create().await;
    let ids = populate(&mut engine).await;
    let partition = engine.partition_store().partition_id();
    let mut tx = engine.partition_store().transaction();
    let mut poison = Vec::new();
    EntryByStageKey::prefix(partition, &mut poison)
        .stage(Stage::Inbox)
        .transitioned_at(Reverse(at(100, 0)));
    tx.raw_put_cf(KeyKind::SecondaryIndex, poison, []);
    let mut poison = Vec::new();
    EntryNextAtByStageKey::prefix(partition, &mut poison)
        .stage(Stage::Inbox)
        .next_at(RoughTimestamp::new(102));
    tx.raw_put_cf(KeyKind::SecondaryIndex, poison, []);
    tx.commit().await.unwrap();
    drop(tx);
    let ms = at(0, 0).to_unix_millis().as_u64();
    assert_ids(
        &engine,
        TABLES[0].0,
        &format!(
            "stage = 'inbox' AND transitioned_at <= to_timestamp_millis({})",
            ms + 1
        ),
        &ids[..3],
    )
    .await;
    assert_ids(&engine, TABLES[1].0, &format!("stage = 'inbox' AND next_at IN (to_timestamp_millis({ms}), to_timestamp_millis({}), to_timestamp_millis({}), NULL)", ms + 3000, ms + 1), &[ids[0], ids[2]]).await;

    for next_at in [false, true] {
        let schema = if next_at {
            IdxEntryNextAtByStageBuilder::schema()
        } else {
            IdxEntryByStageBuilder::schema()
        };
        let time = if next_at {
            "next_at"
        } else {
            "transitioned_at"
        };
        let millis = ms + if next_at { 1000 } else { 0 };
        let predicate = logical2physical(
            &col("stage")
                .eq(lit("inbox"))
                .and(col(time).eq(lit(ScalarValue::TimestampMillisecond(
                    Some(millis as i64),
                    None,
                ))))
                .and(col("canonical_id").eq(lit(ids[1].to_string()))),
            &schema,
        );
        let remote = crate::decode_expr(
            &TaskContext::default(),
            &schema,
            &crate::encode_expr(&predicate).unwrap(),
        )
        .unwrap();
        for predicate in [predicate, remote] {
            for range in [KeyRange::FULL, KeyRange::new(100, 199)] {
                let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
                let store = engine.partition_store();
                if next_at {
                    let filter = Filter::<EntryNextAtByStage>::new(range, Some(predicate.clone()));
                    store
                        .scan_entry_next_at_by_stage(range, &filter, None, move |key| {
                            sender.send(key.canonical_id.decode().unwrap()).unwrap();
                            ControlFlow::Continue(())
                        })
                        .unwrap()
                        .await
                        .unwrap();
                } else {
                    let filter = Filter::<EntryByStage>::new(range, Some(predicate.clone()));
                    store
                        .scan_entry_by_stage(range, &filter, None, move |key| {
                            sender.send(key.canonical_id.decode().unwrap()).unwrap();
                            ControlFlow::Continue(())
                        })
                        .unwrap()
                        .await
                        .unwrap();
                }
                assert_eq!(
                    receiver.recv().await,
                    if range == KeyRange::FULL {
                        Some(ids[1])
                    } else {
                        None
                    }
                );
                assert!(receiver.recv().await.is_none());
            }
        }
    }
    for (table, _) in TABLES {
        let result = engine
            .execute(format!("SELECT COUNT(*) FROM {table}"))
            .await;
        assert!(
            match result {
                Err(_) => true,
                Ok(result) => result.stream.try_collect::<Vec<_>>().await.is_err(),
            },
            "malformed selected key must fail: {table}"
        );
    }
}

#[derive(Debug, Clone)]
struct Partitions(Vec<(PartitionId, Partition)>);

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_entry_index_defers_unprojected_stage_decoding() {
    let mut engine = MockQueryEngine::create().await;
    let partition = engine.partition_store().partition_id();
    let id = EntryId::new(EntryKind::Invocation, [1; 16])
        .to_base_id(100)
        .canonicalize(Seq::new(1));
    for (table, next, service, object) in [
        ("_idx_entry_by_service", false, true, false),
        ("_idx_entry_by_stage", false, false, false),
        ("_idx_entry_next_at_by_stage", true, false, false),
        ("_idx_entry_next_at_by_service", true, true, false),
        ("_idx_entry_by_virtual_object", false, true, true),
        ("_idx_entry_next_at_by_virtual_object", true, true, true),
    ] {
        let mut bytes = Vec::new();
        match (next, service, object) {
            (false, true, false) => {
                EntryByStageServiceKey::prefix(partition, &mut bytes);
            }
            (false, false, false) => {
                EntryByStageKey::prefix(partition, &mut bytes);
            }
            (true, false, false) => {
                EntryNextAtByStageKey::prefix(partition, &mut bytes);
            }
            (true, true, false) => {
                EntryNextAtByStageServiceKey::prefix(partition, &mut bytes);
            }
            (false, true, true) => {
                EntryByVirtualObjectStageKey::prefix(partition, &mut bytes)
                    .service_name("svc")
                    .scope(None::<&str>)
                    .key("key");
            }
            (true, true, true) => {
                EntryNextAtByVirtualObjectStageKey::prefix(partition, &mut bytes)
                    .service_name("svc")
                    .scope(None::<&str>)
                    .key("key");
            }
            _ => unreachable!(),
        }
        "future-stage".encode_field(&mut bytes);
        if service && !object {
            "svc".encode_field(&mut bytes);
        }
        if next {
            RoughTimestamp::new(100).encode_field(&mut bytes);
            id.seq().encode_field(&mut bytes);
        } else {
            Reverse(at(0, 1)).encode_field(&mut bytes);
        }
        id.encode_field(&mut bytes);
        let mut tx = engine.partition_store().transaction();
        tx.raw_put_cf(KeyKind::SecondaryIndex, bytes, []);
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(
            strings(
                &select(&engine, &format!("SELECT canonical_id FROM {table}")).await,
                "canonical_id"
            ),
            [id.to_string()]
        );
        let result = engine.execute(&format!("SELECT stage FROM {table}")).await;
        assert!(
            match result {
                Err(_) => true,
                Ok(result) => result.stream.try_collect::<Vec<_>>().await.is_err(),
            },
            "{table}"
        );
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_entry_tables_filter_scopes_sequences_and_follow_lifecycle() {
    let mut engine = MockQueryEngine::create().await;
    let ids = populate(&mut engine).await;
    for (table, time) in NEW_ENTRY_TABLES {
        assert_ids(&engine, table, "true", &ids).await;
        assert_ids(
            &engine,
            table,
            "service_name LIKE 'sv%' AND stage = 'inbox'",
            &ids[..3],
        )
        .await;
        assert_ids(
            &engine,
            table,
            &format!("canonical_id IN ('{}', '{}', '{}')", ids[0], ids[1], ids[0]),
            &ids[..2],
        )
        .await;
        assert_ids(
            &engine,
            table,
            "partition_key IN (100, 400)",
            &[ids[0], ids[2], ids[5]],
        )
        .await;
        let projected = select(
            &engine,
            &format!("SELECT * FROM {table} WHERE canonical_id = '{}'", ids[1]),
        )
        .await;
        assert!(projected[0].column_by_name("partition_id").is_none());
        assert!(projected[0].column_by_name("status").is_none());
        assert_eq!(strings(&projected, "service_name"), ["svc"]);
        assert_eq!(
            strings(&projected, "entry_id"),
            [ids[1].to_base_entry_id().to_string()]
        );
        if table.contains("virtual_object") {
            assert_eq!(strings(&projected, "scope"), ["tenant"]);
            assert_eq!(strings(&projected, "key"), ["b"]);
            assert_ids(
                &engine,
                table,
                "scope IS NULL",
                &[ids[0], ids[2], ids[3], ids[5]],
            )
            .await;
            assert_ids(
                &engine,
                table,
                "scope = 'tenant' AND key LIKE 'b%'",
                &[ids[1], ids[4]],
            )
            .await;
            assert_ids(&engine, table, "scope = ''", &[]).await;
        }
        if time == "next_at" {
            assert_ids(&engine, table, "seq >= 1 AND seq < 3", &ids[1..3]).await;
            assert_ids(&engine, table, "seq IN (0, 5, 0)", &[ids[0], ids[5]]).await;
            assert_eq!(
                projected[0]
                    .column_by_name("seq")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                1
            );
        }
    }

    // Check native predicates after remote expression serialization, without any
    // residual DataFusion filtering that could conceal a missing storage binding.
    macro_rules! native {
        ($target:ty, $builder:ty, $scan:ident, $expr:expr) => {{
            let schema = <$builder>::schema();
            let predicate = logical2physical(&$expr, &schema);
            let remote = crate::decode_expr(
                &TaskContext::default(),
                &schema,
                &crate::encode_expr(&predicate).unwrap(),
            )
            .unwrap();
            for predicate in [predicate, remote] {
                for range in [KeyRange::FULL, KeyRange::new(100, 199)] {
                    let filter = Filter::<$target>::new(range, Some(predicate.clone()));
                    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
                    engine
                        .partition_store()
                        .$scan(range, &filter, None, move |key| {
                            sender.send(key.canonical_id.decode().unwrap()).unwrap();
                            ControlFlow::Continue(())
                        })
                        .unwrap()
                        .await
                        .unwrap();
                    assert_eq!(
                        receiver.recv().await,
                        (range == KeyRange::FULL).then_some(ids[1])
                    );
                    assert!(receiver.recv().await.is_none());
                }
            }
        }};
    }
    native!(
        EntryNextAtByService,
        super::entry_next_at_by_service::schema::IdxEntryNextAtByServiceBuilder,
        scan_entry_next_at_by_service,
        col("stage")
            .eq(lit("inbox"))
            .and(col("service_name").eq(lit("svc")))
            .and(col("seq").eq(lit(1u64)))
    );
    native!(
        EntryByVirtualObject,
        super::by_virtual_object::schema::IdxEntryByVirtualObjectBuilder,
        scan_entry_by_virtual_object,
        col("scope")
            .eq(lit("tenant"))
            .and(col("key").eq(lit("b")))
            .and(col("stage").eq(lit("inbox")))
    );
    native!(
        EntryNextAtByVirtualObject,
        super::entry_next_at_by_virtual_object::schema::IdxEntryNextAtByVirtualObjectBuilder,
        scan_entry_next_at_by_virtual_object,
        col("scope")
            .eq(lit("tenant"))
            .and(col("key").eq(lit("b")))
            .and(col("seq").eq(lit(1u64)))
    );

    let qid = VQueueId::custom(200, "stage-1");
    let target = EntryTargetRef::VirtualObject {
        service: "svc",
        scope: Some("tenant"),
        key: "b",
        handler: HandlerRef::StateMutation,
    };
    let context = EntryContext {
        qid: &qid,
        target: &target,
    };
    let mut tx = engine.partition_store().transaction();
    let header = tx
        .get_vqueue_entry_status(&ids[1].to_base_entry_id())
        .await
        .unwrap()
        .unwrap();
    let before = EntryStateRef::from_header(&header);
    let key = before.entry_key.set_run_at(Some(RoughTimestamp::MAX));
    let mut stats = header.stats().clone();
    stats.transitioned_at = at(10, 1);
    let after = EntryStateRef {
        stage: Stage::Paused,
        entry_key: &key,
        stats: &stats,
        ..before
    };
    tx.update_vqueue_entry_status(&context, before, after);
    tx.commit().await.unwrap();
    drop(tx);
    for (table, time) in NEW_ENTRY_TABLES {
        assert_ids(&engine, table, "stage = 'inbox'", &[ids[0], ids[2]]).await;
        let millis = if time == "next_at" {
            RoughTimestamp::MAX.as_unix_millis().as_u64()
        } else {
            stats.transitioned_at.to_unix_millis().as_u64()
        };
        assert_ids(
            &engine,
            table,
            &format!("stage = 'paused' AND {time} = to_timestamp_millis({millis})"),
            &ids[1..2],
        )
        .await;
    }
    let mut tx = engine.partition_store().transaction();
    tx.delete_vqueue_entry_status(&context, after);
    tx.commit().await.unwrap();
    drop(tx);
    for (table, _) in NEW_ENTRY_TABLES {
        assert_ids(
            &engine,
            table,
            "true",
            &[ids[0], ids[2], ids[3], ids[4], ids[5]],
        )
        .await;
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_queue_table_covers_counts_and_translates_descending_filters() {
    use super::busy_vqueue::schema::IdxBusyVqueueBuilder;
    use restate_partition_store::index::SecondaryIndexKey;

    let mut engine = MockQueryEngine::create().await;
    let partition = engine.partition_store().partition_id();
    let qids =
        [0, 1, 2, 3].map(|i| VQueueId::custom(if i < 2 { 100 } else { 200 }, format!("q{i}")));
    let mut tx = engine.partition_store().transaction();
    for (i, count) in [0u64, 1, 3, 5].into_iter().enumerate() {
        let key = BusyVQueueKey::borrowed(
            Reverse(count),
            Reverse(at((i / 2) as u64, i as u64)),
            [None, Some(""), Some("tenant"), Some("tenant")][i],
            qids[i].clone(),
        );
        let mut value = Vec::new();
        if count != 0 {
            value.push(Stage::Inbox as u8);
            value.extend_from_slice(&count.to_be_bytes());
        }
        tx.update_covering_secondary_index(None, Some((&key, &value)));
    }
    tx.commit().await.unwrap();
    drop(tx);

    // No queue metadata was written: every result comes from the covering index.
    let rows = select(
        &engine,
        "SELECT * FROM _idx_busy_vqueue ORDER BY total_non_completed DESC",
    )
    .await;
    assert_eq!(
        strings(&rows, "vqueue_id"),
        qids.iter()
            .rev()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    let counts = rows
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("num_inbox")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(counts, [5, 3, 1, 0]);
    for column in ["num_running", "num_suspended", "num_paused", "num_finished"] {
        assert!(rows.iter().all(|batch| {
            batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .iter()
                .all(|&n| n == 0)
        }));
    }
    for (predicate, indices) in [
        ("total_non_completed = 0", vec![0]),
        (
            "total_non_completed > 1 AND total_non_completed < 5",
            vec![2],
        ),
        (
            "total_non_completed >= 1 AND total_non_completed <= 3",
            vec![1, 2],
        ),
        ("total_non_completed IN (0, 5, 0)", vec![0, 3]),
        ("total_non_completed < 0", vec![]),
        ("total_non_completed <= 0", vec![0]),
        ("total_non_completed > 5", vec![]),
        ("scope IS NULL", vec![0]),
        ("scope = ''", vec![1]),
        ("scope LIKE 'ten%'", vec![2, 3]),
        ("num_inbox > 1", vec![2, 3]),
    ] {
        let mut actual = strings(
            &select(
                &engine,
                &format!("SELECT vqueue_id FROM _idx_busy_vqueue WHERE {predicate}"),
            )
            .await,
            "vqueue_id",
        );
        actual.sort();
        let mut expected = indices
            .into_iter()
            .map(|i| qids[i].to_string())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(actual, expected, "{predicate}");
    }
    let schema = IdxBusyVqueueBuilder::schema();
    assert!(
        engine
            .execute("SELECT vqueue_id FROM _idx_busy_vqueue WHERE vqueue_id = 'invalid'")
            .await
            .is_err()
    );
    for (expression, indices) in [
        (col("vqueue_id").eq(lit("invalid")), vec![]),
        (
            col("total_non_completed")
                .gt(lit(1u64))
                .and(col("total_non_completed").lt(lit(5u64))),
            vec![2],
        ),
        (
            col("total_non_completed")
                .gt_eq(lit(1u64))
                .and(col("total_non_completed").lt_eq(lit(3u64))),
            vec![1, 2],
        ),
        (
            col("total_non_completed").in_list(vec![lit(0u64), lit(5u64)], false),
            vec![0, 3],
        ),
        (
            col("last_modified").eq(lit(ScalarValue::TimestampMillisecond(
                Some(at(0, 0).to_unix_millis().as_u64() as i64),
                None,
            ))),
            vec![0, 1],
        ),
        (
            col("vqueue_id").in_list(
                vec![
                    lit(ScalarValue::LargeUtf8(Some(qids[0].to_string()))),
                    lit(ScalarValue::LargeUtf8(Some(qids[2].to_string()))),
                ],
                false,
            ),
            vec![0, 2],
        ),
    ] {
        let physical = logical2physical(&expression, &schema);
        let remote = crate::decode_expr(
            &TaskContext::default(),
            &schema,
            &crate::encode_expr(&physical).unwrap(),
        )
        .unwrap();
        for predicate in [physical, remote] {
            for range in [KeyRange::FULL, KeyRange::new(100, 199)] {
                let filter = Filter::<BusyVQueue>::new(range, Some(predicate.clone()));
                let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
                engine
                    .partition_store()
                    .scan_busy_vqueues(range, &filter, None, move |key, _| {
                        sender
                            .send(key.vqueue_id.decode().unwrap().to_string())
                            .unwrap();
                        ControlFlow::Continue(())
                    })
                    .unwrap()
                    .await
                    .unwrap();
                let mut actual = Vec::new();
                while let Some(id) = receiver.recv().await {
                    actual.push(id);
                }
                actual.sort();
                let mut expected = indices
                    .iter()
                    .filter(|&&i| range == KeyRange::FULL || i < 2)
                    .map(|&i| qids[i].to_string())
                    .collect::<Vec<_>>();
                expected.sort();
                assert_eq!(actual, expected);
            }
        }
    }
    let selected = select(
        &engine,
        &format!(
            "SELECT vqueue_id FROM _idx_busy_vqueue WHERE vqueue_id IN ('{}','{}','{}')",
            qids[0], qids[1], qids[0]
        ),
    )
    .await;
    assert_eq!(strings(&selected, "vqueue_id").len(), 2);
    // Removing the raw index entry is reflected without any primary-record lookup.
    let mut encoded = Vec::new();
    BusyVQueueKey::borrowed(Reverse(0), Reverse(at(0, 0)), None::<&str>, qids[0].clone())
        .encode_key(partition, &mut encoded);
    let mut tx = engine.partition_store().transaction();
    tx.raw_delete_cf(KeyKind::SecondaryIndex, encoded);
    tx.commit().await.unwrap();
    drop(tx);
    assert!(
        strings(
            &select(
                &engine,
                "SELECT vqueue_id FROM _idx_busy_vqueue WHERE total_non_completed = 0"
            )
            .await,
            "vqueue_id"
        )
        .is_empty()
    );
}

#[async_trait]
impl SelectPartitions for Partitions {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(self.0.clone())
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn stage_tables_register_for_partition_queries_and_sort_across_stores() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let partitions = Partitions(vec![
        (
            PartitionId::from(0),
            Partition::new(PartitionId::from(0), KeyRange::new(100, 199)),
        ),
        (
            PartitionId::from(1),
            Partition::new(PartitionId::from(1), KeyRange::new(200, 299)),
        ),
    ]);
    let mut ids = Vec::new();
    for (i, (_, partition)) in partitions.0.iter().enumerate() {
        let mut store = manager.open(partition, None).await.unwrap();
        let mut config = Configuration::default();
        config.common.experimental.set_indexes_v1(true);
        store
            .verify_and_run_migrations(Default::default(), &config)
            .await
            .unwrap();
        let id = EntryId::new(
            if i == 0 {
                EntryKind::Invocation
            } else {
                EntryKind::StateMutation
            },
            [i as u8 + 1; 16],
        )
        .to_base_id(110 + i as u64 * 100)
        .canonicalize(Seq::new(i as u64));
        let qid = VQueueId::custom(id.partition_key(), "partitioned");
        let target = EntryTargetRef::VirtualObject {
            service: "svc",
            scope: None,
            key: "key",
            handler: HandlerRef::UserHandler("handle"),
        };
        let entry = EntryKey::new(
            false,
            RoughTimestamp::new(200 - i as u32),
            id.seq(),
            *id.as_entry_id(),
        );
        let mut tx = store.transaction();
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &target,
            },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: &entry,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(at(i as u64, 0), entry.run_at()),
            },
        );
        tx.commit().await.unwrap();
        ids.push(id);
    }
    let scanners =
        RemoteScannerManager::local_only(restate_core::MetadataBuilder::default().to_metadata());
    let mut options = QueryEngineOptions::default();
    options
        .datafusion_options
        .insert("datafusion.execution.target_partitions".into(), "1".into());
    let ctx = QueryContext::create(
        &options,
        PartitionTables::new(partitions, manager, scanners.clone()),
    )
    .await
    .unwrap();
    for (table, time) in TABLES.into_iter().chain(NEW_ENTRY_TABLES) {
        assert!(scanners.local_partition_scanner(table).is_some());
        let direction = if time == "next_at" { "ASC" } else { "DESC" };
        let batches = ctx
            .execute(&format!(
                "SELECT canonical_id FROM {table} ORDER BY {time} {direction} LIMIT 1"
            ))
            .await
            .unwrap()
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(strings(&batches, "canonical_id"), [ids[1].to_string()]);
        for (column, expected) in [
            ("canonical_id", ids[1].to_string()),
            ("entry_id", ids[1].to_base_entry_id().to_string()),
        ] {
            let batches = ctx.execute(&format!("SELECT canonical_id FROM {table} WHERE {column} IN ('{expected}', '{expected}')")).await.unwrap().stream.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(strings(&batches, "canonical_id"), [ids[1].to_string()]);
        }
    }
}
