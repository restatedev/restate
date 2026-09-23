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
    Int64Array, LargeStringArray, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_expr::planner::logical2physical;
use futures::TryStreamExt;

use restate_partition_store::PartitionStoreManager;
use restate_partition_store::index::{EntryByStageKey, EntryNextAtByStageKey};
use restate_partition_store::keys::KeyKind;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::filter::Filter;
use restate_storage_api::index::{EntryByStage, EntryNextAtByStage};
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, ReadVQueueTable, Stage, Status,
    WriteVQueueTable,
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
            scope: None,
            key: "key",
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
            "stage = 'inbox' AND status LIKE 'sched%'",
            &ids[1..2],
        )
        .await;
        assert_ids(
            &engine,
            table,
            "status = 'not-a-status' OR status IS NULL",
            &[],
        )
        .await;
        assert_ids(
            &engine,
            table,
            "status NOT IN ('new', 'scheduled')",
            &ids[2..],
        )
        .await;
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
            [
                "partition_id",
                "stage",
                time,
                "status",
                "canonical_id",
                "entry_id",
                "partition_key"
            ]
        );
        assert_eq!(strings(&projected, "canonical_id"), [ids[1].to_string()]);
        assert_eq!(
            strings(&projected, "entry_id"),
            [ids[1].to_base_entry_id().to_string()]
        );
        assert_eq!(strings(&projected, "status"), ["scheduled"]);
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

        let explain = select(&engine, &format!("EXPLAIN ANALYZE SELECT canonical_id FROM {table} WHERE stage = 'inbox' AND status = 'scheduled'")).await;
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
        assert_ids(&engine, table, "status = 'new'", &[]).await;
        assert_ids(&engine, table, "status = 'scheduled'", &ids[..2]).await;
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
                .and(col("status").eq(lit("scheduled")))
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
                            sender.send(key.canonical_id).unwrap();
                            ControlFlow::Continue(())
                        })
                        .unwrap()
                        .await
                        .unwrap();
                } else {
                    let filter = Filter::<EntryByStage>::new(range, Some(predicate.clone()));
                    store
                        .scan_entry_by_stage(range, &filter, None, move |key| {
                            sender.send(key.canonical_id).unwrap();
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
        let mut tx = store.transaction();
        tx.update_secondary_index(
            None,
            Some(&EntryByStageKey::borrowed(
                Stage::Inbox,
                Reverse(at(i as u64, 0)),
                Status::New,
                id,
            )),
        );
        tx.update_secondary_index(
            None,
            Some(&EntryNextAtByStageKey::borrowed(
                Stage::Inbox,
                RoughTimestamp::new(200 - i as u32),
                id.seq(),
                Status::New,
                id,
            )),
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
    for (table, time) in TABLES {
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
