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
    Int64Array, LargeStringArray, TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_expr::planner::logical2physical;
use futures::TryStreamExt;

use restate_partition_store::PartitionStoreManager;
use restate_partition_store::index::InvocationByServiceStageKey;
use restate_partition_store::keys::KeyKind;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, ReadVQueueTable, Stage, Status,
    WriteVQueueTable,
};
use restate_types::clock::{RoughTimestamp, UniqueTimestamp};
use restate_types::config::{Configuration, QueryEngineOptions};
use restate_types::errors::GenericError;
use restate_types::identifiers::{BaseEntryId, InvocationId, InvocationUuid};
use restate_types::partition_table::Partition;
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};

use crate::context::{PartitionTables, QueryContext, SelectPartitions};
use crate::mocks::MockQueryEngine;
use crate::partition_store_scanner::{ScanLocalPartition, ScanLocalPartitionFilter};
use crate::remote_query_scanner_manager::RemoteScannerManager;

use super::schema::IdxInvocationByServiceBuilder;
use super::table::{InvocationIndexFilter, InvocationIndexScanner};

const MS: u64 = 1_744_000_000_000;

fn timestamp(unix_millis: u64, logical: u64) -> UniqueTimestamp {
    UniqueTimestamp::try_from_parts(
        unix_millis - UniqueTimestamp::MIN.to_unix_millis().as_u64(),
        logical,
    )
    .unwrap()
}

async fn select(engine: &MockQueryEngine, sql: &str) -> Vec<RecordBatch> {
    engine
        .execute(sql)
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap()
}

async fn ids(engine: &MockQueryEngine, sql: &str) -> Vec<String> {
    select(engine, sql)
        .await
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("invocation_id")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .iter()
                .map(|id| id.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn assert_ids(engine: &MockQueryEngine, predicate: &str, expected: &[InvocationId]) {
    let mut actual = ids(
        engine,
        &format!("SELECT invocation_id FROM _idx_invocation_by_service WHERE {predicate}"),
    )
    .await;
    actual.sort();
    let mut expected = expected.iter().map(ToString::to_string).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected, "{predicate}");
}

async fn populate(engine: &mut MockQueryEngine) -> Vec<InvocationId> {
    let mut config = Configuration::default();
    config.common.experimental.set_indexes_v1(true);
    engine
        .partition_store()
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();
    let mut tx = engine.partition_store().transaction();
    let mut ids = Vec::new();
    for (i, (service, stage, pk, millis, logical)) in [
        ("alpha", Stage::Inbox, 100, MS, 1),
        ("alpha", Stage::Inbox, 200, MS, 2),
        ("alpha", Stage::Running, 100, MS + 1, 0),
        ("beta", Stage::Inbox, 300, MS + 2, 0),
        ("beta", Stage::Finished, 200, MS + 3, 0),
        ("gamma", Stage::Inbox, 400, MS + 4, 0),
    ]
    .into_iter()
    .enumerate()
    {
        let id = InvocationId::from_parts(pk, InvocationUuid::from_u128(i as u128 + 1));
        let qid = VQueueId::custom(pk, format!("index-{i}"));
        let key = EntryKey::new(false, RoughTimestamp::new(1), i as u64, EntryId::from(id));
        let stats = EntryStatistics::new(timestamp(millis, logical), key.run_at());
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &EntryTargetRef::Service {
                    service,
                    scope: None,
                    handler: "handle",
                },
            },
            EntryStateRef {
                stage,
                status: Status::New,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &stats,
            },
        );
        ids.push(id);
    }
    let qid = VQueueId::custom(100, "mutation");
    let key = EntryKey::new(
        false,
        RoughTimestamp::new(1),
        9u64,
        EntryId::new(EntryKind::StateMutation, [9; 16]),
    );
    tx.create_vqueue_entry_status(
        &EntryContext {
            qid: &qid,
            target: &EntryTargetRef::VirtualObject {
                service: "alpha",
                scope: None,
                key: "obj",
                handler: HandlerRef::StateMutation,
            },
        },
        EntryStateRef {
            stage: Stage::Inbox,
            status: Status::New,
            entry_key: &key,
            metadata: &EntryMetadata::default(),
            stats: &EntryStatistics::new(timestamp(MS, 3), key.run_at()),
        },
    );
    tx.commit().await.unwrap();
    ids
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_index_scan_filters_projects_orders_and_tracks_lifecycle() {
    let mut engine = MockQueryEngine::create().await;
    assert!(
        ids(
            &engine,
            "SELECT invocation_id FROM _idx_invocation_by_service"
        )
        .await
        .is_empty()
    );
    let entries = populate(&mut engine).await;
    let count = select(
        &engine,
        "SELECT COUNT(*) AS n FROM _idx_invocation_by_service",
    )
    .await;
    assert_eq!(
        count[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        6
    );
    assert_ids(
        &engine,
        "service_name = 'alpha' AND stage = 'inbox'",
        &entries[..2],
    )
    .await;
    assert_ids(
        &engine,
        "service_name LIKE 'al%' AND stage >= 'running'",
        &entries[2..3],
    )
    .await;
    assert_ids(&engine, "stage = 'finished'", &entries[4..5]).await;
    assert_ids(
        &engine,
        "service_name IN ('alpha', 'alpha', 'beta', NULL)",
        &entries[..5],
    )
    .await;
    assert_ids(
        &engine,
        "stage = 'not-a-stage' OR service_name IS NULL",
        &[],
    )
    .await;
    assert_ids(
        &engine,
        "partition_key IN (100, 200)",
        &[entries[0], entries[1], entries[2], entries[4]],
    )
    .await;
    assert_ids(
        &engine,
        &format!(
            "invocation_id IN ('{}', '{}', '{}')",
            entries[0], entries[1], entries[0]
        ),
        &entries[..2],
    )
    .await;
    assert_ids(
        &engine,
        &format!("invocation_id NOT IN ('{}', '{}')", entries[0], entries[1]),
        &entries[2..],
    )
    .await;
    // Millisecond equality must retain both distinct HLC values.
    assert_ids(
        &engine,
        &format!("transitioned_at = to_timestamp_millis({MS})"),
        &entries[..2],
    )
    .await;
    assert_ids(
        &engine,
        &format!("transitioned_at > to_timestamp_millis({MS})"),
        &entries[2..],
    )
    .await;
    assert_ids(&engine, "invocation_id IS NULL", &[]).await;
    assert_ids(
        &engine,
        "service_name = 'alpha' AND service_name = 'beta'",
        &[],
    )
    .await;
    let above = entries
        .iter()
        .copied()
        .filter(|id| id.to_string() > entries[0].to_string())
        .collect::<Vec<_>>();
    assert_ids(
        &engine,
        &format!("invocation_id > '{}'", entries[0]),
        &above,
    )
    .await;
    assert_eq!(ids(&engine, &format!("SELECT invocation_id FROM _idx_invocation_by_service WHERE transitioned_at > to_timestamp_millis({}) LIMIT 1", MS + 3)).await, [entries[5].to_string()]);

    let ordered = ids(&engine, "SELECT invocation_id FROM _idx_invocation_by_service ORDER BY transitioned_at_hlc DESC LIMIT 3").await;
    assert_eq!(
        ordered,
        [
            entries[5].to_string(),
            entries[4].to_string(),
            entries[3].to_string()
        ]
    );
    let projected = select(&engine, "SELECT invocation_id, transitioned_at_hlc, transitioned_at, service_name FROM _idx_invocation_by_service WHERE service_name = 'alpha' AND stage = 'inbox' ORDER BY transitioned_at_hlc DESC LIMIT 1").await;
    assert_eq!(
        projected[0]
            .column(0)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        entries[1].to_string()
    );
    assert_eq!(
        projected[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        timestamp(MS, 2).as_u64()
    );
    assert_eq!(
        projected[0]
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        MS as i64
    );
    assert_eq!(
        projected[0]
            .column(3)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        "alpha"
    );
    assert_eq!(ids(&engine, "SELECT invocation_id FROM _idx_invocation_by_service WHERE ends_with(service_name, 'a') AND stage = 'running' LIMIT 1").await, [entries[2].to_string()]);

    // Join the index locator to its source header; no invocation-primary records
    // were inserted, so the index scan itself cannot rely on primary lookups.
    let joined = ids(&engine, "SELECT i.invocation_id FROM _idx_invocation_by_service i JOIN sys_vqueue_entry_status s ON i.invocation_id = s.entry_id WHERE i.stage = 'finished'").await;
    assert_eq!(joined, [entries[4].to_string()]);

    let mut tx = engine.partition_store().transaction();
    let before = tx
        .get_vqueue_entry_status(&BaseEntryId::from(entries[0]))
        .await
        .unwrap()
        .unwrap();
    let before = EntryStateRef::from_header(&before);
    let qid = VQueueId::custom(100, "index-0");
    let target = EntryTargetRef::Service {
        scope: None,
        service: "alpha",
        handler: "handle",
    };
    let context = EntryContext {
        qid: &qid,
        target: &target,
    };
    let mut stats = before.stats.clone();
    stats.transitioned_at = timestamp(MS + 5, 0);
    let after = EntryStateRef {
        stage: Stage::Paused,
        stats: &stats,
        ..before
    };
    tx.update_vqueue_entry_status(&context, before, after);
    tx.commit().await.unwrap();
    drop(tx);
    assert_ids(
        &engine,
        "service_name = 'alpha' AND stage = 'inbox'",
        &entries[1..2],
    )
    .await;
    assert_ids(&engine, "stage = 'paused'", &entries[..1]).await;
    let mut tx = engine.partition_store().transaction();
    tx.delete_vqueue_entry_status(&context, after);
    tx.commit().await.unwrap();
    drop(tx);
    assert_ids(&engine, "service_name = 'alpha'", &entries[1..3]).await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_constraints_survive_remote_predicates_and_skip_unselected_keys() {
    let mut engine = MockQueryEngine::create().await;
    let entries = populate(&mut engine).await;
    let partition = engine.partition_store().partition_id();
    let mut poison = Vec::new();
    InvocationByServiceStageKey::prefix(partition, &mut poison).service_name("between");
    let mut tx = engine.partition_store().transaction();
    tx.raw_put_cf(KeyKind::SecondaryIndex, poison, []);
    tx.commit().await.unwrap();
    drop(tx);
    assert_ids(
        &engine,
        "service_name IN ('alpha', 'gamma')",
        &[entries[0], entries[1], entries[2], entries[5]],
    )
    .await;

    let schema = IdxInvocationByServiceBuilder::schema();
    let predicate = logical2physical(
        &col("service_name")
            .eq(lit("alpha"))
            .and(col("invocation_id").eq(lit(entries[1].to_string()))),
        &schema,
    );
    let remote = crate::decode_expr(
        &TaskContext::default(),
        &schema,
        &crate::encode_expr(&predicate).unwrap(),
    )
    .unwrap();
    for predicate in [predicate, remote] {
        let filter = InvocationIndexFilter::new(KeyRange::FULL, Some(predicate));
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        InvocationIndexScanner::for_each_row(engine.partition_store(), filter, move |(_, key)| {
            sender.send(key.invocation_id).unwrap();
            ControlFlow::Continue(())
        })
        .unwrap()
        .await
        .unwrap();
        assert_eq!(receiver.recv().await, Some(entries[1]));
        assert!(receiver.recv().await.is_none());
    }
    let error = engine
        .execute("SELECT COUNT(*) FROM _idx_invocation_by_service")
        .await;
    let failed = match error {
        Err(_) => true,
        Ok(result) => result.stream.try_collect::<Vec<_>>().await.is_err(),
    };
    assert!(
        failed,
        "a malformed selected index entry must fail the query"
    );
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
async fn partition_tables_register_the_scanner_and_sort_across_stores() {
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
    let mut expected = Vec::new();
    for (index, (_, partition)) in partitions.0.iter().enumerate() {
        let mut store = manager.open(partition, None).await.unwrap();
        let id = InvocationId::from_parts(
            110 + index as u64 * 100,
            InvocationUuid::from_u128(index as u128 + 1),
        );
        let mut tx = store.transaction();
        tx.update_secondary_index(
            None,
            Some(&InvocationByServiceStageKey::borrowed(
                "svc",
                Stage::Inbox,
                Reverse(timestamp(MS + index as u64, 0)),
                id,
            )),
        );
        tx.commit().await.unwrap();
        expected.push(id.to_string());
    }
    let scanners =
        RemoteScannerManager::local_only(restate_core::MetadataBuilder::default().to_metadata());
    let mut options = QueryEngineOptions::default();
    // Concatenate both physical stores into one DataFusion partition. Declaring
    // a local index ordering as global would let the planner omit a required sort.
    options
        .datafusion_options
        .insert("datafusion.execution.target_partitions".into(), "1".into());
    let ctx = QueryContext::create(
        &options,
        PartitionTables::new(partitions, manager, scanners.clone()),
    )
    .await
    .unwrap();
    assert!(
        scanners
            .local_partition_scanner(super::table::NAME)
            .is_some()
    );
    let batches = ctx.execute("SELECT invocation_id FROM _idx_invocation_by_service ORDER BY transitioned_at_hlc DESC LIMIT 1")
        .await.unwrap().stream.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        expected[1]
    );
    let query = format!(
        "SELECT COUNT(*) FROM _idx_invocation_by_service WHERE invocation_id IN ('{}', '{}', '{}')",
        expected[0], expected[1], expected[0]
    );
    let batches = ctx
        .execute(&query)
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}
