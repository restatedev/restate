// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BufMut;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_storage_query_api::scan::Scan;
use restate_storage_query_api::selection::{Selection, SelectionPrecision, ValuePredicate};
use restate_storage_query_api::tables::stats::deployment_stats::{
    DeploymentStats, DeploymentStatsRow,
};
use restate_storage_query_api::tables::stats::service_stats::{
    ServiceStats, ServiceStatsBatchBuilder, ServiceStatsClause, ServiceStatsRow,
};
use restate_storage_query_api::tables::stats::virtual_object_stats::{
    VirtualObjectStats, VirtualObjectStatsRow,
};
use restate_storage_query_api::test_util::SchemaProvider;
use restate_storage_query_api::test_util::datafusion::arrow::compute::concat_batches;
use restate_storage_query_api::test_util::datafusion::arrow::record_batch::RecordBatch;
use restate_storage_query_api::test_util::datafusion::datasource::MemTable;
use restate_storage_query_api::test_util::datafusion::prelude::SessionContext;
use restate_storage_query_api::{LogicalSchema, SchemaBinding, Table};
use restate_types::partition_table::Partition;
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

use crate::keys::KeyKind;
use crate::keys::header::IndexScope;
use crate::keys::selection::{KeySelection, OrderedKeySchema};
use crate::stats::Stat;
use crate::{PartitionStore, PartitionStoreManager};

use super::bucketed_gauge::GaugeBucket;
use super::{
    AggregatedStatsMut, DeploymentStats as DeploymentStatsTable, DeploymentStatsBinding,
    DeploymentStatsKey, ServiceStats as ServiceStatsTable, ServiceStatsBinding, ServiceStatsKey,
    StageStatus, VirtualObjectStats as VirtualObjectStatsTable, VirtualObjectStatsBinding,
    VirtualObjectStatsKey, complete_gauges, ready_index_scope,
};

#[derive(Debug, Default)]
struct ScanMetrics {
    emitted_records: AtomicUsize,
}

#[derive(Debug)]
struct Backend {
    store: PartitionStore,
    metrics: Arc<ScanMetrics>,
}

impl<T: Table> Scan<T> for Backend
where
    PartitionStore: Scan<T>,
{
    fn scan(
        &self,
        request: Arc<T::Scan>,
        mut emit: impl for<'a> FnMut(T::Record<'a>) -> ControlFlow<restate_storage_api::Result<()>>
        + Send
        + 'static,
    ) -> restate_storage_api::Result<impl Future<Output = restate_storage_api::Result<()>> + Send>
    {
        let metrics = Arc::clone(&self.metrics);
        <PartitionStore as Scan<T>>::scan(&self.store, request, move |record| {
            metrics.emitted_records.fetch_add(1, Ordering::Relaxed);
            emit(record)
        })
    }
}

async fn assert_sql_cases<B: SchemaBinding>(
    store: PartitionStore,
    reference: RecordBatch,
    cases: &[(&str, usize)],
) where
    PartitionStore: Scan<B::Table>,
{
    let context = SessionContext::new();
    context
        .register_table(
            "candidate",
            Arc::new(SchemaProvider::<B, _>::new(Arc::new(store))),
        )
        .unwrap();
    context
        .register_table(
            "reference",
            Arc::new(MemTable::try_new(B::Schema::arrow_schema(), vec![vec![reference]]).unwrap()),
        )
        .unwrap();
    for &(suffix, count) in cases {
        // Residual dependencies are deliberately absent from the final projection.
        let query = |table| format!("SELECT service_name, num_entries FROM {table} {suffix}");
        let actual = context
            .sql(&query("candidate"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected = context.sql(&query("reference")).await.unwrap();
        let schema = Arc::new(expected.schema().as_arrow().clone());
        let expected = expected.collect().await.unwrap();
        let actual = concat_batches(&schema, &actual).unwrap();
        assert_eq!(
            actual,
            concat_batches(&schema, &expected).unwrap(),
            "candidates_plus_residual_equal_sql_truth: {}: {suffix}",
            B::Schema::NAME
        );
        assert_eq!(
            actual.num_rows(),
            count,
            "explicit_sql_result_count: {}: {suffix}",
            B::Schema::NAME
        );
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn deployment_and_object_schemas_expand_buckets_before_sql_residuals() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    complete_gauges(&mut store).await;

    let mut deployments =
        <DeploymentStats as LogicalSchema>::RowBuilder::new(DeploymentStats::arrow_schema());
    let mut objects =
        <VirtualObjectStats as LogicalSchema>::RowBuilder::new(VirtualObjectStats::arrow_schema());
    let deployment_id = ready_index_scope::<DeploymentStatsTable>(&store).index;
    let object_id = ready_index_scope::<VirtualObjectStatsTable>(&store).index;
    let mut transaction = store.transaction();
    #[rustfmt::skip]
    let fixtures = [
        // deployment service scope         handler     key      partition key stage           status            count
        ("dp_a", "alpha", None,             None,       "object", 11,           Stage::Inbox,   Status::New,      2),
        ("dp_a", "alpha", Some(""),         Some("run"), "object", 22,           Stage::Running, Status::Started,  5),
        ("dp_b", "beta",  Some("tenant"),   Some("run"), "other",  33,           Stage::Unknown, Status::Unknown,  3),
        ("dp_a", "alpha", Some(""),         Some("run"), "object", 22,           Stage::Finished,Status::Succeeded,1),
    ];
    for (deployment, service, scope, handler, key, partition_key, stage, status, count) in fixtures
    {
        let bucket = StageStatus::new(stage, status);
        let kind = if handler.is_some() {
            EntryKind::Invocation
        } else {
            EntryKind::StateMutation
        };
        for _ in 0..count {
            AggregatedStatsMut::new(&mut transaction, deployment_id)
                .increment_stage_status::<DeploymentStatsTable, _>(
                    DeploymentStatsKey::borrowed(deployment, service),
                    bucket,
                );
            AggregatedStatsMut::new(&mut transaction, object_id)
                .increment_stage_status::<VirtualObjectStatsTable, _>(
                    VirtualObjectStatsKey::borrowed(
                        service,
                        scope,
                        key,
                        handler,
                        kind,
                        partition_key,
                    ),
                    bucket,
                );
        }
        deployments.append(&DeploymentStatsRow {
            deployment_id: deployment.into(),
            service_name: service.into(),
            stage,
            status,
            num_entries: count,
        });
        objects.append(&VirtualObjectStatsRow {
            service_name: service.into(),
            scope: scope.map(ReString::from),
            key: key.into(),
            handler: handler.map(ReString::from),
            kind,
            partition_key,
            stage,
            status,
            num_entries: count,
        });
    }
    transaction.commit().await.unwrap();

    drop(transaction);

    #[rustfmt::skip]
    let deployment_cases = [
        ("ORDER BY num_entries",                                              4),
        ("WHERE deployment_id = 'dp_a' AND num_entries > 2 ORDER BY num_entries",1),
        ("WHERE deployment_id LIKE 'dp_%' AND stage = 'running'",              1),
        ("WHERE stage = 'unknown' AND status = 'unknown'",                     1),
        ("WHERE deployment_id IS NULL",                                       0),
        ("WHERE stage = 'invalid' OR status = 'invalid'",                      0),
        ("WHERE stage <> 'inbox' ORDER BY num_entries DESC LIMIT 1",           1),
    ];
    assert_sql_cases::<DeploymentStatsBinding>(
        store.clone(),
        deployments.finish().unwrap(),
        &deployment_cases,
    )
    .await;
    #[rustfmt::skip]
    let object_cases = [
        ("ORDER BY num_entries",                                              4),
        ("WHERE scope IS NULL AND handler IS NULL",                            1),
        ("WHERE scope = '' AND num_entries > 2",                               1),
        ("WHERE scope LIKE '%' ORDER BY num_entries",                          3),
        ("WHERE partition_key = 22 AND stage = 'finished'",                    1),
        ("WHERE kind = 'state-mutation'",                                      1),
        ("WHERE stage = 'unknown' AND status = 'unknown'",                     1),
        ("WHERE scope IS NULL OR (partition_key = 22 AND status = 'started') ORDER BY num_entries",2),
        ("WHERE kind = 'unknown'",                                             0),
        ("WHERE handler IS NOT NULL AND num_entries > 2 ORDER BY num_entries LIMIT 1",1),
    ];
    assert_sql_cases::<VirtualObjectStatsBinding>(store, objects.finish().unwrap(), &object_cases)
        .await;
}

/// Seed actual persisted keys and counter values, and independently construct the
/// logical rows used by DataFusion's reference table.
async fn seed(store: &mut PartitionStore) -> Vec<ServiceStatsRow> {
    type Fixture<'a> = (
        &'a str,
        Option<&'a str>,
        EntryKind,
        &'a [(Stage, Status, u64)],
    );
    let fixtures: &[Fixture<'_>] = &[
        (
            "alpha",
            None,
            EntryKind::StateMutation,
            &[(Stage::Inbox, Status::New, 2)],
        ),
        (
            "alpha",
            Some("run"),
            EntryKind::Invocation,
            &[
                (Stage::Inbox, Status::New, 1),
                (Stage::Running, Status::Started, 5),
            ],
        ),
        (
            "alpha",
            Some("stop"),
            EntryKind::Invocation,
            &[(Stage::Finished, Status::Succeeded, 4)],
        ),
        (
            "beta",
            Some("run"),
            EntryKind::Invocation,
            &[(Stage::Running, Status::Started, 3)],
        ),
        (
            "gamma",
            Some(""),
            EntryKind::Invocation,
            &[(Stage::Paused, Status::Started, 2)],
        ),
    ];
    let partition = ready_index_scope::<ServiceStatsTable>(store);
    let mut transaction = store.transaction();
    let mut rows = Vec::new();
    for &(service, handler, kind, buckets) in fixtures {
        let mut key = Vec::new();
        ServiceStatsTable::encode_key(
            partition,
            ServiceStatsKey::borrowed(service, handler, kind),
            &mut key,
        );
        let mut value = Vec::new();
        for &(stage, status, count) in buckets {
            StageStatus::new(stage, status).encode(&mut value);
            value.put_u64(count);
            rows.push(ServiceStatsRow {
                service_name: service.into(),
                handler: handler.map(ReString::from),
                kind,
                stage,
                status,
                num_entries: count,
            });
        }
        transaction.raw_put_cf(KeyKind::Stats, key, value);
    }
    let mut foreign = Vec::new();
    ServiceStatsTable::encode_key(
        IndexScope {
            partition: PartitionId::from(u16::from(partition.partition) + 1),
            ..partition
        },
        ServiceStatsKey::borrowed("alpha", Some("run"), EntryKind::Invocation),
        &mut foreign,
    );
    transaction.raw_put_cf(KeyKind::Stats, foreign, [0xff]);
    transaction.commit().await.unwrap();
    rows
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_stats_candidates_and_residual_agree_with_sql_truth() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    complete_gauges(&mut store).await;
    let rows = seed(&mut store).await;
    <PartitionStore as Scan<ServiceStatsTable>>::scan(
        &store,
        Arc::new(KeySelection::all().restrict_scope(IndexScope {
            partition: PartitionId::from(u16::from(store.partition_id()) + 1),
            ..ready_index_scope::<ServiceStatsTable>(&store)
        })),
        |_| panic!("explicit_partition_restriction_cannot_override_local_owner"),
    )
    .unwrap()
    .await
    .unwrap();
    let selection = Selection::All
        .and(ServiceStatsClause::ServiceName(ValuePredicate::Equal(
            "alpha".into(),
        )))
        .and(ServiceStatsClause::Handler(ValuePredicate::Equal(Some(
            "run".into(),
        ))));
    let prepared = ServiceStatsBinding::prepare(&selection).unwrap();
    assert_eq!(
        prepared.precision(),
        SelectionPrecision::Exact,
        "key_only_selection_is_exact"
    );
    let emitted = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&emitted);
    ServiceStatsBinding::read(&store, &prepared, move |_| {
        observed.fetch_add(1, Ordering::Relaxed);
        ControlFlow::Break(Ok(()))
    })
    .unwrap()
    .await
    .unwrap();
    assert_eq!(
        emitted.load(Ordering::Relaxed),
        1,
        "early_stop_interrupts_bucket_expansion"
    );
    let error = ServiceStatsBinding::read(&store, &prepared, |_| {
        ControlFlow::Break(Err(restate_storage_api::StorageError::DataIntegrityError))
    })
    .unwrap()
    .await
    .unwrap_err();
    assert!(
        matches!(error, restate_storage_api::StorageError::DataIntegrityError),
        "callback_error_is_preserved"
    );
    ServiceStatsBinding::read(
        &store,
        &ServiceStatsBinding::prepare(&Selection::Empty).unwrap(),
        |_| panic!("empty_selection_never_emits"),
    )
    .unwrap()
    .await
    .unwrap();
    let payload = Selection::All.and(ServiceStatsClause::Stage(ValuePredicate::Equal(
        Stage::Running,
    )));
    assert!(
        ServiceStatsKey::prepare_selection(&payload).is_err(),
        "strict_key_preparation_rejects_payload_clauses"
    );
    assert_eq!(
        ServiceStatsBinding::prepare(&payload).unwrap().precision(),
        SelectionPrecision::Inexact,
        "omitted_payload_requires_residual"
    );
    let impossible = payload.and(ServiceStatsClause::ServiceName(ValuePredicate::In(vec![])));
    assert_eq!(
        ServiceStatsBinding::prepare(&impossible)
            .unwrap()
            .precision(),
        SelectionPrecision::Exact,
        "impossible_key_proves_complete_selection_empty"
    );

    let mut reference = ServiceStatsBatchBuilder::new(ServiceStats::arrow_schema());
    for row in &rows {
        reference.append(row);
    }
    let metrics = Arc::new(ScanMetrics::default());
    let backend = Arc::new(Backend {
        store: store.clone(),
        metrics: Arc::clone(&metrics),
    });
    let context = SessionContext::new();
    context
        .register_table(
            "candidate",
            Arc::new(SchemaProvider::<ServiceStatsBinding, _>::new(backend)),
        )
        .unwrap();
    context
        .register_table(
            "reference",
            Arc::new(
                MemTable::try_new(
                    ServiceStats::arrow_schema(),
                    vec![vec![reference.finish().unwrap()]],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    assert!(
        ServiceStats::arrow_schema()
            .fields()
            .iter()
            .all(|field| field.is_nullable()),
        "Arrow metadata remains permissive independently of native nullability"
    );
    assert_eq!(
        ServiceStatsKey::FIELDS
            .iter()
            .map(|field| field.tag())
            .collect::<Vec<_>>(),
        ["service_name", "handler", "kind"],
        "ordinary_stat_key_retains_persisted_field_order"
    );

    #[rustfmt::skip]
    let cases = [
        // Query suffix                                                           result rows  maximum physical records
        ("ORDER BY service_name, handler NULLS FIRST, num_entries",                  6,           5),
        ("WHERE service_name = 'alpha' AND num_entries > 2 ORDER BY num_entries",   2,           3),
        ("WHERE service_name = 'alpha' AND num_entries = 0 ORDER BY num_entries",   0,           3),
        ("WHERE handler IS NULL ORDER BY service_name",                            1,           1),
        ("WHERE handler < 'run' ORDER BY service_name",                             1,           1),
        ("WHERE handler LIKE 'ru%' AND stage = 'running' ORDER BY service_name",    2,           2),
        ("WHERE kind = 'unknown' ORDER BY service_name",                            0,           0),
        ("WHERE service_name IS NULL ORDER BY service_name",                       0,           0),
        ("WHERE num_entries IS NULL ORDER BY service_name",                        0,           5),
        ("WHERE stage = 'not-a-stage' ORDER BY service_name",                       0,           0),
        ("WHERE stage NOT IN ('unknown', 'finished', 'inbox', 'paused', \
                              'running', 'suspended') ORDER BY service_name",     0,           0),
        ("WHERE (stage = 'not-a-stage' AND service_name = 'alpha') OR \
                (status = 'not-a-status' AND handler = 'run') \
          ORDER BY service_name",                                                 0,           0),
        ("WHERE stage = 'not-a-stage' OR num_entries > 3 ORDER BY num_entries",     2,           5),
        ("WHERE service_name = 'alpha' AND length(status) > 3 \
          ORDER BY num_entries LIMIT 1",                                          1,           3),
        ("WHERE stage = 'running' ORDER BY service_name",                           2,           5),
        ("WHERE service_name = 'alpha' OR handler = 'run' \
          ORDER BY service_name, handler NULLS FIRST, num_entries",                5,           5),
    ];
    for (suffix, count, max_records) in cases {
        metrics.emitted_records.store(0, Ordering::Relaxed);
        // The projection intentionally omits columns used only by WHERE.
        let query =
            |table| format!("SELECT service_name, handler, num_entries FROM {table} {suffix}");
        let actual = context
            .sql(&query("candidate"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected = context.sql(&query("reference")).await.unwrap();
        let schema = Arc::new(expected.schema().as_arrow().clone());
        let expected = expected.collect().await.unwrap();
        let actual = concat_batches(&schema, &actual).unwrap();
        let expected = concat_batches(&schema, &expected).unwrap();
        assert_eq!(
            actual, expected,
            "candidates_plus_residual_equal_sql_truth: {suffix}"
        );
        assert_eq!(actual.num_rows(), count, "explicit result count: {suffix}");
        assert!(
            metrics.emitted_records.load(Ordering::Relaxed) <= max_records,
            "key_constraints_bound_physical_reads: {suffix}; emitted {}, maximum {max_records}",
            metrics.emitted_records.load(Ordering::Relaxed),
        );
    }

    // The oracle collector must propagate a scan error even after receiving rows.
    // Its callback-owned builder is returned on drop, but partial data must never
    // become a successful result. The corrupted key sorts after valid records.
    let mut key = Vec::new();
    ServiceStatsTable::encode_key(
        ready_index_scope::<ServiceStatsTable>(&store),
        ServiceStatsKey::borrowed("gamma", Some(""), EntryKind::Invocation),
        &mut key,
    );
    let mut transaction = store.transaction();
    transaction.raw_put_cf(KeyKind::Stats, key, vec![0xff]);
    transaction.commit().await.unwrap();
    drop(transaction);
    metrics.emitted_records.store(0, Ordering::Relaxed);
    let result = context
        .sql("SELECT * FROM candidate")
        .await
        .unwrap()
        .collect()
        .await;
    assert!(
        result.is_err(),
        "collector_does_not_turn_partial_scan_into_success"
    );
    assert!(
        metrics.emitted_records.load(Ordering::Relaxed) > 1,
        "failure_follows_valid_records"
    );
}
