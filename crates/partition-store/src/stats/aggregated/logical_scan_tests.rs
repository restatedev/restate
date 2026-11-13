// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Statistics bindings expand stored buckets before SQL applies residuals and
//! limits. Compare admitted snapshot reads with independently constructed rows,
//! including nullable dimensions, early stops and errors during expansion.

use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::sync::Arc;

use bytes::BufMut;
use tokio_util::sync::CancellationToken;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::scan::{RowConsumer, ScanOutcome};
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_storage_api::{StorageError, Transaction};
use restate_storage_query_api::scan::{CursorScan, Scan};
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
use restate_storage_query_api::test_util::BindingTableProvider;
use restate_storage_query_api::test_util::datafusion::arrow::compute::concat_batches;
use restate_storage_query_api::test_util::datafusion::arrow::record_batch::RecordBatch;
use restate_storage_query_api::test_util::datafusion::datasource::MemTable;
use restate_storage_query_api::test_util::datafusion::prelude::SessionContext;
use restate_storage_query_api::{AsyncRowSink, LogicalSchema, RowSink, SchemaBinding, Table};
use restate_types::partitions::Partition;
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

use crate::keys::header::IndexScope;
use crate::keys::selection::{KeySelection, OrderedKeySchema};
use crate::keys::{KeyDecoder, KeyKind};
use crate::stats::Stat;
use crate::{PartitionStore, PartitionStoreManager, QueryIoExecutor, ReadSnapshot, Result};

use super::bucketed_gauge::GaugeBucket;
use super::{
    AggregatedStatsMut, DeploymentStats as DeploymentStatsTable, DeploymentStatsBinding,
    DeploymentStatsKey, ServiceStats as ServiceStatsTable, ServiceStatsBinding, ServiceStatsKey,
    StageStatus, VirtualObjectStats as VirtualObjectStatsTable, VirtualObjectStatsBinding,
    VirtualObjectStatsKey, complete_gauges, ready_index_scope,
};

/// Compares SQL results and their explicit counts. The projection deliberately
/// omits columns used only by residual predicates.
async fn assert_sql_cases<B: SchemaBinding>(
    snapshot: Arc<ReadSnapshot>,
    executor: Arc<QueryIoExecutor>,
    reference: RecordBatch,
    cases: &[(&str, usize)],
) where
    ReadSnapshot: CursorScan<B::Table>,
    for<'a> <B::Table as Table>::Record<'a>: Copy + Send,
{
    let context = SessionContext::new();
    context
        .register_table(
            "candidate",
            Arc::new(BindingTableProvider::<B, _>::new(snapshot, executor)),
        )
        .unwrap();
    context
        .register_table(
            "reference",
            Arc::new(MemTable::try_new(B::Schema::arrow_schema(), vec![vec![reference]]).unwrap()),
        )
        .unwrap();
    for &(suffix, count) in cases {
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
            "candidates plus residual equal SQL truth: {}: {suffix}",
            B::Schema::NAME
        );
        assert_eq!(actual.num_rows(), count, "{}: {suffix}", B::Schema::NAME);
    }
}

#[restate_core::test]
async fn deployment_and_object_schemas_expand_buckets_before_sql_residuals() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    complete_gauges(&mut store).await;
    let executor = Arc::new(QueryIoExecutor::new(NonZeroUsize::new(1).unwrap()));

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
    let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();
    let snapshot = Arc::new(snapshot);

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
        Arc::clone(&snapshot),
        Arc::clone(&executor),
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
    assert_sql_cases::<VirtualObjectStatsBinding>(
        snapshot,
        executor,
        objects.finish().unwrap(),
        &object_cases,
    )
    .await;
    closed.await;
}

/// Seed stored keys and counters, and independently construct the logical rows.
/// A malformed value in the next partition's header detects ownership leaks.
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

/// Counts physical records without decoding their buckets.
#[derive(Default)]
struct Records(usize);

impl RowConsumer for Records {
    type Item<'a> = (KeyDecoder<'a, ServiceStatsKey>, &'a [u8]);

    async fn consume(&mut self, _: Self::Item<'_>) -> Result<ControlFlow<()>> {
        self.0 += 1;
        Ok(ControlFlow::Continue(()))
    }
}

/// Stops between bucket rows, or fails on a reserved write. Successful appends
/// always write their row; stopping belongs to `reserve`.
struct Rows {
    builder: ServiceStatsBatchBuilder,
    limit: Option<usize>,
    fail: bool,
}

impl Rows {
    fn new(limit: Option<usize>, fail: bool) -> Self {
        Self {
            builder: ServiceStatsBatchBuilder::new(ServiceStats::arrow_schema()),
            limit,
            fail,
        }
    }
}

impl RowSink<ServiceStats> for Rows {
    fn append_with(
        &mut self,
        write: impl FnOnce(&mut ServiceStatsBatchBuilder) -> Result<()>,
    ) -> ControlFlow<Result<()>> {
        if self.fail {
            return ControlFlow::Break(Err(StorageError::DataIntegrityError));
        }
        match write(&mut self.builder) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(Err(error)),
        }
    }
}

impl AsyncRowSink<ServiceStats> for Rows {
    async fn reserve(&mut self) -> Result<ControlFlow<()>> {
        Ok(if self.limit == Some(self.builder.num_rows()) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        })
    }
}

#[restate_core::test]
async fn service_stats_candidates_and_residual_agree_with_sql_truth() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    complete_gauges(&mut store).await;
    let rows = seed(&mut store).await;
    let executor = Arc::new(QueryIoExecutor::new(NonZeroUsize::new(1).unwrap()));
    let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();
    let snapshot = Arc::new(snapshot);

    let foreign = Arc::new(KeySelection::all().restrict_scope(IndexScope {
        partition: PartitionId::from(u16::from(store.partition_id()) + 1),
        ..ready_index_scope::<ServiceStatsTable>(&store)
    }));
    let ScanOutcome::Exhausted(records) = snapshot
        .scan::<ServiceStatsTable, _>(&executor, "stat-owner", foreign, Records::default())
        .unwrap()
        .await
        .unwrap()
    else {
        panic!("foreign ownership restriction must exhaust");
    };
    assert_eq!(
        records.0, 0,
        "explicit scope cannot override the snapshot's owner"
    );
    let selection = Selection::All
        .and(ServiceStatsClause::ServiceName(ValuePredicate::Equal(
            "alpha".into(),
        )))
        .and(ServiceStatsClause::Handler(ValuePredicate::Equal(Some(
            "run".into(),
        ))));
    let prepared = ServiceStatsBinding::prepare(&selection).unwrap();
    assert_eq!(prepared.precision(), SelectionPrecision::Exact);
    let records = snapshot
        .scan::<ServiceStatsTable, _>(
            &executor,
            "stat-point",
            Arc::clone(prepared.request()),
            Records::default(),
        )
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        records.into_inner().0,
        1,
        "one physical record holds two buckets"
    );
    let outcome = ServiceStatsBinding::read(
        snapshot.as_ref(),
        &executor,
        &prepared,
        Rows::new(Some(1), false),
    )
    .unwrap()
    .await
    .unwrap();
    let ScanOutcome::Stopped(mut limited) = outcome else {
        panic!("limit stops inside bucket expansion")
    };
    assert_eq!(limited.builder.finish().unwrap().num_rows(), 1);
    let error = ServiceStatsBinding::read(
        snapshot.as_ref(),
        &executor,
        &prepared,
        Rows::new(None, true),
    )
    .unwrap()
    .await;
    assert!(
        matches!(error, Err(StorageError::DataIntegrityError)),
        "reserved writer error is preserved"
    );
    let empty = ServiceStatsBinding::prepare(&Selection::Empty).unwrap();
    let outcome =
        ServiceStatsBinding::read(snapshot.as_ref(), &executor, &empty, Rows::new(None, true))
            .unwrap()
            .await
            .unwrap();
    let ScanOutcome::Exhausted(mut empty_rows) = outcome else {
        panic!("empty selection exhausts without a write")
    };
    assert_eq!(empty_rows.builder.finish().unwrap().num_rows(), 0);
    // Reuse the request after stopping and failing. Cursor and bucket state belong
    // to each execution, so both buckets must still be present.
    let outcome = ServiceStatsBinding::read(
        snapshot.as_ref(),
        &executor,
        &prepared,
        Rows::new(None, false),
    )
    .unwrap()
    .await
    .unwrap();
    let ScanOutcome::Exhausted(mut all_rows) = outcome else {
        panic!("unlimited read exhausts")
    };
    assert_eq!(all_rows.builder.finish().unwrap().num_rows(), 2);

    let payload = Selection::All.and(ServiceStatsClause::Stage(ValuePredicate::Equal(
        Stage::Running,
    )));
    assert!(ServiceStatsKey::prepare_selection(&payload).is_err());
    assert_eq!(
        ServiceStatsBinding::prepare(&payload).unwrap().precision(),
        SelectionPrecision::Inexact
    );
    let impossible = payload.and(ServiceStatsClause::ServiceName(ValuePredicate::In(vec![])));
    assert_eq!(
        ServiceStatsBinding::prepare(&impossible)
            .unwrap()
            .precision(),
        SelectionPrecision::Exact
    );
    assert!(
        ServiceStats::arrow_schema()
            .fields()
            .iter()
            .all(|field| field.is_nullable())
    );
    assert_eq!(
        ServiceStatsKey::FIELDS
            .iter()
            .map(|field| field.tag())
            .collect::<Vec<_>>(),
        ["service_name", "handler", "kind"]
    );

    let mut reference = ServiceStatsBatchBuilder::new(ServiceStats::arrow_schema());
    for row in &rows {
        reference.append(row);
    }
    #[rustfmt::skip]
    let cases = [
        ("ORDER BY service_name, handler NULLS FIRST, num_entries",                  6),
        ("WHERE service_name = 'alpha' AND num_entries > 2 ORDER BY num_entries",   2),
        ("WHERE service_name = 'alpha' AND num_entries = 0 ORDER BY num_entries",   0),
        ("WHERE handler IS NULL ORDER BY service_name",                            1),
        ("WHERE handler < 'run' ORDER BY service_name",                             1),
        ("WHERE handler LIKE 'ru%' AND stage = 'running' ORDER BY service_name",    2),
        ("WHERE kind = 'unknown' ORDER BY service_name",                            0),
        ("WHERE service_name IS NULL ORDER BY service_name",                       0),
        ("WHERE num_entries IS NULL ORDER BY service_name",                        0),
        ("WHERE stage = 'not-a-stage' ORDER BY service_name",                       0),
        ("WHERE stage NOT IN ('unknown', 'finished', 'inbox', 'paused', \
                              'running', 'suspended') ORDER BY service_name",     0),
        ("WHERE (stage = 'not-a-stage' AND service_name = 'alpha') OR \
                (status = 'not-a-status' AND handler = 'run') \
          ORDER BY service_name",                                                 0),
        ("WHERE stage = 'not-a-stage' OR num_entries > 3 ORDER BY num_entries",     2),
        ("WHERE service_name = 'alpha' AND length(status) > 3 \
          ORDER BY num_entries LIMIT 1",                                          1),
        ("WHERE stage = 'running' ORDER BY service_name",                           2),
        ("WHERE service_name = 'alpha' OR handler = 'run' \
          ORDER BY service_name, handler NULLS FIRST, num_entries",                5),
    ];
    assert_sql_cases::<ServiceStatsBinding>(
        Arc::clone(&snapshot),
        Arc::clone(&executor),
        reference.finish().unwrap(),
        &cases,
    )
    .await;

    // Corrupt the last key after admission. The original snapshot still sees all
    // six rows; a fresh one fails rather than returning its buffered valid prefix.
    let mut key = Vec::new();
    ServiceStatsTable::encode_key(
        ready_index_scope::<ServiceStatsTable>(&store),
        ServiceStatsKey::borrowed("gamma", Some(""), EntryKind::Invocation),
        &mut key,
    );
    let mut transaction = store.transaction();
    transaction.raw_put_cf(KeyKind::Stats, key, [0xff]);
    transaction.commit().await.unwrap();
    drop(transaction);
    let (fresh, fresh_closed) = store.query_read(CancellationToken::new()).unwrap();
    let context = SessionContext::new();
    for (name, read) in [("original", snapshot), ("corrupt", Arc::new(fresh))] {
        context
            .register_table(
                name,
                Arc::new(BindingTableProvider::<ServiceStatsBinding, _>::new(
                    read,
                    Arc::clone(&executor),
                )),
            )
            .unwrap();
    }
    let original = context
        .sql("SELECT * FROM original")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(original.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
    assert!(
        context
            .sql("SELECT * FROM corrupt")
            .await
            .unwrap()
            .collect()
            .await
            .is_err()
    );
    drop(context);
    closed.await;
    fresh_closed.await;
}
