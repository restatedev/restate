// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use futures::TryStreamExt;

use restate_clock::{MockClock, UniqueTimestamp, time::MillisSinceEpoch};
use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
use restate_partition_store::PartitionStore;
use restate_partition_store::keys::KeyKind;
use restate_partition_store::keys::header::IndexScope;
use restate_partition_store::registry::IndexKind;
use restate_partition_store::stats::Stat;
use restate_partition_store::stats::aggregated::{
    ServiceStats as StoredServiceStats, ServiceStatsKey,
};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
    stats::EntryStatistics,
};
use restate_storage_query_api::{
    QueryEngine, QueryOptions, QuerySession, SessionOptions, SessionTable,
};
use restate_types::GenerationalNodeId;
use restate_types::cluster_state::NodeState;
use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::Partition;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};

use crate::context::DataFusionQueryEngine;
use crate::local_scanners::register_partition_scanners;
use crate::remote_query_scanner_client::create_remote_scanner_service;
use crate::remote_query_scanner_manager::{PartitionLocation, PartitionLocator};
use crate::remote_query_scanner_server::RemoteQueryScannerServer;
use crate::table_providers::ScanPredicate;
use crate::{DataFusionEnv, UserTables};

use super::*;

const TABLES: [&str; 3] = [
    "sys_service_stats",
    "sys_deployment_stats",
    "sys_virtual_object_stats",
];

#[derive(Clone, Debug)]
struct Partitions(Vec<Partition>);

#[async_trait]
impl SelectPartitions for Partitions {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(self.0.iter().map(|p| (p.partition_id, p.clone())).collect())
    }
}

struct LocalAndRpc;

impl PartitionLocator for LocalAndRpc {
    fn get_partition_target_node(&self, id: PartitionId) -> anyhow::Result<PartitionLocation> {
        Ok(if id == PartitionId::from(1) {
            PartitionLocation::Local
        } else {
            // Exercise the production open/next/close RPC path over loopback.
            PartitionLocation::Remote {
                node_id: GenerationalNodeId::new(1, 1).into(),
            }
        })
    }
}

struct Fixture {
    stores: Vec<PartitionStore>,
    manager: Arc<PartitionStoreManager>,
    env: DataFusionEnv,
    engine: QueryEngine,
    worker: RemoteScannerManager,
}

impl Fixture {
    async fn new(remote_objects: bool) -> Self {
        let mut core = TestCoreEnvBuilder::with_incoming_only_connector().add_mock_nodes_config();
        let env = DataFusionEnv::new(
            32 * 1024 * 1024,
            None,
            Some(2),
            &HashMap::from([("datafusion.execution.batch_size".into(), "2".into())]),
        )
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partitions = Partitions(vec![
            Partition::new(PartitionId::from(1), KeyRange::new(0, 99)),
            Partition::new(PartitionId::from(2), KeyRange::new(100, 199)),
        ]);
        let mut stores = Vec::new();
        for partition in &partitions.0 {
            stores.push(manager.open(partition, None).await.unwrap());
        }
        let coordinator = RemoteScannerManager::new(
            create_remote_scanner_service(core.networking.clone()),
            Arc::new(LocalAndRpc),
            core.metadata.clone(),
        );
        register_partition_scanners(manager.clone(), env.io_executor().clone(), &coordinator);
        let worker = RemoteScannerManager::local_only(core.metadata.clone());
        worker.register_partition_scanner::<SysServiceStatsTable>(Arc::new(
            SysServiceStatsTable::create_local_scanner(manager.clone(), env.io_executor().clone()),
        ));
        worker.register_partition_scanner::<SysDeploymentStatsTable>(Arc::new(
            SysDeploymentStatsTable::create_local_scanner(
                manager.clone(),
                env.io_executor().clone(),
            ),
        ));
        if remote_objects {
            worker.register_partition_scanner::<SysVirtualObjectStatsTable>(Arc::new(
                SysVirtualObjectStatsTable::create_local_scanner(
                    manager.clone(),
                    env.io_executor().clone(),
                ),
            ));
        }
        let server =
            RemoteQueryScannerServer::new(env.clone(), worker.clone(), &mut core.router_builder);
        let _core = core.build().await;
        TaskCenter::with_current(|tc| {
            tc.cluster_state()
                .clone()
                .updater()
                .upsert_node_state(GenerationalNodeId::new(1, 1), NodeState::Alive);
        });
        // Spawned like production, so shutdown stops it before the stores close.
        TaskCenter::spawn(
            TaskKind::SystemService,
            "test-statistics-scanner",
            server.run(),
        )
        .unwrap();
        env.register_tables(UserTables::new(partitions, coordinator))
            .await
            .unwrap();
        let engine = DataFusionQueryEngine::from_env(env.clone(), None);
        Self {
            stores,
            manager,
            env,
            engine,
            worker,
        }
    }

    fn session(&self) -> Arc<dyn QuerySession> {
        self.engine
            .create_session(SessionOptions::default())
            .unwrap()
    }
}

async fn ready(store: &mut PartitionStore) {
    let mut config = Configuration::pinned().as_ref().clone();
    config.common.experimental.set_track_status_gauges(true);
    store
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();
}

async fn query(session: &dyn QuerySession, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    Ok(session
        .execute(sql, QueryOptions {})
        .await?
        .stream
        .try_collect()
        .await?)
}

async fn assert_rows(session: &dyn QuerySession, sql: &str, expected: &[&[Option<&str>]]) {
    let batches = query(session, sql).await.unwrap();
    let actual: Vec<Vec<Option<String>>> = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        (!column.is_null(row)).then(|| array_value_to_string(column, row).unwrap())
                    })
                    .collect()
            })
        })
        .collect();
    let expected: Vec<Vec<Option<String>>> = expected
        .iter()
        .map(|row| row.iter().map(|value| value.map(str::to_owned)).collect())
        .collect();
    assert_eq!(actual, expected, "{sql}");
}

/// Entry writes, not raw statistics writes, establish the normal fixture.
async fn populate(store: &mut PartitionStore, second: bool) {
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let created = UniqueTimestamp::try_from_unix_millis(now).unwrap();
    let pk = if second { 110 } else { 10 };
    let mut tx = store.transaction();
    let mut id = 0u8;
    for (target, partition_key, stage, status, deployment, count) in [
        (
            EntryTargetRef::VirtualObject {
                scope: Some("tenant"),
                service: "counter",
                key: "object",
                handler: HandlerRef::UserHandler("inc"),
            },
            pk,
            Stage::Running,
            Status::Started,
            Some("dp_one"),
            if second { 3 } else { 2 },
        ),
        (
            EntryTargetRef::VirtualObject {
                scope: Some("tenant"),
                service: "counter",
                key: "object",
                handler: HandlerRef::UserHandler("inc"),
            },
            pk,
            Stage::Finished,
            Status::Succeeded,
            Some("dp_one"),
            if second { 0 } else { 1 },
        ),
        (
            EntryTargetRef::VirtualObject {
                scope: second.then_some(""),
                service: "counter",
                key: "object",
                handler: HandlerRef::StateMutation,
            },
            pk + 1,
            Stage::Inbox,
            Status::New,
            None,
            1,
        ),
        (
            EntryTargetRef::Service {
                scope: None,
                service: "other",
                handler: "inc",
            },
            pk + 2,
            Stage::Running,
            Status::Started,
            Some("dp_two"),
            if second { 0 } else { 4 },
        ),
    ] {
        let kind = if matches!(
            &target,
            EntryTargetRef::VirtualObject {
                handler: HandlerRef::StateMutation,
                ..
            }
        ) {
            EntryKind::StateMutation
        } else {
            EntryKind::Invocation
        };
        let qid = VQueueId::custom(partition_key, "stats-test");
        for _ in 0..count {
            id += 1;
            let key = EntryKey::new(false, now, u64::from(id), EntryId::new(kind, [id; 16]));
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target,
                },
                EntryStateRef {
                    stage,
                    status,
                    entry_key: &key,
                    metadata: &EntryMetadata {
                        deployment: deployment.map(Into::into),
                        ..Default::default()
                    },
                    stats: &EntryStatistics::new(created, key.run_at()),
                },
            );
        }
    }
    tx.commit().await.unwrap();
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn statistics_have_shared_shapes_and_sum_before_count_filters_and_limits() {
    let mut config = Configuration::default();
    config.common.experimental.set_query_stats_tables(true);
    Configuration::set(config);
    let mut fixture = Fixture::new(true).await;
    for (i, store) in fixture.stores.iter_mut().enumerate() {
        ready(store).await;
        populate(store, i == 1).await;
    }
    let session = fixture.session();
    for (table, schema) in TABLES.into_iter().zip([
        ServiceStats::arrow_schema(),
        DeploymentStats::arrow_schema(),
        VirtualObjectStats::arrow_schema(),
    ]) {
        let result = session
            .execute(&format!("SELECT * FROM {table}"), QueryOptions {})
            .await
            .unwrap();
        assert_eq!(
            result.stream.schema(),
            schema,
            "{table}: column order, types and nullability"
        );
        result.stream.try_collect::<Vec<_>>().await.unwrap();
    }
    use Some as s;
    assert_rows(session.as_ref(),
        "SELECT * FROM sys_service_stats ORDER BY service_name, handler NULLS FIRST, kind, stage, status",
        &[
            &[s("counter"), None, s("state-mutation"), s("inbox"), s("new"), s("2")],
            &[s("counter"), s("inc"), s("invocation"), s("finished"), s("succeeded"), s("1")],
            &[s("counter"), s("inc"), s("invocation"), s("running"), s("started"), s("5")],
            &[s("other"), s("inc"), s("invocation"), s("running"), s("started"), s("4")],
        ]).await;
    assert_rows(
        session.as_ref(),
        "SELECT * FROM sys_deployment_stats ORDER BY deployment_id, service_name, stage, status",
        &[
            &[
                s("dp_one"),
                s("counter"),
                s("finished"),
                s("succeeded"),
                s("1"),
            ],
            &[
                s("dp_one"),
                s("counter"),
                s("running"),
                s("started"),
                s("5"),
            ],
            &[s("dp_two"), s("other"), s("running"), s("started"), s("4")],
        ],
    )
    .await;
    assert_rows(session.as_ref(),
        "SELECT * FROM sys_virtual_object_stats ORDER BY service_name, scope NULLS FIRST, key, handler NULLS FIRST, kind, stage, status, partition_key",
        &[
            &[s("counter"), None, s("object"), None, s("state-mutation"), s("inbox"), s("new"), s("1"), s("11")],
            &[s("counter"), s(""), s("object"), None, s("state-mutation"), s("inbox"), s("new"), s("1"), s("111")],
            &[s("counter"), s("tenant"), s("object"), s("inc"), s("invocation"), s("finished"), s("succeeded"), s("1"), s("10")],
            &[s("counter"), s("tenant"), s("object"), s("inc"), s("invocation"), s("running"), s("started"), s("2"), s("10")],
            &[s("counter"), s("tenant"), s("object"), s("inc"), s("invocation"), s("running"), s("started"), s("3"), s("110")],
        ]).await;
    for table in ["sys_service_stats", "sys_deployment_stats"] {
        for suffix in ["WHERE num_entries > 4", "ORDER BY num_entries DESC LIMIT 1"] {
            assert_rows(
                session.as_ref(),
                &format!("SELECT service_name, num_entries FROM {table} {suffix}"),
                &[&[s("counter"), s("5")]],
            )
            .await;
        }
    }
    // Several point reads to the same partition must not duplicate its rows.
    // The projection deliberately omits the routing column and every filter column.
    assert_rows(session.as_ref(),
        "SELECT num_entries FROM sys_virtual_object_stats WHERE partition_key IN (10, 11, 10, 110, 111) AND stage = 'running' ORDER BY num_entries",
        &[&[s("2")], &[s("3")]]).await;
    assert_rows(session.as_ref(),
        "SELECT COUNT(*) FROM sys_virtual_object_stats WHERE handler IS NULL AND (scope IS NULL OR scope = '')",
        &[&[s("2")]]).await;

    // One stored service gauge holds several stage/status buckets, so LIMIT can
    // stop between them. Storage plans from the initial filter even after the
    // live filter changes: with the live filter replaced by `true`, the read still
    // selects only counter/inc and never decodes the corrupt gauge of another
    // service. With `false`, the rows are filtered out.
    let store = &mut fixture.stores[0];
    let (snapshot, _) = store.query_read(Default::default()).unwrap();
    let scope = IndexScope {
        partition: store.partition_id(),
        index: snapshot
            .ready_index(StoredServiceStats::DEFINITION, IndexKind::Aggregate)
            .unwrap(),
    };
    drop(snapshot);
    let mut key = Vec::new();
    StoredServiceStats::encode_key(
        scope,
        ServiceStatsKey::borrowed("broken", Some("run"), EntryKind::Invocation),
        &mut key,
    );
    let mut tx = store.transaction();
    tx.raw_put_cf(KeyKind::Stats, &key, b"\xff");
    tx.commit().await.unwrap();
    drop(tx);

    let scanner = SysServiceStatsTable::create_local_scanner(
        fixture.manager.clone(),
        fixture.env.io_executor().clone(),
    );
    let schema = ServiceStats::arrow_schema();
    let text = |value: &str| {
        datafusion::logical_expr::lit(datafusion::scalar::ScalarValue::LargeUtf8(Some(
            value.to_owned(),
        )))
    };
    let initial = datafusion::physical_expr::planner::logical2physical(
        &col("service_name")
            .eq(text("counter"))
            .and(col("handler").eq(text("inc"))),
        &schema,
    );
    let (predicate, update) = ScanPredicate::updatable(initial);
    let rows = |limit| {
        scanner
            .scan_partition(
                PartitionId::from(1),
                KeyRange::new(0, 99),
                schema.clone(),
                Some(predicate.clone()),
                1,
                limit,
                Default::default(),
            )
            .unwrap()
            .try_collect::<Vec<_>>()
    };
    let count =
        |batches: Vec<RecordBatch>| batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(count(rows(Some(1)).await.unwrap()), 1);
    update
        .update(datafusion::physical_expr::expressions::lit(true))
        .unwrap();
    assert_eq!(count(rows(None).await.unwrap()), 2);
    update
        .update(datafusion::physical_expr::expressions::lit(false))
        .unwrap();
    assert_eq!(count(rows(None).await.unwrap()), 0);
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn exposure_is_session_scoped_and_remote_failures_never_become_partial_totals() {
    let mut config = Configuration::default();
    Configuration::set(config.clone());
    let mut fixture = Fixture::new(false).await;
    let hidden = fixture.session();
    for table in TABLES {
        assert!(
            hidden
                .execute(&format!("SELECT * FROM {table}"), QueryOptions {})
                .await
                .is_err()
        );
    }
    let aliases = || SessionOptions {
        tables: Some(
            TABLES
                .iter()
                .enumerate()
                .map(|(i, table)| SessionTable::new(*table, format!("selected_{i}")))
                .collect(),
        ),
        ..Default::default()
    };
    let hidden_aliases = fixture.engine.create_session(aliases()).unwrap();
    assert_rows(
        hidden_aliases.as_ref(),
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'",
        &[&[Some("0")]],
    )
    .await;
    config.common.experimental.set_track_status_gauges(true);
    Configuration::set(config.clone());
    assert!(
        fixture
            .session()
            .execute("SELECT * FROM sys_service_stats", QueryOptions {})
            .await
            .is_err()
    );
    // Exposure alone makes tables visible, without enabling maintenance.
    config.common.experimental.set_track_status_gauges(false);
    config.common.experimental.set_query_stats_tables(true);
    Configuration::set(config.clone());
    let visible = fixture.session();
    let visible_aliases = fixture.engine.create_session(aliases()).unwrap();
    assert_rows(visible.as_ref(),
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%stats%'",
        &[&[Some("3")]]).await;
    assert_rows(
        visible_aliases.as_ref(),
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'",
        &[&[Some("3")]],
    )
    .await;
    assert!(
        hidden
            .execute("SELECT * FROM sys_service_stats", QueryOptions {})
            .await
            .is_err()
    );

    ready(&mut fixture.stores[0]).await;
    populate(&mut fixture.stores[0], false).await;
    // The local partition is healthy, but its remote partner has no gauges.
    for table in ["sys_service_stats", "sys_deployment_stats"] {
        let error = query(
            visible.as_ref(),
            &format!("SELECT SUM(num_entries) FROM {table}"),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("not ready"), "{error}");
    }
    ready(&mut fixture.stores[1]).await;
    populate(&mut fixture.stores[1], true).await;
    // An old worker without this scanner fails open rather than contributing zero.
    assert!(
        query(
            visible.as_ref(),
            "SELECT SUM(num_entries) FROM sys_virtual_object_stats"
        )
        .await
        .is_err()
    );
    fixture
        .worker
        .register_partition_scanner::<SysVirtualObjectStatsTable>(Arc::new(
            SysVirtualObjectStatsTable::create_local_scanner(
                fixture.manager.clone(),
                fixture.env.io_executor().clone(),
            ),
        ));
    assert_rows(
        visible_aliases.as_ref(),
        "SELECT SUM(num_entries) FROM selected_0",
        &[&[Some("12")]],
    )
    .await;

    // A malformed remote counter must propagate a scan error, even if another
    // partition already returned valid contributions.
    let store = &mut fixture.stores[1];
    let (snapshot, _) = store.query_read(Default::default()).unwrap();
    let scope = IndexScope {
        partition: store.partition_id(),
        index: snapshot
            .ready_index(StoredServiceStats::DEFINITION, IndexKind::Aggregate)
            .unwrap(),
    };
    drop(snapshot);
    let mut key = Vec::new();
    StoredServiceStats::encode_key(
        scope,
        ServiceStatsKey::borrowed("broken", Some("run"), EntryKind::Invocation),
        &mut key,
    );
    let mut tx = store.transaction();
    tx.raw_put_cf(KeyKind::Stats, &key, b"\xff");
    tx.commit().await.unwrap();
    drop(tx);
    assert!(
        query(
            visible.as_ref(),
            "SELECT SUM(num_entries) FROM sys_service_stats"
        )
        .await
        .is_err()
    );
    assert_rows(
        visible.as_ref(),
        "SELECT SUM(num_entries) FROM sys_deployment_stats",
        &[&[Some("10")]],
    )
    .await;

    config.common.experimental.set_query_stats_tables(false);
    Configuration::set(config);
    assert!(
        fixture
            .session()
            .execute("SELECT * FROM sys_deployment_stats", QueryOptions {})
            .await
            .is_err()
    );
    assert_rows(
        visible.as_ref(),
        "SELECT SUM(num_entries) FROM sys_deployment_stats",
        &[&[Some("10")]],
    )
    .await;
}
