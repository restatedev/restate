// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Regression corpus for https://github.com/restatedev/restate/issues/5214.
//! Both engines read the same persisted rows. References use fixture values and
//! an unfiltered primary scan, with Rust-computed extrema/counts as an anchor.

use std::collections::{BTreeMap, HashMap};
use std::ops::ControlFlow;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::TimestampMillisecondArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Time;
use futures::{FutureExt, TryStreamExt};

use restate_clock::MockClock;
use restate_core::network::NetworkSender;
use restate_core::network::transport_connector::test_util::MockConnector;
use restate_core::test_env::{TestCoreEnv, TestCoreEnvBuilder, create_mock_nodes_config};
use restate_core::{TaskCenter, TaskKind};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_platform::sync::Mutex;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, ScanInvocationStatusTable,
    ScanInvocationStatusTableRange, StatusTimestamps, WriteInvocationStatusTable,
};
use restate_storage_query_api::{
    AdminUser, QueryEngine, QueryOptions, SessionOptions, SessionTable,
};
use restate_storage_query_datafusion as v1;
use restate_types::cluster_state::NodeState;
use restate_types::errors::GenericError;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionId};
use restate_types::partition_table::{Partition, PartitionTable};
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, Version};

use crate::DataFusionEnv;
use crate::context::{DataFusionQueryEngine, SelectPartitions};
use crate::distributed::DistributedQueryServer;
use crate::invocation_status::schema::SysInvocationStatusTable;
use crate::remote_query_scanner_manager::{
    PartitionLocation, PartitionLocator, RemoteScannerManager,
};
use crate::table_providers::ScanPartition;

use super::{
    Anchor, Case, Cell, Comparison, ExpectedOutcome, compare_results, run_candidate,
    run_memtable_as,
};

const BASE: u64 = 1_787_231_779_702;
const PARTITIONS: u16 = 16;
const TABLE: &str = "sys_invocation_status";
const DYNAMIC_FILTER_OPTION: &str = "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown";

#[derive(Clone, Copy, Debug)]
enum Layout {
    MinimumLast,
    MaximumLast,
    AllNull,
}

impl Layout {
    fn value(self, partition: usize, row: usize) -> Option<i64> {
        if row != 0 {
            return None;
        }
        // Four values among 32 rows, with intervening all-NULL partitions.
        // Partitions 3, 7, 11 and 15 share a legacy lane at parallelism two/four;
        // partitions 3 and 15 also share an owner-local lane in v2.
        let offset = match partition {
            3 => 27,
            7 => 51,
            11 => 76,
            15 => 0,
            _ => return None,
        };
        match self {
            Self::MinimumLast => Some((BASE + offset) as i64),
            Self::MaximumLast => Some((BASE + 76 - offset) as i64),
            Self::AllNull => None,
        }
    }
}

#[derive(Clone, Debug)]
struct Partitions(Vec<(PartitionId, Partition)>);

#[async_trait]
impl SelectPartitions for Partitions {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(self.0.clone())
    }
}

#[async_trait]
impl v1::context::SelectPartitions for Partitions {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(self.0.clone())
    }
}

fn owner(partition: PartitionId) -> GenerationalNodeId {
    GenerationalNodeId::new(u32::from(partition) % 3 + 1, 1)
}

struct Placement(GenerationalNodeId);

impl PartitionLocator for Placement {
    fn get_partition_target_node(
        &self,
        partition: PartitionId,
        _: crate::placement::PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(if owner(partition) == self.0 {
            PartitionLocation::Local
        } else {
            PartitionLocation::Remote {
                node_id: owner(partition).into(),
            }
        })
    }
}

impl v1::remote_query_scanner_manager::PartitionLocator for Placement {
    fn get_partition_target_node(
        &self,
        partition: PartitionId,
    ) -> anyhow::Result<v1::remote_query_scanner_manager::PartitionLocation> {
        use v1::remote_query_scanner_manager::PartitionLocation;
        Ok(if owner(partition) == self.0 {
            PartitionLocation::Local
        } else {
            PartitionLocation::Remote {
                node_id: owner(partition).into(),
            }
        })
    }
}

fn options(batch_size: usize, dynamic_filters: bool) -> HashMap<String, String> {
    HashMap::from([
        (
            "datafusion.execution.batch_size".into(),
            batch_size.to_string(),
        ),
        (DYNAMIC_FILTER_OPTION.into(), dynamic_filters.to_string()),
    ])
}

fn v1_env(parallelism: usize, batch_size: usize, dynamic_filters: bool) -> v1::DataFusionEnv {
    v1::DataFusionEnv::new(
        64 * 1024 * 1024,
        None,
        Some(parallelism),
        &options(batch_size, dynamic_filters),
    )
    .unwrap()
    .with_mock_clock(MockClock::new())
    .unwrap()
}

fn v2_env(parallelism: usize, batch_size: usize, dynamic_filters: bool) -> DataFusionEnv {
    DataFusionEnv::new(
        64 * 1024 * 1024,
        None,
        Some(parallelism),
        &options(batch_size, dynamic_filters),
    )
    .unwrap()
    .with_mock_clock(MockClock::new())
    .unwrap()
}

struct Fixture<N> {
    network: N,
    partitions: Partitions,
    stores: Vec<PartitionStore>,
    v1_local: v1::remote_query_scanner_manager::RemoteScannerManager,
    v1_mixed: v1::remote_query_scanner_manager::RemoteScannerManager,
    v2_mixed: RemoteScannerManager,
    v2_predicates: Arc<Mutex<BTreeMap<String, usize>>>,
}

#[derive(Debug)]
struct ObservedScanner {
    node: GenerationalNodeId,
    inner: Arc<dyn ScanPartition>,
    predicates: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl ScanPartition for ObservedScanner {
    fn scan_partition(
        &self,
        partition: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        assert_eq!(
            owner(partition),
            self.node,
            "scan must execute on its owner"
        );
        let label = predicate
            .as_ref()
            .map_or_else(|| "<none>".to_owned(), ToString::to_string);
        *self.predicates.lock().entry(label).or_default() += 1;
        self.inner.scan_partition(
            partition, range, projection, predicate, batch_size, limit, compute,
        )
    }
}

async fn setup() -> Fixture<impl NetworkSender> {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let table = PartitionTable::with_equally_sized_partitions(Version::MIN, PARTITIONS);
    let partitions = Partitions(
        table
            .iter()
            .map(|(id, partition)| (*id, partition.clone()))
            .collect(),
    );
    let mut stores = Vec::new();
    for (_, partition) in &partitions.0 {
        stores.push(manager.open(partition, None).await.unwrap());
    }
    type Workers = HashMap<
        GenerationalNodeId,
        (
            v1::remote_query_scanner_manager::RemoteScannerManager,
            RemoteScannerManager,
        ),
    >;
    let workers = Arc::new(OnceLock::<Workers>::new());
    let start_servers =
        |legacy, distributed, router: &mut restate_core::network::MessageRouterBuilder| {
            let v1_server = v1::remote_query_scanner_server::RemoteQueryScannerServer::new(
                v1_env(1, 2, true),
                legacy,
                router,
            );
            let v2_server = DistributedQueryServer::new(v2_env(1, 2, true), distributed, router);
            // Poll once to start both service receivers before MockConnector routes RPCs.
            let mut v1_task = Box::pin(v1_server.run());
            let mut v2_task = Box::pin(v2_server.run());
            assert!(v1_task.as_mut().now_or_never().is_none());
            assert!(v2_task.as_mut().now_or_never().is_none());
            TaskCenter::current()
                .spawn(
                    TaskKind::NetworkMessageHandler,
                    "min-max-v1-scanners",
                    v1_task,
                )
                .unwrap();
            TaskCenter::current()
                .spawn(TaskKind::NetworkMessageHandler, "min-max-v2-tasks", v2_task)
                .unwrap();
        };
    let (connector, _) = MockConnector::new({
        let workers = Arc::clone(&workers);
        move |node, router: &mut restate_core::network::MessageRouterBuilder| {
            let (v1, v2) = &workers.get().unwrap()[&node];
            start_servers(v1.clone(), v2.clone(), router);
        }
    });
    let mut nodes = create_mock_nodes_config(1, 1);
    for id in 2..=3 {
        nodes.upsert_node(
            create_mock_nodes_config(id, 1)
                .find_node_by_id(GenerationalNodeId::new(id, 1))
                .unwrap()
                .clone(),
        );
    }
    let mut coordinator = TestCoreEnvBuilder::with_transport_connector(connector)
        .set_nodes_config(nodes)
        .set_partition_table(table);
    let v1_local = v1::remote_query_scanner_manager::RemoteScannerManager::local_only(
        coordinator.metadata.clone(),
    );
    v1::local_scanners::register_partition_scanners(Arc::clone(&manager), &v1_local);
    let v1_mixed = v1::remote_query_scanner_manager::RemoteScannerManager::new(
        v1::remote_query_scanner_client::create_remote_scanner_service(
            coordinator.networking.clone(),
        ),
        Arc::new(Placement(coordinator.my_node_id)),
        coordinator.metadata.clone(),
    );
    v1::local_scanners::register_partition_scanners(Arc::clone(&manager), &v1_mixed);
    let v2_mixed = RemoteScannerManager::new(
        crate::remote_query_scanner_client::create_remote_scanner_service(
            coordinator.networking.clone(),
        ),
        Arc::new(Placement(coordinator.my_node_id)),
        coordinator.metadata.clone(),
    );
    let v2_predicates = Arc::new(Mutex::new(BTreeMap::new()));
    let register_v2 = |scanners: &RemoteScannerManager, node| {
        scanners.register_partition_scanner::<SysInvocationStatusTable>(Arc::new(
            ObservedScanner {
                node,
                inner: Arc::new(SysInvocationStatusTable::create_local_scanner(Arc::clone(
                    &manager,
                ))),
                predicates: Arc::clone(&v2_predicates),
            },
        ));
    };
    register_v2(&v2_mixed, coordinator.my_node_id);
    start_servers(
        v1_mixed.clone(),
        v2_mixed.clone(),
        &mut coordinator.router_builder,
    );
    let coordinator = coordinator.build().await;
    // ScannerTask watches peer liveness; the mock transport has no failure detector.
    for id in 1..=3 {
        TaskCenter::current()
            .cluster_state()
            .clone()
            .updater()
            .upsert_node_state(GenerationalNodeId::new(id, 1), NodeState::Alive);
    }
    let mut remote = HashMap::new();
    for id in 2..=3 {
        let node = TestCoreEnv::create_with_single_node(id, 1).await;
        let v1 = v1::remote_query_scanner_manager::RemoteScannerManager::local_only(
            node.metadata.clone(),
        );
        v1::local_scanners::register_partition_scanners(Arc::clone(&manager), &v1);
        let v2 = RemoteScannerManager::local_only(node.metadata.clone());
        register_v2(&v2, node.metadata.my_node_id());
        remote.insert(node.metadata.my_node_id(), (v1, v2));
    }
    workers.set(remote).unwrap();
    Fixture {
        network: coordinator.networking,
        partitions,
        stores,
        v1_local,
        v1_mixed,
        v2_mixed,
        v2_predicates,
    }
}

impl<N: NetworkSender> Fixture<N> {
    async fn engines(
        &self,
        parallelism: usize,
        batch_size: usize,
        dynamic_filters: bool,
    ) -> Vec<(&'static str, Arc<dyn QueryEngine<AdminUser>>)> {
        let mut engines = Vec::new();
        for (name, scanners) in [("v1-local", &self.v1_local), ("v1-mixed", &self.v1_mixed)] {
            engines.push((
                name,
                v1::context::DataFusionQueryEngine::with_tables(
                    v1_env(parallelism, batch_size, dynamic_filters),
                    None,
                    v1::UserTables::new(self.partitions.clone(), scanners.clone()),
                )
                .await
                .unwrap(),
            ));
        }
        let env = v2_env(parallelism, batch_size, dynamic_filters)
            .with_distributed_execution(self.network.clone());
        env.register_table::<SysInvocationStatusTable>(SysInvocationStatusTable::create_provider(
            self.partitions.clone(),
            &self.v2_mixed,
        ))
        .unwrap();
        engines.push((
            "v2-mixed",
            DataFusionQueryEngine::from_inventory(
                env,
                None,
                vec![SessionTable::for_table::<SysInvocationStatusTable>(TABLE)],
            ),
        ));
        engines
    }

    async fn populate(&mut self, layout: Layout) -> (Vec<Option<i64>>, Vec<Option<i64>>) {
        let mut fixture = Vec::new();
        let primary = Arc::new(Mutex::new(Vec::new()));
        for (index, store) in self.stores.iter_mut().enumerate() {
            let key = store.partition_key_range().start();
            let mut tx = store.transaction();
            for row in 0..2 {
                let scheduled = layout.value(index, row);
                fixture.push(scheduled);
                let mut metadata = InFlightInvocationMetadata::mock();
                metadata.timestamps = StatusTimestamps::new(
                    MillisSinceEpoch::new(BASE),
                    MillisSinceEpoch::new(BASE + 76),
                    None,
                    scheduled.map(|time| MillisSinceEpoch::new(time as u64)),
                    None,
                    None,
                );
                let id = InvocationId::from_parts(key, InvocationUuid::from_u128(row as u128 + 1));
                tx.put_invocation_status(&id, &InvocationStatus::Invoked(metadata))
                    .unwrap();
            }
            tx.commit().await.unwrap();
            drop(tx);
            let output = Arc::clone(&primary);
            store
                .for_each_invocation_status_lazy(
                    ScanInvocationStatusTableRange::PartitionKey(KeyRange::FULL),
                    move |(_, status)| {
                        output.lock().push(
                            status
                                .inner
                                .scheduled_transition_time
                                .map(|time| time as i64),
                        );
                        ControlFlow::<Result<(), anyhow::Error>>::Continue(())
                    },
                )
                .unwrap()
                .await
                .unwrap();
        }
        let primary = primary.lock().clone();
        assert_eq!(
            primary, fixture,
            "broad primary scan must match fixture values before SQL"
        );
        (fixture, primary)
    }
}

fn batch(values: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "scheduled_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            true,
        )])),
        vec![Arc::new(
            TimestampMillisecondArray::from(values.to_vec()).with_timezone("+00:00"),
        )],
    )
    .unwrap()
}

fn cases(values: &[Option<i64>]) -> Vec<(Case, Vec<Cell>)> {
    let min = values
        .iter()
        .flatten()
        .min()
        .map_or(Cell::Null, |value| Cell::TimestampMillisecond(*value));
    let max = values
        .iter()
        .flatten()
        .max()
        .map_or(Cell::Null, |value| Cell::TimestampMillisecond(*value));
    [
        ("min", "SELECT MIN(scheduled_at) AS lo FROM sys_invocation_status", vec![min.clone()]),
        ("max", "SELECT MAX(scheduled_at) AS hi FROM sys_invocation_status", vec![max.clone()]),
        ("min-max", "SELECT MIN(scheduled_at) AS lo, MAX(scheduled_at) AS hi FROM sys_invocation_status", vec![min.clone(), max.clone()]),
        ("max-min", "SELECT MAX(scheduled_at) AS hi, MIN(scheduled_at) AS lo FROM sys_invocation_status", vec![max.clone(), min.clone()]),
        ("min-max-count-all", "SELECT MIN(scheduled_at) AS lo, MAX(scheduled_at) AS hi, COUNT(*) AS n FROM sys_invocation_status", vec![min.clone(), max.clone(), Cell::Int64(values.len() as i64)]),
        ("min-max-count-non-null", "SELECT MIN(scheduled_at) AS lo, MAX(scheduled_at) AS hi, COUNT(scheduled_at) AS n FROM sys_invocation_status", vec![min, max, Cell::Int64(values.iter().flatten().count() as i64)]),
    ].into_iter().map(|(name, sql, expected)| (Case { name, sql, comparison: Comparison::Ordered, expected_rows: 1, anchor: Anchor::None, outcome: ExpectedOutcome::Success }, expected)).collect()
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Known failure: https://github.com/restatedev/restate/issues/5214"]
async fn issue_5214_sparse_min_max_v1_v2() {
    let mut fixture = setup().await;
    let mut failures = Vec::new();
    let mut totals = BTreeMap::<_, (usize, usize)>::new();
    for layout in [Layout::MinimumLast, Layout::MaximumLast, Layout::AllNull] {
        let (values, primary) = fixture.populate(layout).await;
        assert_eq!(values.len(), 32);
        assert_eq!(
            values.iter().flatten().count(),
            if matches!(layout, Layout::AllNull) {
                0
            } else {
                4
            }
        );
        let mut references = Vec::new();
        for (case, expected) in cases(&values) {
            let logical = run_memtable_as(TABLE, case.sql, batch(&values))
                .await
                .unwrap();
            let primary = run_memtable_as(TABLE, case.sql, batch(&primary))
                .await
                .unwrap();
            assert_eq!(
                logical.rows,
                vec![expected],
                "{}: independent Rust anchor",
                case.name
            );
            compare_results(&case, &logical, &primary).unwrap();
            references.push((case, logical));
        }
        for (parallelism, batch_size) in [(2, 1), (4, 2), (16, 2)] {
            for dynamic_filters in [true, false] {
                for (name, engine) in fixture
                    .engines(parallelism, batch_size, dynamic_filters)
                    .await
                {
                    let session = engine.create_session(SessionOptions::default()).unwrap();
                    for (case, logical) in &references {
                        let repeats = if logical.schema.fields().len() == 2 {
                            4
                        } else {
                            1
                        };
                        for attempt in 0..repeats {
                            let actual = match session.execute(case.sql, QueryOptions {}).await {
                                Ok(result) => run_candidate(result).await,
                                Err(error) => Err(error.to_string()),
                            };
                            let tally = totals
                                .entry((name, dynamic_filters, case.name))
                                .or_default();
                            tally.0 += 1;
                            if let Err(error) =
                                actual.and_then(|actual| compare_results(case, logical, &actual))
                            {
                                tally.1 += 1;
                                failures.push(format!("{name} {layout:?} parallelism={parallelism} batch={batch_size} dynamic_filters={dynamic_filters} attempt={attempt} {}: {error}", case.name));
                            }
                        }
                    }
                    if matches!(layout, Layout::MinimumLast) && parallelism == 2 {
                        let batches = match session.execute("EXPLAIN ANALYZE SELECT MIN(scheduled_at), MAX(scheduled_at) FROM sys_invocation_status", QueryOptions {}).await {
                            Ok(result) => result.stream.try_collect::<Vec<_>>().await.map_err(|error| error.to_string()),
                            Err(error) => Err(error.to_string()),
                        };
                        let batches = match batches {
                            Ok(batches) => batches,
                            Err(error) => {
                                failures.push(format!("{name} dynamic_filters={dynamic_filters} EXPLAIN ANALYZE: {error}"));
                                continue;
                            }
                        };
                        let text = batches
                            .iter()
                            .flat_map(|batch| {
                                (0..batch.num_rows())
                                    .map(|row| array_value_to_string(batch.column(1), row).unwrap())
                            })
                            .collect::<Vec<_>>()
                            .join("\n");
                        eprintln!(
                            "issue_5214_plan {name} dynamic_filters={dynamic_filters}\n{text}"
                        );
                        if name == "v2-mixed" {
                            assert!(
                                text.contains("NetworkCoalesceExec"),
                                "v2 must execute distributed tasks"
                            );
                        }
                    }
                }
            }
        }
    }
    for ((engine, dynamic_filters, query), (runs, wrong)) in totals {
        eprintln!(
            "issue_5214 engine={engine} dynamic_filters={dynamic_filters} query={query} runs={runs} wrong={wrong}"
        );
    }
    eprintln!(
        "issue_5214 v2_scan_predicates={:?}",
        fixture.v2_predicates.lock()
    );
    assert!(
        failures.is_empty(),
        "{} incorrect executions:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
