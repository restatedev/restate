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
use futures::{FutureExt, TryStreamExt};
use tokio_util::sync::CancellationToken;

use restate_clock::MockClock;
use restate_core::network::NetworkSender;
use restate_core::network::transport_connector::test_util::MockConnector;
use restate_core::test_env::{TestCoreEnv, TestCoreEnvBuilder, create_mock_nodes_config};
use restate_core::{TaskCenter, TaskKind, cancellation_watcher};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_platform::sync::Mutex;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
    filters::ScanEntryIdFilter, stats::EntryStatistics,
};
use restate_storage_query_api::{QueryEngine, QueryOptions, SessionOptions};
use restate_storage_query_datafusion as v1;
use restate_types::clock::UniqueTimestamp;
use restate_types::cluster_state::NodeState;
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::{Partition, PartitionTable};
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, VQueueId};
use restate_types::{GenerationalNodeId, Version};

use crate::DataFusionEnv;
use crate::access::PrimaryRead;
use crate::context::{DataFusionQueryEngine, SelectPartitions};
use crate::distributed::DistributedQueryServer;
use crate::source_registry::{PartitionLocation, PartitionLocator, SourceRegistry};
use crate::table_providers::{PreparedPartitionRead, ScanPartition};
use crate::tables::vqueue_entry_status::schema::SysVqueueEntryStatusTable;

use super::{
    Anchor, Case, Cell, Comparison, ExpectedOutcome, compare_results, run_candidate,
    run_memtable_as,
};

const BASE: u64 = 1_787_231_779_702;
const PARTITIONS: u16 = 16;
const TABLE: &str = "sys_vqueue_entry_status";
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
                node_id: owner(partition),
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

/// The V1 and V2 sources of each remote worker.
type Workers = HashMap<
    GenerationalNodeId,
    (
        v1::remote_query_scanner_manager::RemoteScannerManager,
        SourceRegistry,
    ),
>;

struct Fixture<N> {
    network: N,
    partitions: Partitions,
    stores: Vec<PartitionStore>,
    v1_local: v1::remote_query_scanner_manager::RemoteScannerManager,
    v1_mixed: v1::remote_query_scanner_manager::RemoteScannerManager,
    v2_mixed: SourceRegistry,
    v2_predicates: Arc<Mutex<BTreeMap<String, usize>>>,
    /// The remote workers' sources, whose servers start on first connection.
    _workers: Arc<OnceLock<Workers>>,
}

#[derive(Debug)]
struct ObservedScanner {
    node: GenerationalNodeId,
    inner: Arc<dyn ScanPartition>,
    predicates: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl ScanPartition for ObservedScanner {
    fn supports_primary_lookup(&self) -> bool {
        self.inner.supports_primary_lookup()
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        let label = predicate
            .as_ref()
            .map_or_else(|| "<none>".to_owned(), ToString::to_string);
        let prepared =
            Arc::clone(&self.inner).prepare_read(access, projection, predicate, limit)?;
        Ok(Arc::new(ObservedRead {
            scanner: self,
            prepared,
            label,
        }))
    }
}

#[derive(Debug)]
struct ObservedRead {
    scanner: Arc<ObservedScanner>,
    prepared: Arc<dyn PreparedPartitionRead>,
    label: String,
}

impl PreparedPartitionRead for ObservedRead {
    fn capture_requirements(&self) -> restate_worker_api::PartitionCaptureRequirements {
        self.prepared.capture_requirements()
    }

    fn execute(
        &self,
        context: crate::table_providers::PartitionReadContext,
        batch_size: usize,
        metrics: crate::table_providers::ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        assert_eq!(
            owner(context.partition_id()),
            self.scanner.node,
            "scan_must_execute_on_its_owner"
        );
        *self
            .scanner
            .predicates
            .lock()
            .entry(self.label.clone())
            .or_default() += 1;
        self.prepared.execute(context, batch_size, metrics)
    }

    fn usable(
        &self,
        context: &crate::table_providers::PartitionReadContext,
    ) -> anyhow::Result<bool> {
        self.prepared.usable(context)
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
    let workers = Arc::new(OnceLock::<Workers>::new());
    let start_servers =
        |legacy, distributed, router: &mut restate_core::network::MessageRouterBuilder| {
            let v1_server = v1::remote_query_scanner_server::RemoteQueryScannerServer::new(
                v1_env(1, 2, true),
                legacy,
                router,
            );
            let v2_server = DistributedQueryServer::new(v2_env(1, 2, true), distributed, router);
            // Poll once to start the V1 receiver before MockConnector routes RPCs.
            let mut v1_task = Box::pin(v1_server.run());
            assert!(v1_task.as_mut().now_or_never().is_none());
            // Like production, shutdown waits for both servers to stop, releasing their
            // storage owners before teardown checks run. Polled before spawning, the V1
            // server's own shutdown watcher is bound outside its task, so stop it when
            // its task is cancelled.
            TaskCenter::current()
                .spawn(TaskKind::SystemService, "min-max-v1-scanners", async move {
                    tokio::select! {
                        result = v1_task => result,
                        () = cancellation_watcher() => Ok(()),
                    }
                })
                .unwrap();
            crate::mocks::spawn_query_server("min-max-v2-tasks", v2_server);
        };
    // Queries keep the connector through their network, possibly past teardown
    // (see `keep_network_past_teardown`), so it must not own storage: it only
    // borrows the workers' sources. The fixture owns them, and so does each
    // server it starts.
    let (connector, _) = MockConnector::new({
        let workers = Arc::downgrade(&workers);
        move |node, router: &mut restate_core::network::MessageRouterBuilder| {
            // A node first reached once the test has returned serves nothing.
            let Some(workers) = workers.upgrade() else {
                return;
            };
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
    v1::local_scanners::register_partition_scanners(
        Arc::clone(&manager),
        crate::mocks::query_io(),
        &v1_local,
    );
    let v1_mixed = v1::remote_query_scanner_manager::RemoteScannerManager::new(
        v1::remote_query_scanner_client::create_remote_scanner_service(
            coordinator.networking.clone(),
        ),
        Arc::new(Placement(coordinator.my_node_id)),
        coordinator.metadata.clone(),
    );
    v1::local_scanners::register_partition_scanners(
        Arc::clone(&manager),
        crate::mocks::query_io(),
        &v1_mixed,
    );
    let v2_mixed = SourceRegistry::new(
        Arc::new(Placement(coordinator.my_node_id)),
        coordinator.metadata.clone(),
        TaskCenter::current().cluster_state().clone(),
    );
    let v2_predicates = Arc::new(Mutex::new(BTreeMap::new()));
    let register_v2 = |scanners: &SourceRegistry, node| {
        scanners.register_offline_reads(Arc::clone(&manager), crate::mocks::query_io());
        scanners.register_partition_scanner::<SysVqueueEntryStatusTable>(Arc::new(
            ObservedScanner {
                node,
                inner: Arc::new(SysVqueueEntryStatusTable::create_local_scanner()),
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
    crate::mocks::keep_network_past_teardown(coordinator.networking.clone());
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
        v1::local_scanners::register_partition_scanners(
            Arc::clone(&manager),
            crate::mocks::query_io(),
            &v1,
        );
        let v2 = SourceRegistry::local_only(node.metadata.clone());
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
        _workers: workers,
    }
}

impl<N: NetworkSender> Fixture<N> {
    async fn engines(
        &self,
        parallelism: usize,
        batch_size: usize,
        dynamic_filters: bool,
        include_v1: bool,
    ) -> Vec<(&'static str, Arc<QueryEngine>)> {
        let mut engines = Vec::new();
        for (name, scanners) in [("v1-local", &self.v1_local), ("v1-mixed", &self.v1_mixed)]
            .into_iter()
            .filter(|_| include_v1)
        {
            let env = v1_env(parallelism, batch_size, dynamic_filters);
            env.register_tables(v1::UserTables::new(
                self.partitions.clone(),
                scanners.clone(),
            ))
            .await
            .unwrap();
            engines.push((
                name,
                Arc::new(v1::context::DataFusionQueryEngine::from_env(env, None)),
            ));
        }
        let env = v2_env(parallelism, batch_size, dynamic_filters)
            .with_distributed_execution(self.network.clone());
        env.register_table::<SysVqueueEntryStatusTable>(
            "public",
            TABLE,
            SysVqueueEntryStatusTable::create_provider(self.partitions.clone(), &self.v2_mixed),
        )
        .unwrap();
        engines.push((
            "v2-mixed",
            Arc::new(DataFusionQueryEngine::from_env(env, None)),
        ));
        engines
    }

    async fn populate(&mut self, layout: Layout) -> (Vec<Option<i64>>, Vec<Option<i64>>) {
        let mut fixture = Vec::new();
        let mut primary = Vec::new();
        for (index, store) in self.stores.iter_mut().enumerate() {
            let key = store.partition_key_range().start();
            let qid = VQueueId::custom(key, "sparse-min-max");
            let now = MillisSinceEpoch::new(BASE);
            let mut tx = store.transaction();
            for row in 0..2 {
                let first_attempt = layout.value(index, row);
                fixture.push(first_attempt);
                let entry_key = EntryKey::new(
                    false,
                    now,
                    1,
                    EntryId::new(EntryKind::Invocation, (row as u128 + 1).to_be_bytes()),
                );
                let mut stats = EntryStatistics::new(
                    UniqueTimestamp::try_from_unix_millis(now).unwrap(),
                    entry_key.run_at(),
                );
                stats.first_attempt_at = first_attempt.map(|time| {
                    UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(time as u64))
                        .unwrap()
                });
                tx.create_vqueue_entry_status(
                    &EntryContext {
                        qid: &qid,
                        target: &EntryTargetRef::Service {
                            scope: None,
                            service: "test",
                            handler: "handler",
                        },
                    },
                    EntryStateRef {
                        stage: Stage::Running,
                        status: Status::Started,
                        entry_key: &entry_key,
                        metadata: &EntryMetadata::default(),
                        stats: &stats,
                    },
                );
            }
            tx.commit().await.unwrap();
            drop(tx);
            let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
            snapshot
                .for_each_vqueue_entry_status_blocking(
                    ScanEntryIdFilter::PartitionKey(KeyRange::FULL),
                    |_, header| {
                        primary.push(
                            header
                                .stats
                                .first_attempt_at
                                .map(|time| time.to_unix_millis().as_u64() as i64),
                        );
                        ControlFlow::Continue(())
                    },
                )
                .unwrap();
        }
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
            "first_attempt_at",
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
        ("min", "SELECT MIN(first_attempt_at) AS lo FROM sys_vqueue_entry_status", vec![min.clone()]),
        ("max", "SELECT MAX(first_attempt_at) AS hi FROM sys_vqueue_entry_status", vec![max.clone()]),
        ("min-max", "SELECT MIN(first_attempt_at) AS lo, MAX(first_attempt_at) AS hi FROM sys_vqueue_entry_status", vec![min.clone(), max.clone()]),
        ("max-min", "SELECT MAX(first_attempt_at) AS hi, MIN(first_attempt_at) AS lo FROM sys_vqueue_entry_status", vec![max.clone(), min.clone()]),
        ("min-max-count-all", "SELECT MIN(first_attempt_at) AS lo, MAX(first_attempt_at) AS hi, COUNT(*) AS n FROM sys_vqueue_entry_status", vec![min.clone(), max.clone(), Cell::Int64(values.len() as i64)]),
        ("min-max-count-non-null", "SELECT MIN(first_attempt_at) AS lo, MAX(first_attempt_at) AS hi, COUNT(first_attempt_at) AS n FROM sys_vqueue_entry_status", vec![min, max, Cell::Int64(values.iter().flatten().count() as i64)]),
    ].into_iter().map(|(name, sql, expected)| (Case { name, sql, comparison: Comparison::Ordered, expected_rows: 1, anchor: Anchor::None, outcome: ExpectedOutcome::Success }, expected)).collect()
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Known failure: https://github.com/restatedev/restate/issues/5214"]
async fn issue_5214_sparse_min_max_v1_v2() {
    run_sparse_min_max(true).await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_min_max_v2_preserves_nulls_and_extrema() {
    run_sparse_min_max(false).await;
}

async fn run_sparse_min_max(include_v1: bool) {
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
                    .engines(parallelism, batch_size, dynamic_filters, include_v1)
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
                        let batches = match session.execute("EXPLAIN ANALYZE SELECT MIN(first_attempt_at), MAX(first_attempt_at) FROM sys_vqueue_entry_status", QueryOptions {}).await {
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
                        if name.starts_with("v1") && dynamic_filters {
                            // Without the filter in the scan, this corpus no longer
                            // exercises #5214. Check the scan's own predicate, not
                            // just any operator in the plan.
                            let scan_predicates: Vec<_> = text
                                .lines()
                                .filter(|line| line.contains("PartitionedExecutionPlan:"))
                                .map(|line| {
                                    line.split_once(", predicate=")
                                        .map(|(_, predicate)| predicate)
                                })
                                .collect();
                            assert!(
                                !scan_predicates.is_empty()
                                    && scan_predicates.iter().all(|predicate| predicate
                                        .is_some_and(|p| p.contains("DynamicFilter"))),
                                "{name}: the aggregate's dynamic filter must reach every scan\n{text}"
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
    let v2_predicates = fixture.v2_predicates.lock();
    eprintln!("issue_5214 v2_scan_predicates={v2_predicates:?}");
    // Current behaviour, to revisit when V2 accepts dynamic filters: no V2 scan
    // receives one, which is also why V2 doesn't hit #5214 today.
    assert!(
        v2_predicates.keys().all(|predicate| predicate == "<none>"),
        "a V2 scan received a dynamic filter: {v2_predicates:?}"
    );
    assert!(
        failures.is_empty(),
        "{} incorrect executions:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
