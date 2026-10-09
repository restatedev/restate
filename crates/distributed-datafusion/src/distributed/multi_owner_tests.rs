// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashMap};
use std::ops::RangeBounds;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::JoinType;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlanProperties, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_distributed::{NetworkBoundary, NetworkCoalesceExec, Stage};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use tokio::time::Instant;

use restate_core::network::NetworkSender;
use restate_core::network::transport_connector::test_util::MockConnector;
use restate_core::partitions::PartitionRouting;
use restate_core::test_env::{TestCoreEnv, TestCoreEnvBuilder, create_mock_nodes_config};
use restate_core::{Metadata, TaskCenter, TaskKind};
use restate_memory::ByteCount;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_platform::sync::Mutex;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::state_table::WriteStateTable;
use restate_storage_query_api::{
    AdminUser, QueryEngine, QueryEngineTable, QueryOptions, QuerySession, SessionOptions,
    SessionTable,
};
use restate_types::identifiers::{
    InvocationId, InvocationUuid, LeaderEpoch, PartitionId, ServiceId, WithPartitionKey,
};
use restate_types::nodes_config::Role;
use restate_types::partition_table::{Partition, PartitionTable};
use restate_types::partitions::state::{LeadershipState, PartitionReplicaSetStates};
use restate_types::sharding::KeyRange;
use restate_types::{GenerationalNodeId, Version};
use restate_util_string::ReString;

use crate::access::{PrimaryKeyKind, PrimaryRead};
use crate::context::{DataFusionQueryEngine, SelectPartitions};
use crate::invocation_state::schema::SysInvocationStateTable;
use crate::invocation_status::schema::SysInvocationStatusTable;
use crate::node_fan_out::{AllNodeLocator, NodeFanOutTableProvider, RoleBasedNodeLocator};
use crate::placement::{PartitionPlacement, StoragePlacementOptions};
use crate::query_correctness::run_distributed_state_corpus;
use crate::remote_query_scanner_manager::{
    PartitionLocation, PartitionLocator, RemoteScannerManager, create_partition_locator,
};
use crate::state::schema::StateTable;
use crate::table_providers::{Scan, ScanPartition};

use super::DistributedQueryServer;
use super::tests::{NoLegacyScanner, environment};
use super::worker::TaskObservation;

#[path = "primary_access_tests.rs"]
mod primary_access;

fn owner(partition: PartitionId) -> GenerationalNodeId {
    GenerationalNodeId::new(u32::from(partition) % 3 + 1, 1)
}

#[derive(Clone, Debug)]
struct Partitions(Vec<(PartitionId, Partition)>);

#[async_trait]
impl SelectPartitions for Partitions {
    async fn get_live_partitions(
        &self,
    ) -> Result<Vec<(PartitionId, Partition)>, restate_types::errors::GenericError> {
        Ok(self.0.clone())
    }
}

#[derive(Debug, Default)]
struct Reads {
    ranges: Vec<(GenerationalNodeId, PartitionId, KeyRange)>,
    accesses: Vec<(PartitionId, PrimaryRead)>,
    rows: usize,
}

#[derive(Debug)]
struct CheckedScanner {
    node: GenerationalNodeId,
    check_owner: bool,
    inner: Arc<dyn ScanPartition>,
    reads: Arc<Mutex<Reads>>,
}

impl ScanPartition for CheckedScanner {
    fn primary_key_kind(&self) -> Option<PrimaryKeyKind> {
        self.inner.primary_key_kind()
    }

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
        self.read_partition(
            partition,
            range,
            PrimaryRead::Range,
            projection,
            predicate,
            batch_size,
            limit,
            compute,
        )
    }

    fn read_partition(
        &self,
        partition: PartitionId,
        range: KeyRange,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        if self.check_owner {
            assert_eq!(
                owner(partition),
                self.node,
                "scanner accessed another owner's storage"
            );
        }
        self.reads.lock().ranges.push((self.node, partition, range));
        self.reads.lock().accesses.push((partition, access.clone()));
        let stream = self.inner.read_partition(
            partition,
            range,
            access,
            Arc::clone(&projection),
            predicate,
            batch_size,
            limit,
            compute,
        )?;
        let reads = Arc::clone(&self.reads);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projection,
            stream.inspect(move |batch| {
                if let Ok(batch) = batch {
                    reads.lock().rows += batch.num_rows();
                }
            }),
        )))
    }
}

type PlacementCalls = Arc<Mutex<Vec<(GenerationalNodeId, PartitionId, PartitionPlacement)>>>;

struct ObservedLocator {
    node: GenerationalNodeId,
    inner: Arc<dyn PartitionLocator>,
    calls: PlacementCalls,
}

impl PartitionLocator for ObservedLocator {
    fn get_partition_target_node(
        &self,
        partition: PartitionId,
        placement: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        self.calls.lock().push((self.node, partition, placement));
        self.inner.get_partition_target_node(partition, placement)
    }
}

#[derive(Debug, Clone)]
struct NodeRows {
    node: GenerationalNodeId,
    reads: Arc<Mutex<Vec<GenerationalNodeId>>>,
}

impl QueryEngineTable for NodeRows {
    fn identity() -> ReString {
        "test_node_rows".into()
    }
}

impl NodeRows {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("plain_node_id", DataType::Utf8, false),
            Field::new("gen_node_id", DataType::Utf8, false),
        ]))
    }
}

impl Scan for NodeRows {
    fn scan(
        &self,
        projection: SchemaRef,
        _: &[datafusion::logical_expr::Expr],
        _: usize,
        _: Option<usize>,
    ) -> SendableRecordBatchStream {
        self.reads.lock().push(self.node);
        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![self.node.as_plain().to_string()])),
                Arc::new(StringArray::from(vec![self.node.to_string()])),
            ],
        )
        .unwrap();
        let indices: Vec<_> = projection
            .fields()
            .iter()
            .map(|field| schema.index_of(field.name()).unwrap())
            .collect();
        Box::pin(RecordBatchStreamAdapter::new(
            projection,
            stream::iter([batch.project(&indices).map_err(Into::into)]),
        ))
    }
}

struct MultiFixture<N> {
    network: N,
    metadata: Metadata,
    scanners: RemoteScannerManager,
    partitions: Partitions,
    stores: Vec<PartitionStore>,
    store_manager: Arc<PartitionStoreManager>,
    tasks: Arc<Mutex<Vec<TaskObservation>>>,
    reads: Arc<Mutex<Reads>>,
    node_reads: Arc<Mutex<Vec<GenerationalNodeId>>>,
    placements: PlacementCalls,
    states: PartitionReplicaSetStates,
}

async fn setup() -> MultiFixture<impl NetworkSender> {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let table = PartitionTable::with_equally_sized_partitions(Version::MIN, 8);
    let partitions = Partitions(table.iter().map(|(id, p)| (*id, p.clone())).collect());
    let mut stores = Vec::new();
    let states = PartitionReplicaSetStates::default();
    for (id, partition) in &partitions.0 {
        stores.push(manager.open(partition, None).await.unwrap());
        states.note_observed_leader(
            *id,
            LeadershipState {
                current_leader: owner(*id),
                current_leader_epoch: LeaderEpoch::from(1),
            },
        );
    }
    let workers = Arc::new(OnceLock::<HashMap<GenerationalNodeId, RemoteScannerManager>>::new());
    let tasks = Arc::new(Mutex::new(vec![]));
    let (connector, _) = MockConnector::new({
        let workers = Arc::clone(&workers);
        let tasks = Arc::clone(&tasks);
        move |node, router: &mut restate_core::network::MessageRouterBuilder| {
            let mut server = DistributedQueryServer::new(
                environment(1, 128),
                workers.get().unwrap()[&node].clone(),
                router,
            );
            server.observations = Arc::clone(&tasks);
            // Start the receiver before the connector can dispatch its first RPC.
            let mut task = Box::pin(server.run());
            assert!(task.as_mut().now_or_never().is_none());
            TaskCenter::current()
                .spawn(TaskKind::NetworkMessageHandler, "multi-owner-query", task)
                .unwrap();
        }
    });
    let mut nodes = create_mock_nodes_config(1, 1);
    for id in 2..=4 {
        let mut node = create_mock_nodes_config(id, 1)
            .find_node_by_id(GenerationalNodeId::new(id, 1))
            .unwrap()
            .clone();
        if id == 4 {
            node.roles = Role::Admin.into();
        }
        nodes.upsert_node(node);
    }
    let mut coordinator = TestCoreEnvBuilder::with_transport_connector(connector)
        .set_nodes_config(nodes)
        .set_partition_table(table);
    let reads = Arc::new(Mutex::new(Reads::default()));
    let node_reads = Arc::new(Mutex::new(vec![]));
    let placements: PlacementCalls = Default::default();
    let make_scanners = |metadata: Metadata, node: GenerationalNodeId| {
        let locator = create_partition_locator(
            PartitionRouting::new(states.clone(), TaskCenter::current()),
            metadata.clone(),
        );
        let scanners = RemoteScannerManager::new(
            Arc::new(NoLegacyScanner),
            Arc::new(ObservedLocator {
                node,
                inner: locator,
                calls: Arc::clone(&placements),
            }),
            metadata,
        );
        scanners.register_partition_scanner::<StateTable>(Arc::new(CheckedScanner {
            node,
            check_owner: true,
            inner: Arc::new(StateTable::create_local_scanner(Arc::clone(&manager))),
            reads: Arc::clone(&reads),
        }));
        scanners.register_partition_scanner::<SysInvocationStatusTable>(Arc::new(CheckedScanner {
            node,
            check_owner: true,
            inner: Arc::new(SysInvocationStatusTable::create_local_scanner(Arc::clone(
                &manager,
            ))),
            reads: Arc::clone(&reads),
        }));
        scanners.register_node_scanner::<NodeRows>(Arc::new(NodeRows {
            node,
            reads: Arc::clone(&node_reads),
        }));
        scanners
    };
    let scanners = make_scanners(coordinator.metadata.clone(), coordinator.my_node_id);
    let mut server = DistributedQueryServer::new(
        environment(1, 128),
        scanners.clone(),
        &mut coordinator.router_builder,
    );
    server.observations = Arc::clone(&tasks);
    let mut task = Box::pin(server.run());
    assert!(task.as_mut().now_or_never().is_none());
    TaskCenter::current()
        .spawn(TaskKind::NetworkMessageHandler, "local-query-worker", task)
        .unwrap();
    let coordinator = coordinator.build().await;
    let mut remote = HashMap::new();
    for id in 2..=3 {
        let worker = TestCoreEnv::create_with_single_node(id, 1).await;
        remote.insert(
            worker.metadata.my_node_id(),
            make_scanners(worker.metadata, GenerationalNodeId::new(id, 1)),
        );
    }
    let unavailable = TestCoreEnv::create_with_single_node(4, 1).await;
    remote.insert(
        GenerationalNodeId::new(4, 1),
        RemoteScannerManager::local_only(unavailable.metadata),
    );
    workers.set(remote).unwrap();
    MultiFixture {
        network: coordinator.networking,
        metadata: coordinator.metadata,
        scanners,
        partitions,
        stores,
        store_manager: manager,
        tasks,
        reads,
        node_reads,
        placements,
        states,
    }
}

impl<N: NetworkSender> MultiFixture<N> {
    fn engine(&self, parallelism: usize) -> Arc<dyn QueryEngine<AdminUser>> {
        self.engine_with_pushdown(parallelism, 2, true)
    }

    fn engine_with_pushdown(
        &self,
        parallelism: usize,
        batch_size: usize,
        pushdown: bool,
    ) -> Arc<dyn QueryEngine<AdminUser>> {
        let env = environment(parallelism, batch_size)
            .with_storage_placement(StoragePlacementOptions {
                require_leader: true,
            })
            .with_distributed_execution(self.network.clone());
        let env = if pushdown {
            env
        } else {
            env.without_distributed_operator_pushdown()
        };
        env.register_table::<StateTable>(StateTable::create_provider(
            self.partitions.clone(),
            &self.scanners,
        ))
        .unwrap();
        env.register_table::<NodeRows>(Arc::new(NodeFanOutTableProvider::new(
            NodeRows::schema(),
            Arc::new(RoleBasedNodeLocator::new(
                Role::Worker,
                self.metadata.clone(),
            )),
            self.scanners.clone(),
            None,
            NodeRows::identity(),
        )))
        .unwrap();
        env.register_provider(
            "all_node_rows".into(),
            Arc::new(NodeFanOutTableProvider::new(
                NodeRows::schema(),
                Arc::new(AllNodeLocator::new(self.metadata.clone())),
                self.scanners.clone(),
                None,
                NodeRows::identity(),
            )),
        )
        .unwrap();
        DataFusionQueryEngine::from_inventory(
            env,
            None,
            vec![
                SessionTable::for_table::<StateTable>("state"),
                SessionTable::for_table::<NodeRows>("node_rows"),
                SessionTable::new("all_node_rows", "all_node_rows"),
            ],
        )
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_owner_coverage_and_scoped_selection() {
    let mut fixture = setup().await;
    for parallelism in [1, 2, 4, 48] {
        let engine = fixture.engine(parallelism);
        let metadata = run_distributed_state_corpus(&mut fixture.stores, engine.as_ref())
            .await
            .unwrap_or_else(|error| {
                let tasks = fixture.tasks.lock();
                if let Some(last) = tasks.last() {
                    for task in tasks.iter().filter(|task| {
                        task.id.query_ts == last.id.query_ts
                            && task.id.session_id == last.id.session_id
                    }) {
                        eprintln!(
                            "parallelism={parallelism} owner={} task={}\n{}",
                            task.owner, task.id.task, task.plan
                        );
                    }
                }
                panic!("{error}")
            });
        let tasks = fixture.tasks.lock();
        let count_tasks: Vec<_> = tasks
            .iter()
            .filter(|task| {
                task.id.query_ts == metadata[4].query_ts
                    && task.id.session_id == metadata[4].session_id
            })
            .collect();
        assert_eq!(
            count_tasks.len(),
            3,
            "all owners must run below owner-count parallelism"
        );
        assert_eq!(
            count_tasks
                .iter()
                .map(|task| task.owner.raw_id())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2, 3])
        );
        assert!(
            count_tasks
                .iter()
                .all(|task| task.plan.contains("TableScanExec"))
        );
        assert!(
            count_tasks
                .iter()
                .map(|task| task.output.lock().bytes)
                .sum::<usize>()
                > 0
        );
    }
    let engine = fixture.engine(1);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    fixture.reads.lock().ranges.clear();
    let result = session
        .execute("SELECT COUNT(*) FROM state", QueryOptions {})
        .await
        .unwrap();
    result.stream.try_collect::<Vec<_>>().await.unwrap();
    let mut ranges: Vec<_> = fixture
        .reads
        .lock()
        .ranges
        .iter()
        .map(|(_, p, r)| (*p, *r))
        .collect();
    ranges.sort_unstable();
    let expected: Vec<_> = fixture
        .partitions
        .0
        .iter()
        .map(|(p, partition)| (*p, partition.key_range))
        .collect();
    assert_eq!(
        ranges, expected,
        "each logical partition must be scanned exactly once"
    );
    assert!(
        fixture
            .placements
            .lock()
            .iter()
            .all(|(_, _, placement)| placement.options.require_leader),
        "options must survive planning, encoding and execution"
    );

    let (first_id, first) = &fixture.partitions.0[0];
    let (second_id, second) = &fixture.partitions.0[1];
    for (predicate, expected) in [
        (
            format!(
                "partition_key IN ({}, {}, {})",
                first.key_range.start(),
                second.key_range.end(),
                second.key_range.end()
            ),
            vec![
                (
                    *first_id,
                    KeyRange::new(first.key_range.start(), first.key_range.start()),
                ),
                (
                    *second_id,
                    KeyRange::new(second.key_range.end(), second.key_range.end()),
                ),
            ],
        ),
        (
            format!(
                "partition_key >= {} AND partition_key < {}",
                first.key_range.end(),
                second.key_range.end()
            ),
            vec![
                (
                    *first_id,
                    KeyRange::new(first.key_range.end(), first.key_range.end()),
                ),
                (
                    *second_id,
                    KeyRange::new(second.key_range.start(), second.key_range.end() - 1),
                ),
            ],
        ),
        ("partition_key = 1 AND partition_key = 2".to_owned(), vec![]),
    ] {
        fixture.reads.lock().ranges.clear();
        session
            .execute(
                &format!("SELECT COUNT(*) FROM state WHERE {predicate}"),
                QueryOptions {},
            )
            .await
            .unwrap()
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut got: Vec<_> = fixture
            .reads
            .lock()
            .ranges
            .iter()
            .map(|(_, id, range)| (*id, *range))
            .collect();
        got.sort_unstable();
        assert_eq!(got, expected, "{predicate}");
    }

    for (predicate, expected) in [
        ("plain_node_id = 'N2'", vec![2]),
        ("gen_node_id = 'N3:1'", vec![3]),
        ("plain_node_id IN ('N1', 'N3', 'N1')", vec![1, 3]),
        ("gen_node_id = 'N2:9'", vec![]),
        ("plain_node_id = 'N4'", vec![]), // Ineligible role, even though present in metadata.
        ("plain_node_id = 'N1' AND gen_node_id = 'N2:1'", vec![]),
        ("plain_node_id = 'N1' OR gen_node_id = 'N3:1'", vec![1, 3]),
    ] {
        fixture.node_reads.lock().clear();
        let before = fixture.tasks.lock().len();
        let result = session
            .execute(
                &format!(
                    "SELECT plain_node_id FROM node_rows WHERE {predicate} ORDER BY plain_node_id"
                ),
                QueryOptions {},
            )
            .await
            .unwrap();
        let diagnostics = result.diagnostics;
        let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
        let rows: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(|row| array_value_to_string(batch.column(0), row).unwrap())
            })
            .collect();
        assert_eq!(
            rows,
            expected
                .iter()
                .map(|id| format!("N{id}"))
                .collect::<Vec<_>>(),
            "{predicate}"
        );
        let mut read_nodes: Vec<_> = fixture
            .node_reads
            .lock()
            .iter()
            .map(|node| node.raw_id())
            .collect();
        read_nodes.sort_unstable();
        assert_eq!(
            read_nodes, expected,
            "pruning must precede dispatch: {predicate}"
        );
        assert_eq!(fixture.tasks.lock().len() - before, expected.len());
        assert!(diagnostics.warnings().is_empty());
    }

    let result = session
        .execute("SELECT COUNT(*) FROM all_node_rows", QueryOptions {})
        .await
        .unwrap();
    let diagnostics = result.diagnostics;
    let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), "3");
    let warnings = diagnostics.warnings();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].node_id.as_str(), "N4");
    assert!(
        warnings[0].message.contains("unavailable"),
        "{}",
        warnings[0].message
    );

    let result = session
        .execute(
            "SELECT COUNT(*) FROM state CROSS JOIN node_rows",
            QueryOptions {},
        )
        .await
        .unwrap();
    let diagnostics = result.diagnostics;
    let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        array_value_to_string(batches[0].column(0), 0).unwrap(),
        "27"
    );
    assert!(diagnostics.warnings().is_empty());

    // Change observed ownership after selection. Execution must validate the
    // serialized placement contract rather than forward or silently omit work.
    let result = session
        .execute("SELECT COUNT(*) FROM state", QueryOptions {})
        .await
        .unwrap();
    fixture.states.note_observed_leader(
        PartitionId::MIN,
        LeadershipState {
            current_leader: GenerationalNodeId::new(2, 1),
            current_leader_epoch: LeaderEpoch::from(2),
        },
    );
    let error = result.stream.try_collect::<Vec<_>>().await.unwrap_err();
    assert!(
        error.to_string().contains("storage ownership changed"),
        "{error}"
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_estimates_choose_live_state_as_join_build_side() {
    let fixture = setup().await;
    let env = environment(48, 128).with_distributed_execution(fixture.network.clone());
    let ctx = SessionContext::new_with_state(env.build_session_state().unwrap());
    ctx.register_table(
        "sys_invocation_status",
        SysInvocationStatusTable::create_provider(fixture.partitions.clone(), &fixture.scanners),
    )
    .unwrap();
    ctx.register_table(
        "sys_invocation_state",
        SysInvocationStateTable::create_provider(fixture.partitions.clone(), &fixture.scanners),
    )
    .unwrap();

    // The UI query must build on the small live-state table, even though the SQL
    // puts the potentially millions of retained invocation rows on the left.
    let plan = ctx
        .sql(
            "SELECT ss.target_service_name AS service_name,
                CASE
                    WHEN ss.status = 'inboxed' THEN 'pending'
                    WHEN ss.status = 'invoked' AND sis.in_flight IS TRUE THEN 'running'
                    WHEN ss.status = 'invoked' THEN 'ready-yielded-backing-off'
                    WHEN ss.status = 'completed' AND ss.completion_result = 'success' THEN 'succeeded'
                    WHEN ss.status = 'completed' THEN 'failed'
                    ELSE ss.status
                END AS bucket, COUNT(1) AS count
             FROM sys_invocation_status ss
             LEFT JOIN sys_invocation_state sis ON sis.id = ss.id
             WHERE ss.target_service_name IN ('Counter')
             GROUP BY service_name, bucket",
        )
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut joins = 0;
    plan.apply(|node| {
        if let Some(join) = node.downcast_ref::<HashJoinExec>() {
            joins += 1;
            assert_eq!(join.join_type(), &JoinType::Right);
            assert_eq!(join.partition_mode(), &PartitionMode::CollectLeft);
            let statistics = StatisticsContext::new()
                .compute(join.left().as_ref(), &StatisticsArgs::new())
                .unwrap();
            assert_eq!(statistics.num_rows, Precision::Inexact(64 * 1024));
            let build = displayable(join.left().as_ref()).indent(false).to_string();
            assert!(build.contains("table=sys_invocation_state,"), "{build}");
            assert!(!build.contains("table=sys_invocation_status,"), "{build}");
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(joins, 1);

    // A requested key may not exist. Keep estimates inexact so COUNT(*) cannot
    // become the number of requested keys; preserve projected id NDV as well.
    let id = InvocationId::from_parts(0, InvocationUuid::from_u128(1));
    let lookup = ctx
        .sql(&format!(
            "SELECT status, id FROM sys_invocation_status WHERE id = '{id}'"
        ))
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut lookups = 0;
    lookup
        .apply(|node| {
            if node.is::<super::source::MultiGetExec>() {
                lookups += 1;
                let statistics = StatisticsContext::new()
                    .compute(node.as_ref(), &StatisticsArgs::new())
                    .unwrap();
                assert_eq!(statistics.num_rows, Precision::Inexact(1));
                let id_column = node.schema().index_of("id").unwrap();
                assert_eq!(
                    statistics.column_statistics[id_column].distinct_count,
                    Precision::Inexact(1)
                );
                let status_column = node.schema().index_of("status").unwrap();
                assert_eq!(
                    statistics.column_statistics[status_column].distinct_count,
                    Precision::Absent
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
    assert_eq!(lookups, 1);
    assert!(
        fixture.tasks.lock().is_empty(),
        "planning must not dispatch"
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn stage_budgets_cover_sorts_aggregates_and_joins() {
    let fixture = setup().await;
    let env = environment(48, 128).with_distributed_execution(fixture.network.clone());
    let ctx = SessionContext::new_with_state(env.build_session_state().unwrap());
    ctx.register_table(
        "sys_invocation_status",
        SysInvocationStatusTable::create_provider(fixture.partitions.clone(), &fixture.scanners),
    )
    .unwrap();

    for (sql, expected_stages, expected_lanes, owner_operator) in [
        (
            "SELECT status, modified_at FROM sys_invocation_status WHERE partition_key > 0 ORDER BY modified_at DESC",
            3,
            8,
            "SortExec",
        ),
        (
            "SELECT status, modified_at FROM sys_invocation_status WHERE partition_key > 11529215046068469760 ORDER BY modified_at DESC",
            3,
            3,
            "SortExec",
        ),
        (
            "SELECT status, modified_at FROM sys_invocation_status WHERE partition_key > 0 ORDER BY modified_at DESC LIMIT 10 OFFSET 2",
            3,
            8,
            "SortExec",
        ),
        (
            "SELECT status, COUNT(*) FROM sys_invocation_status GROUP BY status",
            3,
            8,
            "AggregateExec",
        ),
        (
            "SELECT a.id FROM sys_invocation_status a JOIN sys_invocation_status b ON a.id = b.id",
            6,
            16,
            "TableScanExec",
        ),
        (
            "SELECT status, COUNT(*) AS n FROM sys_invocation_status GROUP BY status UNION ALL SELECT status, COUNT(*) AS n FROM sys_invocation_status GROUP BY status",
            6,
            16,
            "AggregateExec",
        ),
    ] {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let text = displayable(plan.as_ref()).indent(false).to_string();
        eprintln!("stage_budget sql={sql}\n{text}");
        let mut stages = 0;
        let mut inputs = 0;
        plan.apply(|node| {
            let Some(boundary) = node.downcast_ref::<NetworkCoalesceExec>() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            let Stage::Local(stage) = boundary.input_stage() else {
                panic!("expected owner stage");
            };
            stages += 1;
            inputs += node.output_partitioning().partition_count();
            let mut source_lanes = 0;
            stage.plan.apply(|node| {
                if super::source::source_owner(node.as_ref()).is_some() {
                    source_lanes += node.output_partitioning().partition_count();
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            let mut found_operator = false;
            stage.plan.apply(|node| {
                assert!(
                    node.output_partitioning().partition_count() <= source_lanes,
                    "{text}"
                );
                found_operator |= if owner_operator == "SortExec" {
                    node.is::<SortExec>()
                } else {
                    node.name() == owner_operator
                };
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(
                found_operator,
                "missing owner-local {owner_operator}: {text}"
            );
            assert_eq!(
                node.output_partitioning().partition_count(),
                source_lanes,
                "{text}"
            );
            Ok(TreeNodeRecursion::Jump)
        })
        .unwrap();
        assert_eq!(stages, expected_stages, "{text}");
        assert_eq!(inputs, expected_lanes, "{text}");
        plan.apply(|node| {
            if node.is::<NetworkCoalesceExec>() {
                return Ok(TreeNodeRecursion::Jump);
            }
            assert!(
                node.output_partitioning().partition_count() <= inputs,
                "{text}"
            );
            assert!(!node.is::<SortExec>(), "sort should run at owners: {text}");
            if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                assert_eq!(join.partition_mode(), &PartitionMode::Partitioned);
                assert_eq!(
                    join.left().output_partitioning().partition_count(),
                    join.right().output_partitioning().partition_count(),
                    "{text}"
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
    }
    assert!(
        fixture.tasks.lock().is_empty(),
        "planning must not dispatch"
    );
}

#[derive(Debug)]
struct Measurement {
    elapsed_us: u128,
    scans: usize,
    storage_rows: usize,
    transported_rows: usize,
    ipc: ByteCount,
    remote_ipc: ByteCount,
}

async fn measure(
    fixture: &MultiFixture<impl NetworkSender>,
    session: &dyn QuerySession<AdminUser>,
    sql: &str,
    expected: &[Vec<String>],
    pushdown: bool,
    aggregation: bool,
) -> Measurement {
    *fixture.reads.lock() = Reads::default();
    let start = Instant::now();
    let result = session.execute(sql, QueryOptions {}).await.unwrap();
    let metadata = result.metadata;
    let diagnostics = result.diagnostics;
    let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let rows: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column, row).unwrap())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    assert_eq!(rows, expected);
    assert_eq!(diagnostics.snapshot().output_rows, expected.len() as u64);
    assert!(diagnostics.warnings().is_empty());
    let tasks = fixture.tasks.lock();
    let tasks: Vec<_> = tasks
        .iter()
        .filter(|task| {
            task.id.session_id == metadata.session_id && task.id.query_ts == metadata.query_ts
        })
        .collect();
    assert_eq!(tasks.len(), 3);
    for task in &tasks {
        assert_eq!(
            task.plan.contains("AggregateExec: mode=Partial"),
            pushdown && aggregation,
            "{}",
            task.plan
        );
        if !aggregation {
            assert_eq!(task.plan.contains("FilterExec"), pushdown, "{}", task.plan);
        }
    }
    let reads = fixture.reads.lock();
    Measurement {
        elapsed_us,
        scans: reads.ranges.len(),
        storage_rows: reads.rows,
        transported_rows: tasks.iter().map(|task| task.output.lock().rows).sum(),
        ipc: ByteCount::from(
            tasks
                .iter()
                .map(|task| task.output.lock().bytes)
                .sum::<usize>(),
        ),
        remote_ipc: ByteCount::from(
            tasks
                .iter()
                .filter(|task| task.owner.raw_id() != 1)
                .map(|task| task.output.lock().bytes)
                .sum::<usize>(),
        ),
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_aggregation_reduces_transfer_without_changing_storage_work() {
    let mut fixture = setup().await;
    let empty = fixture
        .engine_with_pushdown(48, 128, true)
        .create_session(SessionOptions::default())
        .unwrap();
    let result = measure(
        &fixture,
        empty.as_ref(),
        "SELECT COUNT(*) FROM state",
        &[vec!["0".into()]],
        true,
        true,
    )
    .await;
    assert_eq!(result.scans, 8);
    assert_eq!(result.storage_rows, 0);

    // Independent, hand-computable reference: two groups, 2,048 rows each;
    // every state key is one byte. Quiescent data spread over eight partitions.
    let records: Vec<_> = (0..4096)
        .map(|i| {
            (
                ServiceId::new(None, "measurement", format!("object-{i}")),
                Bytes::from(vec![b'x'; if i % 2 == 0 { 4 } else { 64 }]),
            )
        })
        .collect();
    for store in &mut fixture.stores {
        let range = store.partition_key_range();
        let mut tx = store.transaction();
        for (service, value) in &records {
            if range.contains(&service.partition_key()) {
                tx.put_user_state(service, &Bytes::from_static(b"k"), value)
                    .unwrap();
            }
        }
        tx.commit().await.unwrap();
    }
    for parallelism in [1, 48] {
        let control = fixture
            .engine_with_pushdown(parallelism, 128, false)
            .create_session(SessionOptions::default())
            .unwrap();
        let pushed = fixture
            .engine_with_pushdown(parallelism, 128, true)
            .create_session(SessionOptions::default())
            .unwrap();
        let sql = "SELECT value_length, COUNT(*) AS n, SUM(key_length) AS key_bytes FROM state GROUP BY value_length ORDER BY value_length";
        let expected = vec![
            vec!["4".into(), "2048".into(), "2048".into()],
            vec!["64".into(), "2048".into(), "2048".into()],
        ];
        let mut timings = [vec![], vec![]];
        for round in 0..6 {
            let mut payloads = [0; 2];
            let mut remote_payloads = [0; 2];
            let order = if round % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for pushdown in order {
                let session = if pushdown { &pushed } else { &control };
                let result =
                    measure(&fixture, session.as_ref(), sql, &expected, pushdown, true).await;
                assert_eq!(result.scans, 8);
                assert_eq!(result.storage_rows, records.len());
                // Two groups per source lane: one lane per owner at parallelism 1,
                // one per storage partition at parallelism 48.
                let partial_rows = if parallelism == 1 { 6 } else { 16 };
                assert_eq!(
                    result.transported_rows,
                    if pushdown { partial_rows } else { 4096 }
                );
                if round > 0 {
                    timings[usize::from(pushdown)].push(result.elapsed_us);
                }
                // ByteCount's Debug preserves exact byte counts in this measurement log.
                eprintln!(
                    "m2_measure parallelism={parallelism} round={round} pushdown={pushdown} {result:?}"
                );
                assert!(result.ipc.as_u64() > 0 && result.remote_ipc.as_u64() > 0);
                payloads[usize::from(pushdown)] = result.ipc.as_u64();
                remote_payloads[usize::from(pushdown)] = result.remote_ipc.as_u64();
            }
            assert!(payloads[1] < payloads[0] / 8);
            assert!(remote_payloads[1] < remote_payloads[0] / 8);
        }
        for (mode, mut samples) in timings.into_iter().enumerate() {
            samples.sort_unstable();
            eprintln!(
                "m2_median parallelism={parallelism} pushdown={} elapsed_us={}",
                mode != 0,
                samples[samples.len() / 2]
            );
        }
        let expected = vec![vec!["1".to_owned()]; 2048];
        let sql = "SELECT key_length FROM state WHERE value_length = 4";
        let a = measure(&fixture, control.as_ref(), sql, &expected, false, false).await;
        let b = measure(&fixture, pushed.as_ref(), sql, &expected, true, false).await;
        assert_eq!(a.storage_rows, b.storage_rows);
        // The scanner already applies this predicate on both paths. Moving the
        // residual FilterExec's projection saves columns and batch framing, not rows.
        assert_eq!(a.transported_rows, 2048);
        assert_eq!(b.transported_rows, 2048);
        assert!(b.ipc < a.ipc && b.remote_ipc < a.remote_ipc);
        eprintln!("m2_filter parallelism={parallelism} control={a:?} pushed={b:?}");
    }
}
