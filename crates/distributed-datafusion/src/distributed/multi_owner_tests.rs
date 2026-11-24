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
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::JoinType;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlanProperties, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_distributed::{NetworkBoundary, NetworkCoalesceExec, Stage};
use futures::{StreamExt, TryStreamExt, stream};
use tokio::time::Instant;

use restate_core::network::NetworkSender;
use restate_core::network::transport_connector::test_util::MockConnector;
use restate_core::partitions::PartitionRouting;
use restate_core::test_env::{TestCoreEnv, TestCoreEnvBuilder, create_mock_nodes_config};
use restate_core::{Metadata, TaskCenter};
use restate_memory::ByteCount;
use restate_metadata_store::MetadataStoreClient;
use restate_partition_store::state_table::{ScopedStateBinding, StateBinding};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_platform::sync::Mutex;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::state_table::WriteStateTable;
use restate_storage_query_api::{
    QueryEngine, QueryMetadata, QueryOptions, QuerySession, SessionOptions, ShardingScheme,
};
use restate_types::identifiers::{
    BaseEntryId, LeaderEpoch, PartitionId, ServiceId, WithPartitionKey,
};
use restate_types::live::Live;
use restate_types::nodes_config::Role;
use restate_types::partition_table::{Partition, PartitionTable};
use restate_types::partitions::state::{LeadershipState, PartitionReplicaSetStates};
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind};
use restate_types::{GenerationalNodeId, Version};
use restate_util_string::ReString;

use crate::QuerySource;
use crate::access::PrimaryRead;
use crate::context::{DataFusionQueryEngine, SelectPartitions};
use crate::local_scanners::register_metadata_scanners;
use crate::mocks::MockSchemas;
use crate::node_fan_out::{AllNodeLocator, NodeFanOutTableProvider, RoleBasedNodeLocator};
use crate::placement::{PartitionPlacement, StoragePlacementOptions};
use crate::query_correctness::run_distributed_state_corpus;
use crate::schema_scan::LookupScanner;
use crate::source_registry::{
    PartitionLocation, PartitionLocator, SourceRegistry, create_partition_locator,
};
use crate::table_providers::{PreparedPartitionRead, Scan, ScanPartition};
use crate::tables::service::schema::SysServiceTable;
use crate::tables::state::{LegacyStateSource, ScopedStateSource, StateTable};
use crate::tables::vqueue_entry_status::schema::SysVqueueEntryStatusTable;

use super::DistributedQueryServer;
use super::tests::environment;
use super::worker::TaskObservation;

#[path = "analyze_tests.rs"]
mod analyze;
#[path = "index_access_tests.rs"]
mod index_access;
#[path = "invocation_tests.rs"]
mod invocation;
#[path = "primary_access_tests.rs"]
mod primary_access;
#[path = "scheduler_tests.rs"]
mod scheduler;
#[path = "shard_join_tests.rs"]
mod shard_join;
#[path = "singleton_tests.rs"]
mod singleton;
#[path = "stats_tests.rs"]
mod stats;

/// The first key of `partition` in the fixtures' partition table.
fn partition_start(partition: PartitionId) -> restate_types::identifiers::PartitionKey {
    PartitionTable::with_equally_sized_partitions(Version::MIN, 8)
        .get(&partition)
        .unwrap()
        .key_range
        .start()
}

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
    partitions: Vec<(GenerationalNodeId, PartitionId)>,
    accesses: Vec<(PartitionId, PrimaryRead)>,
    rows: usize,
    /// Every batch a reader returned: `(columns, rows, batch target)`.
    batches: Vec<(usize, usize, usize)>,
}

#[derive(Debug)]
struct CheckedScanner {
    node: GenerationalNodeId,
    check_owner: bool,
    inner: Arc<dyn ScanPartition>,
    reads: Arc<Mutex<Reads>>,
}

impl ScanPartition for CheckedScanner {
    fn requires_leader(&self) -> bool {
        self.inner.requires_leader()
    }

    fn supports_primary_lookup(&self) -> bool {
        self.inner.supports_primary_lookup()
    }

    fn supports_index(&self, index: &str) -> bool {
        self.inner.supports_index(index)
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        let prepared =
            Arc::clone(&self.inner).prepare_read(access.clone(), projection, predicate, limit)?;
        Ok(Arc::new(CheckedRead {
            scanner: self,
            prepared,
            access,
            limit,
        }))
    }
}

impl CheckedScanner {
    fn observe(
        &self,
        partition: PartitionId,
        access: PrimaryRead,
        batch_size: usize,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        if self.check_owner {
            assert_eq!(
                owner(partition),
                self.node,
                "scanner accessed another owner's storage"
            );
        }
        self.reads.lock().partitions.push((self.node, partition));
        self.reads.lock().accesses.push((partition, access));
        let reads = Arc::clone(&self.reads);
        Box::pin(RecordBatchStreamAdapter::new(
            stream.schema(),
            stream.inspect(move |batch| {
                if let Ok(batch) = batch {
                    let mut reads = reads.lock();
                    reads.rows += batch.num_rows();
                    reads
                        .batches
                        .push((batch.num_columns(), batch.num_rows(), batch_size));
                }
            }),
        ))
    }
}

#[derive(Debug)]
struct CheckedRead {
    scanner: Arc<CheckedScanner>,
    prepared: Arc<dyn PreparedPartitionRead>,
    access: PrimaryRead,
    limit: Option<usize>,
}

impl PreparedPartitionRead for CheckedRead {
    fn capture_requirements(&self) -> restate_worker_api::PartitionCaptureRequirements {
        self.prepared.capture_requirements()
    }

    fn execute(
        &self,
        context: crate::table_providers::PartitionReadContext,
        batch_size: usize,
        metrics: crate::table_providers::ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.check_batch_size(batch_size);
        let partition = context.partition_id();
        let stream = self.prepared.execute(context, batch_size, metrics)?;
        Ok(self
            .scanner
            .observe(partition, self.access.clone(), batch_size, stream))
    }

    fn execute_in_order(
        &self,
        context: crate::table_providers::PartitionReadContext,
        batch_size: usize,
        metrics: crate::table_providers::ReadMetrics,
    ) -> anyhow::Result<(SendableRecordBatchStream, crate::table_providers::InOrder)> {
        self.check_batch_size(batch_size);
        let partition = context.partition_id();
        let (stream, in_order) = self
            .prepared
            .execute_in_order(context, batch_size, metrics)?;
        let stream = self
            .scanner
            .observe(partition, self.access.clone(), batch_size, stream);
        Ok((stream, in_order))
    }

    fn usable(
        &self,
        context: &crate::table_providers::PartitionReadContext,
    ) -> anyhow::Result<bool> {
        self.prepared.usable(context)
    }
}

impl CheckedRead {
    fn check_batch_size(&self, batch_size: usize) {
        assert!(
            batch_size > 0 && self.limit.is_none_or(|limit| batch_size <= limit.max(1)),
            "source_fetch_bounds_the_reader_batch_target"
        );
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

restate_storage_query_api::define_schema! {
    node_rows_schema {
        name: "test_node_rows",
        sharding: ShardingScheme::ByNode(None),
        sharding_columns { plain_node_id: PlainNodeId, gen_node_id: GenerationalNodeId },
        columns {
            plain_node_id: ReString => DataType::Utf8,
            gen_node_id: ReString => DataType::Utf8,
        }
    }
}

impl QuerySource for NodeRows {
    type Schema = NodeRowsSchema;
}

impl NodeRows {
    fn schema() -> SchemaRef {
        NodeRowsSchema::arrow_schema()
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
    scanners: SourceRegistry,
    partitions: Partitions,
    stores: Vec<PartitionStore>,
    store_manager: Arc<PartitionStoreManager>,
    tasks: Arc<Mutex<Vec<TaskObservation>>>,
    /// The sources of the remote workers, whose servers start on first connection.
    _workers: Arc<OnceLock<HashMap<GenerationalNodeId, SourceRegistry>>>,
    reads: Arc<Mutex<Reads>>,
    node_reads: Arc<Mutex<Vec<GenerationalNodeId>>>,
    placements: PlacementCalls,
    states: PartitionReplicaSetStates,
}

async fn setup() -> MultiFixture<impl NetworkSender> {
    setup_with_scanners(|_, _| {}).await
}

async fn setup_with_scanners(
    register: impl Fn(GenerationalNodeId, &SourceRegistry),
) -> MultiFixture<impl NetworkSender> {
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
    let workers = Arc::new(OnceLock::<HashMap<GenerationalNodeId, SourceRegistry>>::new());
    let tasks = Arc::new(Mutex::new(vec![]));
    // Queries keep the connector through their network, possibly past teardown
    // (see `keep_network_past_teardown`), so it must not own storage: it only
    // borrows the workers' sources and observations. The fixture owns them, and
    // so does each server it starts.
    let (connector, _) = MockConnector::new({
        let workers = Arc::downgrade(&workers);
        let tasks = Arc::downgrade(&tasks);
        move |node, router: &mut restate_core::network::MessageRouterBuilder| {
            // A node first reached once the test has returned serves nothing.
            let (Some(workers), Some(tasks)) = (workers.upgrade(), tasks.upgrade()) else {
                return;
            };
            let mut server = DistributedQueryServer::new(
                environment(1, 128),
                workers.get().unwrap()[&node].clone(),
                router,
            );
            server.observations = tasks;
            crate::mocks::spawn_query_server("multi-owner-query", server);
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
        } else if id == 2 {
            node.roles |= Role::LogServer;
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
        let scanners = SourceRegistry::new(
            Arc::new(ObservedLocator {
                node,
                inner: locator,
                calls: Arc::clone(&placements),
            }),
            metadata,
            TaskCenter::current().cluster_state().clone(),
        );
        scanners.register_offline_reads(Arc::clone(&manager), crate::mocks::query_io());
        register(node, &scanners);
        stats::register_scanners(&scanners, node, &reads);
        scanners.register_partition_scanner::<LegacyStateSource>(Arc::new(CheckedScanner {
            node,
            check_owner: true,
            inner: Arc::new(LookupScanner::<StateBinding>::new()),
            reads: Arc::clone(&reads),
        }));
        scanners.register_partition_scanner::<ScopedStateSource>(Arc::new(CheckedScanner {
            node,
            check_owner: true,
            inner: Arc::new(LookupScanner::<ScopedStateBinding>::new()),
            reads: Arc::clone(&reads),
        }));
        scanners.register_partition_scanner::<SysVqueueEntryStatusTable>(Arc::new(
            CheckedScanner {
                node,
                check_owner: true,
                inner: Arc::new(SysVqueueEntryStatusTable::create_local_scanner()),
                reads: Arc::clone(&reads),
            },
        ));
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
    crate::mocks::spawn_query_server("local-query-worker", server);
    let coordinator = coordinator.build().await;
    crate::mocks::keep_network_past_teardown(coordinator.networking.clone());
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
        SourceRegistry::local_only(unavailable.metadata),
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
        _workers: workers,
        reads,
        node_reads,
        placements,
        states,
    }
}

impl<N: NetworkSender> MultiFixture<N> {
    fn engine(&self, parallelism: usize) -> Arc<QueryEngine> {
        self.engine_with_pushdown(parallelism, 2, true)
    }

    fn engine_with_pushdown(
        &self,
        parallelism: usize,
        batch_size: usize,
        pushdown: bool,
    ) -> Arc<QueryEngine> {
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
        env.register_table::<StateTable>(
            "public",
            "state",
            StateTable::create_provider(self.partitions.clone(), &self.scanners),
        )
        .unwrap();
        env.register_table::<NodeRows>(
            "public",
            "node_rows",
            Arc::new(NodeFanOutTableProvider::new(
                NodeRows::schema(),
                Arc::new(RoleBasedNodeLocator::new(
                    Role::Worker,
                    self.metadata.clone(),
                )),
                None,
                NodeRows::identity(),
            )),
        )
        .unwrap();
        env.register_provider(
            "all_node_rows".into(),
            "restate.public.all_node_rows".into(),
            Arc::new(NodeFanOutTableProvider::new(
                NodeRows::schema(),
                Arc::new(AllNodeLocator::new(self.metadata.clone())),
                None,
                NodeRows::identity(),
            )),
        )
        .unwrap();
        Arc::new(DataFusionQueryEngine::from_env(env, None))
    }
}

async fn assert_sql(
    session: &dyn QuerySession,
    invariant: &str,
    sql: &str,
    expected: &[&[&str]],
) -> QueryMetadata {
    let (metadata, batches) = tokio::time::timeout(Duration::from_secs(10), async {
        let result = session.execute(sql, QueryOptions {}).await.unwrap();
        let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
        assert!(
            result.diagnostics.warnings().is_empty(),
            "{invariant}: {sql}"
        );
        (result.metadata, batches)
    })
    .await
    .expect("owner tasks must terminate");
    let mut actual: Vec<_> = batches
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
    actual.sort();
    let mut expected: Vec<Vec<_>> = expected
        .iter()
        .map(|row| row.iter().map(|cell| (*cell).to_owned()).collect())
        .collect();
    expected.sort();
    assert_eq!(actual, expected, "{invariant}: {sql}");
    metadata
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
        for task in &count_tasks {
            let partitions = fixture
                .partitions
                .0
                .iter()
                .filter(|(partition, _)| owner(*partition) == task.owner)
                .count();
            for source in ["state.legacy", "state.scoped"] {
                assert_eq!(
                    task.plan
                        .matches(&format!("TableScanExec: table={source},"))
                        .count(),
                    1,
                    "each_physical_source_has_one_template_per_owner: {}",
                    task.plan
                );
                let line = task
                    .plan
                    .lines()
                    .find(|line| line.contains(&format!("TableScanExec: table={source},")))
                    .unwrap();
                assert!(
                    line.contains(&format!("reads={partitions},")),
                    "each_physical_source_keeps_every_selected_partition: {line}"
                );
            }
            assert!(task.plan.contains("UnionExec"), "{}", task.plan);
        }
        assert!(
            count_tasks
                .iter()
                .map(|task| task.output.lock().bytes)
                .sum::<usize>()
                > 0
        );
        let scoped_tasks: Vec<_> = tasks
            .iter()
            .filter(|task| {
                task.id.query_ts == metadata[2].query_ts
                    && task.id.session_id == metadata[2].session_id
            })
            .collect();
        assert!(!scoped_tasks.is_empty());
        assert!(
            scoped_tasks
                .iter()
                .all(|task| !task.plan.contains("table=state.legacy")),
            "non_null_scope_excludes_the_legacy_physical_source"
        );
        // The query names complete state keys, so the scoped layout looks them up.
        assert_eq!(
            scoped_tasks
                .iter()
                .map(|task| task
                    .plan
                    .matches("MultiGetExec: table=state.scoped,")
                    .count())
                .sum::<usize>(),
            1,
            "scope_equality_targets_one_partition_and_one_eligible_layout"
        );
    }
    let engine = fixture.engine(1);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    fixture.reads.lock().partitions.clear();
    let result = session
        .execute("SELECT COUNT(*) FROM state", QueryOptions {})
        .await
        .unwrap();
    result.stream.try_collect::<Vec<_>>().await.unwrap();
    let mut partitions: Vec<_> = fixture
        .reads
        .lock()
        .partitions
        .iter()
        .map(|(_, p)| *p)
        .collect();
    partitions.sort_unstable();
    let expected: Vec<_> = fixture
        .partitions
        .0
        .iter()
        .flat_map(|(p, _)| [*p, *p])
        .collect();
    assert_eq!(
        partitions, expected,
        "each_physical_layout_scans_each_partition_once"
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
                first.key_range.start() + 2,
                second.key_range.end()
            ),
            vec![*first_id, *second_id],
        ),
        (
            format!(
                "partition_key IN ({}, {}, {})",
                first.key_range.start(),
                second.key_range.end(),
                second.key_range.end()
            ),
            vec![*first_id, *second_id],
        ),
        (
            format!(
                "partition_key >= {} AND partition_key < {}",
                first.key_range.end(),
                second.key_range.end()
            ),
            vec![*first_id, *second_id],
        ),
        ("partition_key = 1 AND partition_key = 2".to_owned(), vec![]),
    ] {
        fixture.reads.lock().partitions.clear();
        let before = fixture.tasks.lock().len();
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
            .partitions
            .iter()
            .map(|(_, id)| *id)
            .collect();
        got.sort_unstable();
        let per_source: Vec<_> = expected.iter().flat_map(|id| [*id, *id]).collect();
        assert_eq!(got, per_source, "{predicate}");
        assert_eq!(
            fixture.tasks.lock()[before..]
                .iter()
                .map(|task| task.plan.matches("TableScanExec:").count())
                .sum::<usize>(),
            per_source.len(),
            "disjoint_ranges_share_one_partition_read_per_physical_source: {predicate}"
        );
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

    // Unsharded metadata routes to this node when it serves the source, even under
    // distributed planning.
    register_metadata_scanners(
        Live::from_value(MockSchemas::default()),
        MetadataStoreClient::new_in_memory(),
        None,
        &fixture.scanners,
    );
    let env = environment(2, 128).with_distributed_execution(fixture.network.clone());
    env.register_tables(crate::MetadataTables::new(fixture.scanners.clone()))
        .await
        .unwrap();
    let metadata = DataFusionQueryEngine::from_env(env, None)
        .create_session(SessionOptions::default())
        .unwrap();
    let before = fixture.tasks.lock().len();
    let batches = metadata
        .execute("SELECT COUNT(*) FROM sys_service", QueryOptions {})
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), "0");
    assert_eq!(
        fixture.tasks.lock().len(),
        before,
        "metadata reads need no storage tasks"
    );

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
async fn source_estimates_choose_metadata_as_join_build_side() {
    let fixture = setup().await;
    let env = environment(48, 128).with_distributed_execution(fixture.network.clone());
    let ctx = SessionContext::new_with_state(env.build_session_state().unwrap());
    ctx.register_table(
        "sys_vqueue_entry_status",
        SysVqueueEntryStatusTable::create_provider(fixture.partitions.clone(), &fixture.scanners),
    )
    .unwrap();
    register_metadata_scanners(
        Live::from_value(MockSchemas::default()),
        MetadataStoreClient::new_in_memory(),
        None,
        &fixture.scanners,
    );
    ctx.register_table(
        "sys_service",
        SysServiceTable::create_provider(fixture.scanners.clone()),
    )
    .unwrap();

    // Build on the small metadata table even when SQL puts retained entries on the left.
    let plan = ctx
        .sql(
            "SELECT e.status, s.ty, COUNT(*)
             FROM sys_vqueue_entry_status e
              LEFT JOIN sys_service s ON e.deployment = s.deployment_id
             GROUP BY e.status, s.ty",
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
            assert_eq!(statistics.num_rows, Precision::Inexact(1024));
            let build = displayable(join.left().as_ref()).indent(false).to_string();
            assert!(build.contains("ServiceMetadataScanner"), "{build}");
            assert!(!build.contains("table=sys_vqueue_entry_status,"), "{build}");
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(joins, 1);

    // A requested key may not exist. Keep estimates inexact so COUNT(*) cannot
    // become the number of requested keys; preserve projected id NDV as well.
    let id = BaseEntryId::new(0, EntryId::new(EntryKind::Invocation, [1; 16]));
    let lookup = ctx
        .sql(&format!(
            "SELECT status, entry_id FROM sys_vqueue_entry_status WHERE entry_id = '{id}'"
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
                let id_column = node.schema().index_of("entry_id").unwrap();
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
        "sys_vqueue_entry_status",
        SysVqueueEntryStatusTable::create_provider(fixture.partitions.clone(), &fixture.scanners),
    )
    .unwrap();

    for (sql, expected_stages, expected_lanes, owner_operator) in [
        (
            "SELECT status, transitioned_at FROM sys_vqueue_entry_status WHERE partition_key > 0 ORDER BY transitioned_at DESC",
            3,
            8,
            "SortExec",
        ),
        (
            "SELECT status, transitioned_at FROM sys_vqueue_entry_status WHERE partition_key > 11529215046068469760 ORDER BY transitioned_at DESC",
            3,
            3,
            "SortExec",
        ),
        (
            "SELECT status, transitioned_at FROM sys_vqueue_entry_status WHERE partition_key > 0 ORDER BY transitioned_at DESC LIMIT 10 OFFSET 2",
            3,
            8,
            "SortExec",
        ),
        (
            "SELECT status, COUNT(*) FROM sys_vqueue_entry_status GROUP BY status",
            3,
            8,
            "AggregateExec",
        ),
        (
            "SELECT a.entry_id FROM sys_vqueue_entry_status a JOIN sys_vqueue_entry_status b ON a.sequence_number = b.sequence_number",
            6,
            16,
            "TableScanExec",
        ),
        (
            // Entry IDs are sharding columns: the join runs at each owner, merging
            // both reads in their native order.
            "SELECT a.entry_id FROM sys_vqueue_entry_status a JOIN sys_vqueue_entry_status b ON a.entry_id = b.entry_id",
            3,
            8,
            "SortMergeJoinExec",
        ),
        (
            "SELECT status, COUNT(*) AS n FROM sys_vqueue_entry_status GROUP BY status UNION ALL SELECT status, COUNT(*) AS n FROM sys_vqueue_entry_status GROUP BY status",
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
            assert_ne!(node.name(), "PlanningInput", "{text}");
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
                    return Ok(TreeNodeRecursion::Jump);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            let mut found_operator = false;
            stage.plan.apply(|node| {
                assert_ne!(node.name(), "PlanningInput", "{text}");
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
    // Match the live-node LIMIT reproducer: both state layouts across 24 partitions.
    // Planning must group across the layout unions, not just within each layout.
    let table = PartitionTable::with_equally_sized_partitions(Version::MIN, 24);
    for owner_count in [1, 3] {
        for (id, _) in table.iter() {
            fixture.states.note_observed_leader(
                *id,
                LeadershipState {
                    current_leader: if owner_count == 1 {
                        GenerationalNodeId::new(2, 1)
                    } else {
                        owner(*id)
                    },
                    current_leader_epoch: LeaderEpoch::from(owner_count + 1),
                },
            );
        }
        ctx.register_table(
            "state",
            StateTable::create_provider(
                Partitions(
                    table
                        .iter()
                        .map(|(id, partition)| (*id, partition.clone()))
                        .collect(),
                ),
                &fixture.scanners,
            ),
        )
        .unwrap();
        let plan = ctx
            .sql("SELECT * FROM state LIMIT 10")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let text = displayable(plan.as_ref()).indent(false).to_string();
        eprintln!("state_limit owners={owner_count}\n{text}");
        assert_eq!(
            text.matches("NetworkCoalesceExec").count(),
            owner_count as usize,
            "both_layouts_share_one_stage_per_owner: {text}"
        );
        for source in ["state.legacy", "state.scoped"] {
            let scans: Vec<_> = text
                .lines()
                .filter(|line| line.contains(&format!("table={source},")))
                .collect();
            assert_eq!(
                scans.len(),
                owner_count as usize,
                "each_layout_has_one_template_per_owner: {text}"
            );
            let partitions: usize = scans
                .iter()
                .map(|line| {
                    line.split("reads=")
                        .nth(1)
                        .unwrap()
                        .split(',')
                        .next()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap()
                })
                .sum();
            assert_eq!(partitions, 24, "each_layout_keeps_every_partition: {text}");
            assert!(
                scans.iter().all(|line| line.contains("fetch=10")),
                "safe_limit_reaches_both_layouts: {text}"
            );
        }
        assert!(
            text.matches("GlobalLimitExec").count() <= 1,
            "state_limit_has_no_redundant_branch_caps: {text}"
        );
        ctx.deregister_table("state").unwrap();
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
    session: &dyn QuerySession,
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
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.owner.raw_id())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 2, 3])
    );
    assert_eq!(tasks.len(), 3, "compact_sources_share_one_task_per_owner");
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
        scans: reads.partitions.len(),
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
    assert_eq!(
        result.scans, 16,
        "two_physical_layouts_cover_eight_partitions"
    );
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
                assert_eq!(
                    result.scans, 16,
                    "two_physical_layouts_cover_eight_partitions"
                );
                assert_eq!(result.storage_rows, records.len());
                // Two groups per scheduling lane, capped by the owner's partitions.
                let lanes: usize = [3, 3, 2]
                    .into_iter()
                    .map(|count| count.min(parallelism))
                    .sum();
                assert_eq!(
                    result.transported_rows,
                    if pushdown { 2 * lanes } else { 4096 }
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
        assert_eq!(a.storage_rows, records.len());
        assert_eq!(a.storage_rows, b.storage_rows);
        // Partition reads emit only the rows meeting the scan's predicate, so
        // owners send the 2,048 matches with or without operator pushdown, and
        // read the same.
        assert_eq!(a.transported_rows, 2048);
        assert_eq!(b.transported_rows, 2048);
        eprintln!("m2_filter parallelism={parallelism} control={a:?} pushed={b:?}");
    }
}
