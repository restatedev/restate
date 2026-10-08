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
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{FutureExt, StreamExt, TryStreamExt, stream};

use restate_core::network::NetworkSender;
use restate_core::network::transport_connector::test_util::MockConnector;
use restate_core::partitions::PartitionRouting;
use restate_core::test_env::{TestCoreEnv, TestCoreEnvBuilder, create_mock_nodes_config};
use restate_core::{Metadata, TaskCenter, TaskKind};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_platform::sync::Mutex;
use restate_rocksdb::RocksDbManager;
use restate_storage_query_api::{
    AdminUser, QueryEngine, QueryEngineTable, QueryOptions, SessionOptions, SessionTable,
};
use restate_types::identifiers::{LeaderEpoch, PartitionId};
use restate_types::nodes_config::Role;
use restate_types::partition_table::{Partition, PartitionTable};
use restate_types::partitions::state::{LeadershipState, PartitionReplicaSetStates};
use restate_types::sharding::KeyRange;
use restate_types::{GenerationalNodeId, Version};
use restate_util_string::ReString;

use crate::context::{DataFusionQueryEngine, SelectPartitions};
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
    rows: usize,
}

#[derive(Debug)]
struct CheckedScanner {
    node: GenerationalNodeId,
    inner: Arc<dyn ScanPartition>,
    reads: Arc<Mutex<Reads>>,
}

impl ScanPartition for CheckedScanner {
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
            "scanner accessed another owner's storage"
        );
        self.reads.lock().ranges.push((self.node, partition, range));
        let stream = self.inner.scan_partition(
            partition,
            range,
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
            inner: Arc::new(StateTable::create_local_scanner(Arc::clone(&manager))),
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
        tasks,
        reads,
        node_reads,
        placements,
        states,
    }
}

impl<N: NetworkSender> MultiFixture<N> {
    fn engine(&self, parallelism: usize) -> Arc<dyn QueryEngine<AdminUser>> {
        let env = environment(parallelism, 2)
            .with_storage_placement(StoragePlacementOptions {
                require_leader: true,
            })
            .with_distributed_execution(self.network.clone());
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
    for parallelism in [1, 2, 4] {
        let engine = fixture.engine(parallelism);
        let metadata = run_distributed_state_corpus(&mut fixture.stores, engine.as_ref()).await;
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
                .all(|task| task.plan.contains("StorageScanExec"))
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
