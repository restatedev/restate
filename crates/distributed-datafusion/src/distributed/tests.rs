// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_proto::physical_plan::{DeduplicatingProtoConverter, PhysicalExtensionCodec};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};

use restate_clock::{MockClock, UniqueTimestamp};
use restate_core::network::transport_connector::test_util::MockConnector;
use restate_core::network::{NetworkSender, Swimlane};
use restate_core::test_env::{TestCoreEnv, TestCoreEnvBuilder, create_mock_nodes_config};
use restate_core::{TaskCenter, TaskCenterFutureExt, TaskKind};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_platform::sync::Mutex;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::state_table::WriteStateTable;
use restate_storage_query_api::{
    AdminUser, QueryEngine, QueryEngineTable, QueryOptions, QueryStatus, SessionOptions,
    SessionTable,
};
use restate_types::identifiers::{PartitionId, ServiceId};
use restate_types::net::distributed_query::{QueryTaskId, QueryTaskInstall, QueryTaskRequest};
use restate_types::net::remote_query_scanner::RemoteQueryScannerOpen;
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_types::{GenerationalNodeId, NodeId};

use crate::DataFusionEnv;
use crate::context::DataFusionQueryEngine;
use crate::mocks::MockPartitionSelector;
use crate::query_correctness::run_distributed_state_corpus;
use crate::remote_query_scanner_client::{RemoteScanner, RemoteScannerService};
use crate::remote_query_scanner_manager::{
    PartitionLocation, PartitionLocator, RemoteScannerManager,
};
use crate::state::schema::{StateBuilder, StateTable};
use crate::table_providers::ScanPartition;

use super::DistributedQueryServer;
use super::source::{SourceCodec, SourceExec};
use super::transport::rpc;
use super::worker::TaskObservation;

const OWNER: GenerationalNodeId = GenerationalNodeId::new(2, 1);

#[derive(Debug)]
pub(super) struct NoLegacyScanner;

#[async_trait]
impl RemoteScannerService for NoLegacyScanner {
    async fn open(
        &self,
        _: NodeId,
        _: RemoteQueryScannerOpen,
    ) -> Result<RemoteScanner, DataFusionError> {
        panic!("distributed task query used the legacy scanner transport")
    }
}

struct RemoteOwner;

impl PartitionLocator for RemoteOwner {
    fn get_partition_target_node(
        &self,
        _: PartitionId,
        _: crate::placement::PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Remote {
            node_id: OWNER.into(),
        })
    }
}

struct Fixture<N> {
    network: N,
    scanners: RemoteScannerManager,
    store: PartitionStore,
    manager: Arc<PartitionStoreManager>,
    observations: Arc<Mutex<Vec<TaskObservation>>>,
}

pub(super) fn environment(target_partitions: usize, batch_size: usize) -> DataFusionEnv {
    DataFusionEnv::new(
        64 * 1024 * 1024,
        None,
        Some(target_partitions),
        &HashMap::from([(
            "datafusion.execution.batch_size".into(),
            batch_size.to_string(),
        )]),
    )
    .unwrap()
    .with_mock_clock(MockClock::new())
    .unwrap()
}

async fn setup(fail_after_data: bool) -> Fixture<impl NetworkSender> {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let worker_scanners = Arc::new(OnceLock::<RemoteScannerManager>::new());
    let observations = Arc::new(Mutex::new(vec![]));
    let (connector, _connections) = MockConnector::new({
        let worker_scanners = Arc::clone(&worker_scanners);
        let observations = Arc::clone(&observations);
        move |node, router: &mut restate_core::network::MessageRouterBuilder| {
            assert_eq!(node, OWNER, "task was routed to a non-owner");
            let mut server = DistributedQueryServer::new(
                environment(1, 128),
                worker_scanners.get().unwrap().clone(),
                router,
            );
            server.observations = Arc::clone(&observations);
            // Start the receiver before the connector can dispatch its first RPC.
            let mut task = Box::pin(server.run());
            assert!(task.as_mut().now_or_never().is_none());
            TaskCenter::current()
                .spawn(
                    TaskKind::NetworkMessageHandler,
                    "distributed-query-test",
                    task,
                )
                .unwrap();
        }
    });
    let mut nodes = create_mock_nodes_config(1, 1);
    nodes.upsert_node(
        create_mock_nodes_config(2, 1)
            .find_node_by_id(OWNER)
            .unwrap()
            .clone(),
    );
    let coordinator = TestCoreEnvBuilder::with_transport_connector(connector)
        .set_nodes_config(nodes)
        .build()
        .await;
    // The injected worker metadata belongs to N2, while the loopback transport's
    // task-center/global metadata remains N1. RPC, codecs and Arrow IPC are real.
    let owner = TestCoreEnv::create_with_single_node(2, 1).await;
    let local = RemoteScannerManager::local_only(owner.metadata);
    let scanner: Arc<dyn ScanPartition> =
        Arc::new(StateTable::create_local_scanner(Arc::clone(&manager)));
    local.register_partition_scanner::<StateTable>(if fail_after_data {
        Arc::new(FailAfterData(scanner))
    } else {
        scanner
    });
    worker_scanners.set(local).unwrap();
    let scanners = RemoteScannerManager::new(
        Arc::new(NoLegacyScanner),
        Arc::new(RemoteOwner),
        coordinator.metadata,
    );
    Fixture {
        network: coordinator.networking,
        scanners,
        store,
        manager,
        observations,
    }
}

impl<N: NetworkSender> Fixture<N> {
    fn engine(
        &self,
        target_partitions: usize,
        batch_size: usize,
    ) -> Arc<dyn QueryEngine<AdminUser>> {
        let env = environment(target_partitions, batch_size)
            .with_distributed_execution(self.network.clone());
        env.register_table::<StateTable>(StateTable::create_provider(
            MockPartitionSelector,
            &self.scanners,
        ))
        .unwrap();
        DataFusionQueryEngine::from_inventory(
            env,
            None,
            vec![SessionTable::for_table::<StateTable>("state")],
        )
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_owner_runtime_matches_independent_references() {
    let mut fixture = setup(false).await;
    for (partitions, batch_size) in [(1, 2), (4, 128)] {
        let engine = fixture.engine(partitions, batch_size);
        let metadata =
            run_distributed_state_corpus(std::slice::from_mut(&mut fixture.store), engine.as_ref())
                .await
                .unwrap();
        assert_eq!(metadata.len(), 14);
        assert!(
            metadata
                .windows(2)
                .all(|pair| pair[0].session_id == pair[1].session_id
                    && pair[0].query_ts < pair[1].query_ts)
        );
        let observations = fixture.observations.lock();
        let tasks: Vec<_> = observations
            .iter()
            .filter(|task| task.id.session_id == metadata[0].session_id)
            .collect();
        let mut unique = HashSet::new();
        for task in &tasks {
            assert!(
                unique.insert(task.id.clone()),
                "installed the same task more than once"
            );
            assert!(
                metadata
                    .iter()
                    .any(|query| query.query_ts == task.id.query_ts)
            );
            assert_eq!(task.context.session_id(), task.id.session_id.as_str());
            assert_eq!(
                task.context
                    .session_config()
                    .get_extension::<UniqueTimestamp>()
                    .as_deref(),
                Some(&task.id.query_ts)
            );
            assert_eq!(task.context.session_config().batch_size(), batch_size);
            assert_eq!(
                task.context.session_config().target_partitions(),
                partitions
            );
        }
        // COUNT(*) has one source/one output lane even with one requested task.
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.id.query_ts == metadata[4].query_ts)
                .count(),
            1
        );
        assert_eq!(
            tasks
                .iter()
                .find(|task| task.id.query_ts == metadata[4].query_ts)
                .unwrap()
                .partitions,
            1
        );
        if partitions > 1 {
            assert!(
                tasks.iter().any(|task| task.partitions > 1),
                "fixture must exercise several output lanes from one installed task"
            );
        }
    }
    let session = fixture
        .engine(1, 2)
        .create_session(SessionOptions::default())
        .unwrap();
    let count = async |sql| {
        let result = session.execute(sql, QueryOptions {}).await.unwrap();
        let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
        (
            result.metadata,
            array_value_to_string(batches[0].column(0), 0).unwrap(),
        )
    };
    let (a, b) = tokio::join!(
        count("SELECT COUNT(*) FROM state WHERE scope = 'tenant-a'"),
        count("SELECT COUNT(*) FROM state WHERE scope IS NULL")
    );
    assert_eq!(a.1, "5");
    assert_eq!(b.1, "3");
    assert_eq!(a.0.session_id, b.0.session_id);
    assert_ne!(a.0.query_ts, b.0.query_ts);
    for query in [a.0, b.0] {
        assert_eq!(
            fixture
                .observations
                .lock()
                .iter()
                .filter(|task| task.id.session_id == query.session_id
                    && task.id.query_ts == query.query_ts)
                .count(),
            1
        );
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn explain_shows_distributed_stages_and_analyze_executes_them() {
    let fixture = setup(false).await;
    let session = fixture
        .engine(1, 2)
        .create_session(SessionOptions::default())
        .unwrap();
    for analyze in [false, true] {
        let sql = if analyze {
            "EXPLAIN ANALYZE VERBOSE SELECT COUNT(*) FROM state"
        } else {
            "EXPLAIN VERBOSE SELECT COUNT(*) FROM state"
        };
        let result = session.execute(sql, QueryOptions {}).await.unwrap();
        let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
        let text = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(|row| array_value_to_string(batch.column(1), row).unwrap())
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("DistributedExec"), "{text}");
        assert!(text.contains("NetworkCoalesceExec"), "{text}");
        assert!(text.contains("StorageScanExec"), "{text}");
        assert!(text.contains("owner=N2:1"), "{text}");
        assert_eq!(
            fixture.observations.lock().len(),
            usize::from(analyze),
            "EXPLAIN must not install tasks; ANALYZE must execute remotely"
        );
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_protocol_and_required_storage_fail_closed() {
    let fixture = setup(false).await;
    let connection = fixture
        .network
        .get_connection(OWNER, Swimlane::Datafusion)
        .in_tc_as_task(
            &TaskCenter::current(),
            TaskKind::InPlace,
            "query-test-connect",
        )
        .await
        .unwrap();
    let invalid = QueryTaskInstall {
        version: restate_types::net::distributed_query::DISTRIBUTED_QUERY_PROTOCOL_VERSION + 1,
        id: QueryTaskId {
            session_id: "unsupported".into(),
            query_ts: UniqueTimestamp::MIN,
            runtime_query_id: [1; 16],
            stage: 1,
            task: 0,
        },
        plan: Default::default(),
        options: Default::default(),
        runtime_headers: Default::default(),
        query_start_time_ns: 0,
    };
    let error = rpc(&connection, QueryTaskRequest::Install(invalid))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported query task protocol version")
    );
    assert!(fixture.observations.lock().is_empty());

    let plan = SourceExec::for_scan(
        StateTable::identity(),
        &fixture.scanners,
        vec![(PartitionId::MIN, KeyRange::FULL)],
        Default::default(),
        1,
        StateBuilder::schema(),
        Arc::new(Statistics::new_unknown(&StateBuilder::schema())),
        vec![],
        None,
        None,
    )
    .unwrap();
    let context = environment(1, 2).build_session_state().unwrap().task_ctx();
    assert!(
        plan.execute(0, Arc::clone(&context)).is_err(),
        "unbound sources cannot execute on the coordinator"
    );
    let mut encoded = Vec::new();
    SourceCodec::default()
        .try_encode(plan, &mut encoded, &DeduplicatingProtoConverter::default())
        .unwrap();
    let wrong_owner = SourceCodec {
        manager: Some(fixture.scanners.clone()),
    };
    let error = wrong_owner
        .try_decode(
            &encoded,
            &[],
            &context,
            &DeduplicatingProtoConverter::default(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("wrong owner"));

    // The selected partition is required data: losing it cannot become an empty success.
    fixture.manager.close(PartitionId::MIN).await;
    let result = fixture
        .engine(1, 2)
        .create_session(SessionOptions::default())
        .unwrap()
        .execute("SELECT COUNT(*) FROM state", QueryOptions {})
        .await
        .unwrap();
    let error = result.stream.try_collect::<Vec<_>>().await.unwrap_err();
    assert!(
        error.to_string().contains("doesn't exist on this node"),
        "{error}"
    );
}

#[derive(Debug)]
struct FailAfterData(Arc<dyn ScanPartition>);

impl ScanPartition for FailAfterData {
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let input = self.0.scan_partition(
            partition_id,
            range,
            Arc::clone(&projection),
            predicate,
            batch_size,
            limit,
            elapsed_compute,
        )?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projection,
            input.chain(stream::once(async {
                Err(DataFusionError::Execution(
                    "injected late storage failure".into(),
                ))
            })),
        )))
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_transport_preserves_rows_before_terminal_failure() {
    let mut fixture = setup(true).await;
    let mut transaction = fixture.store.transaction();
    for key in ["a", "b", "c"] {
        transaction
            .put_user_state(
                &ServiceId::new(None, "service", "object"),
                &Bytes::copy_from_slice(key.as_bytes()),
                Bytes::from_static(b"value"),
            )
            .unwrap();
    }
    transaction.commit().await.unwrap();
    drop(transaction);
    let result = fixture
        .engine(1, 1)
        .create_session(SessionOptions::default())
        .unwrap()
        .execute("SELECT * FROM state", QueryOptions {})
        .await
        .unwrap();
    let diagnostics = result.diagnostics;
    let mut stream = result.stream;
    let mut rows = 0;
    loop {
        match stream
            .next()
            .await
            .expect("terminal error must be delivered")
        {
            Ok(batch) => rows += batch.num_rows(),
            Err(error) => {
                assert!(
                    error.to_string().contains("injected late storage failure"),
                    "{error}"
                );
                break;
            }
        }
    }
    assert_eq!(rows, 3);
    assert_eq!(diagnostics.snapshot().status, QueryStatus::Failed);
    assert_eq!(diagnostics.snapshot().output_rows, 3);
}
