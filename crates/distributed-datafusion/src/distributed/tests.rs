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
use std::time::Duration;

use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_proto::physical_plan::{DeduplicatingProtoConverter, PhysicalExtensionCodec};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};

use restate_clock::{MockClock, UniqueTimestamp};
use restate_core::network::transport_connector::test_util::MockConnector;
use restate_core::network::{BackPressureMode, NetworkSender, ServiceMessage, Swimlane};
use restate_core::test_env::{TestCoreEnv, TestCoreEnvBuilder, create_mock_nodes_config};
use restate_core::{TaskCenter, TaskCenterFutureExt, TaskKind};
use restate_limiter::{RuleBook, rule_book::RuleBookObserver};
use restate_partition_store::state_table::{ScopedStateBinding, StateBinding};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_platform::sync::Mutex;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::state_table::WriteStateTable;
use restate_storage_query_api::tables::state::StateBatchBuilder as StateBuilder;
use restate_storage_query_api::{QueryEngine, QueryOptions, QueryStatus, SessionOptions};
use restate_types::GenerationalNodeId;
use restate_types::identifiers::{PartitionId, ServiceId};
use restate_types::net::distributed_query::{
    PartitionReadError, QueryErrorCode, QueryFailureKind, QueryTaskClose, QueryTaskError,
    QueryTaskId, QueryTaskInstall, QueryTaskReply, QueryTaskReplyBody, QueryTaskRequest,
};
use restate_types::net::node::{GetNodeState, GossipService, NodeStateResponse};
use restate_types::net::{CURRENT_PROTOCOL_VERSION, ProtocolVersion};
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_worker_api::invoker::InvocationStatusReport;
use restate_worker_api::{
    OfflinePartitionQueryAccess, PartitionQueryAccess, PartitionQueryStream,
    ProcessorsManagerCommand, ProcessorsManagerHandle, SchedulerStatusEntry, UserLimitCounterEntry,
};

use crate::access::PrimaryRead;
use crate::context::DataFusionQueryEngine;
use crate::mocks::MockPartitionSelector;
use crate::query_correctness::run_distributed_state_corpus;
use crate::schema_scan::LookupScanner;
use crate::source_registry::{PartitionLocation, PartitionLocator, SourceRegistry};
use crate::table_providers::{PreparedPartitionRead, ReadMetrics, ScanPartition};
use crate::tables::state::{LegacyStateSource, ScopedStateSource, StateTable};
use crate::{DataFusionEnv, QuerySource};

use super::DistributedQueryServer;
use super::source::{SourceCodec, plan_partition_access};
use super::transport::rpc;
use super::worker::TaskObservation;

const OWNER: GenerationalNodeId = GenerationalNodeId::new(2, 1);

pub(super) struct UnusedRuleBook;

impl RuleBookObserver for UnusedRuleBook {
    fn notify_observed(&self, _: RuleBook) {
        unreachable!("query admission does not interpret metadata")
    }
    fn get(&self) -> Arc<RuleBook> {
        unreachable!("query admission does not interpret metadata")
    }
}

/// The invoker rows test leaders report: `rows` gives each partition's. The
/// manager's handle reads them after the processor admits a capture.
pub(super) struct TestInvokers(
    pub(super) Box<dyn Fn(PartitionId) -> Vec<InvocationStatusReport> + Send + Sync>,
);

impl TestInvokers {
    pub(super) fn empty() -> Arc<Self> {
        Arc::new(Self(Box::new(|_| Vec::new())))
    }
}

impl PartitionQueryAccess for TestInvokers {
    fn scan_invoker_status(
        &self,
        partition_id: PartitionId,
        _: KeyRange,
    ) -> PartitionQueryStream<InvocationStatusReport> {
        stream::iter((self.0)(partition_id).into_iter().map(Ok)).boxed()
    }

    fn scan_scheduler_status(
        &self,
        _: PartitionId,
        _: KeyRange,
    ) -> PartitionQueryStream<SchedulerStatusEntry> {
        unreachable!("the processor captures the scheduler")
    }

    fn scan_user_limit_counters(
        &self,
        _: PartitionId,
        _: KeyRange,
    ) -> PartitionQueryStream<UserLimitCounterEntry> {
        unreachable!("the processor captures the user limits")
    }
}

struct RemoteOwner;

impl PartitionLocator for RemoteOwner {
    fn get_partition_target_node(
        &self,
        _: PartitionId,
        _: crate::placement::PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Remote { node_id: OWNER })
    }
}

struct Fixture<N> {
    network: N,
    scanners: SourceRegistry,
    owner_scanners: SourceRegistry,
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
    setup_at_version(fail_after_data, CURRENT_PROTOCOL_VERSION).await
}

async fn setup_at_version(
    fail_after_data: bool,
    protocol_version: ProtocolVersion,
) -> Fixture<impl NetworkSender> {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let worker_scanners = Arc::new(OnceLock::<SourceRegistry>::new());
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
            // Like production, shutdown waits for the server to drain, releasing its
            // storage owners before teardown checks run.
            TaskCenter::current()
                .spawn(TaskKind::SystemService, "distributed-query-test", task)
                .unwrap();
            // An ordinary node service next to the query service, so tests can
            // check that query failures leave the connection usable for it.
            let mut gossip = router
                .register_service::<GossipService>(BackPressureMode::Lossy)
                .start();
            TaskCenter::current()
                .spawn(
                    TaskKind::NetworkMessageHandler,
                    "distributed-query-test-gossip",
                    async move {
                        while let Some(message) = gossip.next().await {
                            let ServiceMessage::Rpc(message) = message else {
                                continue;
                            };
                            let (reply, GetNodeState {}) =
                                message.into_typed::<GetNodeState>().split();
                            reply.send(NodeStateResponse {
                                partition_processor_state: None,
                                uptime: Duration::from_secs(7),
                            });
                        }
                        Ok(())
                    },
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
    let coordinator = TestCoreEnvBuilder::with_transport_connector(
        connector.with_max_protocol_version(protocol_version),
    )
    .set_nodes_config(nodes)
    .build()
    .await;
    // The injected worker metadata belongs to N2, while the loopback transport's
    // task-center/global metadata remains N1. RPC, codecs and Arrow IPC are real.
    let owner = TestCoreEnv::create_with_single_node(2, 1).await;
    let local = SourceRegistry::local_only(owner.metadata);
    local.register_offline_reads(Arc::clone(&manager), crate::mocks::query_io());
    let scanner: Arc<dyn ScanPartition> = Arc::new(LookupScanner::<StateBinding>::new());
    local.register_partition_scanner::<LegacyStateSource>(if fail_after_data {
        Arc::new(FailAfterData(scanner))
    } else {
        scanner
    });
    local.register_partition_scanner::<ScopedStateSource>(Arc::new(LookupScanner::<
        ScopedStateBinding,
    >::new()));
    worker_scanners.set(local.clone()).unwrap();
    let scanners = SourceRegistry::new(
        Arc::new(RemoteOwner),
        coordinator.metadata,
        TaskCenter::current().cluster_state().clone(),
    );
    Fixture {
        network: coordinator.networking,
        scanners,
        owner_scanners: local,
        store,
        manager,
        observations,
    }
}

impl<N: NetworkSender> Fixture<N> {
    fn engine(&self, target_partitions: usize, batch_size: usize) -> Arc<QueryEngine> {
        let env = environment(target_partitions, batch_size)
            .with_distributed_execution(self.network.clone());
        env.register_table::<StateTable>(
            "public",
            "state",
            StateTable::create_provider(MockPartitionSelector, &self.scanners),
        )
        .unwrap();
        Arc::new(DataFusionQueryEngine::from_env(env, None))
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
        // Both physical layouts share one owner task and one final count lane.
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
    let mut options = SessionOptions::default();
    options
        .headers
        .append("x-query-context", http::HeaderValue::from_static("first"));
    options.headers.append(
        "x-query-context",
        http::HeaderValue::from_bytes(b"\x80second").unwrap(),
    );
    let session = fixture.engine(1, 2).create_session(options).unwrap();
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
        let observations = fixture.observations.lock();
        let task = observations
            .iter()
            .find(|task| {
                task.id.session_id == query.session_id && task.id.query_ts == query.query_ts
            })
            .unwrap();
        let forwarded = task
            .context
            .session_config()
            .get_extension::<restate_storage_query_api::QueryMetadata>()
            .unwrap();
        assert_eq!(forwarded.session_id, query.session_id);
        assert_eq!(forwarded.query_ts, query.query_ts);
        assert_eq!(
            forwarded.headers, query.headers,
            "remote_metadata_preserves_repeated_and_opaque_header_values"
        );
        assert_eq!(forwarded.redacted_sql, query.redacted_sql);
        assert_eq!(forwarded.planning_duration, query.planning_duration);
        assert_eq!(
            observations
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
    for (mode, analyze, verbose) in [
        ("EXPLAIN", false, false),
        ("EXPLAIN VERBOSE", false, true),
        ("EXPLAIN ANALYZE", true, false),
        ("EXPLAIN ANALYZE VERBOSE", true, true),
    ] {
        let sql = format!(
            "{mode} SELECT COUNT(*) FROM state \
             WHERE service_key IN ('a','b','c','d','e','f','g','h','i')"
        );
        let batches = tokio::time::timeout(Duration::from_secs(10), async {
            let result = session.execute(&sql, QueryOptions {}).await.unwrap();
            result.stream.try_collect::<Vec<_>>().await.unwrap()
        })
        .await
        .expect("task installation and execution must not wait for control-channel closure");
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
        assert!(text.contains("TableScanExec"), "{text}");
        assert!(text.contains("owner=N2:1"), "{text}");
        assert!(
            !text.contains("KeyRange("),
            "scan_descriptors_carry_partition_ids: {text}"
        );
        assert!(
            !text.contains("work="),
            "explain_uses_readable_source_summaries: {text}"
        );
        assert!(
            !text.contains("schema=["),
            "verbose_omits_implicit_schema_dumps: {text}"
        );
        assert_eq!(
            text.lines()
                .any(|line| line.contains("TableScanExec:") && line.contains("projection=[")),
            verbose,
            "{text}"
        );
        if !verbose {
            assert!(text.contains("partitions=[0], reads=1"), "{text}");
            assert!(
                text.matches("<set#1: 9 values, 9 distinct").count() >= 2,
                "{text}"
            );
            assert!(!text.contains("<set#2"), "{text}");
        } else {
            assert!(!text.contains("<set#"), "{text}");
            assert!(text.contains("a, b, c, d, e, f, g, h, i"), "{text}");
        }
        let observations = std::mem::take(&mut *fixture.observations.lock());
        assert_eq!(
            observations.len(),
            usize::from(analyze),
            "EXPLAIN must not install tasks; ANALYZE must execute remotely"
        );
        for task in observations {
            assert!(
                !task.plan.contains("<set#"),
                "worker receives executable native predicates"
            );
            assert!(
                task.plan.contains("a, b, c, d, e, f, g, h, i"),
                "{}",
                task.plan
            );
            eprintln!("{mode} native worker plan:\n{}", task.plan);
        }
        eprintln!("{mode} coordinator output:\n{text}");
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn finished_query_tasks_are_removed_from_the_owner() {
    let fixture = setup(false).await;
    let result = fixture
        .engine(1, 2)
        .create_session(SessionOptions::default())
        .unwrap()
        .execute("SELECT COUNT(*) FROM state", QueryOptions {})
        .await
        .unwrap();
    result.stream.try_collect::<Vec<_>>().await.unwrap();
    let installed = std::mem::take(&mut *fixture.observations.lock());
    assert!(
        !installed.is_empty(),
        "the query must install a remote task"
    );
    // Closing a finished task drops it on the owner, so only the observation's
    // reference to the task context remains.
    tokio::time::timeout(Duration::from_secs(10), async {
        while installed
            .iter()
            .any(|task| Arc::strong_count(&task.context) > 1)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("finished query tasks must be removed from the owner");
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn older_fabric_rejects_query_tasks_and_keeps_ordinary_rpc_working() {
    use datafusion_distributed::ChannelResolver;

    use restate_core::network::RpcReplyError;
    use restate_types::net::RpcRequest;
    use restate_types::net::codec::{EncodeError, WireDecode, WireEncode};
    use restate_types::net::distributed_query::DistributedQueryService;

    let fixture = setup_at_version(false, ProtocolVersion::V6).await;
    let resolver = super::transport::RestateChannelResolver::new(fixture.network.clone());
    let result = resolver
        .get_worker_client_for_url(&super::worker_url(OWNER).unwrap())
        .await;
    let Err(error) = result else {
        panic!("an older owner must be rejected before any task is sent")
    };
    assert!(
        error.to_string().contains(&format!(
            "node {OWNER} does not support distributed queries"
        )),
        "{error}"
    );

    let connection = fixture
        .network
        .get_connection(OWNER, Swimlane::Datafusion)
        .in_tc_as_task(
            &TaskCenter::current(),
            TaskKind::InPlace,
            "query-test-v6-connect",
        )
        .await
        .unwrap();
    assert_eq!(connection.protocol_version(), ProtocolVersion::V6);

    // Bypass the sender's codec floor to emulate an older custom binary. The
    // payload is deliberately invalid: the worker must reject before decoding it.
    struct OldTask;
    impl WireEncode for OldTask {
        fn encode_to_bytes(&self, _: ProtocolVersion) -> Result<Bytes, EncodeError> {
            Ok(Bytes::from_static(b"\xff"))
        }
    }
    impl WireDecode for OldTask {
        type Error = anyhow::Error;

        fn try_decode(_: impl bytes::Buf, _: ProtocolVersion) -> anyhow::Result<Self> {
            unreachable!("the test only sends this request")
        }
    }
    impl RpcRequest for OldTask {
        const TYPE: &'static str = QueryTaskRequest::TYPE;
        type Service = DistributedQueryService;
        type Response = QueryTaskReply;
    }
    let error = connection
        .reserve()
        .await
        .unwrap()
        .send_rpc(OldTask, None)
        .unwrap()
        .await
        .unwrap_err();
    assert!(matches!(error, RpcReplyError::MessageUnrecognized));
    assert!(fixture.observations.lock().is_empty());
    assert!(!connection.is_closed());
    let state = connection
        .reserve()
        .await
        .unwrap()
        .send_rpc(GetNodeState::default(), None)
        .unwrap()
        .await
        .unwrap();
    assert_eq!(state.uptime, Duration::from_secs(7));
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_task_and_required_storage_fail_closed() {
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
        id: QueryTaskId {
            session_id: "unsupported".into(),
            query_ts: UniqueTimestamp::MIN,
            runtime_query_id: [1; 16],
            stage: 0,
            task: 0,
        },
        plan: Default::default(),
        options: Default::default(),
        runtime_headers: Default::default(),
        query_start_time_ns: 0,
        query_headers: vec![],
        redacted_sql: Default::default(),
        planning_duration: Default::default(),
    };
    let id = invalid.id.clone();
    let error = rpc(&connection, QueryTaskRequest::Install(invalid))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("invalid owner-bound task identity")
    );
    assert!(fixture.observations.lock().is_empty());
    // An invalid task fails only that query request. The connection
    // stays open and keeps serving both query and ordinary RPCs.
    assert!(!connection.is_closed());
    assert!(matches!(
        rpc(&connection, QueryTaskRequest::Close(QueryTaskClose { id })).await,
        Ok(QueryTaskReplyBody::Closed(()))
    ));
    // This is the Datafusion swimlane connection. Ordinary node traffic usually
    // goes over other swimlanes; sending it here on purpose shows that the
    // failed query request didn't break the connection for other services.
    let state = connection
        .reserve()
        .await
        .expect("connection stays open")
        .send_rpc(GetNodeState::default(), None)
        .unwrap()
        .await
        .unwrap();
    assert_eq!(state.uptime, Duration::from_secs(7));
    // Valid query messages require the negotiated fabric version, independently
    // of task identity and storage validation.
    assert_eq!(connection.protocol_version(), CURRENT_PROTOCOL_VERSION);

    let plan = plan_partition_access(
        StateTable::identity(),
        &fixture.scanners,
        vec![(
            PartitionId::MIN,
            Partition::new(PartitionId::MIN, KeyRange::FULL),
        )],
        Default::default(),
        StateBuilder::schema(),
        Arc::new(Statistics::new_unknown(&StateBuilder::schema())),
        vec![],
        None,
        None,
        crate::access::Access::Scan,
        false,
        2,
    )
    .unwrap();
    let context = environment(1, 2).build_session_state().unwrap().task_ctx();
    assert!(
        plan.execute(0, Arc::clone(&context))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .is_err(),
        "unbound sources cannot execute on the coordinator"
    );
    let mut encoded = Vec::new();
    SourceCodec::default()
        .try_encode(
            Arc::clone(plan.children()[0]),
            &mut encoded,
            &DeduplicatingProtoConverter::default(),
        )
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

fn remote_error(error: &DataFusionError) -> &QueryTaskError {
    let mut cause: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(remote) = cause.downcast_ref::<QueryTaskError>() {
            return remote;
        }
        cause = cause
            .source()
            .expect("structured_error_survives_datafusion_wrappers");
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn processor_admission_errors_reach_the_remote_query_consumer() {
    let fixture = setup(false).await;
    let (commands, mut receiver) = tokio::sync::mpsc::channel(32);
    fixture.owner_scanners.register_processor_reads(
        ProcessorsManagerHandle::new(
            commands,
            Arc::new(UnusedRuleBook),
            Arc::new(OfflinePartitionQueryAccess::new([])),
        ),
        crate::mocks::query_io(),
    );
    let session = fixture
        .engine(1, 2)
        .create_session(SessionOptions::default())
        .unwrap();
    let partition = PartitionId::MIN;
    for reason in [
        PartitionReadError::NotHosted(partition),
        PartitionReadError::Initializing(partition),
        PartitionReadError::Stopping(partition),
        PartitionReadError::Broken {
            partition,
            reason: "ahead of log".into(),
        },
        PartitionReadError::NotLeader {
            partition,
            leader: None,
        },
        PartitionReadError::NotLeader {
            partition,
            leader: Some(GenerationalNodeId::new(3, 4)),
        },
        PartitionReadError::Busy(partition),
    ] {
        let result = session
            .execute("SELECT COUNT(*) FROM state", QueryOptions {})
            .await
            .unwrap();
        let (error, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                async { result.stream.try_collect::<Vec<_>>().await.unwrap_err() },
                async {
                    let ProcessorsManagerCommand::ReadPartition(request) =
                        receiver.recv().await.unwrap()
                    else {
                        panic!("expected admission")
                    };
                    assert_eq!(request.partition, partition);
                    assert_eq!(request.metadata.session_id, result.metadata.session_id);
                    assert_eq!(request.metadata.query_ts, result.metadata.query_ts);
                    request.response.send(Err(reason.clone())).unwrap();
                },
            )
        })
        .await
        .expect("admission_failure_is_terminal_without_waiting_or_retrying");
        assert_eq!(
            error.to_string(),
            format!("External error: {OWNER}: admission: {reason}"),
            "reported_once_with_origin_and_kind"
        );
        let received = remote_error(&error);
        let expected = QueryTaskError {
            origin: Some(OWNER),
            ..reason.to_task_error()
        };
        assert_eq!(
            received, &expected,
            "reason_and_origin_survive_owner_transport"
        );
        assert_eq!(received.partition_read_error().unwrap(), Some(reason));
        assert!(
            receiver.try_recv().is_err(),
            "failed_admission_is_not_retried"
        );
        assert_eq!(result.diagnostics.snapshot().status, QueryStatus::Failed);
    }
}

#[test]
fn task_failure_forwarding_preserves_unknown_and_extended_details() {
    use bilrost::Message;

    use restate_types::net::ProtocolVersion;
    use restate_types::net::codec::{WireDecode, WireEncode};

    #[derive(bilrost::Message)]
    struct FuturePartitionDetails {
        #[bilrost(1)]
        partition: PartitionId,
        #[bilrost(100)]
        future_field: Option<String>,
    }
    let failure = |kind, code, details| QueryTaskError {
        origin: Some(OWNER),
        kind,
        message: "original failure message".into(),
        code,
        details,
    };
    for original in [
        failure(
            QueryFailureKind::STORAGE,
            QueryErrorCode::default(),
            Bytes::new(),
        ),
        failure(
            QueryFailureKind(999),
            QueryErrorCode(12345),
            Bytes::from_static(b"\xffopaque future details"),
        ),
        failure(
            QueryFailureKind::ADMISSION,
            QueryErrorCode::PARTITION_BUSY,
            FuturePartitionDetails {
                partition: PartitionId::MIN,
                future_field: Some("retained".into()),
            }
            .encode_to_vec()
            .into(),
        ),
        failure(
            QueryFailureKind::ADMISSION,
            QueryErrorCode::PARTITION_BUSY,
            Bytes::from_static(b"\xffinvalid known details"),
        ),
    ] {
        // Interpretation is optional, including malformed known details. Forward
        // the original bytes rather than reconstructing a recognized variant.
        let _ = original.partition_read_error();
        let error = DataFusionError::Context(
            "forwarding".into(),
            Box::new(DataFusionError::Shared(Arc::new(
                DataFusionError::External(Box::new(original.clone())),
            ))),
        );
        let reply = super::worker::task_reply(Err(error), GenerationalNodeId::new(5, 6));
        let bytes = WireEncode::encode_to_bytes(&reply, ProtocolVersion::V7).unwrap();
        let forwarded = QueryTaskReply::try_decode(bytes, ProtocolVersion::V7)
            .unwrap()
            .into_result()
            .unwrap_err();
        assert_eq!(
            forwarded, original,
            "forwarding_keeps_kind_message_raw_details_and_original_origin"
        );
    }
}

#[derive(Debug)]
struct FailAfterData(Arc<dyn ScanPartition>);

impl ScanPartition for FailAfterData {
    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        Ok(Arc::new(FailAfterRead(
            Arc::clone(&self.0).prepare_read(access, projection, predicate, limit)?,
        )))
    }
}

#[derive(Debug)]
struct FailAfterRead(Arc<dyn PreparedPartitionRead>);

impl PreparedPartitionRead for FailAfterRead {
    fn capture_requirements(&self) -> restate_worker_api::PartitionCaptureRequirements {
        self.0.capture_requirements()
    }

    fn execute(
        &self,
        context: crate::table_providers::PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let input = self.0.execute(context, batch_size, metrics)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            input.schema(),
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
