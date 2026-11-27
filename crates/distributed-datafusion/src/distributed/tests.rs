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
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_proto::physical_plan::{DeduplicatingProtoConverter, PhysicalExtensionCodec};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};

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
    MetricLevel, MetricRecord, MetricRecordValue, NamedValue, PartitionReadError, QueryErrorCode,
    QueryFailureKind, QueryTaskClose, QueryTaskError, QueryTaskId, QueryTaskInstall,
    QueryTaskReply, QueryTaskReplyBody, QueryTaskReport, QueryTaskRequest, TaskOutcome,
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
use super::worker::{Hold, Tamper, TaskObservation, WorkerEvent};

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
    /// The owner's end of each connection, in the order they were made.
    connections: tokio::sync::mpsc::UnboundedReceiver<restate_core::network::MockPeerConnection>,
    scanners: SourceRegistry,
    /// The owner's sources, whose server starts on first connection.
    owner_scanners: Arc<OnceLock<SourceRegistry>>,
    store: PartitionStore,
    manager: Arc<PartitionStoreManager>,
    observations: Arc<Mutex<Vec<TaskObservation>>>,
    tamper: Arc<Mutex<Tamper>>,
    /// Holds the owner's state reads, when set.
    read_hold: Arc<Mutex<Option<ReadHold>>>,
}

/// A test's hold on the owner's state reads: each read tells `entered` its
/// snapshot's completion, then waits for `release` before its first row.
/// `entered` comes as the read is built, during the task's Execute: a Next
/// waits on the read only once the worker has received it (`next-received`).
#[derive(Clone)]
struct ReadHold {
    entered: tokio::sync::mpsc::UnboundedSender<tokio_util::sync::WaitForCancellationFutureOwned>,
    release: Arc<tokio::sync::Notify>,
}

/// An owner's state scanner that a test can hold ([`ReadHold`]).
#[derive(derive_more::Debug)]
struct Held(
    Arc<dyn ScanPartition>,
    #[debug(skip)] Arc<Mutex<Option<ReadHold>>>,
);

impl ScanPartition for Held {
    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        Ok(Arc::new(HeldRead(
            Arc::clone(&self.0).prepare_read(access, projection, predicate, limit)?,
            Arc::clone(&self.1),
        )))
    }
}

#[derive(derive_more::Debug)]
struct HeldRead(
    Arc<dyn PreparedPartitionRead>,
    #[debug(skip)] Arc<Mutex<Option<ReadHold>>>,
);

impl PreparedPartitionRead for HeldRead {
    fn capture_requirements(&self) -> restate_worker_api::PartitionCaptureRequirements {
        self.0.capture_requirements()
    }

    fn execute(
        &self,
        context: crate::table_providers::PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let hold = self.1.lock().clone();
        let closed = hold
            .as_ref()
            .map(|_| {
                context
                    .storage_read()
                    .map(|(snapshot, _)| snapshot.closed())
            })
            .transpose()?;
        let input = self.0.execute(context, batch_size, metrics)?;
        let Some((hold, closed)) = hold.zip(closed) else {
            return Ok(input);
        };
        let _ = hold.entered.send(closed);
        let release = hold.release;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            input.schema(),
            stream::once(async move {
                release.notified().await;
                None
            })
            .filter_map(std::future::ready)
            .chain(input),
        )))
    }
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
    let owner_scanners = Arc::new(OnceLock::<SourceRegistry>::new());
    let observations = Arc::new(Mutex::new(vec![]));
    let tamper = Arc::new(Mutex::new(Tamper::default()));
    // Queries keep the connector through their network, possibly past teardown
    // (see `keep_network_past_teardown`), so it must not own storage: it only
    // borrows the owner's sources and the observations.
    let (connector, connections) = MockConnector::new({
        let owner_scanners = Arc::downgrade(&owner_scanners);
        let observations = Arc::downgrade(&observations);
        let tamper = Arc::clone(&tamper);
        move |node, router: &mut restate_core::network::MessageRouterBuilder| {
            assert_eq!(node, OWNER, "task was routed to a non-owner");
            // An owner first reached once the test has returned serves nothing.
            let (Some(owner_scanners), Some(observations)) =
                (owner_scanners.upgrade(), observations.upgrade())
            else {
                return;
            };
            let mut server = DistributedQueryServer::new(
                environment(1, 128),
                owner_scanners.get().unwrap().clone(),
                router,
            );
            server.observations = observations;
            server.tamper = Arc::clone(&tamper);
            crate::mocks::spawn_query_server("distributed-query-test", server);
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
    crate::mocks::keep_network_past_teardown(coordinator.networking.clone());
    // The injected worker metadata belongs to N2, while the loopback transport's
    // task-center/global metadata remains N1. RPC, codecs and Arrow IPC are real.
    let owner = TestCoreEnv::create_with_single_node(2, 1).await;
    let local = SourceRegistry::local_only(owner.metadata);
    local.register_offline_reads(Arc::clone(&manager), crate::mocks::query_io());
    let scanner: Arc<dyn ScanPartition> = Arc::new(LookupScanner::<StateBinding>::new());
    let read_hold = Arc::new(Mutex::new(None));
    local.register_partition_scanner::<LegacyStateSource>(Arc::new(Held(
        if fail_after_data {
            Arc::new(FailAfterData(scanner))
        } else {
            scanner
        },
        Arc::clone(&read_hold),
    )));
    local.register_partition_scanner::<ScopedStateSource>(Arc::new(LookupScanner::<
        ScopedStateBinding,
    >::new()));
    owner_scanners.set(local).unwrap();
    let scanners = SourceRegistry::new(
        Arc::new(RemoteOwner),
        coordinator.metadata,
        TaskCenter::current().cluster_state().clone(),
    );
    Fixture {
        network: coordinator.networking,
        connections,
        scanners,
        owner_scanners,
        store,
        manager,
        observations,
        tamper,
        read_hold,
    }
}

async fn owner_event(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<WorkerEvent>,
    kind: &str,
    id: &QueryTaskId,
) -> WorkerEvent {
    let mut seen = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = events.recv().await.expect("worker event channel");
            if event.kind == kind && event.id == *id {
                return event;
            }
            seen.push(event);
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no {kind} for {id:?}: {seen:?}"))
}

async fn query_connection(network: &impl NetworkSender) -> restate_core::network::Connection {
    network
        .get_connection(OWNER, Swimlane::Datafusion)
        .in_tc_as_task(
            &TaskCenter::current(),
            TaskKind::InPlace,
            "owner-loss-connect",
        )
        .await
        .unwrap()
}

/// Aborts the owner's reactor of a connection, without a drain: both ends see
/// the connection close.
async fn break_connection(
    peer: restate_core::network::MockPeerConnection,
    client: &restate_core::network::Connection,
) {
    let reactor = TaskCenter::take_task(peer.reactor_task_id).unwrap();
    reactor.abort();
    assert!(reactor.await.is_err());
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(peer.conn.closed(), client.closed());
    })
    .await
    .expect("both ends close");
}

/// The worker's context of a task, without the test's own reference to it: it
/// is destroyed once the worker releases the task.
fn take_worker_context(
    fixture: &Fixture<impl NetworkSender>,
    id: &QueryTaskId,
) -> std::sync::Weak<datafusion::execution::TaskContext> {
    let mut observations = fixture.observations.lock();
    let at = observations.iter().position(|task| task.id == *id).unwrap();
    Arc::downgrade(&observations.remove(at).context)
}

async fn context_released(context: std::sync::Weak<datafusion::execution::TaskContext>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while context.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the worker releases the task without waiting for it to idle");
}

fn empty_task_install() -> QueryTaskInstall {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::{ExecutionPlan, empty::EmptyExec};
    use datafusion_distributed::MaybeEncoded;

    let context = environment(1, 128)
        .build_session_state()
        .unwrap()
        .task_ctx();
    QueryTaskInstall {
        id: QueryTaskId {
            session_id: context.session_id().into(),
            query_ts: UniqueTimestamp::MIN,
            runtime_query_id: *uuid::Uuid::new_v4().as_bytes(),
            stage: 1,
            task: 0,
        },
        plan: MaybeEncoded::<Arc<dyn ExecutionPlan>>::Decoded(Arc::new(EmptyExec::new(Arc::new(
            Schema::empty(),
        ))))
        .encode(&context)
        .unwrap()
        .into(),
        options: Default::default(),
        runtime_headers: OutputTask::headers()
            .iter()
            .map(|(k, v)| (k.as_str().into(), v.to_str().unwrap().into()))
            .collect(),
        query_start_time_ns: 0,
        query_headers: vec![],
        redacted_sql: Default::default(),
        planning_duration: Default::default(),
        analyze: None,
    }
}

/// A task serving a request never idles, so only its coordinator can end it.
/// When the connection that carried the request closes, nobody can.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_is_released_when_the_connection_of_its_request_closes() {
    use restate_types::net::distributed_query::{QueryTaskExecute, QueryTaskNext};

    let mut fixture = setup(false).await;
    let mut transaction = fixture.store.transaction();
    transaction
        .put_user_state(
            &ServiceId::new(None, "service", "object"),
            &Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    drop(transaction);
    let (events, mut events_rx) = tokio::sync::mpsc::unbounded_channel();
    fixture.tamper.lock().events = Some(events);

    // A Next waiting on a storage read that holds a snapshot.
    let (entered, mut reads) = tokio::sync::mpsc::unbounded_channel();
    *fixture.read_hold.lock() = Some(ReadHold {
        entered,
        release: Arc::new(tokio::sync::Notify::new()),
    });
    let mut stream = fixture
        .engine(1, 1)
        .create_session(SessionOptions::default())
        .unwrap()
        .execute("SELECT * FROM state", QueryOptions {})
        .await
        .unwrap()
        .stream;
    let consumer = tokio::spawn(async move { stream.next().await });
    let snapshot_closed = tokio::time::timeout(Duration::from_secs(5), reads.recv())
        .await
        .unwrap()
        .unwrap();
    let id = fixture.observations.lock()[0].id.clone();
    owner_event(&mut events_rx, "next-polling", &id).await;
    let context = take_worker_context(&fixture, &id);
    let connection = query_connection(&fixture.network).await;
    let peer = fixture.connections.recv().await.unwrap();
    break_connection(peer, &connection).await;
    let next = owner_event(&mut events_rx, "next", &id).await;
    assert_eq!(next.outcome, "owner lost");
    tokio::time::timeout(Duration::from_secs(5), snapshot_closed)
        .await
        .expect("the read's snapshot is dropped");
    context_released(context).await;
    assert!(consumer.await.unwrap().unwrap().is_err());
    *fixture.read_hold.lock() = None;

    // The task is gone for a new connection too, which serves new tasks. An
    // Execute waiting inside the runtime ends the same way.
    let connection = query_connection(&fixture.network).await;
    let peer = fixture.connections.recv().await.unwrap();
    let error = rpc(
        &connection,
        QueryTaskRequest::Next(QueryTaskNext { id, partition: 0 }),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("not installed"), "{error}");
    let install = empty_task_install();
    let id = install.id.clone();
    rpc(&connection, QueryTaskRequest::Install(install))
        .await
        .unwrap();
    let context = take_worker_context(&fixture, &id);
    let (entered, mut executes) = tokio::sync::mpsc::unbounded_channel();
    fixture.tamper.lock().execute_inside_hold = Some(Hold {
        entered,
        release: Arc::new(tokio::sync::Notify::new()),
    });
    let execute = tokio::spawn({
        let (connection, id) = (connection.clone(), id.clone());
        async move {
            rpc(
                &connection,
                QueryTaskRequest::Execute(QueryTaskExecute {
                    id,
                    partition_start: 0,
                    partition_end: 1,
                }),
            )
            .await
        }
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), executes.recv())
            .await
            .unwrap()
            .unwrap(),
        id
    );
    break_connection(peer, &connection).await;
    let executed = owner_event(&mut events_rx, "execute", &id).await;
    assert_eq!(executed.outcome, "owner lost");
    context_released(context).await;
    assert!(execute.await.unwrap().is_err());
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

    /// A Close reply cancels the runtime's control input, but its detached
    /// cleanup task can still own the task context and, through the worker's
    /// source registry, our partition store manager. Wait until only our
    /// observations retain the contexts before tearing storage down.
    async fn wait_for_task_cleanup(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self
                .observations
                .lock()
                .iter()
                .any(|task| Arc::strong_count(&task.context) > 1)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("finished query tasks must be removed from the owner");
    }
}

/// A real worker channel over the fixture's native RPC connection. An EmptyExec
/// supplies the installed output contract; the typed output hook supplies only
/// the unusual lane batches being tested, after the runtime has executed it.
struct OutputTask {
    client: Box<dyn datafusion_distributed::WorkerChannel>,
    context: Arc<datafusion::execution::TaskContext>,
    key: datafusion_distributed::TaskKey,
    _control: futures::stream::BoxStream<
        'static,
        datafusion::common::Result<datafusion_distributed::WorkerToCoordinatorMsg>,
    >,
}

impl OutputTask {
    fn headers() -> http::HeaderMap {
        use datafusion::common::config::{ConfigExtension, ExtensionOptions};
        use datafusion_distributed::DistributedConfig;

        let mut headers = http::HeaderMap::new();
        for entry in super::runtime_config().entries() {
            if let Some(value) = entry.value {
                headers.insert(
                    http::HeaderName::from_bytes(
                        format!(
                            "x-datafusion-distributed-config-{}.{}",
                            DistributedConfig::PREFIX,
                            entry.key
                        )
                        .as_bytes(),
                    )
                    .unwrap(),
                    value.parse().unwrap(),
                );
            }
        }
        headers
    }

    async fn install<N: NetworkSender>(
        fixture: &Fixture<N>,
        schema: SchemaRef,
        partitions: usize,
    ) -> datafusion::common::Result<Self> {
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion_distributed::{ChannelResolver, MaybeEncoded, SetPlanRequest, TaskKey};

        let mut state = environment(partitions, 128).build_session_state().unwrap();
        let session_id = state.task_ctx().session_id();
        state
            .config_mut()
            .set_extension(Arc::new(UniqueTimestamp::MIN));
        state
            .config_mut()
            .set_extension(Arc::new(restate_storage_query_api::QueryMetadata {
                session_id: session_id.into(),
                query_ts: UniqueTimestamp::MIN,
                headers: Default::default(),
                redacted_sql: "IPC transport fixture".into(),
                planning_duration: Default::default(),
            }));
        let context = state.task_ctx();
        let key = TaskKey {
            query_id: uuid::Uuid::new_v4(),
            stage_id: 1,
            task_number: 0,
        };
        let url = super::worker_url(OWNER).unwrap();
        let mut client = super::transport::RestateChannelResolver::new(fixture.network.clone())
            .get_worker_client_for_url(&url)
            .await?;
        let control = client
            .coordinator_channel(
                Self::headers(),
                SetPlanRequest {
                    task_key: key,
                    task_count: 1,
                    plan: MaybeEncoded::Decoded(Arc::new(
                        EmptyExec::new(schema).with_partitions(partitions),
                    )),
                    dynamic_filter_remote_producer_ids: vec![],
                    work_unit_feed_declarations: vec![],
                    target_worker_url: url,
                    query_start_time_ns: 0,
                },
                stream::pending().boxed(),
                Default::default(),
                &context,
            )
            .await?;
        Ok(Self {
            client,
            context,
            key,
            _control: control,
        })
    }

    async fn execute(
        &mut self,
        end: usize,
    ) -> datafusion::common::Result<Vec<BoxStream<'static, datafusion::common::Result<RecordBatch>>>>
    {
        self.client
            .execute_task(
                Default::default(),
                datafusion_distributed::ExecuteTaskRequest {
                    task_key: self.key,
                    target_partition_start: 0,
                    target_partition_end: end,
                    producer_head: datafusion_distributed::ProducerHead::None,
                },
                Default::default(),
                &self.context,
            )
            .await
    }
}

fn dictionary_output(
    partition: usize,
    input: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    use datafusion::arrow::array::{ArrayRef, DictionaryArray, Int32Array, StringArray};
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::arrow::record_batch::RecordBatch;

    let schema = input.schema();
    let first = Arc::new(StringArray::from(vec![format!("lane-{partition}-first")])) as ArrayRef;
    let replacement = Arc::new(StringArray::from(vec![format!(
        "lane-{partition}-replacement"
    )])) as ArrayRef;
    let batches = [Arc::clone(&first), first, replacement].map(|values| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 0]), values)
                    .unwrap(),
            )],
        )
        .map_err(Into::into)
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream::iter(batches)))
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipc_preserves_interleaved_lanes_and_ends_on_connection_loss_without_replay() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use restate_core::network::DrainReason;

    let fixture = setup(false).await;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]));
    fixture.tamper.lock().output = Some(dictionary_output);
    let (events, mut replies) = tokio::sync::mpsc::unbounded_channel();
    fixture.tamper.lock().events = Some(events);
    let mut task = OutputTask::install(&fixture, schema, 3).await.unwrap();
    let mut lanes = task.execute(3).await.unwrap();
    // Interleave two lanes through dictionary replacement and EOS.
    for suffix in ["first", "first", "replacement"] {
        for partition in [1, 0] {
            let batch = lanes[partition].next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(
                array_value_to_string(batch.column(0), 0).unwrap(),
                format!("lane-{partition}-{suffix}")
            );
        }
    }
    for lane in &mut lanes[..2] {
        assert!(lane.next().await.is_none());
    }
    // Leave the third lane advanced with two batches still unread at actual
    // connection loss. This is not a drain after successful EOS.
    let batch = lanes[2].next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        array_value_to_string(batch.column(0), 0).unwrap(),
        "lane-2-first"
    );
    let connection = fixture
        .network
        .get_connection(OWNER, Swimlane::QueryEngine)
        .in_tc_as_task(
            &TaskCenter::current(),
            TaskKind::InPlace,
            "ipc-loss-connect",
        )
        .await
        .unwrap();
    let closed = connection.closed();
    connection
        .drain(DrainReason::ConnectionDrain)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), closed)
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), lanes[2].next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("closed"), "{error}");
    assert!(
        lanes[2].next().await.is_none(),
        "one terminal error, no retry"
    );
    let seen = std::iter::from_fn(|| replies.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        seen.iter()
            .filter(|event| event.kind == "next-received")
            .count(),
        9, // Four pulls for each completed lane and one for the unfinished lane.
        "{seen:?}"
    );
    assert_eq!(fixture.observations.lock().len(), 1, "no reinstallation");
}

fn zero_body_output(
    partition: usize,
    input: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    use datafusion::arrow::array::{Int32Array, RecordBatchOptions};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    let schema = input.schema();
    let batches = match partition {
        0 => vec![
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(7)),
            )
            .unwrap()),
            Ok(RecordBatch::new_empty(Arc::clone(&schema))),
            Err(DataFusionError::Execution("later lane failure".into())),
        ],
        1 => vec![],
        _ => vec![Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "wrong",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap())],
    };
    Box::pin(RecordBatchStreamAdapter::new(schema, stream::iter(batches)))
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipc_delivers_zero_body_batches_before_failure_and_discards_failed_lanes() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use restate_types::net::distributed_query::QueryTaskNext;

    let fixture = setup(false).await;
    fixture.tamper.lock().output = Some(zero_body_output);
    let mut task = OutputTask::install(&fixture, Arc::new(Schema::empty()), 3)
        .await
        .unwrap();
    let mut lanes = task.execute(3).await.unwrap();
    assert_eq!(lanes[0].next().await.unwrap().unwrap().num_rows(), 7);
    assert!(
        lanes[1].next().await.is_none(),
        "empty sibling must finish independently"
    );
    assert_eq!(lanes[0].next().await.unwrap().unwrap().num_rows(), 0);
    assert!(
        lanes[0]
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("later lane failure")
    );
    assert!(lanes[0].next().await.is_none());
    assert!(
        lanes[2]
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("batch schema")
    );
    let id = fixture.observations.lock().last().unwrap().id.clone();
    let connection = fixture
        .network
        .get_connection(OWNER, Swimlane::QueryEngine)
        .in_tc_as_task(
            &TaskCenter::current(),
            TaskKind::InPlace,
            "ipc-test-connect",
        )
        .await
        .unwrap();
    for partition in 0..3 {
        let error = rpc(
            &connection,
            QueryTaskRequest::Next(QueryTaskNext {
                id: id.clone(),
                partition,
            }),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("already ended"), "{error}");
    }

    // A mismatched second stream must prevent publication of the entire range.
    fixture.tamper.lock().output = Some(|partition, input| {
        if partition == 0 {
            input
        } else {
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::new(vec![Field::new(
                    "wrong",
                    DataType::Int32,
                    false,
                )])),
                stream::empty(),
            ))
        }
    });
    let mut task = OutputTask::install(&fixture, Arc::new(Schema::empty()), 2)
        .await
        .unwrap();
    let Err(error) = task.execute(2).await else {
        panic!("stream mismatch must fail Execute");
    };
    assert!(error.to_string().contains("stream schema"), "{error}");
    let id = fixture.observations.lock().last().unwrap().id.clone();
    let error = rpc(
        &connection,
        QueryTaskRequest::Next(QueryTaskNext { id, partition: 0 }),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("not running"), "{error}");

    // Reject a different Installed schema before the task can be executed.
    fixture.tamper.lock().installed_schema = Some(Arc::new(Schema::new(vec![Field::new(
        "wrong",
        DataType::Int32,
        false,
    )])));
    let Err(error) = OutputTask::install(&fixture, Arc::new(Schema::empty()), 1).await else {
        panic!("mismatched install must fail");
    };
    assert!(
        error.to_string().contains("installed task output schema"),
        "{error}"
    );
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
    assert!(
        !fixture.observations.lock().is_empty(),
        "the query must install a remote task"
    );
    fixture.wait_for_task_cleanup().await;
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
        .get_connection(OWNER, Swimlane::QueryEngine)
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
        .get_connection(OWNER, Swimlane::QueryEngine)
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
        analyze: None,
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
        Ok(QueryTaskReplyBody::Closed(_))
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
        crate::source_registry::SourceOrders::default(),
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
    fixture.wait_for_task_cleanup().await;
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
    let owner_scanners = fixture.owner_scanners.get().unwrap();
    owner_scanners.register_processor_reads(
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

    // Under ANALYZE, a report the coordinator can't use doesn't hide the
    // data's own failure.
    fixture.tamper.lock().report = Some(vec![0xff; 16]);
    let error = fixture
        .engine(1, 1)
        .create_session(SessionOptions::default())
        .unwrap()
        .execute("EXPLAIN ANALYZE SELECT * FROM state", QueryOptions {})
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("injected late storage failure"),
        "{error}"
    );
}

/// A report the coordinator can't use costs only the report. A worker from
/// before reports, and a malformed, an oversized or an out-of-domain report,
/// each show as a marker on the stage, and the query succeeds.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn unusable_reports_degrade_to_markers() {
    use bilrost::Message;

    let fixture = setup(false).await;
    let session = fixture
        .engine(1, 2)
        .create_session(SessionOptions::default())
        .unwrap();
    let analyze = async || {
        let result = session
            .execute(
                "EXPLAIN ANALYZE SELECT COUNT(*) FROM state",
                QueryOptions {},
            )
            .await
            .unwrap();
        let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(|row| array_value_to_string(batch.column(1), row).unwrap())
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let text = analyze().await;
    assert!(text.contains("tasks_completed=1"), "{text}");
    fixture.tamper.lock().legacy = true;
    let text = analyze().await;
    assert!(text.contains("metrics_unsupported=1"), "{text}");
    fixture.tamper.lock().legacy = false;
    // A well-formed report over the size a worker can build.
    let oversized = QueryTaskReport {
        task: vec![MetricRecord {
            value: MetricRecordValue::Count(NamedValue {
                name: "n".repeat(70_000).into(),
                value: 1,
            }),
            level: MetricLevel::SUMMARY,
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec();
    for report in [vec![0xff; 16], oversized] {
        fixture.tamper.lock().report = Some(report);
        let text = analyze().await;
        assert!(text.contains("metrics_invalid=1"), "{text}");
        assert!(!text.contains("tasks_completed"), "{text}");
    }
    // A report from a newer worker, with a metric level this binary doesn't
    // know, decodes: it's checked against its stage like any report, rather
    // than being invalid. This one describes no plan, so it doesn't attach;
    // the unit tests show that only such a metric is dropped.
    let newer = QueryTaskReport {
        outcome: TaskOutcome::COMPLETED,
        task: vec![MetricRecord {
            value: MetricRecordValue::Count(NamedValue {
                name: "n".into(),
                value: 1,
            }),
            level: MetricLevel(99),
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec();
    fixture.tamper.lock().report = Some(newer);
    let text = analyze().await;
    assert!(text.contains("metrics_shape_mismatch=1"), "{text}");
    assert!(!text.contains("metrics_invalid"), "{text}");
}

/// A Close racing a task's blocked requests, over the native transport, each
/// held at an explicit barrier:
///
/// - **a blocked Next:** its read is held before its first row; the query is
///   dropped, so the coordinator closes the task. The Next ends as closed,
///   the task closes once, and the read's snapshot actually closes;
/// - **a blocked Execute**, under ANALYZE: held before it runs; the query is
///   dropped, and the Close arrives while it's held. Released, the Execute ends
///   as closed and publishes nothing; the Close then settles once, with its
///   report.
///
/// Every event names the owner's task, as installed.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_ends_a_blocked_execute_or_next_once() {
    let mut fixture = setup(false).await;
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
    let (events, mut events_rx) = tokio::sync::mpsc::unbounded_channel::<WorkerEvent>();
    fixture.tamper.lock().events = Some(events);
    // The first event of `kind` for task `id` not yet taken, received within
    // 10 s: one request's events may arrive before or after another's.
    let mut seen: Vec<WorkerEvent> = Vec::new();
    let mut taken: Vec<bool> = Vec::new();
    let mut next_event =
        async |kind: &str, id: &QueryTaskId, seen: &mut Vec<WorkerEvent>, taken: &mut Vec<bool>| {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(at) = seen
                    .iter()
                    .enumerate()
                    .position(|(at, event)| !taken[at] && event.kind == kind && event.id == *id)
                {
                    taken[at] = true;
                    return seen[at].clone();
                }
                let event = tokio::time::timeout_at(deadline, events_rx.recv())
                    .await
                    .unwrap_or_else(|_| panic!("no {kind} event: {seen:#?}"))
                    .unwrap();
                seen.push(event);
                taken.push(false);
            }
        };
    let engine = fixture.engine(1, 1);

    // A blocked Next.
    let (entered, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    *fixture.read_hold.lock() = Some(ReadHold {
        entered,
        release: Arc::new(tokio::sync::Notify::new()),
    });
    let mut stream = engine
        .create_session(SessionOptions::default())
        .unwrap()
        .execute("SELECT * FROM state", QueryOptions {})
        .await
        .unwrap()
        .stream;
    let consumer = tokio::spawn(async move { stream.next().await.map(|batch| batch.is_ok()) });
    let closed = tokio::time::timeout(Duration::from_secs(10), entered_rx.recv())
        .await
        .expect("the read is held")
        .unwrap();
    let task = fixture.observations.lock()[0].id.clone();
    assert_eq!(fixture.observations.lock()[0].owner, OWNER);
    next_event("next-received", &task, &mut seen, &mut taken).await;
    next_event("next-polling", &task, &mut seen, &mut taken).await;
    let connection = fixture
        .network
        .get_connection(OWNER, Swimlane::QueryEngine)
        .in_tc_as_task(
            &TaskCenter::current(),
            TaskKind::InPlace,
            "ipc-concurrent-connect",
        )
        .await
        .unwrap();
    let error = rpc(
        &connection,
        QueryTaskRequest::Next(restate_types::net::distributed_query::QueryTaskNext {
            id: task.clone(),
            partition: 0,
        }),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("concurrent pull"), "{error}");
    let concurrent = next_event("next", &task, &mut seen, &mut taken).await;
    assert!(concurrent.outcome.contains("concurrent pull"));
    consumer.abort();
    let next = next_event("next", &task, &mut seen, &mut taken).await;
    let close = next_event("close", &task, &mut seen, &mut taken).await;
    assert!(next.outcome.contains("query task closed"), "{seen:#?}");
    assert_eq!(close.outcome, "closed report=false");
    tokio::time::timeout(Duration::from_secs(5), closed)
        .await
        .expect("the held read's snapshot closes");
    *fixture.read_hold.lock() = None;

    // A blocked Execute, under ANALYZE.
    let (entered, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    fixture.tamper.lock().execute_hold = Some(Hold {
        entered,
        release: Arc::clone(&release),
    });
    let mut stream = engine
        .create_session(SessionOptions::default())
        .unwrap()
        .execute("EXPLAIN ANALYZE SELECT * FROM state", QueryOptions {})
        .await
        .unwrap()
        .stream;
    let consumer = tokio::spawn(async move { stream.next().await.map(|batch| batch.is_ok()) });
    let task = tokio::time::timeout(Duration::from_secs(10), entered_rx.recv())
        .await
        .expect("the Execute is held")
        .unwrap();
    let installed = fixture.observations.lock()[1].id.clone();
    assert_eq!(task, installed);
    consumer.abort();
    // The Close has cancelled the task before the Execute goes on: receiving
    // the Close only starts closing it.
    next_event("cancelled", &task, &mut seen, &mut taken).await;
    release.notify_one();
    let execute = next_event("execute", &task, &mut seen, &mut taken).await;
    let close = next_event("close", &task, &mut seen, &mut taken).await;
    assert!(execute.outcome.contains("query task closed"), "{seen:#?}");
    assert_eq!(close.outcome, "closed report=true");
    // Each task closed once.
    fixture.tamper.lock().events = None;
    while let Ok(event) = events_rx.try_recv() {
        seen.push(event);
    }
    for id in [&installed, &next.id] {
        assert_eq!(
            seen.iter()
                .filter(|event| event.kind == "close" && event.id == *id)
                .count(),
            1,
            "{seen:#?}"
        );
    }
}

/// A task's own failure stays the query's error however its telemetry fails
/// too. An Execute fails as a protocol failure: without ANALYZE; under ANALYZE
/// with a malformed report; and from a worker without reports. Each time the
/// query fails with that failure, worded the same.
///
/// The ordering covered is the native one for a failed task: the control
/// failure ends the query first; the coordinator's control stream for the
/// task still ends, and closes it. Receipts from the coordinator, waited for
/// as well as the failure, show for each task: without ANALYZE, the close;
/// with a malformed report, that its worker was to report, then the report
/// read and degraded to invalid (the soft-failure branch), after the control
/// failure; from a worker without reports, that it wasn't to report, then
/// the close.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_failure_stays_the_error_whatever_the_telemetry() {
    let mut fixture = setup(false).await;
    let mut transaction = fixture.store.transaction();
    transaction
        .put_user_state(
            &ServiceId::new(None, "service", "object"),
            &Bytes::from_static(b"a"),
            Bytes::from_static(b"value"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    drop(transaction);
    let (receipts, mut receipts_rx) = tokio::sync::mpsc::unbounded_channel();
    *super::transport::TELEMETRY.lock().unwrap() = Some(receipts);
    fixture.tamper.lock().execute_error = Some("injected execute failure".to_owned());
    let engine = fixture.engine(1, 1);
    let mut errors = Vec::new();
    for (at, (sql, legacy, report, expected)) in [
        ("SELECT * FROM state", false, None, &["closed"][..]),
        (
            "EXPLAIN ANALYZE SELECT * FROM state",
            false,
            Some(vec![0xff; 16]),
            &["installed report=true", "invalid"][..],
        ),
        (
            "EXPLAIN ANALYZE SELECT * FROM state",
            true,
            None,
            &["installed report=false", "closed"][..],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        {
            let mut tamper = fixture.tamper.lock();
            tamper.legacy = legacy;
            tamper.report = report;
        }
        let error = engine
            .create_session(SessionOptions::default())
            .unwrap()
            .execute(sql, QueryOptions {})
            .await
            .unwrap()
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err()
            .to_string();
        // The owner's failure, with its owner and kind.
        assert!(
            error.contains("N2:1: protocol: injected execute failure")
                && !error.contains("metrics"),
            "{sql}: {error}"
        );
        errors.push(error);
        // This task's telemetry, up to what became of it.
        let task = fixture.observations.lock()[at].id.clone();
        let mut seen = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let terminal = ["closed unread", "closed", "received", "invalid", "missing"];
        while seen
            .last()
            .is_none_or(|what: &String| !terminal.contains(&what.as_str()))
        {
            let (id, what) = tokio::time::timeout_at(deadline, receipts_rx.recv())
                .await
                .unwrap_or_else(|_| panic!("{sql}: {seen:?}"))
                .unwrap();
            if id == task {
                seen.push(what);
            }
        }
        assert_eq!(seen, expected, "{sql} legacy={legacy}");
    }
    // The same failure, worded the same, each time.
    assert!(
        errors.windows(2).all(|pair| pair[0] == pair[1]),
        "{errors:#?}"
    );
}

/// A Close sent while the coordinator still waits on the task, under
/// ANALYZE, each step held at a barrier and correlated with the owner's
/// installed task:
///
/// - **a blocked Next:** the Close cancels the task (cancellation applied is
///   the barrier), the Next ends as closed, and the coordinator's own consumer
///   gets that failure. The closing drops the task's stream and the copy
///   settles; while the closing is held after that, the read's snapshot
///   completes. A repeated Close arrives then; released,
///   the closing finishes once and both Closes get the same report, which
///   says the task stopped part-way, its one copy cancelled before it merged;
/// - **an Execute held inside its cancellable work:** the Close cancels it
///   there, it ends as closed, the consumer gets that failure, and the report
///   says the task stopped part-way with nothing unsettled.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_cancels_a_waiting_task_and_finalizes_it_once() {
    use bilrost::OwnedMessage;
    use restate_types::net::distributed_query::TaskOutcome;

    let mut fixture = setup(false).await;
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
    let (events, mut events_rx) = tokio::sync::mpsc::unbounded_channel::<WorkerEvent>();
    fixture.tamper.lock().events = Some(events);
    let mut seen: Vec<WorkerEvent> = Vec::new();
    let mut taken: Vec<bool> = Vec::new();
    // The first event of `kind` for task `id` not yet taken, within 10 s.
    let mut next_event =
        async |kind: &str, id: &QueryTaskId, seen: &mut Vec<WorkerEvent>, taken: &mut Vec<bool>| {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(at) = seen
                    .iter()
                    .enumerate()
                    .position(|(at, event)| !taken[at] && event.kind == kind && event.id == *id)
                {
                    taken[at] = true;
                    return seen[at].clone();
                }
                let event = tokio::time::timeout_at(deadline, events_rx.recv())
                    .await
                    .unwrap_or_else(|_| panic!("no {kind} event: {seen:#?}"))
                    .unwrap();
                seen.push(event);
                taken.push(false);
            }
        };
    let connection = fixture
        .network
        .get_connection(OWNER, Swimlane::QueryEngine)
        .in_tc_as_task(
            &TaskCenter::current(),
            TaskKind::InPlace,
            "query-test-connect",
        )
        .await
        .unwrap();
    let close = |id: &QueryTaskId| {
        let (connection, id) = (connection.clone(), id.clone());
        tokio::spawn(async move {
            rpc(&connection, QueryTaskRequest::Close(QueryTaskClose { id })).await
        })
    };
    let report = |event: &WorkerEvent| {
        QueryTaskReport::decode(event.report.clone().expect("a report").as_slice()).unwrap()
    };
    let engine = fixture.engine(1, 1);
    let consume = |sql: &'static str| {
        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            let mut stream = engine
                .create_session(SessionOptions::default())
                .unwrap()
                .execute(sql, QueryOptions {})
                .await
                .unwrap()
                .stream;
            while let Some(batch) = stream.next().await {
                if let Err(error) = batch {
                    return error.to_string();
                }
            }
            "ended".to_owned()
        })
    };

    // A blocked Next.
    let (entered, mut read_entered) = tokio::sync::mpsc::unbounded_channel();
    *fixture.read_hold.lock() = Some(ReadHold {
        entered,
        release: Arc::new(tokio::sync::Notify::new()),
    });
    let (entered, mut finalizing) = tokio::sync::mpsc::unbounded_channel();
    let finalize = Arc::new(tokio::sync::Notify::new());
    fixture.tamper.lock().finalize_hold = Some(Hold {
        entered,
        release: Arc::clone(&finalize),
    });
    let consumer = consume("EXPLAIN ANALYZE SELECT * FROM state");
    let snapshot = tokio::time::timeout(Duration::from_secs(10), read_entered.recv())
        .await
        .expect("the read is held")
        .unwrap();
    let (task, owner) = {
        let observations = fixture.observations.lock();
        (observations[0].id.clone(), observations[0].owner)
    };
    assert_eq!(owner, OWNER);
    next_event("next-received", &task, &mut seen, &mut taken).await;
    let first = close(&task);
    next_event("cancelled", &task, &mut seen, &mut taken).await;
    let next = next_event("next", &task, &mut seen, &mut taken).await;
    assert!(next.outcome.contains("query task closed"), "{seen:#?}");
    let failure = tokio::time::timeout(Duration::from_secs(10), consumer)
        .await
        .unwrap()
        .unwrap();
    assert!(failure.contains("query task closed"), "{failure}");
    // Held after its copies settled, with the stream gone: the read's
    // snapshot completes meanwhile.
    let held = tokio::time::timeout(Duration::from_secs(10), finalizing.recv())
        .await
        .expect("the closing is held")
        .unwrap();
    assert_eq!(held, task);
    let settled = next_event("settled", &task, &mut seen, &mut taken).await;
    assert_eq!(settled.outcome, "idle=true settled=true");
    tokio::time::timeout(Duration::from_secs(5), snapshot)
        .await
        .expect("the read's snapshot closes while the closing is held");
    let repeated = close(&task);
    next_event("close-received", &task, &mut seen, &mut taken).await;
    next_event("close-received", &task, &mut seen, &mut taken).await;
    finalize.notify_one();
    for reply in [first, repeated] {
        let reply = tokio::time::timeout(Duration::from_secs(10), reply)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(reply, QueryTaskReplyBody::Closed(_)));
    }
    let closed = next_event("close", &task, &mut seen, &mut taken).await;
    let repeated = next_event("close-repeated", &task, &mut seen, &mut taken).await;
    assert_eq!(closed.report, repeated.report);
    let sent = report(&closed);
    assert_eq!(
        (sent.outcome, sent.unsettled_lanes),
        (TaskOutcome::INCOMPLETE, 1),
        "{sent:?}"
    );
    *fixture.read_hold.lock() = None;
    fixture.tamper.lock().finalize_hold = None;

    // An Execute held inside its cancellable work.
    let (entered, mut execute_entered) = tokio::sync::mpsc::unbounded_channel();
    fixture.tamper.lock().execute_inside_hold = Some(Hold {
        entered,
        release: Arc::new(tokio::sync::Notify::new()),
    });
    let consumer = consume("EXPLAIN ANALYZE SELECT * FROM state");
    let held = tokio::time::timeout(Duration::from_secs(10), execute_entered.recv())
        .await
        .expect("the Execute is held")
        .unwrap();
    let (installed, owner) = {
        let observations = fixture.observations.lock();
        (observations[1].id.clone(), observations[1].owner)
    };
    assert_eq!((&held, owner), (&installed, OWNER));
    let only = close(&installed);
    next_event("cancelled", &installed, &mut seen, &mut taken).await;
    let execute = next_event("execute", &installed, &mut seen, &mut taken).await;
    assert!(execute.outcome.contains("query task closed"), "{seen:#?}");
    let failure = tokio::time::timeout(Duration::from_secs(10), consumer)
        .await
        .unwrap()
        .unwrap();
    assert!(failure.contains("query task closed"), "{failure}");
    tokio::time::timeout(Duration::from_secs(10), only)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let closed = next_event("close", &installed, &mut seen, &mut taken).await;
    let sent = report(&closed);
    assert_eq!(
        (sent.outcome, sent.unsettled_lanes),
        (TaskOutcome::INCOMPLETE, 0),
        "{sent:?}"
    );

    // Each task finalized once, counted after every reply was received.
    while let Ok(event) = events_rx.try_recv() {
        seen.push(event);
    }
    for id in [&task, &installed] {
        assert_eq!(
            seen.iter()
                .filter(|event| event.kind == "finalized" && event.id == *id)
                .count(),
            1,
            "{seen:#?}"
        );
    }
}
