// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};
use std::task::Poll;

use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::{collect, display::DisplayableExecutionPlan};
use futures::{Stream, future::poll_fn};

use googletest::matcher::{Matcher, MatcherResult};
use serde_json::Value;
use tokio::sync::watch;

use restate_core::network::protobuf::network::Message;
use restate_core::network::{ConnectError, Destination, MockConnector, Swimlane, TransportConnect};
use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
use restate_metadata_store::MetadataStoreClient;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_types::cluster_state::NodeState;
use restate_types::config::QueryEngineOptions;
use restate_types::deployment::{DeploymentAddress, Headers};
use restate_types::errors::GenericError;
use restate_types::identifiers::{DeploymentId, PartitionId, ServiceRevision, WithPartitionKey};
use restate_types::live::Live;
use restate_types::net::address::{AdvertisedAddress, HttpIngressPort};
use restate_types::net::remote_query_scanner::RemoteQueryScannerOpen;
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::partition_table::{Partition, PartitionTable};
use restate_types::schema::deployment::test_util::MockDeploymentMetadataRegistry;
use restate_types::schema::deployment::{Deployment, DeploymentResolver};
use restate_types::schema::service::test_util::MockServiceMetadataResolver;
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};
use restate_types::sharding::KeyRange;
use restate_types::{GenerationalNodeId, NodeId, RestateVersion, Version};
use restate_worker_api::invoker::{InvocationStatusReport, StatusHandle};
use restate_worker_api::{SchedulerStatusEntry, UserLimitCounterEntry};

use super::context::QueryContext;
use crate::context::{PartitionLeaderStatusHandle, SelectPartitions};
use crate::remote_query_scanner_client::{RemoteScanner, RemoteScannerService};
use crate::remote_query_scanner_manager::{
    PartitionLocation, PartitionLocator, RemoteScannerManager,
};
use crate::remote_query_scanner_server::RemoteQueryScannerServer;

#[derive(Debug, Clone, Default)]
pub struct MockStatusHandle(Arc<RwLock<Vec<InvocationStatusReport>>>);

impl MockStatusHandle {
    pub fn with(self, invocation_status_report: InvocationStatusReport) -> Self {
        self.push(invocation_status_report);
        self
    }

    pub(crate) fn push(&self, invocation_status_report: InvocationStatusReport) {
        self.0.write().unwrap().push(invocation_status_report);
    }
}

impl StatusHandle for MockStatusHandle {
    type Iterator = std::vec::IntoIter<InvocationStatusReport>;

    async fn read_status(&self, keys: KeyRange) -> Self::Iterator {
        self.0
            .read()
            .unwrap()
            .iter()
            .filter(|status| keys.contains(&status.invocation_id().partition_key()))
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[derive(Default, Clone, Debug)]
pub(crate) struct MockSchemas(
    pub(crate) MockServiceMetadataResolver,
    pub(crate) MockDeploymentMetadataRegistry,
);

impl ServiceMetadataResolver for MockSchemas {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        self.0.resolve_latest_service(service_name)
    }

    fn resolve_latest_service_openapi(
        &self,
        _: impl AsRef<str>,
        _ingress_address: AdvertisedAddress<HttpIngressPort>,
    ) -> Option<Value> {
        todo!()
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.0.list_services()
    }

    fn list_service_names(&self) -> Vec<String> {
        self.0.list_service_names()
    }
}

impl PartitionLeaderStatusHandle for MockStatusHandle {
    type SchedulerStatus = SchedulerStatusEntry;
    type SchedulerStatusIterator = std::iter::Empty<Self::SchedulerStatus>;

    type UserLimitCounter = UserLimitCounterEntry;
    type UserLimitCounterIterator = std::iter::Empty<Self::UserLimitCounter>;

    fn read_scheduler_status(
        &self,
        _keys: KeyRange,
    ) -> impl Future<Output = Self::SchedulerStatusIterator> + Send {
        std::future::ready(std::iter::empty())
    }

    fn read_user_limit_counters(
        &self,
        _keys: KeyRange,
    ) -> impl Future<Output = Self::UserLimitCounterIterator> + Send {
        std::future::ready(std::iter::empty())
    }
}

impl DeploymentResolver for MockSchemas {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        self.1.resolve_latest_deployment_for_service(service_name)
    }

    fn find_deployment(
        &self,
        deployment_address: &DeploymentAddress,
        additional_headers: &Headers,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.1
            .find_deployment(deployment_address, additional_headers)
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        self.1.get_deployment(deployment_id)
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.1.get_deployment_and_services(deployment_id)
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        self.1.get_deployments()
    }
}

#[derive(Clone, Debug)]
struct MockPartitionSelector;

#[async_trait]
impl SelectPartitions for MockPartitionSelector {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        let id = PartitionId::MIN;
        let partition = Partition::new(id, KeyRange::FULL);
        Ok(vec![(id, partition)])
    }
}

#[allow(dead_code)]
pub(crate) struct MockQueryEngine(Arc<PartitionStoreManager>, PartitionStore, QueryContext);

#[derive(Debug)]
struct NoopSvc;

#[async_trait]
impl RemoteScannerService for NoopSvc {
    async fn open(
        &self,
        _peer: NodeId,
        _req: RemoteQueryScannerOpen,
    ) -> Result<RemoteScanner, DataFusionError> {
        panic!("remote service should not be used")
    }
}

struct AlwaysLocalPartitionLocator;

impl PartitionLocator for AlwaysLocalPartitionLocator {
    fn get_partition_target_node(
        &self,
        _partition_id: PartitionId,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Local)
    }
}

impl MockQueryEngine {
    pub async fn create_with(
        status: impl PartitionLeaderStatusHandle<
            SchedulerStatus = SchedulerStatusEntry,
            UserLimitCounter = UserLimitCounterEntry,
        >,
        schemas: impl DeploymentResolver
        + ServiceMetadataResolver
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    ) -> Self {
        // Prepare Rocksdb
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true)
            .await
            .expect("DB creation succeeds");
        let partition_store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();

        // Matches MockPartitionSelector's single partition
        Self(
            manager.clone(),
            partition_store,
            QueryContext::with_user_tables(
                &QueryEngineOptions::default(),
                MockPartitionSelector,
                manager,
                Some(status),
                Live::from_value(schemas),
                RemoteScannerManager::new(
                    Arc::new(NoopSvc),
                    Arc::new(AlwaysLocalPartitionLocator) as Arc<dyn PartitionLocator>,
                    // The mock locator always returns `Local`, so the manager
                    // never invokes `allocate_scanner_id` and never reads
                    // `my_node_id`. A blank Metadata is sufficient here.
                    restate_core::MetadataBuilder::default().to_metadata(),
                ),
                MetadataStoreClient::new_in_memory(),
                None,
            )
            .await
            .unwrap(),
        )
    }

    pub async fn create() -> Self {
        Self::create_with(MockStatusHandle::default(), MockSchemas::default()).await
    }

    pub fn partition_store(&mut self) -> &mut PartitionStore {
        &mut self.1
    }

    pub async fn execute(
        &self,
        sql: impl AsRef<str> + Send,
    ) -> Result<crate::context::QueryResult, crate::context::QueryError> {
        self.2.execute(sql.as_ref()).await
    }
}

const QUERY_COORDINATOR_NODE_ID: GenerationalNodeId = GenerationalNodeId::new(1, 1);
const REMOTE_QUERY_PARTITIONS: u16 = 3;

fn remote_query_partition_table() -> PartitionTable {
    PartitionTable::with_equally_sized_partitions(Version::MIN, REMOTE_QUERY_PARTITIONS)
}

#[derive(Clone, Debug)]
struct FixedPartitionSelector(Arc<Vec<(PartitionId, Partition)>>);

impl FixedPartitionSelector {
    fn new(partitions: impl IntoIterator<Item = (PartitionId, Partition)>) -> Self {
        Self(Arc::new(partitions.into_iter().collect()))
    }
}

#[async_trait]
impl SelectPartitions for FixedPartitionSelector {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(self.0.as_ref().clone())
    }
}

#[derive(Debug)]
struct RemotePartitionLocator {
    owners: Arc<BTreeMap<PartitionId, GenerationalNodeId>>,
}

impl PartitionLocator for RemotePartitionLocator {
    fn get_partition_target_node(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<PartitionLocation> {
        let node_id = self
            .owners
            .get(&partition_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("partition {partition_id} has no test owner"))?;
        Ok(PartitionLocation::Remote {
            node_id: NodeId::from(node_id),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RemoteScan {
    pub(crate) node_id: NodeId,
    pub(crate) partition_id: PartitionId,
    pub(crate) table: String,
}

#[derive(Debug)]
struct RecordingRemoteScannerService {
    inner: Arc<dyn RemoteScannerService>,
    scans: Arc<RwLock<Vec<RemoteScan>>>,
}

#[async_trait]
impl RemoteScannerService for RecordingRemoteScannerService {
    async fn open(
        &self,
        peer: NodeId,
        request: RemoteQueryScannerOpen,
    ) -> Result<RemoteScanner, DataFusionError> {
        self.scans.write().unwrap().push(RemoteScan {
            node_id: peer,
            partition_id: request.partition_id,
            table: request.table.clone(),
        });
        self.inner.open(peer, request).await
    }
}

#[derive(Clone)]
struct RemoteNodeScanner {
    node_id: GenerationalNodeId,
    query_context: QueryContext,
    scanner_manager: RemoteScannerManager,
}

#[derive(Clone)]
struct ReadyConnector<T> {
    inner: T,
    server_ready: Arc<BTreeMap<GenerationalNodeId, watch::Receiver<bool>>>,
}

impl<T: TransportConnect> TransportConnect for ReadyConnector<T> {
    fn connect(
        &self,
        destination: &Destination,
        swimlane: Swimlane,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError>,
    > + Send {
        let mut server_ready = match destination {
            Destination::Node(node_id) => self
                .server_ready
                .get(node_id)
                .unwrap_or_else(|| panic!("no query-test scanner readiness signal for {node_id}"))
                .clone(),
            destination => panic!("query-test connector does not support {destination:?}"),
        };
        async move {
            let output_stream = self
                .inner
                .connect(destination, swimlane, output_stream)
                .await?;
            if !*server_ready.borrow() {
                server_ready.changed().await.map_err(|_| {
                    ConnectError::Transport(
                        "query-test scanner server stopped before accepting requests".to_owned(),
                    )
                })?;
            }
            Ok(output_stream)
        }
    }
}

pub(crate) struct MockRemoteQueryEngine {
    partition_stores: BTreeMap<PartitionId, PartitionStore>,
    partition_table: PartitionTable,
    partition_owners: Arc<BTreeMap<PartitionId, GenerationalNodeId>>,
    query_context: QueryContext,
    remote_scans: Arc<RwLock<Vec<RemoteScan>>>,
}

impl MockRemoteQueryEngine {
    pub(crate) async fn create_with(
        status: impl PartitionLeaderStatusHandle<
            SchedulerStatus = SchedulerStatusEntry,
            UserLimitCounter = UserLimitCounterEntry,
        >,
        schemas: impl DeploymentResolver
        + ServiceMetadataResolver
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    ) -> Self {
        RocksDbManager::init();

        let partition_table = remote_query_partition_table();
        let partitions = partition_table
            .iter()
            .map(|(partition_id, partition)| (*partition_id, partition.clone()))
            .collect::<Vec<_>>();
        let owners = Arc::new(
            partitions
                .iter()
                .enumerate()
                .map(|(index, (partition_id, _))| {
                    (
                        *partition_id,
                        GenerationalNodeId::new(
                            u32::try_from(index).expect("test node index to fit in u32") + 2,
                            1,
                        ),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        );

        let mut partition_stores = BTreeMap::new();
        let mut remote_nodes = Vec::with_capacity(partitions.len());
        for (partition_id, partition) in &partitions {
            let manager = PartitionStoreManager::create(true)
                .await
                .expect("DB creation succeeds");
            let partition_store = manager.open(partition, None).await.unwrap();
            partition_stores.insert(*partition_id, partition_store);

            let scanner_manager = RemoteScannerManager::local_only(
                restate_core::MetadataBuilder::default().to_metadata(),
            );
            let query_context = QueryContext::with_user_tables(
                &QueryEngineOptions::default(),
                FixedPartitionSelector::new([(*partition_id, partition.clone())]),
                manager,
                Some(status.clone()),
                Live::from_value(schemas.clone()),
                scanner_manager.clone(),
                MetadataStoreClient::new_in_memory(),
                None,
            )
            .await
            .unwrap();
            remote_nodes.push(RemoteNodeScanner {
                node_id: owners[partition_id],
                query_context,
                scanner_manager,
            });
        }

        let mut nodes_config = NodesConfiguration::new_for_testing();
        for node_id in std::iter::once(QUERY_COORDINATOR_NODE_ID).chain(owners.values().copied()) {
            nodes_config.upsert_node(
                NodeConfig::builder()
                    .name(format!("query-test-{node_id}"))
                    .current_generation(node_id)
                    .address(AdvertisedAddress::default())
                    .roles(Role::Admin | Role::Worker)
                    .binary_version(RestateVersion::current())
                    .build(),
            );
        }

        let (server_ready_tx, server_ready_rx) = owners
            .values()
            .copied()
            .map(|node_id| {
                let (tx, rx) = watch::channel(false);
                ((node_id, tx), (node_id, rx))
            })
            .unzip::<_, _, BTreeMap<_, _>, BTreeMap<_, _>>();
        let server_ready_tx = Arc::new(server_ready_tx);
        let server_ready_rx = Arc::new(server_ready_rx);
        let remote_nodes = Arc::new(remote_nodes);
        let (connector, _connections) = MockConnector::new({
            let remote_nodes = Arc::clone(&remote_nodes);
            let server_ready = Arc::clone(&server_ready_tx);
            move |node_id, router_builder| {
                let remote_node = remote_nodes
                    .iter()
                    .find(|remote_node| remote_node.node_id == node_id)
                    .unwrap_or_else(|| panic!("no query-test scanner server for {node_id}"));
                let server = RemoteQueryScannerServer::new(
                    remote_node.query_context.clone(),
                    remote_node.scanner_manager.clone(),
                    router_builder,
                );
                let server_ready = server_ready[&node_id].clone();
                TaskCenter::spawn_unmanaged(
                    TaskKind::DfScanner,
                    format!("query-test-scanner-server-{node_id}"),
                    async move {
                        let mut run = Box::pin(server.run());
                        let completed = poll_fn(|cx| match run.as_mut().poll(cx) {
                            Poll::Ready(result) => Poll::Ready(Some(result)),
                            Poll::Pending => Poll::Ready(None),
                        })
                        .await;
                        server_ready.send_replace(true);
                        match completed {
                            Some(result) => result,
                            None => run.await,
                        }
                    },
                )
                .expect("remote scanner server to start");
            }
        });
        let core_env = TestCoreEnvBuilder::with_transport_connector(ReadyConnector {
            inner: connector,
            server_ready: server_ready_rx,
        })
        .set_my_node_id(QUERY_COORDINATOR_NODE_ID)
        .set_nodes_config(nodes_config)
        .set_partition_table(partition_table.clone())
        .build()
        .await;
        TaskCenter::current()
            .cluster_state_updater()
            .upsert_node_state(QUERY_COORDINATOR_NODE_ID, NodeState::Alive);

        let remote_scans = Arc::new(RwLock::new(Vec::new()));
        let remote_scanner = Arc::new(RecordingRemoteScannerService {
            inner: crate::remote_query_scanner_client::create_remote_scanner_service(
                core_env.networking.clone(),
            ),
            scans: Arc::clone(&remote_scans),
        });
        let scanner_manager = RemoteScannerManager::new(
            remote_scanner,
            Arc::new(RemotePartitionLocator {
                owners: Arc::clone(&owners),
            }),
            core_env.metadata.clone(),
        );
        let coordinator_manager = PartitionStoreManager::create(true)
            .await
            .expect("DB creation succeeds");
        let query_context = QueryContext::with_user_tables(
            &QueryEngineOptions::default(),
            FixedPartitionSelector::new(partitions),
            coordinator_manager,
            Some(status),
            Live::from_value(schemas),
            scanner_manager,
            core_env.metadata_store_client,
            None,
        )
        .await
        .unwrap();

        Self {
            partition_stores,
            partition_table,
            partition_owners: owners,
            query_context,
            remote_scans,
        }
    }

    pub(crate) fn partition_table(&self) -> &PartitionTable {
        &self.partition_table
    }

    pub(crate) fn partition_stores_mut(&mut self) -> &mut BTreeMap<PartitionId, PartitionStore> {
        &mut self.partition_stores
    }

    pub(crate) fn clear_remote_scans(&self) {
        self.remote_scans.write().unwrap().clear();
    }

    pub(crate) fn remote_scans(&self) -> Vec<RemoteScan> {
        self.remote_scans.read().unwrap().clone()
    }

    pub(crate) fn remote_owner(&self, partition_id: PartitionId) -> Option<NodeId> {
        self.partition_owners
            .get(&partition_id)
            .copied()
            .map(NodeId::from)
    }

    pub(crate) async fn execute(
        &self,
        sql: impl AsRef<str> + Send,
    ) -> Result<crate::context::QueryResult, crate::context::QueryError> {
        self.query_context.execute(sql.as_ref()).await
    }

    pub(crate) async fn explain_analyze_tree(
        &self,
        sql: impl AsRef<str> + Send,
    ) -> Result<String, crate::context::QueryError> {
        let session = self.query_context.as_ref();
        let state = session.state();
        let statement =
            state.sql_to_statement(sql.as_ref(), &datafusion::config::Dialect::PostgreSQL)?;
        let logical_plan = state.statement_to_plan(statement).await?;
        let dataframe = session.execute_logical_plan(logical_plan).await?;
        let task_context = Arc::new(dataframe.task_ctx());
        let physical_plan = dataframe.create_physical_plan().await?;

        collect(Arc::clone(&physical_plan), task_context).await?;

        let analyzed_plan = DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
            .set_tree_maximum_render_width(0);
        Ok(format!(
            "{}\n\nMetrics:\n{}",
            analyzed_plan.tree_render(),
            analyzed_plan.indent(false),
        ))
    }
}

// --- Matchers for rows
struct RecordBatchRowNamedColumnMatcher<InnerMatcher, F, T> {
    row: usize,
    column: String,
    f: F,
    t_data: PhantomData<T>,
    inner: InnerMatcher,
}

impl<InnerMatcher, F, T> Matcher for RecordBatchRowNamedColumnMatcher<InnerMatcher, F, T>
where
    F: Fn(&ArrayRef, usize) -> Option<T>,
    InnerMatcher: Matcher<ActualT = T>,
{
    type ActualT = RecordBatch;

    fn matches(&self, actual: &Self::ActualT) -> MatcherResult {
        let column = actual.column_by_name(&self.column);
        if column.is_none() {
            return MatcherResult::NoMatch;
        }

        if let Some(val) = (self.f)(column.unwrap(), self.row) {
            self.inner.matches(&val)
        } else {
            MatcherResult::NoMatch
        }
    }

    fn describe(&self, matcher_result: MatcherResult) -> String {
        match matcher_result {
            MatcherResult::Match => format!(
                "contains row {} and column '{}' which {:?}",
                self.row,
                self.column,
                self.inner.describe(MatcherResult::Match)
            ),
            MatcherResult::NoMatch => format!(
                "doesn't contain row {} and column '{}' which {:?}",
                self.row,
                self.column,
                self.inner.describe(MatcherResult::NoMatch)
            ),
        }
    }
}

pub fn row_column_matcher<T>(
    row: usize,
    column: &str,
    extractor: impl Fn(&ArrayRef, usize) -> Option<T>,
    inner: impl Matcher<ActualT = T>,
) -> impl Matcher<ActualT = RecordBatch> {
    RecordBatchRowNamedColumnMatcher {
        row,
        column: column.to_string(),
        f: extractor,
        inner,
        t_data: Default::default(),
    }
}

#[macro_export]
macro_rules! row {
    ($idx:expr, {$($column:literal => $arrayty:ty: $matcher:expr),* $(,)?}) => {
        googletest::prelude::all!(
            $(
                $crate::mocks::row_column_matcher($idx, $column, |column, row| {
                    use datafusion::arrow::array::Array;

                    let column = column.as_any().downcast_ref::<$arrayty>()
                        .expect(concat!("Downcast ref to ", stringify!($arrayty)));
                    if (column.len() <= row) {
                        return None
                    }

                    Some(column.value(row).to_owned())
                }, $matcher)
            ),*
        )
    };
}
