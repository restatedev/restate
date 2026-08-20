// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;

use googletest::matcher::{Matcher, MatcherResult};
use serde_json::Value;

use restate_metadata_store::MetadataStoreClient;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_types::NodeId;
use restate_types::config::QueryEngineOptions;
use restate_types::deployment::{DeploymentAddress, Headers};
use restate_types::errors::GenericError;
use restate_types::identifiers::{DeploymentId, PartitionId, ServiceRevision};
use restate_types::live::Live;
use restate_types::net::address::{AdvertisedAddress, HttpIngressPort};
use restate_types::net::remote_query_scanner::RemoteQueryScannerOpen;
use restate_types::partition_table::Partition;
use restate_types::schema::deployment::test_util::MockDeploymentMetadataRegistry;
use restate_types::schema::deployment::{Deployment, DeploymentResolver};
use restate_types::schema::service::test_util::MockServiceMetadataResolver;
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};
use restate_types::sharding::KeyRange;
use restate_worker_api::invoker::{InvocationStatusReport, StatusHandle};
use restate_worker_api::{SchedulerStatusEntry, UserLimitCounterEntry};

use super::context::QueryContext;
use crate::context::{PartitionLeaderStatusHandle, SelectPartitions};
use crate::remote_query_scanner_client::{RemoteScanner, RemoteScannerService};
use crate::remote_query_scanner_manager::{
    PartitionLocation, PartitionLocator, PartitionUnavailable, RemoteScannerManager,
};

#[derive(Debug, Clone, Default)]
pub struct MockStatusHandle(Vec<InvocationStatusReport>);

impl MockStatusHandle {
    pub fn with(mut self, invocation_status_report: InvocationStatusReport) -> Self {
        self.0.push(invocation_status_report);
        self
    }
}

impl StatusHandle for MockStatusHandle {
    type Iterator = std::vec::IntoIter<InvocationStatusReport>;

    async fn read_status(&self, _keys: KeyRange) -> Self::Iterator {
        self.0.clone().into_iter()
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
struct MockPartitionSelector(Arc<Vec<(PartitionId, Partition)>>);

impl Default for MockPartitionSelector {
    /// The single full-range partition that [`MockQueryEngine`] opens a store for.
    fn default() -> Self {
        let id = PartitionId::MIN;
        Self(Arc::new(vec![(id, Partition::new(id, KeyRange::FULL))]))
    }
}

#[async_trait]
impl SelectPartitions for MockPartitionSelector {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(self.0.as_ref().clone())
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

/// Reports every partition as local except those explicitly marked unroutable, which fail
/// exactly as `MetadataAwarePartitionLocator` does for a partition with no leader and no
/// alive replica.
///
/// Partitions in `unroutable_after_first_lookup` answer the plan-time availability check
/// and then fail, reproducing a partition that loses its last replica between planning and
/// execution.
#[derive(Default)]
struct UnroutablePartitionLocator {
    unroutable: HashSet<PartitionId>,
    unroutable_after_first_lookup: HashSet<PartitionId>,
    lookups: Mutex<HashMap<PartitionId, usize>>,
}

impl PartitionLocator for UnroutablePartitionLocator {
    fn get_partition_target_node(
        &self,
        partition_id: PartitionId,
    ) -> Result<PartitionLocation, PartitionUnavailable> {
        let unavailable = || {
            Err(PartitionUnavailable {
                partition_id,
                reason: "no known leader and no alive node in its replica-set".into(),
            })
        };

        if self.unroutable.contains(&partition_id) {
            return unavailable();
        }

        if self.unroutable_after_first_lookup.contains(&partition_id) {
            let mut lookups = self.lookups.lock().expect("lock is never poisoned");
            let seen = lookups.entry(partition_id).or_default();
            *seen += 1;
            if *seen > 1 {
                return unavailable();
            }
        }

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
        Self::create_inner(
            status,
            schemas,
            MockPartitionSelector::default(),
            UnroutablePartitionLocator::default(),
        )
        .await
    }

    async fn create_inner(
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
        partition_selector: MockPartitionSelector,
        locator: UnroutablePartitionLocator,
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

        Self(
            manager.clone(),
            partition_store,
            QueryContext::with_user_tables(
                &QueryEngineOptions::default(),
                partition_selector,
                manager,
                Some(status),
                Live::from_value(schemas),
                RemoteScannerManager::new(
                    Arc::new(NoopSvc),
                    Arc::new(locator) as Arc<dyn PartitionLocator>,
                    // The mock locator only ever returns `Local` or an error, so the
                    // manager never invokes `allocate_scanner_id` and never reads
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

    /// An engine whose partition table holds `PartitionId::MIN` — the only one with a real
    /// store — plus `extra_partitions`. Ids listed in `unroutable` fail routing; the rest
    /// route locally but have no store, which is the other way a partition scan can fail.
    pub async fn create_with_partitions(
        extra_partitions: impl IntoIterator<Item = PartitionId>,
        unroutable: impl IntoIterator<Item = PartitionId>,
    ) -> Self {
        Self::create_with_locator(
            extra_partitions,
            UnroutablePartitionLocator {
                unroutable: unroutable.into_iter().collect(),
                ..Default::default()
            },
        )
        .await
    }

    /// Like [`Self::create_with_partitions`], but the listed partitions pass the plan-time
    /// availability check and only fail when the scan is actually opened.
    pub async fn create_with_partitions_lost_after_planning(
        extra_partitions: impl IntoIterator<Item = PartitionId>,
        lost: impl IntoIterator<Item = PartitionId>,
    ) -> Self {
        Self::create_with_locator(
            extra_partitions,
            UnroutablePartitionLocator {
                unroutable_after_first_lookup: lost.into_iter().collect(),
                ..Default::default()
            },
        )
        .await
    }

    async fn create_with_locator(
        extra_partitions: impl IntoIterator<Item = PartitionId>,
        locator: UnroutablePartitionLocator,
    ) -> Self {
        let mut partitions = vec![(
            PartitionId::MIN,
            Partition::new(PartitionId::MIN, KeyRange::FULL),
        )];
        partitions.extend(
            extra_partitions
                .into_iter()
                .map(|id| (id, Partition::new(id, KeyRange::FULL))),
        );

        Self::create_inner(
            MockStatusHandle::default(),
            MockSchemas::default(),
            MockPartitionSelector(Arc::new(partitions)),
            locator,
        )
        .await
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
