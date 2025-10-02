// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use super::context::QueryContext;
use crate::context::SelectPartitions;
use crate::remote_query_scanner_client::{RemoteScanner, RemoteScannerService};
use crate::remote_query_scanner_manager::{
    PartitionLocation, PartitionLocator, RemoteScannerManager,
};
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use googletest::matcher::{Matcher, MatcherResult};
use restate_invoker_api::StatusHandle;
use restate_invoker_api::status_handle::test_util::MockStatusHandle;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_types::NodeId;
use restate_types::config::QueryEngineOptions;
use restate_types::deployment::{DeploymentAddress, Headers};
use restate_types::errors::GenericError;
use restate_types::identifiers::{DeploymentId, PartitionId, PartitionKey, ServiceRevision};
use restate_types::live::Live;
use restate_types::net::remote_query_scanner::RemoteQueryScannerOpen;
use restate_types::partition_table::Partition;
use restate_types::schema::deployment::test_util::MockDeploymentMetadataRegistry;
use restate_types::schema::deployment::{Deployment, DeploymentResolver};
use restate_types::schema::service::test_util::MockServiceMetadataResolver;
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};
use serde_json::Value;

#[derive(Default, Clone, Debug)]
pub(crate) struct MockSchemas(
    pub(crate) MockServiceMetadataResolver,
    pub(crate) MockDeploymentMetadataRegistry,
);

impl ServiceMetadataResolver for MockSchemas {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        self.0.resolve_latest_service(service_name)
    }

    fn resolve_latest_service_openapi(&self, _: impl AsRef<str>) -> Option<Value> {
        todo!()
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.0.list_services()
    }

    fn list_service_names(&self) -> Vec<String> {
        self.0.list_service_names()
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
        _: &DeploymentAddress,
        _: &Headers,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        None
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
        let partition_range = 0..=PartitionKey::MAX;
        let partition = Partition::new(id, partition_range);
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
        status: impl StatusHandle + Send + Sync + Debug + Clone + 'static,
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
        let manager = PartitionStoreManager::create()
            .await
            .expect("DB creation succeeds");
        let partition_store = manager
            .open(
                &Partition::new(PartitionId::MIN, PartitionKey::MIN..=PartitionKey::MAX),
                None,
            )
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
                RemoteScannerManager::new(Arc::new(NoopSvc), Arc::new(AlwaysLocalPartitionLocator)),
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
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
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

pub fn row_column<T>(
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
                $crate::mocks::row_column($idx, $column, |column, row| {
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
