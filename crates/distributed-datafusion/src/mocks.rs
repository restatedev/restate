// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use futures::TryStreamExt;

use googletest::matcher::{Matcher, MatcherResult};
use serde_json::Value;

use restate_clock::MockClock;
use restate_metadata_store::MetadataStoreClient;
use restate_partition_store::{PartitionStore, PartitionStoreManager, QueryIoExecutor};
use restate_rocksdb::RocksDbManager;
use restate_storage_query_api::errors::QueryExecutionError;
use restate_storage_query_api::{
    QueryEngine, QueryOptions, QueryResult, QuerySession, SessionOptions,
};
use restate_storage_query_datafusion as v1;
use restate_types::GenerationalNodeId;
use restate_types::config::QueryEngineOptions;
use restate_types::deployment::{DeploymentAddress, Headers};
use restate_types::errors::GenericError;
use restate_types::identifiers::{DeploymentId, PartitionId, ServiceRevision};
use restate_types::live::Live;
use restate_types::net::address::{AdvertisedAddress, HttpIngressPort};
use restate_types::partition_table::Partition;
use restate_types::schema::deployment::{Deployment, DeploymentResolver};
use restate_types::schema::service::test_util::MockServiceMetadataResolver;
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};
use restate_types::sharding::KeyRange;

use super::context::DataFusionQueryEngine;
use crate::catalog::RegisterTable;
use crate::context::SelectPartitions;
use crate::local_scanners::{
    register_live_scanners, register_metadata_scanners, register_partition_scanners,
};
use crate::source_registry::{PartitionLocation, PartitionLocator, SourceRegistry};
use crate::{DataFusionEnv, UserTables};

/// A one-thread query IO pool for tests that read partition storage.
pub(crate) fn query_io() -> QueryIoExecutor {
    QueryIoExecutor::new(NonZeroUsize::MIN)
}

/// Metadata for query tests. Deployments carry their service names as listed by
/// [`DeploymentResolver::get_deployments`], the only deployment read query tables make.
#[derive(Default, Clone, Debug)]
pub(crate) struct MockSchemas {
    pub(crate) services: MockServiceMetadataResolver,
    pub(crate) deployments: Vec<(Deployment, Vec<(String, ServiceRevision)>)>,
}

impl ServiceMetadataResolver for MockSchemas {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        self.services.resolve_latest_service(service_name)
    }

    fn resolve_latest_service_openapi(
        &self,
        _: impl AsRef<str>,
        _ingress_address: AdvertisedAddress<HttpIngressPort>,
    ) -> Option<Value> {
        todo!()
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.services.list_services()
    }

    fn list_service_names(&self) -> Vec<String> {
        self.services.list_service_names()
    }
}

impl DeploymentResolver for MockSchemas {
    fn resolve_latest_deployment_for_service(&self, _: impl AsRef<str>) -> Option<Deployment> {
        unimplemented!("query tables only list deployments")
    }

    fn find_deployment(
        &self,
        _: &DeploymentAddress,
        _: &Headers,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        unimplemented!("query tables only list deployments")
    }

    fn get_deployment(&self, _: &DeploymentId) -> Option<Deployment> {
        unimplemented!("query tables only list deployments")
    }

    fn get_deployment_and_services(
        &self,
        _: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        unimplemented!("query tables only list deployments")
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        self.deployments.clone()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MockPartitionSelector;

#[async_trait]
impl SelectPartitions for MockPartitionSelector {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        let id = PartitionId::MIN;
        let partition = Partition::new(id, KeyRange::FULL);
        Ok(vec![(id, partition)])
    }
}

#[allow(dead_code)]
pub(crate) struct MockQueryEngine(
    Arc<PartitionStoreManager>,
    PartitionStore,
    Arc<QueryEngine>,
    SourceRegistry,
);

struct AlwaysLocalPartitionLocator;

impl PartitionLocator for AlwaysLocalPartitionLocator {
    fn get_partition_target_node(
        &self,
        _partition_id: PartitionId,
        _placement: crate::placement::PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Local)
    }
}

impl MockQueryEngine {
    pub async fn create_with(schemas: MockSchemas) -> Self {
        Self::create_with_catalog(schemas, |sources| {
            UserTables::new(MockPartitionSelector, sources).with_metadata()
        })
        .await
    }

    async fn create_with_catalog<K: RegisterTable>(
        schemas: MockSchemas,
        catalog: impl FnOnce(SourceRegistry) -> K,
    ) -> Self {
        let env = DataFusionEnv::from_options(&QueryEngineOptions::default())
            .unwrap()
            .with_mock_clock(MockClock::new())
            .unwrap();
        Self::create_with_catalog_and_env(schemas, catalog, env).await
    }

    /// Uses caller-supplied session defaults for differential execution tests.
    pub async fn create_with_env(env: DataFusionEnv) -> Self {
        Self::create_with_catalog_and_env(
            MockSchemas::default(),
            |sources| UserTables::new(MockPartitionSelector, sources),
            env,
        )
        .await
    }

    async fn create_with_catalog_and_env<K: RegisterTable>(
        schemas: MockSchemas,
        catalog: impl FnOnce(SourceRegistry) -> K,
        env: DataFusionEnv,
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

        // A single node: local partition reads, and unsharded reads resolve to it.
        let metadata_builder = restate_core::MetadataBuilder::default();
        let metadata = metadata_builder.to_metadata();
        restate_core::MetadataManager::new(metadata_builder, MetadataStoreClient::new_in_memory())
            .writer()
            .set_my_node_id(GenerationalNodeId::new(1, 1));
        let sources = SourceRegistry::new(
            Arc::new(AlwaysLocalPartitionLocator) as Arc<dyn PartitionLocator>,
            metadata,
            Default::default(),
        );
        register_partition_scanners(Arc::clone(&manager), crate::mocks::query_io(), &sources);
        register_live_scanners(&sources);
        register_metadata_scanners(
            Live::from_value(schemas),
            MetadataStoreClient::new_in_memory(),
            None,
            &sources,
        );
        env.register_tables(catalog(sources.clone())).await.unwrap();

        // Matches MockPartitionSelector's single partition
        Self(
            manager.clone(),
            partition_store,
            Arc::new(DataFusionQueryEngine::from_env(env, None)),
            sources,
        )
    }

    pub async fn create() -> Self {
        Self::create_with(MockSchemas::default()).await
    }

    pub fn partition_store(&mut self) -> &mut PartitionStore {
        &mut self.1
    }

    pub fn sources(&self) -> &SourceRegistry {
        &self.3
    }

    pub async fn execute(
        &self,
        sql: impl AsRef<str> + Send,
    ) -> Result<QueryResult, QueryExecutionError> {
        self.2
            .create_session(SessionOptions::default())
            .expect("mock session initializes")
            .execute(sql.as_ref(), QueryOptions {})
            .await
    }
}

/// One full-range partition on this node, for engines that read the same store.
#[derive(Debug, Clone)]
pub(crate) struct LocalPartition;

#[async_trait]
impl SelectPartitions for LocalPartition {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(vec![(
            PartitionId::MIN,
            Partition::new(PartitionId::MIN, KeyRange::FULL),
        )])
    }
}

#[async_trait]
impl v1::context::SelectPartitions for LocalPartition {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        SelectPartitions::get_live_partitions(self).await
    }
}

impl PartitionLocator for LocalPartition {
    fn get_partition_target_node(
        &self,
        _: PartitionId,
        _: crate::placement::PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Local)
    }
}

/// V1 and V2 sessions over the same partition store, both with batch size 1 so
/// results cross batch boundaries.
pub(crate) async fn differential_sessions(
    manager: &Arc<PartitionStoreManager>,
) -> [Arc<dyn QuerySession>; 2] {
    let metadata = restate_core::MetadataBuilder::default().to_metadata();
    let options = HashMap::from([("datafusion.execution.batch_size".to_owned(), "1".to_owned())]);
    let old = v1::DataFusionEnv::new(64 * 1024 * 1024, None, Some(2), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let old_sources =
        v1::remote_query_scanner_manager::RemoteScannerManager::local_only(metadata.clone());
    v1::local_scanners::register_partition_scanners(
        Arc::clone(manager),
        crate::mocks::query_io(),
        &old_sources,
    );
    v1::local_scanners::register_live_scanners(
        Arc::new(restate_worker_api::OfflinePartitionQueryAccess::new([(
            PartitionId::MIN,
            KeyRange::FULL,
        )])),
        &old_sources,
    );
    old.register_tables(v1::UserTables::new(LocalPartition, old_sources))
        .await
        .unwrap();
    let new = DataFusionEnv::new(64 * 1024 * 1024, None, Some(2), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let new_sources = SourceRegistry::new(Arc::new(LocalPartition), metadata, Default::default());
    register_partition_scanners(Arc::clone(manager), crate::mocks::query_io(), &new_sources);
    register_live_scanners(&new_sources);
    new.register_tables(UserTables::new(LocalPartition, new_sources))
        .await
        .unwrap();
    [
        v1::context::DataFusionQueryEngine::from_env(old, None)
            .create_session(SessionOptions::default())
            .unwrap(),
        DataFusionQueryEngine::from_env(new, None)
            .create_session(SessionOptions::default())
            .unwrap(),
    ]
}

/// All rows, each column rendered as text.
pub(crate) async fn rows(session: &dyn QuerySession, sql: &str) -> Vec<Vec<String>> {
    let batches: Vec<_> = session
        .execute(sql, QueryOptions {})
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column, row).unwrap())
                    .collect()
            })
        })
        .collect()
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
