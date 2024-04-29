// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::context::QueryContext;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use googletest::matcher::{Matcher, MatcherResult};
use restate_core::task_center;
use restate_invoker_api::status_handle::mocks::MockStatusHandle;
use restate_invoker_api::StatusHandle;
use restate_rocksdb::RocksDbManager;
use restate_schema_api::deployment::mocks::MockDeploymentMetadataRegistry;
use restate_schema_api::deployment::{Deployment, DeploymentResolver};
use restate_schema_api::service::mocks::MockServiceMetadataResolver;
use restate_schema_api::service::{ServiceMetadata, ServiceMetadataResolver};
use restate_storage_rocksdb::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_types::arc_util::Constant;
use restate_types::config::{CommonOptions, QueryEngineOptions, WorkerOptions};
use restate_types::identifiers::{DeploymentId, PartitionKey, ServiceRevision};
use restate_types::invocation::ServiceType;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::RangeInclusive;

#[derive(Default, Clone, Debug)]
pub(crate) struct MockSchemas(
    pub(crate) MockServiceMetadataResolver,
    pub(crate) MockDeploymentMetadataRegistry,
);

impl ServiceMetadataResolver for MockSchemas {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        self.0.resolve_latest_service(service_name)
    }

    fn resolve_latest_service_type(&self, service_name: impl AsRef<str>) -> Option<ServiceType> {
        self.0.resolve_latest_service_type(service_name)
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.0.list_services()
    }
}

impl DeploymentResolver for MockSchemas {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        self.1.resolve_latest_deployment_for_service(service_name)
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

pub(crate) struct MockQueryEngine(PartitionStore, QueryContext);

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
        task_center().run_in_scope_sync("db-manager-init", None, || {
            RocksDbManager::init(Constant::new(CommonOptions::default()))
        });
        let worker_options = WorkerOptions::default();
        let manager = PartitionStoreManager::create(
            Constant::new(worker_options.storage.clone()),
            Constant::new(worker_options.storage.rocksdb.clone()),
            &[(0, RangeInclusive::new(0, PartitionKey::MAX))],
        )
        .await
        .expect("DB creation succeeds");
        let rocksdb = manager
            .open_partition_store(
                0,
                RangeInclusive::new(0, PartitionKey::MAX),
                OpenMode::CreateIfMissing,
                &worker_options.storage.rocksdb,
            )
            .await
            .expect("column family is open");

        Self(
            rocksdb.clone(),
            QueryContext::from_options(&QueryEngineOptions::default(), rocksdb, status, schemas)
                .unwrap(),
        )
    }

    pub async fn create() -> Self {
        Self::create_with(MockStatusHandle::default(), MockSchemas::default()).await
    }

    pub fn rocksdb_mut(&mut self) -> &mut PartitionStore {
        &mut self.0
    }

    pub async fn execute(
        &self,
        sql: &str,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        self.1.execute(sql).await
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
