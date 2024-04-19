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
use restate_schema_api::component::mocks::MockComponentMetadataResolver;
use restate_schema_api::component::{ComponentMetadata, ComponentMetadataResolver};
use restate_schema_api::deployment::mocks::MockDeploymentMetadataRegistry;
use restate_schema_api::deployment::{Deployment, DeploymentResolver};
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::arc_util::Constant;
use restate_types::config::{CommonOptions, QueryEngineOptions, WorkerOptions};
use restate_types::identifiers::{ComponentRevision, DeploymentId};
use restate_types::invocation::ComponentType;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;

#[derive(Default, Clone, Debug)]
pub(crate) struct MockSchemas(
    pub(crate) MockComponentMetadataResolver,
    pub(crate) MockDeploymentMetadataRegistry,
);

impl ComponentMetadataResolver for MockSchemas {
    fn resolve_latest_component(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentMetadata> {
        self.0.resolve_latest_component(component_name)
    }

    fn resolve_latest_component_type(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentType> {
        self.0.resolve_latest_component_type(component_name)
    }

    fn list_components(&self) -> Vec<ComponentMetadata> {
        self.0.list_components()
    }
}

impl DeploymentResolver for MockSchemas {
    fn resolve_latest_deployment_for_component(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        self.1
            .resolve_latest_deployment_for_component(component_name)
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        self.1.get_deployment(deployment_id)
    }

    fn get_deployment_and_components(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ComponentMetadata>)> {
        self.1.get_deployment_and_components(deployment_id)
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ComponentRevision)>)> {
        self.1.get_deployments()
    }
}

pub(crate) struct MockQueryEngine(RocksDBStorage, QueryContext);

impl MockQueryEngine {
    pub async fn create_with(
        status: impl StatusHandle + Send + Sync + Debug + Clone + 'static,
        schemas: impl DeploymentResolver
            + ComponentMetadataResolver
            + Send
            + Sync
            + Debug
            + Clone
            + 'static,
    ) -> (Self, impl Future<Output = ()>) {
        // Prepare Rocksdb
        task_center().run_in_scope_sync("db-manager-init", None, || {
            RocksDbManager::init(Constant::new(CommonOptions::default()))
        });
        let worker_options = WorkerOptions::default();
        let (rocksdb, writer) = RocksDBStorage::open(
            worker_options.data_dir(),
            Constant::new(worker_options.rocksdb),
        )
        .await
        .expect("RocksDB storage creation should succeed");
        let (signal, watch) = drain::channel();
        let writer_join_handle = writer.run(watch);

        let query_engine = Self(
            rocksdb.clone(),
            QueryContext::from_options(&QueryEngineOptions::default(), rocksdb, status, schemas)
                .unwrap(),
        );

        // Return shutdown future
        (query_engine, async {
            signal.drain().await;
            writer_join_handle.await.unwrap().unwrap();
        })
    }

    pub async fn create() -> (Self, impl Future<Output = ()>) {
        Self::create_with(MockStatusHandle::default(), MockSchemas::default()).await
    }

    pub fn rocksdb_mut(&mut self) -> &mut RocksDBStorage {
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
