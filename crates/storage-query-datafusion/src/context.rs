// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use async_trait::async_trait;
use codederror::CodedError;
use datafusion::catalog::CatalogProviderList;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SQLOptions;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use tracing::{info, instrument};

use restate_core::Metadata;
use restate_storage_query_api::errors::{QueryExecutionError, SessionError};
use restate_storage_query_api::{
    AdminUser, ClusterOperator, NodeWarnings, QueryEngine, QueryOptions, QueryResult, QuerySession,
    SessionOptions,
};
use restate_types::config::ThrottlingOptions;
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;

use crate::catalog::{ClusterTables, RegisterTable, UserTables};
use crate::environment::DataFusionEnv;

type RateLimiter = gardal::SharedTokenBucket<gardal::TokioClock>;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error(transparent)]
    #[code(unknown)]
    Datafusion(#[from] DataFusionError),
}

#[async_trait]
pub trait SelectPartitions: Send + Sync + Debug + 'static {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError>;
}

/// Shared runtime, initialized catalog, and session-admission policy.
pub struct QuerySessionManager<T> {
    env: DataFusionEnv,
    catalog: Arc<dyn CatalogProviderList>,
    rate_limiter: Option<RateLimiter>,
    _phantom: PhantomData<T>,
}

pub struct RestateQuerySession<T> {
    ctx: SessionContext,
    _opts: SessionOptions,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + 'static> QueryEngine<T> for QuerySessionManager<T> {
    fn create_session(
        &self,
        opts: SessionOptions,
    ) -> Result<Arc<dyn QuerySession<T>>, SessionError> {
        if let Some(limiter) = self.rate_limiter.as_ref() {
            limiter.try_consume_one()?;
        }
        let mut state = self.env.build_session_state()?;
        state.register_catalog_list(Arc::clone(&self.catalog));
        Ok(Arc::new(RestateQuerySession {
            ctx: SessionContext::new_with_state(state),
            _opts: opts,
            _phantom: PhantomData,
        }))
    }
}

#[async_trait]
impl<T: Send + Sync> QuerySession<T> for RestateQuerySession<T> {
    #[instrument(target = "query_engine", level="debug", skip_all, fields(session = self.ctx.session_id()))]
    async fn execute(
        &self,
        sql: &str,
        _opts: QueryOptions,
    ) -> Result<QueryResult, QueryExecutionError> {
        let state = self.ctx.state();
        let statement = state.sql_to_statement(sql, &datafusion::config::Dialect::PostgreSQL)?;
        let plan = state.statement_to_plan(statement).await?;
        SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false)
            .verify_plan(&plan)?;
        let df = self.ctx.execute_logical_plan(plan).await?;
        let task_ctx = Arc::new(df.task_ctx());
        let physical_plan = df.create_physical_plan().await?;
        let node_warnings = collect_node_warnings(&physical_plan);
        info!(target: "query_engine", "Executing query: {sql}");
        let stream = execute_stream(physical_plan, task_ctx)?;
        Ok(QueryResult {
            stream,
            node_warnings,
        })
    }
}

impl QuerySessionManager<ClusterOperator> {
    pub async fn with_cluster_tables(
        env: DataFusionEnv,
        tables: ClusterTables,
    ) -> Result<Arc<dyn QueryEngine<ClusterOperator>>, BuildError> {
        Self::with_tables(env, None, tables).await
    }
}

impl<T: Send + Sync + 'static> QuerySessionManager<T> {
    /// Registers a catalog once. Local source capabilities must already be registered separately.
    pub async fn with_tables<K: RegisterTable>(
        env: DataFusionEnv,
        rate_limit: Option<&ThrottlingOptions>,
        tables: K,
    ) -> Result<Arc<dyn QueryEngine<T>>, BuildError> {
        let state = env.build_session_state()?;
        let catalog = Arc::clone(state.catalog_list());
        let bootstrap = SessionContext::new_with_state(state);
        tables.register(&bootstrap).await?;
        let rate_limiter = rate_limit
            .map(|limit| RateLimiter::new(gardal::Limit::from(limit.clone()), gardal::TokioClock));
        Ok(Arc::new(Self {
            env,
            catalog,
            rate_limiter,
            _phantom: PhantomData,
        }))
    }
}

impl QuerySessionManager<AdminUser> {
    pub async fn with_user_tables(
        env: DataFusionEnv,
        rate_limit: Option<&ThrottlingOptions>,
        tables: UserTables<impl SelectPartitions + Clone, impl RegisterTable>,
    ) -> Result<Arc<dyn QueryEngine<AdminUser>>, BuildError> {
        Self::with_tables(env, rate_limit, tables).await
    }
}

fn collect_node_warnings(plan: &Arc<dyn ExecutionPlan>) -> Vec<NodeWarnings> {
    use crate::node_fan_out::NodeFanOutExecutionPlan;
    let mut warnings = Vec::new();
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if let Some(fan_out) = node.downcast_ref::<NodeFanOutExecutionPlan>() {
            warnings.push(fan_out.node_warnings().clone());
        }
        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }
    warnings
}

#[derive(Clone, derive_more::Debug)]
pub struct SelectPartitionsFromMetadata;

#[async_trait]
impl SelectPartitions for SelectPartitionsFromMetadata {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(Metadata::with_current(|m| {
            m.partition_table_ref()
                .iter()
                .map(|(a, b)| (*a, b.clone()))
                .collect()
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;

    use restate_storage_query_api::errors::SessionError;
    use restate_storage_query_api::{AdminUser, QueryEngine, QueryOptions, SessionOptions};

    use super::{DataFusionEnv, QuerySessionManager, RateLimiter};

    #[tokio::test(start_paused = true)]
    async fn sessions_share_catalog_and_admission_but_reject_runtime_mutations() {
        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new()).unwrap();
        let first = env.build_session_state().unwrap();
        let second = env.build_session_state().unwrap();
        assert_ne!(first.session_id(), second.session_id());
        assert!(Arc::ptr_eq(first.runtime_env(), second.runtime_env()));
        let catalog = Arc::clone(first.catalog_list());
        let bootstrap = SessionContext::new_with_state(first);
        bootstrap
            .sql("CREATE VIEW total AS SELECT SUM(n) AS n FROM (VALUES (1), (2), (3)) AS t(n)")
            .await
            .unwrap();
        drop(bootstrap);
        let manager = QuerySessionManager::<AdminUser> {
            env,
            catalog,
            rate_limiter: Some(RateLimiter::new(
                gardal::Limit::per_hour(NonZeroU32::new(1).unwrap())
                    .with_burst(NonZeroU32::new(2).unwrap()),
                gardal::TokioClock,
            )),
            _phantom: std::marker::PhantomData,
        };
        let a = manager.create_session(SessionOptions::default()).unwrap();
        let b = manager.create_session(SessionOptions::default()).unwrap();
        assert!(matches!(
            manager.create_session(SessionOptions::default()),
            Err(SessionError::RateLimited(_))
        ));
        for sql in [
            "SET datafusion.runtime.memory_limit = '1G'",
            "RESET datafusion.runtime.memory_limit",
            "DROP VIEW total",
        ] {
            assert!(a.execute(sql, QueryOptions {}).await.is_err(), "{sql}");
        }
        for session in [a, b] {
            let result = session
                .execute("SELECT n FROM total", QueryOptions {})
                .await
                .unwrap();
            drop(session);
            let batches: Vec<_> = result.stream.try_collect().await.unwrap();
            assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), "6");
        }
    }
}
