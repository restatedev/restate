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
use datafusion::error::DataFusionError;
use datafusion::execution::context::SQLOptions;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use tokio::time::Instant;
use tracing::{info, instrument};

use restate_core::Metadata;
use restate_storage_query_api::errors::{QueryExecutionError, SessionError};
use restate_storage_query_api::{
    AdminUser, ClusterOperator, NodeWarnings, QueryEngine, QueryMetadata, QueryOptions,
    QueryResult, QuerySession, SessionOptions, SessionTable,
};
use restate_types::config::ThrottlingOptions;
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_util_string::ReString;

use crate::catalog::{ClusterTables, RegisterTable, TableInventoryBuilder, UserTables};
use crate::diagnostics::QueryDiagnosticStream;
use crate::environment::DataFusionEnv;
use crate::sql::redact_statement;

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

/// Shared provider inventory, default SQL exposure, and session-admission policy.
pub struct DataFusionQueryEngine<T> {
    env: DataFusionEnv,
    tables: Vec<SessionTable>,
    rate_limiter: Option<RateLimiter>,
    _phantom: PhantomData<T>,
}

pub struct RestateQuerySession<T> {
    ctx: SessionContext,
    session_id: ReString,
    opts: SessionOptions,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + 'static> QueryEngine<T> for DataFusionQueryEngine<T> {
    fn create_session(
        &self,
        opts: SessionOptions,
    ) -> Result<Arc<dyn QuerySession<T>>, SessionError> {
        if let Some(limiter) = self.rate_limiter.as_ref() {
            limiter.try_consume_one()?;
        }
        let ctx = self
            .env
            .create_session(opts.tables.as_deref().unwrap_or(&self.tables))?;
        Ok(Arc::new(RestateQuerySession {
            session_id: ctx.session_id().into(),
            ctx,
            opts,
            _phantom: PhantomData,
        }))
    }
}

#[async_trait]
impl<T: Send + Sync> QuerySession<T> for RestateQuerySession<T> {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    #[instrument(target = "query_engine", level="debug", skip_all, fields(session = %self.session_id))]
    async fn execute(
        &self,
        sql: &str,
        _opts: QueryOptions,
    ) -> Result<QueryResult, QueryExecutionError> {
        let planning_started = Instant::now();
        let state = self.ctx.state();
        let statement = state.sql_to_statement(sql, &datafusion::config::Dialect::PostgreSQL)?;
        let redacted_sql = redact_statement(&statement);
        let plan = state.statement_to_plan(statement).await?;
        SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(self.opts.allow_statements)
            .verify_plan(&plan)?;
        let df = self.ctx.execute_logical_plan(plan).await?;
        let task_ctx = Arc::new(df.task_ctx());
        let physical_plan = df.create_physical_plan().await?;
        let metadata = QueryMetadata {
            session_id: self.session_id.clone(),
            headers: self.opts.headers.clone(),
            redacted_sql,
            planning_duration: planning_started.elapsed(),
        };
        info!(target: "query_engine", session = %metadata.session_id, headers = ?metadata.headers, query = %metadata.redacted_sql, "Executing query");
        let node_warnings = collect_node_warnings(&physical_plan);
        let execution_started = Instant::now();
        let stream = execute_stream(Arc::clone(&physical_plan), task_ctx)?;
        let (stream, diagnostics) = QueryDiagnosticStream::wrap(
            stream,
            physical_plan,
            node_warnings,
            planning_started,
            execution_started,
        );
        Ok(QueryResult {
            stream,
            metadata,
            diagnostics,
        })
    }
}

impl DataFusionQueryEngine<ClusterOperator> {
    /// Registers cluster tables and resolves unqualified queries in `restate.cluster`.
    pub async fn with_cluster_tables(
        env: DataFusionEnv,
        tables: ClusterTables,
    ) -> Result<Arc<dyn QueryEngine<ClusterOperator>>, BuildError> {
        let env = env.with_default_catalog_and_schema("restate", "cluster");
        Self::with_tables(env, None, tables).await
    }
}

impl<T: Send + Sync + 'static> DataFusionQueryEngine<T> {
    /// Registers providers once. Local source capabilities must already be registered separately.
    pub async fn with_tables<K: RegisterTable>(
        env: DataFusionEnv,
        rate_limit: Option<&ThrottlingOptions>,
        tables: K,
    ) -> Result<Arc<dyn QueryEngine<T>>, BuildError> {
        let mut inventory = TableInventoryBuilder::new(&env);
        tables.register(&mut inventory).await?;
        let tables = inventory.finish();
        Ok(Self::from_inventory(env, rate_limit, tables))
    }

    /// Creates an engine over an existing inventory. Unavailable selected identities are omitted.
    pub fn from_inventory(
        env: DataFusionEnv,
        rate_limit: Option<&ThrottlingOptions>,
        tables: Vec<SessionTable>,
    ) -> Arc<dyn QueryEngine<T>> {
        let rate_limiter = rate_limit
            .map(|limit| RateLimiter::new(gardal::Limit::from(limit.clone()), gardal::TokioClock));
        Arc::new(Self {
            env,
            tables,
            rate_limiter,
            _phantom: PhantomData,
        })
    }
}

impl DataFusionQueryEngine<AdminUser> {
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
    use std::time::Duration;

    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;
    use futures::{StreamExt, TryStreamExt};
    use tokio::sync::watch;

    use restate_core::MetadataKind;
    use restate_core::test_env::TestCoreEnv;
    use restate_storage_query_api::errors::SessionError;
    use restate_storage_query_api::{
        AdminUser, QueryEngine, QueryOperatorStats, QueryOptions, QueryStatus, SessionOptions,
        SessionTable,
    };
    use restate_types::Version;
    use restate_types::cluster::cluster_state::LegacyClusterState;

    use crate::catalog::{ClusterTables, UserTables};
    use crate::partition::schema::PartitionTable;
    use crate::remote_query_scanner_manager::RemoteScannerManager;

    use super::{DataFusionEnv, DataFusionQueryEngine, RateLimiter, SelectPartitionsFromMetadata};

    #[tokio::test(start_paused = true)]
    async fn diagnostics_follow_query_lifetime_independently_of_session() {
        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new()).unwrap();
        let engine = DataFusionQueryEngine::<AdminUser>::from_inventory(env, None, vec![]);
        let mut options = SessionOptions::default();
        options
            .headers
            .insert("x-restate-query-client", "ui".parse().unwrap());
        options
            .headers
            .insert("x-restate-query-origin", "built-in".parse().unwrap());
        let session = engine.create_session(options).unwrap();
        let other = engine.create_session(SessionOptions::default()).unwrap();
        assert_ne!(session.session_id(), other.session_id());
        let result = session
            .execute(
                "SELECT SUM(n) FROM (VALUES (1), (2), (3)) AS t(n)",
                QueryOptions {},
            )
            .await
            .unwrap();
        assert_eq!(result.metadata.session_id.as_str(), session.session_id());
        assert_eq!(
            result.metadata.redacted_sql.as_str(),
            "SELECT SUM(n) FROM (VALUES (?), (?), (?)) AS t (n)"
        );
        assert_eq!(result.metadata.headers["x-restate-query-client"], "ui");
        assert_eq!(
            result.metadata.headers["x-restate-query-origin"],
            "built-in"
        );
        let before = result.diagnostics.snapshot();
        assert_eq!(before.status, QueryStatus::Running);
        assert_eq!(before.output_rows, 0);
        tokio::time::advance(Duration::from_secs(2)).await;
        let running = result.diagnostics.snapshot();
        assert!(running.total_duration >= before.total_duration + Duration::from_secs(2));
        assert!(running.execution_duration >= before.execution_duration + Duration::from_secs(2));

        // Two executions on the same session must have separate progress.
        let cancelled = session.execute("SELECT 42", QueryOptions {}).await.unwrap();
        assert_eq!(result.metadata.session_id, cancelled.metadata.session_id);
        drop(session);
        drop(cancelled.stream);
        assert_eq!(
            cancelled.diagnostics.snapshot().status,
            QueryStatus::Cancelled
        );
        assert_eq!(result.diagnostics.snapshot().status, QueryStatus::Running);

        let mut stream = result.stream;
        let mut rows = 0;
        let mut batches = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows() as u64;
            batches += 1;
        }
        let after = result.diagnostics.snapshot();
        assert_eq!(after.status, QueryStatus::Completed);
        assert_eq!(after.output_rows, rows);
        assert_eq!(after.output_batches, batches);
        assert_eq!(rows, 1);
        assert_eq!(before.output_rows, 0);
        fn has_output_metrics(plan: &QueryOperatorStats) -> bool {
            plan.metrics
                .as_ref()
                .is_some_and(|metrics| metrics.output_rows().is_some_and(|n| n > 0))
                || plan.children.iter().any(has_output_metrics)
        }
        assert!(has_output_metrics(&result.diagnostics.plan_metrics()));
        assert!(
            after.total_duration >= result.metadata.planning_duration + after.execution_duration
        );
        drop(stream);
        tokio::time::advance(Duration::from_secs(1)).await;
        let dropped = result.diagnostics.snapshot();
        assert_eq!(dropped.status, QueryStatus::Completed);
        assert_eq!(dropped.execution_duration, after.execution_duration);
        assert_eq!(dropped.total_duration, after.total_duration);
    }

    #[restate_core::test]
    async fn cluster_namespace_preserves_unqualified_queries_and_user_catalog_isolation() {
        let core = TestCoreEnv::create_with_single_node(1, 1).await;
        core.metadata
            .wait_for_version(MetadataKind::PartitionTable, Version::MIN)
            .await
            .unwrap();
        core.metadata
            .wait_for_version(MetadataKind::Logs, Version::MIN)
            .await
            .unwrap();
        let partition_count = core.metadata.partition_table_snapshot().len();
        let log_count = core.metadata.logs_snapshot().iter().count();
        assert!(partition_count > 0 && log_count > 0);

        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new()).unwrap();
        let scanners = RemoteScannerManager::local_only(core.metadata.clone());
        let (_tx, cluster_state) = watch::channel(Arc::new(LegacyClusterState::empty()));
        let cluster = DataFusionQueryEngine::with_cluster_tables(
            env.clone(),
            ClusterTables::new(Default::default(), cluster_state.clone(), scanners.clone()),
        )
        .await
        .unwrap();
        let user = DataFusionQueryEngine::with_user_tables(
            env.clone(),
            None,
            UserTables::new(SelectPartitionsFromMetadata, scanners.clone()),
        )
        .await
        .unwrap();

        async fn query_value<T>(engine: &dyn QueryEngine<T>, sql: &str) -> String {
            let session = engine.create_session(SessionOptions::default()).unwrap();
            let batches: Vec<_> = session
                .execute(sql, QueryOptions {})
                .await
                .unwrap()
                .stream
                .try_collect()
                .await
                .unwrap();
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                1
            );
            array_value_to_string(batches[0].column(0), 0).unwrap()
        }

        for table in [
            "partitions",
            "cluster.partitions",
            "restate.cluster.partitions",
        ] {
            assert_eq!(
                query_value(cluster.as_ref(), &format!("SELECT COUNT(*) FROM {table}")).await,
                partition_count.to_string()
            );
        }
        for table in ["logs_tail_segments", "restate.cluster.logs_tail_segments"] {
            assert_eq!(
                query_value(cluster.as_ref(), &format!("SELECT COUNT(*) FROM {table}")).await,
                log_count.to_string()
            );
        }

        let schema_query = "SELECT DISTINCT table_schema FROM information_schema.tables \
            WHERE table_catalog = 'restate' AND table_schema <> 'information_schema'";
        assert_eq!(query_value(cluster.as_ref(), schema_query).await, "cluster");
        assert_eq!(query_value(user.as_ref(), schema_query).await, "public");

        let cluster_session = cluster.create_session(SessionOptions::default()).unwrap();
        for table in ["public.partitions", "restate.public.partitions"] {
            assert!(
                cluster_session
                    .execute(&format!("SELECT * FROM {table}"), QueryOptions {})
                    .await
                    .is_err()
            );
        }
        let user_session = user.create_session(SessionOptions::default()).unwrap();
        assert!(
            user_session
                .execute("SELECT * FROM restate.cluster.partitions", QueryOptions {})
                .await
                .is_err()
        );

        // Reuse the prebound view under an alias without exposing its base tables.
        let selection = vec![
            SessionTable::for_table::<PartitionTable>("restate.cluster.partitions"),
            SessionTable::new("logs_tail_segments", "restate.public.tail"),
        ];
        let mixed = env.create_session(&selection).unwrap();
        mixed
            .sql("CREATE VIEW logs AS SELECT -1 AS marker")
            .await
            .unwrap();
        assert!(mixed.table_provider("restate.cluster.logs").await.is_err());

        for (sql, expected) in [
            ("SELECT marker FROM logs", "-1".to_owned()),
            (
                "SELECT COUNT(*) FROM cluster.partitions",
                partition_count.to_string(),
            ),
            ("SELECT COUNT(*) FROM tail", log_count.to_string()),
        ] {
            let batches = mixed.sql(sql).await.unwrap().collect().await.unwrap();
            assert_eq!(
                array_value_to_string(batches[0].column(0), 0).unwrap(),
                expected,
                "{sql}"
            );
        }
        let selected = user
            .create_session(SessionOptions {
                tables: Some(selection),
                ..Default::default()
            })
            .unwrap();
        let batches: Vec<_> = selected
            .execute("SELECT COUNT(*) FROM tail", QueryOptions {})
            .await
            .unwrap()
            .stream
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            array_value_to_string(batches[0].column(0), 0).unwrap(),
            log_count.to_string()
        );
        assert!(
            selected
                .execute("SELECT * FROM state", QueryOptions {})
                .await
                .is_err()
        );
        assert!(
            user_session
                .execute("SELECT * FROM tail", QueryOptions {})
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sessions_share_providers_and_admission_but_reject_runtime_mutations() {
        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new()).unwrap();
        let first = env.build_session_state().unwrap();
        let second = env.build_session_state().unwrap();
        assert_ne!(first.session_id(), second.session_id());
        assert!(Arc::ptr_eq(first.runtime_env(), second.runtime_env()));
        let bootstrap = SessionContext::new_with_state(first);
        bootstrap
            .sql("CREATE VIEW total AS SELECT SUM(n) AS n FROM (VALUES (1), (2), (3)) AS t(n)")
            .await
            .unwrap();
        env.register_provider(
            "total".into(),
            bootstrap.table_provider("total").await.unwrap(),
        )
        .unwrap();
        drop(bootstrap);
        let mut manager = DataFusionQueryEngine::<AdminUser> {
            env,
            tables: vec![SessionTable::new("total", "total")],
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

        // The snapshot debugger explicitly enables statements on reusable sessions.
        // SET must persist there without changing defaults for another session.
        manager.rate_limiter = None;
        let session = manager
            .create_session(SessionOptions {
                allow_statements: true,
                ..Default::default()
            })
            .unwrap();
        let other = manager.create_session(SessionOptions::default()).unwrap();
        let setting = "SELECT value FROM information_schema.df_settings WHERE name = 'datafusion.execution.target_partitions'";
        let before = other.execute(setting, QueryOptions {}).await.unwrap();
        let before: Vec<_> = before.stream.try_collect().await.unwrap();
        let original = array_value_to_string(before[0].column(0), 0).unwrap();
        let changed = original.parse::<usize>().unwrap() + 1;
        let set = format!("SET datafusion.execution.target_partitions = {changed}");
        assert!(other.execute(&set, QueryOptions {}).await.is_err());
        session
            .execute(&set, QueryOptions {})
            .await
            .unwrap()
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(
            session
                .execute("DROP VIEW total", QueryOptions {})
                .await
                .is_err()
        );
        for (session, expected) in [(session, changed.to_string()), (other, original)] {
            let result = session.execute(setting, QueryOptions {}).await.unwrap();
            let batches: Vec<_> = result.stream.try_collect().await.unwrap();
            assert_eq!(
                array_value_to_string(batches[0].column(0), 0).unwrap(),
                expected
            );
        }
    }
}
