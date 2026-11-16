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
use std::sync::Arc;

use async_trait::async_trait;
use codederror::CodedError;
use datafusion::common::display::PlanType;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SQLOptions;
use datafusion::logical_expr::{LogicalPlan, Statement};
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use tokio::time::Instant;
use tracing::{info, instrument};

use restate_core::Metadata;
use restate_storage_query_api::errors::{QueryExecutionError, SessionError};
use restate_storage_query_api::{
    NodeWarnings, QueryEngine, QueryEngineVersion, QueryMetadata, QueryOptions, QueryResult,
    QuerySession, SessionOptions,
};
use restate_types::config::{Configuration, ThrottlingOptions};
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_util_sql::redact_statement;
use restate_util_string::ReString;

use crate::QuerySource;
use crate::diagnostics::QueryDiagnosticStream;
use crate::environment::{DataFusionEnv, QueryClock};

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
pub struct DataFusionQueryEngine {
    env: DataFusionEnv,
    rate_limiter: Option<RateLimiter>,
}

pub struct RestateQuerySession {
    ctx: SessionContext,
    session_id: ReString,
    query_clock: Arc<QueryClock>,
    opts: SessionOptions,
}

impl DataFusionQueryEngine {
    fn create_session(
        &self,
        mut opts: SessionOptions,
    ) -> Result<Arc<dyn QuerySession>, SessionError> {
        if opts.apply_rate_limit
            && let Some(limiter) = self.rate_limiter.as_ref()
        {
            limiter.try_consume_one()?;
        }
        let config = Configuration::pinned();
        if config.common.disable_config_sql_table {
            opts.excluded_tables
                .push(crate::tables::config::ConfigTable::identity());
        }
        // The service index's definition isn't in this build yet (#5856).
        opts.excluded_tables
            .push(crate::tables::index::EntryByServiceTable::identity());
        // Hiding an index table hides it from SQL only: the planner still reads
        // the index for queries on its primary table.
        if !config.common.experimental.is_query_index_tables_enabled() {
            opts.excluded_tables
                .push(crate::tables::index::EntryByQueueTable::identity());
        }
        if !config.common.experimental.is_query_stats_tables_enabled() {
            use crate::tables::stats::deployment_stats::schema::SysDeploymentStatsTable;
            use crate::tables::stats::service_stats::schema::SysServiceStatsTable;
            use crate::tables::stats::virtual_object_stats::schema::SysVirtualObjectStatsTable;
            opts.excluded_tables.extend([
                SysServiceStatsTable::identity(),
                SysDeploymentStatsTable::identity(),
                SysVirtualObjectStatsTable::identity(),
            ]);
        }
        let ctx = self.env.create_session(&opts)?;
        Ok(Arc::new(RestateQuerySession {
            session_id: ctx.session_id().into(),
            query_clock: Arc::clone(self.env.query_clock()),
            ctx,
            opts,
        }))
    }
}

#[async_trait]
impl QuerySession for RestateQuerySession {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    #[instrument(target = "query_engine", level="debug", skip_all, fields(session = %self.session_id, query_ts))]
    async fn execute(
        &self,
        sql: &str,
        _opts: QueryOptions,
    ) -> Result<QueryResult, QueryExecutionError> {
        let planning_started = Instant::now();
        let query_ts = self.query_clock.next();
        tracing::Span::current().record("query_ts", query_ts.as_u64());
        // Attach the timestamp to this execution's state, leaving the reusable
        // session untouched. Planning and task contexts inherit the typed extension.
        let mut state = self.ctx.state();
        state.config_mut().set_extension(Arc::new(query_ts));
        let statement = state.sql_to_statement(sql, &datafusion::config::Dialect::PostgreSQL)?;
        let redacted_sql = redact_statement(&statement);
        let plan = state.statement_to_plan(statement).await?;
        SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(self.opts.allow_statements)
            .verify_plan(&plan)?;
        // As in V1: session mutations persist for the session's later queries.
        // Queries, including EXECUTE, keep their isolated execution timestamp.
        let ctx = match &plan {
            LogicalPlan::Statement(
                Statement::SetVariable(_)
                | Statement::ResetVariable(_)
                | Statement::Prepare(_)
                | Statement::Deallocate(_),
            ) => self.ctx.clone(),
            _ => SessionContext::new_with_state(state),
        };
        let df = ctx.execute_logical_plan(plan).await?;
        let task_ctx = df.task_ctx();
        let physical_plan = compact_explain(df.create_physical_plan().await?);
        let metadata = QueryMetadata {
            session_id: self.session_id.clone(),
            query_ts,
            headers: self.opts.headers.clone(),
            redacted_sql,
            planning_duration: planning_started.elapsed(),
        };
        info!(target: "query_engine", session = %metadata.session_id, query_ts = metadata.query_ts.as_u64(), headers = ?metadata.headers, query = %metadata.redacted_sql, "Executing query");
        let node_warnings = collect_node_warnings(&physical_plan);
        let mut config = task_ctx.session_config().clone();
        config.set_extension(Arc::new(metadata.clone()));
        let task_ctx = Arc::new(task_ctx.with_session_config(config));
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

/// Keep verbose optimizer steps without DataFusion's additional full-schema dumps.
/// Explicit `datafusion.explain.show_schema` still renders schemas in the main plans.
fn compact_explain(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let Some(explain) = plan.downcast_ref::<ExplainExec>() else {
        return plan;
    };
    Arc::new(ExplainExec::new(
        explain.schema(),
        explain
            .stringified_plans()
            .iter()
            .filter(|plan| {
                !matches!(
                    plan.plan_type,
                    PlanType::InitialPhysicalPlanWithSchema | PlanType::FinalPhysicalPlanWithSchema
                )
            })
            .cloned()
            .collect(),
        explain.verbose(),
    ))
}

impl DataFusionQueryEngine {
    /// Creates an engine over the environment's shared inventory.
    /// Table registration is independent; each session selects its own SQL exposure.
    pub fn from_env(env: DataFusionEnv, rate_limit: Option<&ThrottlingOptions>) -> QueryEngine {
        let rate_limiter = rate_limit
            .map(|limit| RateLimiter::new(gardal::Limit::from(limit.clone()), gardal::TokioClock));
        Self::with_rate_limiter(env, rate_limiter)
    }

    /// The engine over `env`, admitting sessions through `rate_limiter`. Node setup
    /// passes V1's limiter, so both engines share one `/query` budget.
    pub fn with_rate_limiter(
        env: DataFusionEnv,
        rate_limiter: Option<gardal::SharedTokenBucket<gardal::TokioClock>>,
    ) -> QueryEngine {
        let engine = Self { env, rate_limiter };
        QueryEngine::new(QueryEngineVersion::V2, move |opts| {
            engine.create_session(opts)
        })
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
    use std::collections::{BTreeSet, HashMap};
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::{RecordBatch, StringArray, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::PartitionStream;
    use datafusion::prelude::SessionContext;
    use futures::{StreamExt, TryStreamExt};

    use restate_clock::time::MillisSinceEpoch;
    use restate_clock::{MockClock, UniqueTimestamp};
    use restate_core::test_env::TestCoreEnv;
    use restate_metadata_store::MetadataStoreClient;
    use restate_storage_query_api::errors::SessionError;
    use restate_storage_query_api::{
        QueryEngine, QueryEngineVersion, QueryOperatorStats, QueryOptions, QuerySession,
        QueryStatus, SessionOptions, SessionTable,
    };
    use restate_types::live::Live;

    use crate::QuerySource;
    use crate::catalog::{ClusterTables, TableInventoryBuilder, UserTables};
    use crate::local_scanners::register_metadata_scanners;
    use crate::mocks::MockSchemas;
    use crate::source_registry::SourceRegistry;
    use crate::tables::bifrost_read_stream::BifrostReadStreamsTable;

    use super::{DataFusionEnv, DataFusionQueryEngine, RateLimiter, SelectPartitionsFromMetadata};

    #[derive(Debug)]
    struct QueryTimestampSource(SchemaRef);

    impl PartitionStream for QueryTimestampSource {
        fn schema(&self) -> &SchemaRef {
            &self.0
        }

        fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
            let schema = Arc::clone(&self.0);
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::once(async move {
                    RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(StringArray::from(vec![context.session_id()])),
                            Arc::new(UInt64Array::from(vec![
                                context
                                    .session_config()
                                    .get_extension::<UniqueTimestamp>()
                                    .expect("native query timestamp")
                                    .as_u64(),
                            ])),
                        ],
                    )
                    .map_err(Into::into)
                }),
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_timestamps_isolate_concurrent_executions_and_reach_source_tasks() {
        let physical_time = MillisSinceEpoch::new(1_700_000_000_000);
        let mock_clock = MockClock::with_timestamp(physical_time);
        let env = DataFusionEnv::new(10 * 1024 * 1024, None, Some(2), &HashMap::new())
            .unwrap()
            .with_mock_clock(mock_clock.clone())
            .unwrap();
        let clock = Arc::clone(env.query_clock());
        let other_env = env.clone();
        assert!(Arc::ptr_eq(env.query_clock(), other_env.query_clock()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("session_id", DataType::Utf8, false),
            Field::new("query_ts", DataType::UInt64, false),
        ]));
        let source = Arc::new(QueryTimestampSource(Arc::clone(&schema)));
        env.register_provider(
            "query_timestamp".into(),
            "restate.public.query_timestamp".into(),
            Arc::new(StreamingTable::try_new(schema, vec![source.clone(), source]).unwrap()),
        )
        .unwrap();
        let engine = Arc::new(DataFusionQueryEngine::from_env(env, None));
        let session = engine.create_session(SessionOptions::default()).unwrap();
        // Planning failure still advances the shared HLC.
        let before_failure = clock.snapshot();
        assert!(session.execute("SELECT (", QueryOptions {}).await.is_err());
        let after_failure = clock.snapshot();
        assert!(after_failure > before_failure);

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let session = Arc::clone(&session);
            tasks.spawn(async move {
                // Independent references and execution partitions must share the timestamp.
                let result = session
                    .execute(
                        "SELECT * FROM query_timestamp UNION ALL SELECT * FROM query_timestamp",
                        QueryOptions {},
                    )
                    .await
                    .unwrap();
                let metadata = result.metadata;
                let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
                for batch in batches {
                    for row in 0..batch.num_rows() {
                        assert_eq!(
                            array_value_to_string(batch.column(0), row).unwrap(),
                            metadata.session_id.as_str()
                        );
                        assert_eq!(
                            batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap()
                                .value(row),
                            metadata.query_ts.as_u64()
                        );
                    }
                }
                metadata.query_ts
            });
        }
        let mut timestamps = BTreeSet::new();
        while let Some(result) = tasks.join_next().await {
            assert!(
                timestamps.insert(result.unwrap()),
                "query timestamp was reused"
            );
        }
        assert_eq!(timestamps.len(), 8);
        assert!(*timestamps.first().unwrap() > after_failure);
        assert!(
            timestamps
                .iter()
                .all(|ts| ts.to_unix_millis() == physical_time)
        );

        // A second engine created from an environment clone shares the same timeline.
        mock_clock.advance_ms(10);
        let other_engine = Arc::new(DataFusionQueryEngine::from_env(other_env, None));
        let other = other_engine
            .create_session(SessionOptions::default())
            .unwrap();
        let result = other.execute("SELECT 42", QueryOptions {}).await.unwrap();
        assert_eq!(result.metadata.session_id.as_str(), other.session_id());
        let timestamp = result.metadata.query_ts;
        assert!(timestamp > *timestamps.last().unwrap());
        assert_eq!(timestamp, clock.snapshot());
        assert_eq!(
            timestamp.to_unix_millis().as_u64(),
            physical_time.as_u64() + 10
        );
        assert_ne!(session.session_id(), other.session_id());
    }

    #[tokio::test(start_paused = true)]
    async fn diagnostics_follow_query_lifetime_independently_of_session() {
        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new())
            .unwrap()
            .with_mock_clock(MockClock::new())
            .unwrap();
        let engine = Arc::new(DataFusionQueryEngine::from_env(env, None));
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
        assert_ne!(result.metadata.query_ts, cancelled.metadata.query_ts);
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
        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new())
            .unwrap()
            .with_mock_clock(MockClock::new())
            .unwrap();
        let scanners = SourceRegistry::local_only(core.metadata);
        let legacy_env = restate_storage_query_datafusion::DataFusionEnv::new(
            10 * 1024 * 1024,
            None,
            None,
            &HashMap::new(),
        )
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
        let engine = DataFusionQueryEngine::from_env(env.clone(), None).with_engine(
            restate_storage_query_datafusion::context::DataFusionQueryEngine::from_env(
                legacy_env, None,
            ),
        );
        let before_registration = engine.create_session(SessionOptions::default()).unwrap();
        env.register_tables(ClusterTables::new(scanners.clone(), Default::default()))
            .await
            .unwrap();
        register_metadata_scanners(
            Live::from_value(MockSchemas::default()),
            MetadataStoreClient::new_in_memory(),
            None,
            &scanners,
        );
        env.register_tables(
            UserTables::new(SelectPartitionsFromMetadata, scanners).with_metadata(),
        )
        .await
        .unwrap();
        TableInventoryBuilder::new(&env)
            .add_view(
                "service_count",
                "public",
                "CREATE VIEW restate.public.service_count AS SELECT COUNT(*) AS n FROM sys_service",
            )
            .await
            .unwrap();

        let cluster = engine
            .create_session(SessionOptions {
                schemas: vec!["cluster".into()],
                default_schema: Some("cluster".into()),
                ..Default::default()
            })
            .unwrap();
        let user = engine.create_session(SessionOptions::default()).unwrap();
        let all = engine
            .create_session(SessionOptions {
                schemas: vec!["public".into(), "cluster".into()],
                ..Default::default()
            })
            .unwrap();
        async fn query_value(session: &dyn QuerySession, sql: &str) -> String {
            let batches: Vec<_> = session
                .execute(sql, QueryOptions {})
                .await
                .unwrap()
                .stream
                .try_collect()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            array_value_to_string(batches[0].column(0), 0).unwrap()
        }
        for table in [
            "bifrost_read_streams",
            "cluster.bifrost_read_streams",
            "restate.cluster.bifrost_read_streams",
        ] {
            let batches: Vec<_> = cluster
                .execute(&format!("SELECT * FROM {table} LIMIT 0"), QueryOptions {})
                .await
                .unwrap()
                .stream
                .try_collect()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        }
        let schema_query = "SELECT DISTINCT table_schema FROM information_schema.tables \
            WHERE table_catalog = 'restate' AND table_schema <> 'information_schema'";
        assert_eq!(query_value(cluster.as_ref(), schema_query).await, "cluster");
        assert_eq!(query_value(user.as_ref(), schema_query).await, "public");
        let restricted = engine
            .create_session(SessionOptions {
                schemas: vec!["public".into(), "cluster".into()],
                excluded_tables: vec!["bifrost_read_streams".into()],
                ..Default::default()
            })
            .unwrap();
        let table_count = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'bifrost_read_streams'";
        assert_eq!(query_value(all.as_ref(), table_count).await, "1");
        assert_eq!(query_value(restricted.as_ref(), table_count).await, "0");
        for session in [&restricted, &user] {
            assert!(
                session
                    .execute(
                        "SELECT * FROM restate.cluster.bifrost_read_streams",
                        QueryOptions {}
                    )
                    .await
                    .is_err()
            );
        }
        let schema_count = "SELECT COUNT(DISTINCT table_schema) FROM information_schema.tables \
            WHERE table_catalog = 'restate' AND table_schema <> 'information_schema'";
        assert_eq!(query_value(all.as_ref(), schema_count).await, "2");
        let legacy = engine
            .create_session(SessionOptions {
                engine_version: Some(QueryEngineVersion::V1),
                schemas: vec!["public".into(), "cluster".into()],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(query_value(legacy.as_ref(), schema_count).await, "0");
        assert_eq!(query_value(legacy.as_ref(), "SELECT 1").await, "1");
        assert_eq!(
            query_value(before_registration.as_ref(), schema_count).await,
            "0"
        );
        for table in [
            "public.bifrost_read_streams",
            "restate.public.bifrost_read_streams",
        ] {
            assert!(
                cluster
                    .execute(&format!("SELECT * FROM {table}"), QueryOptions {})
                    .await
                    .is_err()
            );
        }

        // Prebound views preserve their source even when an alias hides that source
        // and the session creates a different table under its original SQL name.
        let selection = vec![
            SessionTable::new(
                BifrostReadStreamsTable::identity(),
                "restate.cluster.bifrost_read_streams",
            ),
            SessionTable::new("service_count", "restate.public.total"),
        ];
        let mixed = env
            .create_session(&SessionOptions {
                tables: Some(selection.clone()),
                ..Default::default()
            })
            .unwrap();
        assert!(mixed.table_provider("sys_service").await.is_err());
        mixed
            .sql("CREATE VIEW sys_service AS SELECT 'shadow' AS name")
            .await
            .unwrap();
        for (sql, expected) in [
            ("SELECT COUNT(*) FROM sys_service", "1"),
            ("SELECT n FROM total", "0"),
        ] {
            let batches = mixed.sql(sql).await.unwrap().collect().await.unwrap();
            assert_eq!(
                array_value_to_string(batches[0].column(0), 0).unwrap(),
                expected,
                "{sql}"
            );
        }
        let selected = engine
            .create_session(SessionOptions {
                tables: Some(selection),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            query_value(selected.as_ref(), "SELECT n FROM total").await,
            "0"
        );
        assert!(
            selected
                .execute("SELECT * FROM sys_service", QueryOptions {})
                .await
                .is_err()
        );
        assert!(
            user.execute("SELECT * FROM total", QueryOptions {})
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sessions_share_providers_and_admission_but_reject_runtime_mutations() {
        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new())
            .unwrap()
            .with_mock_clock(MockClock::new())
            .unwrap();
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
            "restate.public.total".into(),
            bootstrap.table_provider("total").await.unwrap(),
        )
        .unwrap();
        drop(bootstrap);
        let manager = DataFusionQueryEngine {
            env,
            rate_limiter: Some(RateLimiter::new(
                gardal::Limit::per_hour(NonZeroU32::new(1).unwrap())
                    .with_burst(NonZeroU32::new(2).unwrap()),
                gardal::TokioClock,
            )),
        };
        let manager = QueryEngine::new(QueryEngineVersion::V2, move |opts| {
            manager.create_session(opts)
        });
        let bypass = || SessionOptions {
            apply_rate_limit: false,
            ..Default::default()
        };
        let c = manager.create_session(bypass()).unwrap();
        let a = manager.create_session(SessionOptions::default()).unwrap();
        let b = manager.create_session(SessionOptions::default()).unwrap();
        assert!(matches!(
            manager.create_session(SessionOptions::default()),
            Err(SessionError::RateLimited(_))
        ));
        let d = manager.create_session(bypass()).unwrap();
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
        for session in [a, b, c, d] {
            let result = session
                .execute("SELECT n FROM total", QueryOptions {})
                .await
                .unwrap();
            drop(session);
            let batches: Vec<_> = result.stream.try_collect().await.unwrap();
            assert_eq!(array_value_to_string(batches[0].column(0), 0).unwrap(), "6");
        }

        // As in V1, a session that allows statements keeps its SET for its later
        // queries, without changing another session's defaults.
        let session = manager
            .create_session(SessionOptions {
                allow_statements: true,
                ..bypass()
            })
            .unwrap();
        let other = manager.create_session(bypass()).unwrap();
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
