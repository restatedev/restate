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
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;
use tracing::warn;

use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SQLOptions;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;

use codederror::CodedError;
use restate_core::{Metadata, TaskCenter};
use restate_invoker_api::StatusHandle;
use restate_partition_store::PartitionStoreManager;
use restate_sharding::KeyRange;
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::config::QueryEngineOptions;
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::live::Live;
use restate_types::partition_table::Partition;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::schema::deployment::DeploymentResolver;
use restate_types::schema::service::ServiceMetadataResolver;
use restate_worker_api::SchedulerStatusEntry;

use crate::node_fan_out::NodeWarnings;
use crate::remote_query_scanner_manager::RemoteScannerManager;

const SYS_INVOCATION_VIEW: &str = "CREATE VIEW sys_invocation as SELECT
            ss.id,
            ss.target,
            ss.target_service_name,
            ss.target_service_key,
            ss.target_handler_name,
            ss.target_service_ty,
            ss.idempotency_key,
            ss.invoked_by,
            ss.invoked_by_service_name,
            ss.invoked_by_id,
            ss.invoked_by_subscription_id,
            ss.invoked_by_target,
            ss.restarted_from,
            ss.pinned_deployment_id,
            ss.pinned_service_protocol_version,
            ss.trace_id,
            ss.journal_size,
            ss.journal_commands_size,
            ss.created_at,
            ss.created_using_restate_version,
            ss.modified_at,
            ss.inboxed_at,
            ss.scheduled_at,
            ss.scheduled_start_at,
            ss.running_at,
            ss.completed_at,
            ss.completion_retention,
            ss.journal_retention,
            ss.suspended_waiting_for_completions,
            ss.suspended_waiting_for_signals,
            ss.suspended_waiting_future_json,

            sis.retry_count,
            sis.last_start_at,
            sis.next_retry_at,
            sis.last_attempt_deployment_id,
            sis.last_attempt_server,
            sis.last_failure,
            sis.last_failure_error_code,
            sis.last_failure_related_entry_index,
            sis.last_failure_related_entry_name,
            sis.last_failure_related_entry_type,
            sis.last_failure_related_command_index,
            sis.last_failure_related_command_name,
            sis.last_failure_related_command_type,

            arrow_cast(CASE
                WHEN ss.status = 'inboxed' THEN 'pending'
                WHEN ss.status = 'scheduled' THEN 'scheduled'
                WHEN ss.status = 'completed' THEN 'completed'
                WHEN ss.status = 'suspended' THEN 'suspended'
                WHEN ss.status = 'paused' THEN 'paused'
                WHEN sis.in_flight THEN 'running'
                WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
                ELSE 'ready'
            END, 'LargeUtf8') AS status,
            ss.completion_result,
            ss.completion_failure
        FROM sys_invocation_state sis
        RIGHT JOIN sys_invocation_status ss ON ss.id = sis.id";

const CLUSTER_LOGS_TAIL_SEGMENTS_VIEW: &str = "CREATE VIEW logs_tail_segments as SELECT
        l.* FROM logs AS l JOIN (
            SELECT log_id, max(segment_index) AS segment_index FROM logs GROUP BY log_id
        ) m
        ON m.log_id=l.log_id AND l.segment_index=m.segment_index";

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

/// Allows grouping and registration of set of tables and views
/// on the QueryContext.
pub trait RegisterTable: Send + Sync + 'static {
    fn register(&self, ctx: &QueryContext) -> impl Future<Output = Result<(), BuildError>>;
}

/// A leader-state introspection handle that extends invoker status queries with
/// additional query methods for future leader-owned components.
pub trait PartitionLeaderStatusHandle:
    StatusHandle + Send + Sync + Debug + Clone + 'static
{
    type SchedulerStatus;
    type SchedulerStatusIterator: Iterator<Item = Self::SchedulerStatus> + Send;

    type UserLimitCounter;
    type UserLimitCounterIterator: Iterator<Item = Self::UserLimitCounter> + Send;

    fn read_scheduler_status(
        &self,
        keys: KeyRange,
    ) -> impl Future<Output = Self::SchedulerStatusIterator> + Send;

    fn read_user_limit_counters(
        &self,
        keys: KeyRange,
    ) -> impl Future<Output = Self::UserLimitCounterIterator> + Send;
}

/// A no-op registerer that creates a minimal query context with no tables.
/// Useful for nodes that only need to serve remote scanner RPCs (e.g.,
/// log-server-only nodes), where only `task_ctx()` is needed.
pub struct NoTables;

impl RegisterTable for NoTables {
    async fn register(&self, _ctx: &QueryContext) -> Result<(), BuildError> {
        Ok(())
    }
}

/// A query context registerer for user tables
pub struct UserTables<P, S, D> {
    partition_selector: P,
    partition_store_manager: Arc<PartitionStoreManager>,
    partition_leader_status: Option<S>,
    schemas: Live<D>,
    remote_scanner_manager: RemoteScannerManager,
}

impl<P, S, D> UserTables<P, S, D> {
    pub fn new(
        partition_selector: P,
        partition_store_manager: Arc<PartitionStoreManager>,
        partition_leader_status: Option<S>,
        schemas: Live<D>,
        remote_scanner_manager: RemoteScannerManager,
    ) -> Self {
        Self {
            partition_selector,
            partition_store_manager,
            partition_leader_status,
            schemas,
            remote_scanner_manager,
        }
    }
}

impl<P, S, D> RegisterTable for UserTables<P, S, D>
where
    P: SelectPartitions + Clone,
    S: PartitionLeaderStatusHandle<SchedulerStatus = SchedulerStatusEntry>,
    D: DeploymentResolver + ServiceMetadataResolver + Send + Sync + Debug + Clone + 'static,
{
    async fn register(&self, ctx: &QueryContext) -> Result<(), BuildError> {
        // ----- non partitioned tables -----
        crate::deployment::register_self(ctx, self.schemas.clone())?;
        crate::service::register_self(ctx, self.schemas.clone())?;
        // ----- partition-key-based -----
        crate::invocation_state::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_leader_status.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::invocation_status::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::keyed_service_status::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::locks::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::state::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::journal::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::journal_events::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::inbox::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::idempotency::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::promise::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        // VQueues Tables
        crate::vqueue_meta::register_self(
            ctx,
            self.partition_selector.clone(),
            self.partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;

        ctx.datafusion_context.sql(SYS_INVOCATION_VIEW).await?;

        Ok(())
    }
}

pub struct ClusterTables {
    cluster_state: restate_types::cluster_state::ClusterState,
    replica_set_states: PartitionReplicaSetStates,
    cluster_state_watch: watch::Receiver<Arc<LegacyClusterState>>,
    remote_scanner_manager: RemoteScannerManager,
}

impl ClusterTables {
    pub fn new(
        replica_set_states: PartitionReplicaSetStates,
        cluster_state_watch: watch::Receiver<Arc<LegacyClusterState>>,
        remote_scanner_manager: RemoteScannerManager,
    ) -> Self {
        let cluster_state = TaskCenter::with_current(|tc| tc.cluster_state().clone());
        Self {
            cluster_state,
            replica_set_states,
            cluster_state_watch,
            remote_scanner_manager,
        }
    }

    /// Returns a reference to the remote scanner manager. This can be used to
    /// register node-level scanners (e.g., log-server tables) after construction.
    pub fn remote_scanner_manager(&self) -> &RemoteScannerManager {
        &self.remote_scanner_manager
    }
}

impl RegisterTable for ClusterTables {
    async fn register(&self, ctx: &QueryContext) -> Result<(), BuildError> {
        let metadata = Metadata::current();
        crate::node::register_self(ctx, metadata.clone(), self.cluster_state.clone())?;
        crate::partition::register_self(ctx, metadata.clone(), self.replica_set_states.clone())?;
        crate::partition_replica_set::register_self(
            ctx,
            metadata.clone(),
            self.cluster_state.clone(),
            self.replica_set_states.clone(),
        )?;
        crate::log::register_self(ctx, metadata.clone())?;
        crate::partition_state::register_self(ctx, self.cluster_state_watch.clone())?;

        // Node-fan-out tables
        let remote_scanner = self.remote_scanner_manager.remote_scanner_service();
        crate::loglet_worker::register_self(
            ctx,
            metadata.clone(),
            remote_scanner.clone(),
            None, // local scanner is registered separately if this node is also a log-server
        )?;
        crate::bifrost_read_stream::register_self(
            ctx,
            metadata,
            remote_scanner,
            None, // local scanner is registered separately by the node
        )?;

        ctx.datafusion_context
            .sql(CLUSTER_LOGS_TAIL_SEGMENTS_VIEW)
            .await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct QueryContext {
    sql_options: SQLOptions,
    datafusion_context: SessionContext,
}

impl QueryContext {
    pub async fn create<T: RegisterTable>(
        options: &QueryEngineOptions,
        registerer: T,
    ) -> Result<Self, BuildError> {
        let ctx = QueryContext::new(
            options.memory_size.get(),
            options.tmp_dir.clone(),
            options.query_parallelism(),
            &options.datafusion_options,
        )?;

        registerer.register(&ctx).await?;

        Ok(ctx)
    }

    /// A shortcut to create a query context with built in
    /// UserTables
    #[allow(clippy::too_many_arguments)]
    pub async fn with_user_tables(
        options: &QueryEngineOptions,
        partition_selector: impl SelectPartitions + Clone,
        partition_store_manager: Arc<PartitionStoreManager>,
        partition_leader_status: Option<
            impl PartitionLeaderStatusHandle<SchedulerStatus = SchedulerStatusEntry>,
        >,
        schemas: Live<
            impl DeploymentResolver + ServiceMetadataResolver + Send + Sync + Debug + Clone + 'static,
        >,
        remote_scanner_manager: RemoteScannerManager,
    ) -> Result<QueryContext, BuildError> {
        let tables = UserTables::new(
            partition_selector,
            partition_store_manager,
            partition_leader_status,
            schemas,
            remote_scanner_manager,
        );

        Self::create(options, tables).await
    }

    pub(crate) fn register_partitioned_table(
        &self,
        name: impl Into<TableReference>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), DataFusionError> {
        self.datafusion_context
            .register_table(name, provider)
            .map(|_| ())
    }
    pub(crate) fn register_non_partitioned_table(
        &self,
        name: impl Into<TableReference>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), DataFusionError> {
        self.datafusion_context
            .register_table(name, provider)
            .map(|_| ())
    }

    fn new(
        memory_limit: usize,
        temp_folder: Option<String>,
        default_parallelism: Option<usize>,
        datafusion_options: &HashMap<String, String>,
    ) -> Result<Self, DataFusionError> {
        //
        // build the runtime
        //
        let mut runtime_config = RuntimeEnvBuilder::default();
        runtime_config = runtime_config.with_memory_limit(memory_limit, 1.0);

        if let Some(folder) = temp_folder {
            runtime_config = runtime_config.with_temp_file_path(folder);
        }
        let runtime = runtime_config.build_arc().expect("runtime");
        //
        // build the session
        //
        let mut session_config = SessionConfig::new();
        if let Some(target_partitions) = default_parallelism {
            session_config = session_config.with_target_partitions(target_partitions);
        }

        session_config = session_config
            .with_batch_size(128)
            .with_information_schema(true)
            .with_default_catalog_and_schema("restate", "public");

        for (k, v) in datafusion_options {
            session_config.options_mut().set(k, v)?;
        }

        //
        // build the state
        //
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();

        let mut ctx = SessionContext::new_with_state(state);

        match datafusion_functions_json::register_all(&mut ctx) {
            Ok(_) => {}
            Err(err) => {
                warn!("Unable to register json functions {}", err);
            }
        };

        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false);

        Ok(Self {
            sql_options,
            datafusion_context: ctx,
        })
    }

    pub async fn execute(&self, sql: &str) -> datafusion::common::Result<QueryResult> {
        let state = self.datafusion_context.state();
        let statement = state.sql_to_statement(sql, &datafusion::config::Dialect::PostgreSQL)?;
        let plan = state.statement_to_plan(statement).await?;
        self.sql_options.verify_plan(&plan)?;
        let df = self.datafusion_context.execute_logical_plan(plan).await?;

        let task_ctx = Arc::new(df.task_ctx());
        let physical_plan = df.create_physical_plan().await?;

        // Collect NodeWarnings handles from any NodeFanOutExecutionPlan nodes
        // in the plan tree before execution begins.
        let node_warnings = collect_node_warnings(&physical_plan);

        let stream = execute_stream(physical_plan, task_ctx)?;
        Ok(QueryResult {
            stream,
            node_warnings,
        })
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.datafusion_context.task_ctx()
    }
}

impl AsRef<SessionContext> for QueryContext {
    fn as_ref(&self) -> &SessionContext {
        &self.datafusion_context
    }
}

/// Result of a SQL query execution, containing the record batch stream
/// and any per-node warning collectors from fan-out execution plans.
pub struct QueryResult {
    pub stream: SendableRecordBatchStream,
    pub node_warnings: Vec<NodeWarnings>,
}

/// Walks the physical plan tree and collects [`NodeWarnings`] handles from
/// any [`NodeFanOutExecutionPlan`] nodes found.
fn collect_node_warnings(plan: &Arc<dyn ExecutionPlan>) -> Vec<NodeWarnings> {
    use crate::node_fan_out::NodeFanOutExecutionPlan;

    let mut warnings = Vec::new();
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if let Some(fan_out) = node.as_any().downcast_ref::<NodeFanOutExecutionPlan>() {
            warnings.push(fan_out.node_warnings().clone());
        }
        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }
    warnings
}

/// Newtype to add debug implementation which is required for [`SelectPartitions`].
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
