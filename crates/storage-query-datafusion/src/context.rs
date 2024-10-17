// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::max;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use codederror::CodedError;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SQLOptions;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};

use restate_core::worker_api::ProcessorsManagerHandle;
use restate_invoker_api::StatusHandle;
use restate_partition_store::PartitionStoreManager;
use restate_types::config::QueryEngineOptions;
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::live::Live;
use restate_types::schema::deployment::DeploymentResolver;
use restate_types::schema::service::ServiceMetadataResolver;

use crate::{analyzer, physical_optimizer};

const SYS_INVOCATION_VIEW: &str = "CREATE VIEW sys_invocation as SELECT
            ss.id,
            ss.target,
            ss.target_service_name,
            ss.target_service_key,
            ss.target_handler_name,
            ss.target_service_ty,
            ss.invoked_by,
            ss.invoked_by_service_name,
            ss.invoked_by_id,
            ss.invoked_by_target,
            ss.pinned_deployment_id,
            ss.pinned_service_protocol_version,
            ss.trace_id,
            ss.journal_size,
            ss.created_at,
            ss.modified_at,
            ss.inboxed_at,
            ss.scheduled_at,
            ss.running_at,
            ss.completed_at,

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

            CASE
                WHEN ss.status = 'inboxed' THEN 'pending'
                WHEN ss.status = 'scheduled' THEN 'scheduled'
                WHEN ss.status = 'completed' THEN 'completed'
                WHEN ss.status = 'suspended' THEN 'suspended'
                WHEN sis.in_flight THEN 'running'
                WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
                ELSE 'ready'
            END AS status,
            ss.completion_result,
            ss.completion_failure
        FROM sys_invocation_status ss
        LEFT JOIN sys_invocation_state sis ON ss.id = sis.id";

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error(transparent)]
    #[code(unknown)]
    Datafusion(#[from] DataFusionError),
}

#[async_trait]
pub trait SelectPartitions: Send + Sync + Debug + 'static {
    async fn get_live_partitions(&self) -> Result<Vec<PartitionId>, GenericError>;
}

#[derive(Clone)]
pub struct QueryContext {
    sql_options: SQLOptions,
    datafusion_context: SessionContext,
}

impl QueryContext {
    pub async fn create(
        options: &QueryEngineOptions,
        partition_selector: impl SelectPartitions + Clone,
        partition_store_manager: PartitionStoreManager,
        status: impl StatusHandle + Send + Sync + Debug + Clone + 'static,
        schemas: Live<
            impl DeploymentResolver + ServiceMetadataResolver + Send + Sync + Debug + Clone + 'static,
        >,
    ) -> Result<QueryContext, BuildError> {
        let ctx = QueryContext::new(
            options.memory_size.get(),
            options.tmp_dir.clone(),
            options.query_parallelism(),
        );
        crate::deployment::register_self(&ctx, schemas.clone())?;
        crate::service::register_self(&ctx, schemas)?;
        crate::invocation_state::register_self(&ctx, status)?;
        // partition-key-based
        crate::invocation_status::register_self(
            &ctx,
            partition_selector.clone(),
            partition_store_manager.clone(),
        )?;
        crate::keyed_service_status::register_self(
            &ctx,
            partition_selector.clone(),
            partition_store_manager.clone(),
        )?;
        crate::state::register_self(
            &ctx,
            partition_selector.clone(),
            partition_store_manager.clone(),
        )?;
        crate::journal::register_self(
            &ctx,
            partition_selector.clone(),
            partition_store_manager.clone(),
        )?;
        crate::inbox::register_self(
            &ctx,
            partition_selector.clone(),
            partition_store_manager.clone(),
        )?;
        crate::idempotency::register_self(
            &ctx,
            partition_selector.clone(),
            partition_store_manager.clone(),
        )?;
        crate::promise::register_self(&ctx, partition_selector.clone(), partition_store_manager)?;

        let ctx = ctx
            .datafusion_context
            .sql(SYS_INVOCATION_VIEW)
            .await
            .map(|_| ctx)?;

        Ok(ctx)
    }

    fn new(
        memory_limit: usize,
        temp_folder: Option<String>,
        default_parallelism: Option<usize>,
    ) -> Self {
        //
        // build the runtime
        //
        let mut runtime_config = RuntimeConfig::default().with_memory_limit(memory_limit, 1.0);
        if let Some(folder) = temp_folder {
            runtime_config = runtime_config.with_temp_file_path(folder);
        }
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).expect("runtime"));
        //
        // build the session
        //
        let mut session_config = SessionConfig::new();

        // TODO: this value affects the number of partitions created by datafusion while
        // executing a query. Setting this to 1 causes the SymmetricHashJoin to fail
        // which we use for some of the queries.
        // Resolving this issue is tracked at https://github.com/restatedev/restate/issues/1585
        let parallelism = default_parallelism.unwrap_or(2);
        session_config = session_config.with_target_partitions(max(2, parallelism));

        session_config = session_config
            .with_allow_symmetric_joins_without_pruning(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema("restate", "public");
        //
        // build the state
        //
        let mut state_builder = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime)
            .with_default_features();

        // Rewrite the logical plan,  to transparently add a 'partition_key' column to Join's
        // To tables that have a partition key in their schema.
        //
        // For example:
        // 'SELECT  b.service_key FROM sys_invocation_status a JOIN state b on a.target_service_key = b.service_key'
        //
        // Will be rewritten to:
        // 'SELECT  b.service_key FROM sys_invocation_status a JOIN state b on a.target_service_key = b.service_key AND a.partition_key = b.partition_key'
        //
        // This would be used by the SymmetricHashJoin as a watermark.
        state_builder = state_builder.with_analyzer_rule(Arc::new(
            analyzer::UseSymmetricHashJoinWhenPartitionKeyIsPresent::new(),
        ));

        //
        // Prepend the join rewrite optimizer to the list of physical optimizers.
        //
        // It is important that the join rewrite optimizer will run before ProjectionPushdown::try_embed_to_hash_join.
        // because the SymmetricHashJoin doesn't support embedded projections out of the box
        //
        // If we don't do that, then when translating a HashJoinExc to SymmetricHashJoinExc we will lose the embedded projection
        // and the query will fail.
        // For example try the following query without prepending but rather appending:
        //
        // 'SELECT  b.service_key FROM sys_invocation_status a JOIN state b on a.target_service_key = b.service_key'
        //
        // A far more involved but potentially more robust solution would be wrap the SymmetricHashJoin in a ProjectionExec
        // If this would become an issue for any reason, then we can explore that alternative.
        //
        let join_rewrite = Arc::new(physical_optimizer::JoinRewrite::new());
        let mut default_physical_optimizer_rules = PhysicalOptimizer::default().rules;
        default_physical_optimizer_rules.insert(0, join_rewrite);

        state_builder =
            state_builder.with_physical_optimizer_rules(default_physical_optimizer_rules);

        let state = state_builder.build();
        let ctx = SessionContext::new_with_state(state);

        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false);

        Self {
            sql_options,
            datafusion_context: ctx,
        }
    }

    pub async fn execute(
        &self,
        sql: &str,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let state = self.datafusion_context.state();
        let statement = state.sql_to_statement(sql, "postgres")?;
        let plan = state.statement_to_plan(statement).await?;
        self.sql_options.verify_plan(&plan)?;
        let df = self.datafusion_context.execute_logical_plan(plan).await?;
        df.execute_stream().await
    }
}

impl AsRef<SessionContext> for QueryContext {
    fn as_ref(&self) -> &SessionContext {
        &self.datafusion_context
    }
}

#[async_trait]
impl SelectPartitions for ProcessorsManagerHandle {
    async fn get_live_partitions(&self) -> Result<Vec<PartitionId>, GenericError> {
        Ok(self.get_live_partitions().await?)
    }
}
