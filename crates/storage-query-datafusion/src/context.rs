// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use codederror::CodedError;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};

use restate_invoker_api::StatusHandle;
use restate_schema_api::deployment::DeploymentResolver;
use restate_schema_api::service::ServiceMetadataResolver;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::config::QueryEngineOptions;

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
            ss.trace_id,
            ss.journal_size,
            ss.created_at,
            ss.modified_at,

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
                WHEN ss.status = 'completed' THEN 'completed'
                WHEN ss.status = 'suspended' THEN 'suspended'
                WHEN sis.in_flight THEN 'running'
                WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
                ELSE 'ready'
            END AS status
        FROM sys_invocation_status ss
        LEFT JOIN sys_invocation_state sis ON ss.id = sis.id";

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error(transparent)]
    #[code(unknown)]
    Datafusion(#[from] DataFusionError),
}

#[derive(Clone)]
pub struct QueryContext {
    datafusion_context: SessionContext,
}

impl Default for QueryContext {
    fn default() -> Self {
        QueryContext::new(None, None, None)
    }
}

impl QueryContext {
    pub fn from_options(
        options: &QueryEngineOptions,
        rocksdb: RocksDBStorage,
        status: impl StatusHandle + Send + Sync + Debug + Clone + 'static,
        schemas: impl DeploymentResolver
            + ServiceMetadataResolver
            + Send
            + Sync
            + Debug
            + Clone
            + 'static,
    ) -> Result<QueryContext, BuildError> {
        let ctx = QueryContext::new(
            options.memory_limit,
            options.tmp_dir.clone(),
            options.query_parallelism,
        );
        crate::invocation_status::register_self(&ctx, rocksdb.clone())?;
        crate::keyed_service_status::register_self(&ctx, rocksdb.clone())?;
        crate::state::register_self(&ctx, rocksdb.clone())?;
        crate::journal::register_self(&ctx, rocksdb.clone())?;
        crate::invocation_state::register_self(&ctx, status)?;
        crate::inbox::register_self(&ctx, rocksdb.clone())?;
        crate::deployment::register_self(&ctx, schemas.clone())?;
        crate::service::register_self(&ctx, schemas)?;
        crate::idempotency::register_self(&ctx, rocksdb)?;

        // todo: Fix me
        // we need this now because we can't make new async.
        // i'm ashamed!
        let ctx = futures::executor::block_on(async move {
            let ctx = ctx;
            ctx.datafusion_context
                .sql(SYS_INVOCATION_VIEW)
                .await
                .map(|_| ctx)
        })?;

        Ok(ctx)
    }

    fn new(
        memory_limit: Option<usize>,
        temp_folder: Option<String>,
        default_parallelism: Option<usize>,
    ) -> Self {
        //
        // build the runtime
        //
        let mut runtime_config = RuntimeConfig::default();
        runtime_config = runtime_config.with_memory_limit(4 * 1024 * 1024 * 1024, 1.0);
        if let Some(limit) = memory_limit {
            runtime_config = runtime_config.with_memory_limit(limit, 1.0);
        }
        if let Some(folder) = temp_folder {
            runtime_config = runtime_config.with_temp_file_path(folder);
        }
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).expect("runtime"));
        //
        // build the session
        //
        let mut session_config = SessionConfig::new();
        if let Some(p) = default_parallelism {
            session_config = session_config.with_target_partitions(p)
        }
        session_config = session_config
            .with_allow_symmetric_joins_without_pruning(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema("restate", "public");
        //
        // build the state
        //
        let mut state = SessionState::new_with_config_rt(session_config, runtime);

        state = state.add_analyzer_rule(Arc::new(
            analyzer::UseSymmetricHashJoinWhenPartitionKeyIsPresent::new(),
        ));
        state = state.add_physical_optimizer_rule(Arc::new(physical_optimizer::JoinRewrite::new()));

        let ctx = SessionContext::new_with_state(state);

        Self {
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
        let df = self.datafusion_context.execute_logical_plan(plan).await?;
        df.execute_stream().await
    }
}

impl AsRef<SessionContext> for QueryContext {
    fn as_ref(&self) -> &SessionContext {
        &self.datafusion_context
    }
}
