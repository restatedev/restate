// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{analyzer, physical_optimizer};
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};

use std::sync::Arc;

pub struct QueryContext {
    datafusion_context: SessionContext,
}

impl Default for QueryContext {
    fn default() -> Self {
        QueryContext::new(None, None, None)
    }
}

impl QueryContext {
    pub fn new(
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
        let mut state = SessionState::with_config_rt(session_config, runtime);

        state = state.add_analyzer_rule(Arc::new(
            analyzer::UseSymmetricHashJoinWhenPartitionKeyIsPresent::new(),
        ));
        state = state.add_physical_optimizer_rule(Arc::new(physical_optimizer::JoinRewrite::new()));

        let ctx = SessionContext::with_state(state);

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
