// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod cluster_tables;
mod user_tables;

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;

use restate_storage_query_api::{QueryEngineTable, SessionTable};
use restate_util_string::ReString;

pub use cluster_tables::ClusterTables;
pub use user_tables::{MetadataTables, UserTables};

use crate::{BuildError, DataFusionEnv};

/// Populates the shared inventory and declares this component's default SQL exposure.
pub trait RegisterTable: Send + Sync + 'static {
    fn register(
        &self,
        inventory: &mut TableInventoryBuilder<'_>,
    ) -> impl Future<Output = Result<(), BuildError>>;
}

impl RegisterTable for () {
    async fn register(&self, _inventory: &mut TableInventoryBuilder<'_>) -> Result<(), BuildError> {
        Ok(())
    }
}

/// Startup registration helper. Views bind once to providers from the same inventory.
pub struct TableInventoryBuilder<'a> {
    env: &'a DataFusionEnv,
    tables: Vec<SessionTable>,
}

impl<'a> TableInventoryBuilder<'a> {
    pub fn new(env: &'a DataFusionEnv) -> Self {
        Self {
            env,
            tables: Vec::new(),
        }
    }

    pub fn add<T: QueryEngineTable>(
        &mut self,
        schema: &str,
        name: &str,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), BuildError> {
        self.env.register_table::<T>(provider)?;
        self.tables
            .push(SessionTable::for_table::<T>(TableReference::full(
                "restate", schema, name,
            )));
        Ok(())
    }

    pub async fn add_view(
        &mut self,
        identity: &'static str,
        schema: &str,
        sql: &str,
    ) -> Result<(), BuildError> {
        let bootstrap = self.env.create_session(&self.tables)?;
        bootstrap.sql(sql).await?;
        let name = TableReference::full("restate", schema, identity);
        let provider = bootstrap.table_provider(name.clone()).await?;
        self.env
            .register_provider(ReString::from_static(identity), provider)?;
        self.tables
            .push(SessionTable::new(ReString::from_static(identity), name));
        Ok(())
    }

    pub fn finish(self) -> Vec<SessionTable> {
        self.tables
    }
}
