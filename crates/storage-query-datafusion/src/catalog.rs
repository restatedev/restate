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

use datafusion::execution::context::SessionContext;

pub use cluster_tables::ClusterTables;
pub use user_tables::{MetadataTables, UserTables};

use crate::BuildError;

/// Allows grouping and registration of set of tables and views
/// in a bootstrap session's catalog.
pub trait RegisterTable: Send + Sync + 'static {
    fn register(&self, ctx: &SessionContext) -> impl Future<Output = Result<(), BuildError>>;
}

impl RegisterTable for () {
    async fn register(&self, _ctx: &SessionContext) -> Result<(), BuildError> {
        Ok(())
    }
}
