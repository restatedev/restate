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
mod partition_tables;
mod user_tables;

pub use cluster_tables::ClusterTables;
use datafusion::execution::context::SessionContext;
pub use partition_tables::PartitionTables;
pub use user_tables::UserTables;

use crate::BuildError;

/// Allows grouping and registration of set of tables and views
/// in a bootstrap session's catalog.
pub trait RegisterTable: Send + Sync + 'static {
    fn register(&self, ctx: &SessionContext) -> impl Future<Output = Result<(), BuildError>>;
}
