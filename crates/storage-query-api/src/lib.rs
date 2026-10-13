// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod catalog;
mod diagnostics;
mod engine;
pub mod errors;
mod writer;

use std::any::Any;

pub use datafusion_physical_expr_common::metrics;

use restate_types::config::Configuration;
use restate_util_string::ReString;

pub use catalog::{AdminUser, ClusterOperator};
pub use diagnostics::{
    QueryDiagnostics, QueryMetadata, QueryOperatorStats, QueryStats, QueryStatus,
};
pub use engine::{
    NoOpQueryEngine, NodeWarning, NodeWarnings, QueryEngine, QueryOptions, QueryResult,
    QuerySession, SessionOptions, SessionTable,
};
pub use writer::{RecordBatchWriter, WriteRecordBatchStream};

/// A marker identifying a query source independently of its session-local SQL name.
pub trait QueryEngineTable: Any + Send + Sync + Clone + std::fmt::Debug + 'static {
    /// A stable unique identifier for this table. The identifier
    /// must be consistent across all nodes and unique in its bare form
    /// across catalogs and schema prefixes.
    fn identity() -> ReString;

    /// Whether this source may be exposed in a newly created SQL session.
    ///
    /// Evaluated against one configuration snapshot for the whole catalog, including
    /// explicit selections and aliases. Existing sessions and worker-side source
    /// registration are unaffected by subsequent configuration changes.
    fn is_visible(_config: &Configuration, _options: &SessionOptions) -> bool {
        true
    }
}
