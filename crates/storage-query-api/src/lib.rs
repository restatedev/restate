// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared query interfaces and selection analysis.

mod diagnostics;
mod engine;
pub mod errors;
pub mod scan;
pub mod schema;
pub mod selection;
pub mod tables;
mod writer;

pub use scan::Table;
pub use schema::{
    LogicalSchema, PreparedRead, RowBuilder, RowSink, SchemaBinding, SelectionSchema,
    ShardingScheme,
};

#[cfg(feature = "test-util")]
pub mod test_util;

use std::any::Any;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
pub use datafusion_physical_expr_common::metrics;

use restate_util_string::ReString;

pub use diagnostics::{
    QueryDiagnostics, QueryMetadata, QueryOperatorStats, QueryStats, QueryStatus,
};
pub use engine::{
    NodeWarning, NodeWarnings, QueryEngine, QueryEngineVersion, QueryOptions, QueryResult,
    QuerySession, SessionOptions, SessionTable,
};
pub use writer::{RecordBatchWriter, WriteRecordBatchStream};

/// A marker identifying a query source independently of its session-local SQL name.
pub trait QueryEngineTable: Any + Send + Sync + Clone + std::fmt::Debug + 'static {
    /// A stable unique identifier for this table. The identifier
    /// must be consistent across all nodes and unique in its bare form
    /// across catalogs and schema prefixes.
    fn identity() -> ReString;
}

/// An engine-independent scanner for node-local or global, non-partitioned data.
pub trait Scan: std::fmt::Debug + Send + Sync + 'static {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream;
}
