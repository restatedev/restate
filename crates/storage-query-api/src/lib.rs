// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared query interfaces: the query engine contract, and the logical schemas
//! and typed selections that query adapters and storage implementations share.
//!
//! - [`schema`] declares logical tables: their columns, native value types, SQL
//!   view and sharding. [`define_schema!`] generates a schema from one column list.
//! - [`selection`] states which rows of a schema a read must return, as typed
//!   conditions on its columns, and derives candidate domains from SQL predicates.
//! - [`scan`] is the physical side: a table's records and prepared read requests,
//!   as storage defines them, read on the caller's thread. A [`SchemaBinding`]
//!   connects a physical table to a logical schema, so a selection can be prepared
//!   and records become rows.
//! - [`tables`] holds the declarations of the SQL catalog's tables.
//!
//! Physical key layouts, encodings and scan bounds stay in storage crates.

mod diagnostics;
mod engine;
pub mod errors;
pub mod scan;
pub mod schema;
pub mod selection;
pub mod table_docs;
pub mod tables;
mod writer;

pub use scan::Table;
pub use schema::{
    LogicalSchema, PreparedRead, RowBuilder, RowSink, SchemaBinding, SelectionSchema,
    ShardingScheme,
};

use std::any::Any;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
pub use datafusion::physical_plan::metrics;

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
