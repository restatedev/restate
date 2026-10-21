// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logical table schemas shared by selections, query adapters, and indexes.

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
use datafusion::physical_plan::PhysicalExpr;

use crate::selection::{BranchOutcome, ValuePredicateBuilder, analyze_branches};

mod binding;
mod macros;
pub mod rows;

pub use binding::{PreparedRead, SchemaBinding};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod analysis_tests;

#[cfg(test)]
mod row_tests;

/// Native columns, SQL bindings, and predicate analysis for a logical schema.
///
/// This contract describes values and their selection semantics, independently of
/// storage, row materialization, and Arrow representation. [`LogicalSchema`] adds
/// a row family and its query adapter. Physical tables have their own contract;
/// [`SchemaBinding`] connects their records to a logical schema when reading.
pub trait SelectionSchema {
    /// Logical column identities, independent of physical key positions.
    type Column: enum_map::Enum + Copy + Eq + 'static;

    /// One condition on a logical row. A column may be stored in a key or value,
    /// or obtained by fetching a base record, depending on the chosen access path.
    type Clause;

    /// Typed candidate domains for the modeled columns of one Boolean alternative.
    /// These are necessary restrictions, not a promise to satisfy the predicate.
    /// The declaration generates this shape, including internal derived columns.
    type Domains: Clone;

    /// All modeled columns, in declaration order. This is not a sort order.
    const COLUMNS: &'static [Self::Column];

    /// Returns the column constrained by a clause.
    fn column(clause: &Self::Clause) -> Self::Column;

    /// The canonical column name used by adapters and physical key bindings.
    fn tag(column: Self::Column) -> &'static str;

    /// Resolves a canonical column name; returns `None` for unmodeled columns.
    fn column_from_tag(tag: &str) -> Option<Self::Column>;

    /// The SQL binding, or `None` for an internal column used only by native
    /// selections, derivations, or indexes. This may differ from the column's tag.
    fn sql_name(column: Self::Column) -> Option<&'static str>;

    /// Resolves the same SQL bindings used by analysis and the Arrow view.
    fn column_from_sql(name: &str) -> Option<Self::Column> {
        Self::COLUMNS
            .iter()
            .copied()
            .find(|&column| Self::sql_name(column) == Some(name))
    }

    /// Builds a condition using the column's declared native value type.
    /// The adapter supplies literal conversion through `builder`. `None` means
    /// unsupported, not an impossible condition.
    fn value_clause<P: ValuePredicateBuilder>(
        column: Self::Column,
        builder: P,
    ) -> Option<Self::Clause>;

    /// Builds a case-sensitive literal prefix condition on a capable column.
    /// NULL never matches; an empty prefix matches every non-NULL string.
    fn starts_with_clause(column: Self::Column, prefix: &str) -> Option<Self::Clause>;

    /// Borrows the prefix of a prefix clause; other clauses return `None`.
    fn starts_with_prefix(clause: &Self::Clause) -> Option<&str>;

    /// Analyzes one conjunction, including unexpanded Boolean subexpressions.
    /// Intersects declared complete domains, runs derivations once in declaration
    /// order, and rejects a branch as soon as any modeled column is impossible.
    fn analyze_branch(
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<BranchOutcome<Self::Domains>>;

    /// Unions corresponding columns, potentially losing correlations.
    fn union_domains(left: Self::Domains, right: Self::Domains) -> Self::Domains;

    /// Preserves bounded alternatives through normalization and derivation.
    /// Inputs must satisfy [`analyze_branches`]' lifetime and residual contracts.
    /// In particular, keep the original predicate for residual evaluation: a
    /// lossy derivation never replaces its source condition.
    fn analyze(
        filters: &[Arc<dyn PhysicalExpr>],
        max_branches: NonZeroUsize,
    ) -> anyhow::Result<Vec<Self::Domains>> {
        analyze_branches(filters, max_branches, Self::analyze_branch)
    }

    /// Projects surviving alternatives columnwise. `None` means no alternatives
    /// survive; otherwise crossed combinations may be admitted. Derive first,
    /// then summarize only when a consumer does not need branch correlations.
    fn guarantees(branches: &[Self::Domains]) -> Option<Self::Domains> {
        branches.iter().cloned().reduce(Self::union_domains)
    }
}

/// Domain used to select logical shards, before selecting serving replicas.
///
/// Partition-key constraints resolve to partition IDs; partition-ID constraints
/// already identify shards. Node constraints identify nodes directly. `None`
/// means unsharded, not a promise about execution location. Node role eligibility,
/// replica preferences, and mandatory leader requirements are placement concerns.
/// This classification alone does not identify a routing column or select shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardingScheme {
    /// Unsharded.
    None,
    /// Partitioned by node IDs.
    Node,
    /// Partitioned by partition IDs.
    PartitionId,
    /// Partitioned by partition key.
    PartitionKey,
}

/// Query-facing row representation, independent of physical tables.
///
/// A schema owns its row family and Arrow builder. It does not identify a physical
/// source or require one to exist: a [`SchemaBinding`] supplies that association
/// for a source read. The GAT allows borrowed rows without forcing stored records
/// to use that representation. `NAME` is the stable logical identity, independent
/// of any session-local SQL alias.
///
/// Arrow exposure is optional per column. Native types determine selection
/// semantics; Arrow nullability may remain permissive. Initial candidate scans
/// filter physical keys only. Consumers retain the SQL residual and materialize
/// every column it needs before applying the final output projection.
pub trait LogicalSchema: SelectionSchema + Sized + 'static {
    /// Stable source-schema identity, independent of session aliases.
    const NAME: &'static str;
    /// Shard domain; resolving constraints and replica placement is a later step.
    const SHARDING: ShardingScheme;

    /// Row passed to a scan callback, which may borrow materialized data.
    type Row<'a>;
    /// Schema-owned adapter; callers need no concrete row-type equality bound.
    type RowBuilder: RowBuilder<Self>;

    /// Optional representation of a column in this schema's Arrow view.
    fn arrow_type(column: Self::Column) -> Option<DataType>;

    /// Exposes only SQL-bound columns with Arrow representations.
    fn arrow_schema() -> SchemaRef {
        arrow_schema::<Self>(Self::arrow_type)
    }
}

/// Projection-aware, reusable Arrow adapter for a schema's borrowed row family.
/// The input projection must include dependencies of any remaining residuals.
pub trait RowBuilder<S: LogicalSchema>: Send + 'static {
    /// Creates a builder for a projection of the schema's Arrow view, retaining
    /// its field/schema metadata and column order. Duplicate columns are invalid.
    fn new(schema: SchemaRef) -> Self;
    /// Number of rows accumulated since the previous completed batch.
    fn num_rows(&self) -> usize;
    /// Appends projected columns without retaining the borrowed row.
    fn append(&mut self, row: S::Row<'_>);
    /// Finishes a batch and resets the builder, retaining its schema.
    fn finish(
        &mut self,
    ) -> datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>;

    /// Consumes the builder for its final batch, avoiding next-batch reservations.
    fn into_batch(self)
    -> datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>;
}

fn arrow_schema<T: SelectionSchema>(
    arrow_type: impl Fn(T::Column) -> Option<DataType>,
) -> SchemaRef {
    Arc::new(Schema::new(
        T::COLUMNS
            .iter()
            .filter_map(|&column| {
                Some(ArrowField::new(
                    T::sql_name(column)?,
                    arrow_type(column)?,
                    true,
                ))
            })
            .collect::<Vec<_>>(),
    ))
}

/// Dependencies used by the table declaration macro.
#[doc(hidden)]
pub mod __private {
    use std::sync::Arc;

    pub use anyhow;
    pub use datafusion;
    pub use datafusion::arrow::datatypes::{DataType, SchemaRef};
    pub use datafusion::physical_plan::PhysicalExpr;
    pub use enum_map;
    pub use paste::paste;
    pub use restate_util_string::ReString;

    use crate::selection::domain::{self, Domain};
    use crate::selection::{SelectionValue, comparison_domain};

    // Compatibility support for schema-only declaration fixtures until the
    // proc-macro consolidation; Arrow metadata is not part of `SelectionSchema`.
    pub fn arrow_schema<T: crate::SelectionSchema>(
        arrow_type: impl Fn(T::Column) -> Option<DataType>,
    ) -> datafusion::arrow::datatypes::SchemaRef {
        super::arrow_schema::<T>(arrow_type)
    }

    /// Binds a SQL column to its declared native value type.
    pub fn column_domain<V: SelectionValue + Ord + Clone>(
        filters: &[Arc<dyn PhysicalExpr>],
        column: &str,
    ) -> anyhow::Result<Domain<V>> {
        domain::column_domain(filters, column, |op, literal| {
            Ok(comparison_domain(op, literal).unwrap_or_else(Domain::all))
        })
    }
}
