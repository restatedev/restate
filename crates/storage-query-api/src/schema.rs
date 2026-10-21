// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Native columns and SQL bindings for logical selections.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};

use crate::selection::ValuePredicateBuilder;

mod macros;

#[cfg(test)]
mod tests;

/// Native columns and their selection semantics, independent of physical keys.
pub trait SelectionSchema {
    /// Logical column identities, independent of physical key positions.
    type Column: enum_map::Enum + Copy + Eq + 'static;

    /// One condition on a logical row. A column may be stored in a key or value,
    /// or obtained by fetching a base record, depending on the chosen access path.
    type Clause;

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

/// Dependencies used by the declaration macro.
#[doc(hidden)]
pub mod __private {
    pub use datafusion::arrow::datatypes::{DataType, SchemaRef};
    pub use enum_map;
    pub use paste::paste;
    pub use restate_util_string::ReString;

    pub fn arrow_schema<T: crate::SelectionSchema>(
        arrow_type: impl Fn(T::Column) -> Option<DataType>,
    ) -> SchemaRef {
        super::arrow_schema::<T>(arrow_type)
    }
}
