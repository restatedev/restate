// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logical selections for storage scans.
//!
//! A [`Selection`] states *which* records a scan returns, in terms of a table's
//! logical columns and typed values. It does not state *how* to find them: key
//! layout, encodings, and seek strategy belong to the storage implementation;
//! expression syntax and SQL semantics belong to the adapter building the selection.
//!
//! # From a query to a scan
//!
//! - [`define_table!`](crate::define_table) declares a logical table schema,
//!   including its column identities, native types, and supported clauses.
//! - A caller that knows the schema constructs clause variants directly. A
//!   generic adapter, such as a query engine, resolves columns by tag and asks the
//!   target to build clauses. The target selects the column's value type; the
//!   adapter's [`ValuePredicateBuilder`] converts literals with [`SelectionValue`].
//!   One adapter thus serves every table without naming its clause enum.
//! - Storage binds clauses to physical fields by the same tags and uses them to
//!   bound and skip its scan.
//!
//! # Contract
//!
//! - **Conjunctive and possibly partial.** A selection is the AND of its clauses.
//!   An adapter may push down only some conjuncts and evaluate the remaining
//!   conditions itself (the *residual predicate*). Every record needed by the
//!   query must still pass the selection. For example, pushing down only `a` from
//!   `a AND b` is safe; pushing down only `a` from `a OR b` is not.
//! - **Candidate reads.** Physical preparation may retain only supported clauses
//!   and reports its precision relative to this selection. Storage enforces the
//!   prepared restrictions exactly; consumers retain the SQL residual.
//! - **NULL is a value.** `None` is ordered before every `Some`. SQL
//!   three-valued logic is the adapter's job; see [`ValuePredicate`].
//! - **Typed literals.** A literal converts, is
//!   [`Unsupported`](super::LiteralConversionError::Unsupported) (leave the predicate
//!   to the adapter), or is
//!   [`Unrepresentable`](super::LiteralConversionError::Unrepresentable) (no value
//!   equals it).
//! - **Live tightening.** A [`LiveSelection`] can supply stricter selections while a
//!   scan runs.
//!
//! # Example
//!
//! An adapter translating `kind = <string>` and `handler LIKE 'get%'`:
//!
//! ```
//! use restate_storage_query_api::selection::{
//!     LiteralConversionError, Selection, SelectionLiteral, SelectionValue, ValuePredicate,
//!     ValuePredicateBuilder,
//! };
//! use restate_storage_query_api::{SelectionSchema, define_table};
//! use restate_types::vqueues::EntryKind;
//! use restate_util_string::ReString;
//!
//! define_table! {
//!     /// Example table.
//!     pub Entries {
//!         /// The handler name, or NULL when absent.
//!         #[starts_with]
//!         handler: Option<ReString>,
//!         /// The entry kind.
//!         kind: EntryKind,
//!     }
//! }
//!
//! /// `<column> = <string literal>`, converted to whatever type the column has.
//! struct EqString<'a>(&'a str);
//!
//! impl ValuePredicateBuilder for EqString<'_> {
//!     fn build<V: SelectionValue>(self) -> Option<ValuePredicate<V>> {
//!         match V::from_literal(SelectionLiteral::String(self.0)) {
//!             Ok(value) => Some(ValuePredicate::Equal(value)),
//!             // No value can be equal: the comparison matches nothing.
//!             Err(LiteralConversionError::Unrepresentable) => Some(ValuePredicate::In(Vec::new())),
//!             // Not understood: the adapter keeps evaluating it.
//!             Err(LiteralConversionError::Unsupported) => None,
//!         }
//!     }
//! }
//!
//! let kind = Entries::column_from_tag("kind").unwrap();
//! let handler = Entries::column_from_tag("handler").unwrap();
//! let selection = Selection::<Entries>::All
//!     .and(Entries::value_clause(kind, EqString("invocation")).unwrap())
//!     .and(Entries::starts_with_clause(handler, "get").unwrap());
//!
//! // The same selection, built by a caller that knows the schema.
//! let typed = Selection::<Entries>::All
//!     .and(EntriesClause::Kind(ValuePredicate::Equal(EntryKind::Invocation)))
//!     .and(EntriesClause::HandlerStartsWith("get".into()));
//!
//! let Selection::Predicates(columns) = &selection else { unreachable!() };
//! assert!(matches!(
//!     columns.for_column(EntriesColumn::Kind),
//!     [EntriesClause::Kind(ValuePredicate::Equal(EntryKind::Invocation))]
//! ));
//! assert!(matches!(
//!     Entries::value_clause(kind, EqString("unknown")),
//!     Some(EntriesClause::Kind(ValuePredicate::In(kinds))) if kinds.is_empty()
//! ));
//! // `kind` was not declared with `#[starts_with]`.
//! assert!(Entries::starts_with_clause(kind, "inv").is_none());
//! ```

use std::ops::{Bound, RangeBounds};

use enum_map::EnumMap;

use crate::SelectionSchema;

use super::SelectionValue;

/// Builds a predicate after a target selects the column's concrete value type.
///
/// [`SelectionSchema::value_clause`] calls `build` with `V` set to the column's
/// value type. Adapters can implement this to translate their own expression and
/// NULL semantics without exposing those expression types to the storage API.
/// Since `build` is generic, it learns about `V` only through [`SelectionValue`]
/// conversion results. See the [table declaration example](crate::define_table).
pub trait ValuePredicateBuilder {
    /// Returns `None` when the input cannot safely constrain this value type.
    /// To express a condition known to match nothing, return
    /// `Some(ValuePredicate::In(Vec::new()))` instead.
    fn build<V: SelectionValue>(self) -> Option<ValuePredicate<V>>;
}

/// Conditions that records must satisfy to be returned.
///
/// Clauses are combined with logical AND, including multiple clauses on the
/// same column. Contradictory clauses match no records.
///
/// A selection describes matching semantics, not an execution strategy. Candidate
/// reads may enforce only a necessary restriction, such as prepared key clauses.
/// Their consumer must retain residual filtering. Exact key preparation alone
/// does not prove that a selection represents the complete SQL predicate.
#[derive(Default)]
pub enum Selection<T: SelectionSchema> {
    /// Matches every record.
    #[default]
    All,
    /// Matches no records.
    Empty,
    /// Matches records satisfying every clause.
    ///
    /// A container with no clauses is equivalent to `All`.
    Predicates(ColumnPredicates<T>),
}

impl<T: SelectionSchema> Selection<T> {
    /// Adds a condition with logical AND.
    ///
    /// `All` becomes `Predicates`, and `Empty` stays `Empty`.
    /// Clauses are appended without deduplication or contradiction detection;
    /// storage is responsible for evaluating their conjunction.
    pub fn and(self, clause: T::Clause) -> Self {
        let mut predicates = match self {
            Self::All => ColumnPredicates::default(),
            Self::Empty => return Self::Empty,
            Self::Predicates(predicates) => predicates,
        };

        predicates.insert(clause);
        Self::Predicates(predicates)
    }
}

/// A source of selections that may tighten while a scan is running, such as a TopK
/// threshold that tightens as better candidates arrive.
///
/// Each selection must be sound on its own: records it rejects are not needed by the
/// consumer, regardless of earlier or later selections. Selections are complete
/// snapshots, not additional clauses. A scan may replace its previous live selection
/// with the latest one, ANDed with its static selection, and poll as often as it
/// chooses. Records already returned are not retracted, so the consumer must
/// still enforce its current predicate.
pub trait LiveSelection<T: SelectionSchema>: Send {
    /// Returns a snapshot that has not been polled yet, if any.
    ///
    /// `None` means no change, not the absence of a selection. `Some(Selection::Empty)`
    /// lets the scan stop.
    fn poll(&mut self) -> Option<Selection<T>>;
}

/// A predicate over logical values, using the equality and ordering of `V`.
///
/// These are storage-value predicates, not SQL expressions. SQL adapters must
/// translate SQL comparison and NULL semantics into these predicates.
///
/// For `V = Option<U>`, `None` is an ordinary selectable value and sorts before
/// every `Some(value)`. An unbounded lower endpoint therefore includes NULL, and
/// an empty string is a value distinct from both NULL and an empty selection.
/// SQL predicates on a nullable column translate as follows, where `x` is a
/// successfully converted non-NULL literal (using
/// [`SelectionValue::from_ordered_literal`] for an ordered comparison):
///
/// | SQL | `ValuePredicate<Option<U>>` |
/// | --- | --- |
/// | `col IS NULL` | `Equal(None)` |
/// | `col IS NOT NULL` | `Range { lower: Excluded(None), upper: Unbounded }` |
/// | `col = x` | `Equal(Some(x))` |
/// | `col < x` | `Range { lower: Excluded(None), upper: Excluded(Some(x)) }` |
/// | `col = NULL` | `In(vec![])`, because the comparison is never true |
pub enum ValuePredicate<V> {
    /// Matches values equal to the specified value.
    Equal(V),
    /// Matches values equal to any member of the list.
    ///
    /// An empty list matches nothing. List order and duplicate members do not
    /// affect matching semantics or cause matching records to be returned
    /// multiple times.
    In(Vec<V>),
    /// Matches values satisfying both endpoint constraints.
    ///
    /// `Included` admits equality with the endpoint; `Excluded` does not.
    /// `Unbounded` imposes no constraint on that side.
    ///
    /// If no value satisfies both bounds, the predicate matches nothing.
    /// In particular, reversed endpoints match nothing, as do equal endpoints
    /// when either endpoint is excluded. Equal, included endpoints select
    /// exactly that value. Two unbounded endpoints match every value.
    Range { lower: Bound<V>, upper: Bound<V> },
}

impl<V: PartialOrd> ValuePredicate<V> {
    /// Evaluates this native predicate, including ordinary `Option` ordering.
    /// SQL NULL semantics must already have been translated by the adapter.
    pub fn matches(&self, value: &V) -> bool {
        match self {
            Self::Equal(expected) => value == expected,
            Self::In(values) => values.contains(value),
            Self::Range { lower, upper } => (lower.as_ref(), upper.as_ref()).contains(&value),
        }
    }
}

/// Clauses grouped by their logical column, usually built with [`Selection::and`].
///
/// All clauses are conjunctive. A column with no clauses is unconstrained.
pub struct ColumnPredicates<T: SelectionSchema> {
    columns: EnumMap<T::Column, Vec<T::Clause>>,
}

impl<T: SelectionSchema> Default for ColumnPredicates<T> {
    fn default() -> Self {
        Self {
            columns: EnumMap::default(),
        }
    }
}

impl<T: SelectionSchema> ColumnPredicates<T> {
    /// Adds a condition, preserving any existing conditions on the column.
    pub fn insert(&mut self, clause: T::Clause) {
        let column = T::column(&clause);
        self.columns[column].push(clause);
    }

    /// Returns the conditions on a column.
    ///
    /// An empty slice means the column is unconstrained.
    pub fn for_column(&self, column: T::Column) -> &[T::Clause] {
        &self.columns[column]
    }

    /// Iterates every logical column and its clauses, including unconstrained
    /// columns with an empty slice.
    ///
    /// Columns are visited in enum-map index order, which is declaration order for
    /// [`define_table!`](crate::define_table), not physical key order.
    pub fn iter(&self) -> impl Iterator<Item = (T::Column, &[T::Clause])> {
        self.columns
            .iter()
            .map(|(column, clauses)| (column, clauses.as_slice()))
    }

    /// Returns whether no column has any conditions.
    pub fn is_unconstrained(&self) -> bool {
        self.columns.values().all(Vec::is_empty)
    }
}
