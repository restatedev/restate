// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Bound;

use enum_map::EnumMap;

mod macros;
mod value;

pub use value::{FilterLiteral, FilterValue, LiteralConversionError};

/// Dependencies used by the exported filter macro.
#[doc(hidden)]
pub mod __private {
    pub use enum_map;
    pub use paste::paste;
    pub use restate_util_string::ReString;
}

/// A logical record schema whose fields can be filtered.
///
/// The target defines the supported clauses independently of physical key
/// layout, value encoding, and scan strategy.
pub trait FilterTarget {
    /// One condition on a record of this target.
    ///
    /// A clause may constrain a field stored in a key, a value, or both,
    /// depending on the access path.
    type Clause;

    /// Logical field identities, independent of their position in a physical key.
    type Field: enum_map::EnumArray<Vec<Self::Clause>> + Copy + Eq;

    /// Returns the field constrained by a clause.
    ///
    /// Different operations on the same field must return the same identity.
    fn field(clause: &Self::Clause) -> Self::Field;

    /// Returns the canonical tag used to identify a logical field.
    fn tag(field: Self::Field) -> &'static str;

    /// Resolves a canonical field tag.
    ///
    /// Returns `None` for tags that this target does not recognize.
    fn field_from_tag(tag: &str) -> Option<Self::Field>;

    /// Builds a value clause using the selected field's logical value type.
    ///
    /// The builder owns input-specific parsing rules; the target only dispatches
    /// to the field's type and wraps the resulting predicate in its clause variant.
    fn value_clause<P: ValuePredicateBuilder>(
        field: Self::Field,
        builder: P,
    ) -> Option<Self::Clause>;

    /// Builds a case-sensitive literal-prefix clause when the field supports it.
    ///
    /// NULL never matches; an empty prefix matches every non-NULL string.
    /// Returns `None` for fields without prefix support, without copying the input.
    fn starts_with_clause(field: Self::Field, prefix: &str) -> Option<Self::Clause>;

    /// Borrows the literal prefix from a StartsWith clause.
    ///
    /// Returns `None` for other clauses and `Some("")` for an empty prefix.
    /// Use `field` to identify the constrained field; this exposes no physical
    /// encoding or codec capability.
    fn starts_with_prefix(clause: &Self::Clause) -> Option<&str>;
}

/// Builds a predicate after a target selects the field's concrete value type.
///
/// Adapters can implement this to translate their own expression and NULL
/// semantics without exposing those expression types to the storage API.
pub trait ValuePredicateBuilder {
    /// Returns `None` when the input cannot safely constrain this value type.
    fn build<V: FilterValue>(self) -> Option<ValuePredicate<V>>;
}

/// Conditions that records must satisfy to be returned.
///
/// Clauses are combined with logical AND, including multiple clauses on the
/// same field. Contradictory clauses match no records.
///
/// A filter describes matching semantics, not an execution strategy. A clause
/// that cannot contribute iterator bounds must still be evaluated before a
/// record is returned.
#[derive(Default)]
pub enum Filter<T: FilterTarget> {
    /// Matches every record.
    #[default]
    All,
    /// Matches no records.
    Empty,
    /// Matches records satisfying every clause.
    ///
    /// A container with no clauded is equivalent to `All`.
    Predicates(FieldPredicates<T>),
}

impl<T: FilterTarget> Filter<T> {
    /// Adds a condition with logical AND.
    ///
    /// Adding a condition to an empty filter keeps it empty.
    pub fn and(self, clause: T::Clause) -> Self {
        let mut predicates = match self {
            Self::All => FieldPredicates::default(),
            Self::Empty => return Self::Empty,
            Self::Predicates(predicates) => predicates,
        };

        predicates.insert(clause);
        Self::Predicates(predicates)
    }
}

/// A predicate over logical values, using the equality and ordering of `V`.
///
/// These are storage-value predicates, not SQL expressions. SQL adapters must
/// translate SQL comparison and NULL semantics into these predicates.
///
/// For `V = Option<U>`, `None` is an ordinary selectable value and sorts before
/// every `Some(value)`. Consequently:
///
/// - `Equal(None)` selects NULL values.
/// - A range from `Excluded(None)` to `Unbounded` selects all non-NULL values.
/// - An unbounded lower endpoint does not exclude NULL.
/// - An empty string is a value distinct from both NULL and an empty selection.
///
/// When creating filter from SQL queries, `IS NULL` can become `Equal(None)`,
/// whereas equality with NULL must not: that comparison cannot evaluate to true.
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

/// Clauses grouped by their logical field.
///
/// All clauses are conjunctive. A field with no clauses is unconstrained.
pub struct FieldPredicates<T: FilterTarget> {
    fields: EnumMap<T::Field, Vec<T::Clause>>,
}

impl<T: FilterTarget> Default for FieldPredicates<T> {
    fn default() -> Self {
        Self {
            fields: EnumMap::default(),
        }
    }
}

impl<T: FilterTarget> FieldPredicates<T> {
    /// Adds a condition, preserving any existing conditions on the field.
    pub fn insert(&mut self, clause: T::Clause) {
        let field = T::field(&clause);
        self.fields[field].push(clause);
    }

    /// Returns the conditions on a field.
    ///
    /// An empty slice means the field is unconstrained.
    pub fn for_field(&self, field: T::Field) -> &[T::Clause] {
        &self.fields[field]
    }

    /// Iterates logical fields and their clauses, independently of physical key order.
    pub fn iter(&self) -> impl Iterator<Item = (T::Field, &[T::Clause])> {
        self.fields
            .iter()
            .map(|(field, clauses)| (field, clauses.as_slice()))
    }

    /// Returns whether no field has any conditions.
    pub fn is_unconstrained(&self) -> bool {
        self.fields.values().all(Vec::is_empty)
    }
}
