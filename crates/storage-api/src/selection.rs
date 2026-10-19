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
//! logical fields and typed values. It does not state *how* to find them: key
//! layout, encodings, and seek strategy belong to the storage implementation;
//! expression syntax and SQL semantics belong to the adapter building the selection.
//!
//! # From a query to a scan
//!
//! - [`define_table!`](crate::define_table) declares a table marker type, and
//!   [`define_selector!`](crate::define_selector) implements [`Selector`] for it
//!   with a field enum and a typed clause enum.
//! - A caller that knows the schema constructs clause variants directly. A
//!   generic adapter, such as a query engine, resolves fields by tag and asks the
//!   target to build clauses. The target selects the field's value type; the
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
//! - **Exact.** Storage must return only records satisfying every clause, whether
//!   or not a clause narrows the scan, and must reject a clause it cannot evaluate
//!   rather than ignore it.
//! - **NULL is a value.** `None` is ordered before every `Some`. SQL
//!   three-valued logic is the adapter's job; see [`ValuePredicate`].
//! - **Typed literals.** A literal converts, is
//!   [`Unsupported`](LiteralConversionError::Unsupported) (leave the predicate
//!   to the adapter), or is
//!   [`Unrepresentable`](LiteralConversionError::Unrepresentable) (no value
//!   equals it).
//! - **Live tightening.** A [`LiveSelection`] can supply stricter selections while a
//!   scan runs.
//!
//! # Example
//!
//! An adapter translating `kind = <string>` and `handler LIKE 'get%'`:
//!
//! ```
//! use restate_storage_api::selection::{
//!     LiteralConversionError, Selection, SelectionLiteral, SelectionValue, Selector, ValuePredicate,
//!     ValuePredicateBuilder,
//! };
//! use restate_storage_api::{define_selector, define_table};
//! use restate_types::vqueues::EntryKind;
//! use restate_util_string::ReString;
//!
//! define_table! {
//!     /// Example table.
//!     pub Entries;
//! }
//!
//! define_selector! {
//!     pub Entries {
//!         /// The handler name, or NULL when absent.
//!         handler: Option<ReString> => starts_with,
//!         /// The entry kind.
//!         kind: EntryKind,
//!     }
//! }
//!
//! /// `<field> = <string literal>`, converted to whatever type the field has.
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
//! let kind = Entries::field_from_tag("kind").unwrap();
//! let handler = Entries::field_from_tag("handler").unwrap();
//! let selection = Selection::<Entries>::All
//!     .and(Entries::value_clause(kind, EqString("invocation")).unwrap())
//!     .and(Entries::starts_with_clause(handler, "get").unwrap());
//!
//! // The same selection, built by a caller that knows the schema.
//! let typed = Selection::<Entries>::All
//!     .and(EntriesClause::Kind(ValuePredicate::Equal(EntryKind::Invocation)))
//!     .and(EntriesClause::HandlerStartsWith("get".into()));
//!
//! let Selection::Predicates(fields) = &selection else { unreachable!() };
//! assert!(matches!(
//!     fields.for_field(EntriesField::Kind),
//!     [EntriesClause::Kind(ValuePredicate::Equal(EntryKind::Invocation))]
//! ));
//! assert!(matches!(
//!     Entries::value_clause(kind, EqString("unknown")),
//!     Some(EntriesClause::Kind(ValuePredicate::In(kinds))) if kinds.is_empty()
//! ));
//! // `kind` was not declared with `=> starts_with`.
//! assert!(Entries::starts_with_clause(kind, "inv").is_none());
//! ```

use std::ops::Bound;

use enum_map::EnumMap;

mod macros;
mod value;

pub use value::{LiteralConversionError, SelectionLiteral, SelectionValue};

/// Dependencies used by the exported selector macro.
#[doc(hidden)]
pub mod __private {
    pub use enum_map;
    pub use paste::paste;
    pub use restate_util_string::ReString;
}

/// A logical record schema whose fields support selection constraints.
///
/// The target defines the supported clauses independently of physical key
/// layout, value encoding, and scan strategy. Implement it with
/// [`define_selector!`](crate::define_selector).
pub trait Selector {
    /// One condition on a record of this target.
    ///
    /// A clause may constrain a field stored in a key, a value, or both,
    /// depending on the access path.
    type Clause;

    /// Logical field identities, independent of their position in a physical key.
    type Field: enum_map::Enum + Copy + Eq;

    /// Returns the field constrained by a clause.
    ///
    /// Different operations on the same field must return the same identity.
    fn field(clause: &Self::Clause) -> Self::Field;

    /// Returns the canonical tag used to identify a logical field.
    ///
    /// Adapters and physical key schemas refer to fields by tag, for example
    /// by SQL column name.
    fn tag(field: Self::Field) -> &'static str;

    /// Resolves a canonical field tag; the inverse of [`tag`](Self::tag).
    ///
    /// Returns `None` for tags that this target does not recognize.
    fn field_from_tag(tag: &str) -> Option<Self::Field>;

    /// Builds a value clause using the selected field's logical value type.
    ///
    /// The builder owns input-specific parsing rules; the target only dispatches
    /// to the field's type and wraps the resulting predicate in its clause variant.
    /// Returns `None` when the builder does. This means no clause was produced,
    /// not that the condition matches no records.
    fn value_clause<P: ValuePredicateBuilder>(
        field: Self::Field,
        builder: P,
    ) -> Option<Self::Clause>;

    /// Builds a case-sensitive literal-prefix clause when the field supports it.
    ///
    /// NULL never matches; an empty prefix matches every non-NULL string.
    /// The prefix is literal: `%` and `_` have no wildcard meaning.
    /// Returns `None` for fields without prefix support, without copying the input.
    fn starts_with_clause(field: Self::Field, prefix: &str) -> Option<Self::Clause>;

    /// Borrows the literal prefix from a StartsWith clause.
    ///
    /// Returns `None` for other clauses and `Some("")` for an empty prefix.
    /// Use [`field`](Self::field) to identify the constrained field.
    fn starts_with_prefix(clause: &Self::Clause) -> Option<&str>;
}

/// Builds a predicate after a target selects the field's concrete value type.
///
/// [`Selector::value_clause`] calls `build` with `V` set to the field's
/// value type. Adapters can implement this to translate their own expression and
/// NULL semantics without exposing those expression types to the storage API.
/// Since `build` is generic, it learns about `V` only through [`SelectionValue`]
/// conversion results. See the [module example](crate::selection#example).
pub trait ValuePredicateBuilder {
    /// Returns `None` when the input cannot safely constrain this value type.
    /// To express a condition known to match nothing, return
    /// `Some(ValuePredicate::In(Vec::new()))` instead.
    fn build<V: SelectionValue>(self) -> Option<ValuePredicate<V>>;
}

/// Conditions that records must satisfy to be returned.
///
/// Clauses are combined with logical AND, including multiple clauses on the
/// same field. Contradictory clauses match no records.
///
/// A selection describes matching semantics, not an execution strategy. A clause
/// that cannot contribute iterator bounds must still be evaluated before a
/// record is returned.
#[derive(Default)]
pub enum Selection<T: Selector> {
    /// Matches every record.
    #[default]
    All,
    /// Matches no records.
    Empty,
    /// Matches records satisfying every clause.
    ///
    /// A container with no clauses is equivalent to `All`.
    Predicates(FieldPredicates<T>),
}

impl<T: Selector> Selection<T> {
    /// Adds a condition with logical AND.
    ///
    /// `All` becomes `Predicates`, and `Empty` stays `Empty`.
    /// Clauses are appended without deduplication or contradiction detection;
    /// storage is responsible for evaluating their conjunction.
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

/// A source of selections that may tighten while a scan is running, such as a TopK
/// threshold that tightens as better candidates arrive.
///
/// Each selection must be sound on its own: records it rejects are not needed by the
/// consumer, regardless of earlier or later selections. Selections are complete
/// snapshots, not additional clauses. A scan may replace its previous live selection
/// with the latest one, ANDed with its static selection, and poll as often as it
/// chooses. Records already returned are not retracted, so the consumer must
/// still enforce its current predicate.
pub trait LiveSelection<T: Selector>: Send {
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

/// Clauses grouped by their logical field, usually built with [`Selection::and`].
///
/// All clauses are conjunctive. A field with no clauses is unconstrained.
pub struct FieldPredicates<T: Selector> {
    fields: EnumMap<T::Field, Vec<T::Clause>>,
}

impl<T: Selector> Default for FieldPredicates<T> {
    fn default() -> Self {
        Self {
            fields: EnumMap::default(),
        }
    }
}

impl<T: Selector> FieldPredicates<T> {
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

    /// Iterates every logical field and its clauses, including unconstrained
    /// fields with an empty slice.
    ///
    /// Fields are visited in enum-map index order, which is declaration order for
    /// [`define_selector!`](crate::define_selector), not physical key order.
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
