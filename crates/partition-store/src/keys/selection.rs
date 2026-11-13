// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Key selections: a logical selection prepared against one ordered key, so that
//! a scan reads only the keys that can match and seeks past the ones that cannot.
//!
//! # Concepts
//!
//! - **Ordered key**: a fixed **header** (key kind, partition, index, …) followed
//!   by **fields** in declaration order, each written by an ordered codec, so that
//!   keys sort by their first field, then their second, and so on
//!   ([`OrderedKeySchema`], declared with `define_ordered_key!`).
//! - **Terminal field**: an optional last field whose bytes run to the end of the
//!   key, such as raw name bytes. Nothing marks where it ends, so it supports only
//!   equality and membership ([`OrderedKeyField::terminal`]).
//! - **Clause** and **literal**: one condition of a logical selection on one
//!   column, such as `service_name IN ('a', 'c')`, and the values it names.
//! - **Key selection** ([`KeySelection`]): the clauses translated into conditions
//!   on the key's fields. Each clause is bound to its field by column tag, its
//!   literals are encoded with the field's codec ([`codec::FieldPredicateCodec`])
//!   and the conditions are kept in field order. They are all ANDed, several on
//!   one field included.
//! - **Exact** and **inexact**: a prepared selection is exact when every clause
//!   was translated, inexact when a clause was left out because the key lacks the
//!   column or the codec cannot translate the operation. The caller of an inexact
//!   selection keeps the original predicate as a **residual** filter on the rows.
//!   Precision is relative to the selection that was prepared, not to the SQL it
//!   came from.
//! - **Scan interval**: the byte interval the iterator covers ([`PhysicalScan`]),
//!   from the header's scope and the bounds of the leading fields.
//! - **Cursor** ([`KeySelectionCursor`]): checks every visited key field by field
//!   and, when a field is rejected, computes the next key that could match. The
//!   iterator acts on the resulting [`KeyMatch`]: process the key, skip it, seek
//!   to the target, or stop. A seek target is a byte boundary and need not decode
//!   as a complete key.
//! - **Live selection**: a tighter selection delivered while the scan runs. It
//!   replaces the previous live one and only ever rejects more keys; the scan
//!   interval does not change ([`KeySelectionCursor::set_live`]).
//!
//! # A worked example
//!
//! Key `stage | service_name | transitioned_at desc | entry_id` under one fixed
//! header, selection `stage = running AND service_name IN ('a', 'c')`:
//!
//! ```text
//! scan interval   [ running | a ,  running | c⁺ )     c⁺: the first bytes after
//!                                                     every key of service c
//! visited key                          cursor
//! running | a | t9 | e1                Match
//! running | a | t3 | e2                Match
//! running | b | t8 | e3                service_name rejects b; its next
//!                                      candidate is c → Seek(running | c)
//! running | c | t5 | e4                Match
//! end of the interval                  Done
//! ```
//!
//! The interval contains holes, here every key of service `b`, which is why the
//! cursor checks each key instead of trusting the bounds. On a rejection it first
//! tries the next value of the rejected field; when that field has none left, it
//! **carries** into the field before it, like an odometer, and starts the suffix
//! over. The bounds of every field are resolved once, when the selection is
//! prepared; a seek only looks them up.
//!
//! # Preparing
//!
//! Preparation comes in two strictnesses. The generated `prepare_selection`
//! treats a clause the key cannot use, or a constrained column the key does not
//! have, as an error. `prepare_candidates` leaves such clauses out and reports
//! the selection as inexact. A literal that cannot be converted is an error on
//! both paths. Field declaration order determines key order, whatever the order
//! of the clauses. Legacy keys without ordered fields still express their header
//! bounds through [`KeySelection::all`] and [`KeySelection::restrict_scope`].

use std::marker::PhantomData;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use restate_storage_api::StorageError;
use restate_storage_query_api::SelectionSchema;
use restate_storage_query_api::selection::{Selection, SelectionPrecision};
use restate_util_string::{EncodedMemCmpStr, MemCmpPrefix};

use crate::scan::PhysicalScan;
use crate::{Result, ScanMode};

use super::header::{HeaderBounds, KeyHeader, KeySchema};
use super::predicate::{EncodedValuePredicate, EncodedValueRanges};
use super::{FieldCodec, KeyKind, OrderedFieldCodec, TerminalFieldCodec};

pub(crate) mod codec;

#[cfg(test)]
mod tests;

/// The fields of an ordered key in their on-disk order, each with its codec;
/// `define_ordered_key!` implements it. Every field, constrained by a selection or
/// not, must use an ordered codec, except one terminal last field
/// ([`OrderedKeyField::terminal`]). A legacy layout does not qualify just because
/// its values also have ordered codecs: the bytes on disk must be the ones these
/// codecs write.
pub trait OrderedKeySchema {
    /// The physical key kind containing these keys.
    const KEY_KIND: KeyKind;
    /// Fields after the fixed key prefix, in their encoded order.
    const FIELDS: &'static [OrderedKeyField];
    /// Evaluated when a selection is prepared, failing the build if a terminal
    /// field is not the last field.
    #[doc(hidden)]
    const VALID_FIELDS: () = validate_fields(Self::FIELDS);
}

/// Panics unless every terminal field is the last field (so at most one exists).
pub(crate) const fn validate_fields(fields: &[OrderedKeyField]) {
    let mut index = 0;
    while index < fields.len() {
        assert!(
            !fields[index].terminal || index + 1 == fields.len(),
            "a terminal key field must be the last field"
        );
        index += 1;
    }
}

/// Prepares a logical selection over schema `T` against this key, with the
/// widening rules of `prepare_candidates`. Code that knows a key only through
/// its schema binding calls this instead of the generated method.
pub trait PrepareKeySelection<T: SelectionSchema>: OrderedKeySchema + Sized {
    /// Clauses the key cannot use are left out and make the result inexact; a
    /// literal that cannot be converted is an error. Check the result's
    /// [`KeySelection::precision`] to know whether a residual filter is needed.
    fn prepare_candidates(selection: &Selection<T>) -> Result<KeySelection<Self>>;
}

/// One field of an ordered key: the column tag it binds to, and the codec that
/// finds where its bytes end. A logical schema maps the tag to its own column.
pub struct OrderedKeyField {
    tag: &'static str,
    /// Borrows one field and advances the input to the next field.
    take: for<'a> fn(&mut &'a [u8]) -> Result<&'a [u8]>,
    /// Whether the field runs to the end of the key without a prefix-free encoding.
    terminal: bool,
}

impl OrderedKeyField {
    /// The logical column bound to this physical position.
    pub(crate) const fn tag(&self) -> &'static str {
        self.tag
    }

    /// Associates a logical column tag with its physical codec.
    pub(crate) const fn new<C: OrderedFieldCodec>(tag: &'static str) -> Self {
        Self {
            tag,
            take: take_field::<C>,
            terminal: false,
        }
    }

    /// Declares the last field as **terminal**: its bytes run to the end of the
    /// key, like raw name bytes, with nothing marking where they end.
    ///
    /// Such bytes are not prefix-free: `ab` is a prefix of `abc`, so the keys
    /// `… | ab` and `… | abc` sit next to each other, and "the first key after
    /// `ab`" is not a safe seek target: it is past `abc` as well, a different value
    /// that a range `name > 'ab'` must still visit. So a terminal field prepares
    /// only equality and IN. When a value is rejected, the cursor moves to the next
    /// listed value, or gives the field up and carries into the field before it;
    /// it never seeks past a byte prefix. Ranges and prefixes on it stay residual.
    /// A terminal field anywhere but last fails the build of the first selection
    /// prepared for the key.
    pub(crate) const fn terminal<C: TerminalFieldCodec>(tag: &'static str) -> Self {
        Self {
            tag,
            take: take_field::<C>,
            terminal: true,
        }
    }
}

/// Takes one encoded field without constructing an owned value.
fn take_field<'a, C: FieldCodec>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let original = *input;
    C::take_encoded(input)?;
    // Borrow the consumed bytes from the input, rather than from a temporary view.
    Ok(&original[..original.len() - input.len()])
}

/// A prepared condition on a complete encoded field. Prefix conditions retain
/// their exact matching semantics alongside their encoded bounds.
pub(crate) enum PreparedFieldPredicate {
    /// Equality, membership, or a range over encoded field values.
    Value(EncodedValuePredicate),
    /// A union of inclusive intervals in encoded field order.
    Ranges(EncodedValueRanges),
    /// A case-sensitive string-prefix condition.
    Prefix {
        prefix: MemCmpPrefix,
        /// Presence-tagged lower bound for nullable fields. Non-nullable fields
        /// use the prefix's own encoding instead.
        nullable_lower: Option<Box<[u8]>>,
        /// Exclusive byte boundary, presence-tagged for nullable fields. This is
        /// not a complete field value and must not be extended with suffix fields.
        upper: Option<Box<[u8]>>,
    },
}

impl PreparedFieldPredicate {
    /// Whether this is an equality or IN predicate over complete values.
    fn is_finite(&self) -> bool {
        matches!(self, Self::Value(value) if value.is_finite())
    }

    /// Prepares a string-prefix match and its half-open encoded interval.
    pub(crate) fn prefix(value: &str, nullable: bool) -> Self {
        let prefix = MemCmpPrefix::new(value);
        let nullable_lower = nullable.then(|| {
            // Option fields encode None as [0] and Some(s) as [1] + encode(s).
            // Include the presence tag in the bound so it skips NULL values.
            let mut lower = Vec::with_capacity(1 + prefix.encoded_lower_bound().len());
            lower.push(1);
            lower.extend_from_slice(prefix.encoded_lower_bound());
            lower.into_boxed_slice()
        });
        let mut upper = Vec::new();
        if nullable {
            upper.push(1);
        }
        let upper = if prefix.encode_upper_bound(&mut upper) {
            Some(upper.into_boxed_slice())
        } else if nullable {
            // The empty prefix matches every Some(string), but not None.
            upper[0] = 2;
            Some(upper.into_boxed_slice())
        } else {
            None
        };
        Self::Prefix {
            prefix,
            nullable_lower,
            upper,
        }
    }

    /// Checks one field, borrowing value literals from the owning selection.
    fn matches(&self, literals: &[u8], encoded: &[u8]) -> Result<bool> {
        match self {
            Self::Value(predicate) => Ok(predicate.matches(literals, encoded)),
            Self::Ranges(ranges) => Ok(ranges.matches(literals, encoded)),
            Self::Prefix {
                prefix,
                nullable_lower,
                ..
            } => {
                // The prefix matcher expects string bytes without the Option tag.
                let encoded = if nullable_lower.is_some() {
                    match encoded.split_first() {
                        Some((0, [])) => return Ok(false),
                        Some((1, value)) => value,
                        _ => return Err(StorageError::DataIntegrityError),
                    }
                } else {
                    encoded
                };
                let (value, remaining) = EncodedMemCmpStr::try_ref_from_prefix(encoded)
                    .map_err(|_| StorageError::DataIntegrityError)?;
                if !remaining.is_empty() {
                    return Err(StorageError::DataIntegrityError);
                }
                Ok(prefix.matches(value))
            }
        }
    }

    /// Returns field bounds. A prefix's exclusive upper endpoint is already a byte
    /// boundary and can be used directly as the iterator's upper bound.
    fn bounds<'a>(&'a self, literals: &'a [u8]) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
        match self {
            Self::Value(predicate) => predicate.bounds(literals),
            Self::Ranges(ranges) => ranges.bounds(literals),
            Self::Prefix {
                prefix,
                nullable_lower,
                upper,
            } => (
                Included(
                    nullable_lower
                        .as_deref()
                        .unwrap_or_else(|| prefix.encoded_lower_bound()),
                ),
                upper.as_deref().map_or(Unbounded, Excluded),
            ),
        }
    }
}

/// All conditions on one physical field, combined with AND, and the interval
/// enclosing their matches, resolved once when the selection is prepared.
struct PreparedField {
    schema: &'static OrderedKeyField,
    predicates: Vec<PreparedFieldPredicate>,
    bounds: ResolvedBounds,
}

/// Bounds enclosing a field's possible matches; values inside may still be rejected.
enum FieldBounds<'a> {
    /// The field's conditions cannot match any value.
    Empty,
    /// An enclosing interval. Two unbounded endpoints impose no restriction.
    Interval {
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    },
}

/// [`FieldBounds`] kept as offsets into the selection's literal buffer: computed
/// once at preparation, looked up on every seek.
#[derive(Clone, Copy)]
enum ResolvedBounds {
    Empty,
    Interval {
        lower: Bound<Span>,
        upper: Bound<Span>,
    },
}

/// The byte range of one resolved endpoint in the literal buffer.
#[derive(Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
}

impl PreparedField {
    /// The smallest finite candidate set, shared by bound construction and seeks.
    fn finite_predicate(&self, literals: &[u8]) -> Option<&EncodedValuePredicate> {
        self.predicates
            .iter()
            .filter_map(|predicate| match predicate {
                PreparedFieldPredicate::Value(value) => {
                    Some((value.values(literals)?.len(), value))
                }
                _ => None,
            })
            .min_by_key(|(len, _)| *len)
            .map(|(_, predicate)| predicate)
    }

    /// Requires every condition to match; an unconstrained field always matches.
    fn matches(&self, literals: &[u8], encoded: &[u8]) -> Result<bool> {
        for predicate in &self.predicates {
            if !predicate.matches(literals, encoded)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The enclosing interval resolved at preparation, borrowed from `literals`.
    fn bounds<'a>(&self, literals: &'a [u8]) -> FieldBounds<'a> {
        match self.bounds {
            ResolvedBounds::Empty => FieldBounds::Empty,
            ResolvedBounds::Interval { lower, upper } => FieldBounds::Interval {
                lower: lower.map(|span| &literals[span.start..span.end]),
                upper: upper.map(|span| &literals[span.start..span.end]),
            },
        }
    }

    /// Resolves the enclosing interval from the field's conditions and stores its
    /// endpoints in `literals`, after the bytes already there. Runs once, when
    /// the selection is prepared; [`Self::bounds`] then only looks the result up.
    fn resolve_bounds(&self, literals: &mut Vec<u8>) -> Result<ResolvedBounds> {
        let (lower, upper) = match self.compute_bounds(literals)? {
            FieldBounds::Empty => return Ok(ResolvedBounds::Empty),
            FieldBounds::Interval { lower, upper } => {
                (lower.map(<[u8]>::to_vec), upper.map(<[u8]>::to_vec))
            }
        };
        let mut store = |bound: Bound<Vec<u8>>| {
            bound.map(|bytes| {
                let start = literals.len();
                literals.extend_from_slice(&bytes);
                Span {
                    start,
                    end: literals.len(),
                }
            })
        };
        Ok(ResolvedBounds::Interval {
            lower: store(lower),
            upper: store(upper),
        })
    }

    /// Intersects the field's constraints to find an enclosing scan interval.
    fn compute_bounds<'a>(&'a self, literals: &'a [u8]) -> Result<FieldBounds<'a>> {
        // A finite domain lets us resolve intersections exactly, including IN
        // holes, ranges, and prefix constraints, without cloning candidate values.
        let values = self
            .finite_predicate(literals)
            .and_then(|predicate| predicate.values(literals));
        if let Some(values) = values {
            let mut first = None;
            let mut last = None;
            for value in values {
                if self.matches(literals, value)? {
                    first.get_or_insert(value);
                    last = Some(value);
                }
            }
            return Ok(match (first, last) {
                (Some(first), Some(last)) => FieldBounds::Interval {
                    lower: Included(first),
                    upper: Included(last),
                },
                _ => FieldBounds::Empty,
            });
        }

        let mut lower = Unbounded;
        let mut upper = Unbounded;
        for predicate in &self.predicates {
            let (next_lower, next_upper) = predicate.bounds(literals);
            lower = tighter_bound(lower, next_lower, true);
            upper = tighter_bound(upper, next_upper, false);
        }
        if let (Included(lo) | Excluded(lo), Included(hi) | Excluded(hi)) = (lower, upper)
            && (lo > hi
                || (lo == hi && (matches!(lower, Excluded(_)) || matches!(upper, Excluded(_)))))
        {
            return Ok(FieldBounds::Empty);
        }
        Ok(FieldBounds::Interval { lower, upper })
    }

    /// Finds a safe boundary strictly after `current` within this field's domain.
    /// A finite domain supplies a complete value. For a continuous domain, an
    /// exclusive boundary lets the iterator discover the next actual value.
    fn advance<'a>(&'a self, literals: &'a [u8], current: &'a [u8]) -> Result<FieldAdvance<'a>> {
        if let Some(values) = self
            .finite_predicate(literals)
            .and_then(|predicate| predicate.values_after(literals, current))
        {
            for value in values {
                if self.matches(literals, value)? {
                    return Ok(FieldAdvance::Value(value));
                }
            }
            return Ok(FieldAdvance::Exhausted);
        }

        let FieldBounds::Interval { lower, upper } = self.bounds(literals) else {
            return Ok(FieldAdvance::Exhausted);
        };
        match lower {
            Included(value) if current < value => return Ok(FieldAdvance::Value(value)),
            Excluded(value) if current <= value => return Ok(FieldAdvance::After(value)),
            _ => {}
        }
        if matches!(upper, Included(end) | Excluded(end) if current >= end) {
            return Ok(FieldAdvance::Exhausted);
        }
        let mut next = None;
        for predicate in &self.predicates {
            if predicate.matches(literals, current)? {
                continue;
            }
            // Inside the enclosing bounds, an interval union may still have a
            // gap. Seek past every rejecting interval's gap before retrying the
            // conjunction. A prefix mismatch exhausts its contiguous domain.
            let PreparedFieldPredicate::Ranges(ranges) = predicate else {
                return Ok(FieldAdvance::Exhausted);
            };
            let Some(start) = ranges.next_start(literals, current) else {
                return Ok(FieldAdvance::Exhausted);
            };
            next = Some(next.map_or(start, |previous: &[u8]| previous.max(start)));
        }
        Ok(next.map_or(FieldAdvance::After(current), FieldAdvance::Value))
    }
}

enum FieldAdvance<'a> {
    /// A complete encoded value; lower bounds for later fields may be appended.
    Value(&'a [u8]),
    /// End of a value's entire suffix group. This is a byte boundary, not a value.
    After(&'a [u8]),
    /// No later value in this field's domain can match under the current parent.
    Exhausted,
}

/// Chooses the larger lower bound or smaller upper bound.
/// For equal values, an excluded endpoint is more restrictive.
fn tighter_bound<'a>(
    left: Bound<&'a [u8]>,
    right: Bound<&'a [u8]>,
    lower: bool,
) -> Bound<&'a [u8]> {
    let (a, b) = match (left, right) {
        (Unbounded, _) => return right,
        (_, Unbounded) => return left,
        (Included(a) | Excluded(a), Included(b) | Excluded(b)) => (a, b),
    };
    if (lower && a > b) || (!lower && a < b) || (a == b && matches!(left, Excluded(_))) {
        left
    } else {
        right
    }
}

/// A logical selection prepared against key `K`: the AND of its conditions, one
/// group per field in key order, with every literal encoded once into a shared
/// buffer and every field's enclosing bounds resolved once.
///
/// Preparation binds clauses to fields by tag, encodes their literals and proves
/// a field empty where it can. From there the selection yields the scan interval
/// ([`Self::scan`]) and, bound to one store, a cursor ([`Self::cursor`]) that
/// checks each key and computes seeks. The interval encloses the matches; the
/// cursor still checks every visited key.
pub struct KeySelection<K> {
    fields: Vec<PreparedField>,
    /// Shared storage for equality, IN-list, and range literals. Predicates
    /// contain offsets into this buffer rather than individually owned values.
    literals: Box<[u8]>,
    /// Explicitly empty, or a field's conditions were proven impossible.
    empty: bool,
    precision: SelectionPrecision,
    /// A scope the caller restricted the read to ([`Self::restrict_scope`]). The
    /// store's own scope is intersected with it when a cursor is bound, so it can
    /// only narrow the read.
    scope: Option<HeaderBounds>,
    _key: PhantomData<fn() -> K>,
}

/// What to do after checking a key inside the compiled scan interval.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KeyMatch {
    /// The key matches; its value can now be processed.
    Match,
    /// The key does not match; advance normally. Used when a live scope rejects
    /// a key that the static bounds admit.
    Skip,
    /// A strictly forward, in-bounds absolute byte target. Seek to the first key
    /// at or after it and evaluate that key again; the target need not exist.
    Seek(Bytes),
    /// No later key in this scan interval can match.
    Done,
}

impl<K> KeySelection<K> {
    /// A selection that matches no key; its scan is `None`.
    pub fn empty() -> Self {
        Self {
            empty: true,
            ..Self::all()
        }
    }

    /// A selection that accepts every key. The store's own scope still applies
    /// when a cursor is bound.
    pub fn all() -> Self {
        Self {
            fields: Vec::new(),
            literals: Box::default(),
            empty: false,
            precision: SelectionPrecision::Exact,
            scope: None,
            _key: PhantomData,
        }
    }

    /// Precision relative to the native selection used during preparation.
    pub fn precision(&self) -> SelectionPrecision {
        self.precision
    }

    /// Binds logical clauses to physical fields and collects their encoded literals.
    ///
    /// Returns an error if a constrained logical column is absent from `K` or the
    /// preparation callback rejects a clause. Fields absent from the logical
    /// target remain unconstrained. `Selection::All` needs no field preparation;
    /// `Selection::Empty` produces no scan.
    ///
    /// The callback must return predicates for the corresponding physical codec
    /// and preserve any previously appended bytes in `literals`.
    pub(crate) fn new<T: SelectionSchema>(
        selection: &Selection<T>,
        mut prepare: impl FnMut(&T::Clause, &mut Vec<u8>) -> Result<PreparedFieldPredicate>,
    ) -> Result<Self>
    where
        K: OrderedKeySchema,
    {
        if let Selection::Predicates(predicates) = selection {
            for (column, clauses) in predicates.iter() {
                if !clauses.is_empty()
                    && !K::FIELDS
                        .iter()
                        .any(|schema| T::column_from_tag(schema.tag()) == Some(column))
                {
                    return Err(StorageError::Conversion(anyhow::anyhow!(
                        "selection column {} is not available in this key",
                        T::tag(column)
                    )));
                }
            }
        }
        Self::for_key_fields(selection, |clause, literals| {
            prepare(clause, literals).map(Some)
        })
    }

    /// Prepares the clauses the key can use and leaves the rest out: a column the
    /// key lacks, or an operation `prepare` declines with `None`, widens the result
    /// and marks it inexact, so the caller must apply the original predicate to
    /// the rows. Any other error propagates. A predicate `prepare` returns must be
    /// exact for valid stored values, and declining must leave the literal bytes
    /// of earlier clauses in place.
    pub(crate) fn for_key_fields<T: SelectionSchema>(
        selection: &Selection<T>,
        prepare: impl FnMut(&T::Clause, &mut Vec<u8>) -> Result<Option<PreparedFieldPredicate>>,
    ) -> Result<Self>
    where
        K: OrderedKeySchema,
    {
        Self::for_key_fields_with(selection, prepare, |_, _| Ok(None))
    }

    /// [`Self::for_key_fields`], plus restrictions that other columns imply for a
    /// field, e.g. a composite ID constraining its component fields. `derive`
    /// receives each field's tag once, before its clauses are prepared. A derived
    /// predicate must hold for every matching record; the original clause stays
    /// residual, so components prepared independently may admit cross-products.
    pub(crate) fn for_key_fields_with<T: SelectionSchema>(
        selection: &Selection<T>,
        mut prepare: impl FnMut(&T::Clause, &mut Vec<u8>) -> Result<Option<PreparedFieldPredicate>>,
        mut derive: impl FnMut(&'static str, &mut Vec<u8>) -> Result<Option<PreparedFieldPredicate>>,
    ) -> Result<Self>
    where
        K: OrderedKeySchema,
    {
        let () = K::VALID_FIELDS;
        let mut result = Self {
            empty: matches!(selection, Selection::Empty),
            ..Self::all()
        };
        let Selection::Predicates(predicates) = selection else {
            return Ok(result);
        };
        if predicates.is_unconstrained() {
            return Ok(result);
        }
        let mut has_key_clauses = false;
        for (column, _) in predicates.iter().filter(|(_, clauses)| !clauses.is_empty()) {
            if K::FIELDS
                .iter()
                .any(|field| T::column_from_tag(field.tag()) == Some(column))
            {
                has_key_clauses = true;
            } else {
                result.precision = SelectionPrecision::Inexact;
            }
        }
        let mut literals = Vec::new();
        let mut derived = K::FIELDS
            .iter()
            .map(|schema| derive(schema.tag(), &mut literals))
            .collect::<Result<Vec<_>>>()?;
        // Payload-only predicates need neither field preparation nor per-key
        // boundary parsing. The materializer still validates the decoded record.
        if !has_key_clauses && derived.iter().all(Option::is_none) {
            return Ok(result);
        }
        result.fields.reserve(K::FIELDS.len());
        // Key order comes from the schema, not from the logical column enum.
        for (schema, derived) in K::FIELDS.iter().zip(&mut derived) {
            let mut prepared = Vec::new();
            match derived.take() {
                // A terminal field accepts only finite predicates.
                Some(predicate) if !schema.terminal || predicate.is_finite() => {
                    prepared.push(predicate)
                }
                _ => {}
            }
            if let Some(column) = T::column_from_tag(schema.tag()) {
                for clause in predicates.for_column(column) {
                    match prepare(clause, &mut literals)? {
                        // A terminal field accepts only finite predicates.
                        Some(predicate) if !schema.terminal || predicate.is_finite() => {
                            prepared.push(predicate)
                        }
                        _ => result.precision = SelectionPrecision::Inexact,
                    }
                }
            }
            result.fields.push(PreparedField {
                schema,
                predicates: prepared,
                // Resolved below, once it is known that the key is used at all.
                bounds: ResolvedBounds::Empty,
            });
        }
        if result
            .fields
            .iter()
            .all(|field| field.predicates.is_empty())
        {
            // Every key operation was unsupported. Preserve the unconstrained
            // cursor fast path, just as for a selection on payload columns only.
            result.fields = Vec::new();
            return Ok(result);
        }
        // Resolve every field's bounds once, even beyond an unbounded field that
        // stops positioning: a field proven empty empties the whole selection.
        for field in &mut result.fields {
            field.bounds = field.resolve_bounds(&mut literals)?;
            if matches!(field.bounds, ResolvedBounds::Empty) {
                result.empty = true;
                // A necessary restriction proving emptiness also proves the
                // complete selection empty, even if other clauses were skipped.
                result.precision = SelectionPrecision::Exact;
            }
        }
        result.literals = literals.into_boxed_slice();
        Ok(result)
    }

    /// Appends a safe lower suffix. Included endpoints here are complete field
    /// values (a string prefix uses the encoding of its shortest match), so
    /// construction can continue. An exclusive or unbounded endpoint requires
    /// the iterator to discover the next actual value before we constrain its suffix.
    /// Used for both initial positioning and every reseek.
    fn append_minimum(&self, first: usize, target: &mut BytesMut) -> Result<bool> {
        for field in &self.fields[first..] {
            let FieldBounds::Interval { lower, .. } = field.bounds(&self.literals) else {
                return Ok(false);
            };
            match lower {
                Unbounded => break,
                Included(value) => target.extend_from_slice(value),
                Excluded(value) => {
                    let start = target.len();
                    target.extend_from_slice(value);
                    return Ok(advance_prefix(target, start));
                }
            }
        }
        Ok(true)
    }

    /// Builds a prefix scan or half-open range under a fixed physical identity.
    /// Lower bounds use the same suffix construction as reseeks. The upper bound
    /// is an envelope: only singleton fields allow it to extend to another field.
    ///
    /// `prefix` must end immediately before `K::FIELDS`; it must not include any
    /// of those fields. Returns `None` when the constraints prove the scan empty.
    /// Keys inside the returned bounds still require cursor evaluation.
    #[cfg(test)]
    pub(crate) fn scan(&self, prefix: &[u8]) -> Result<Option<PhysicalScan<Bytes>>>
    where
        K: OrderedKeySchema,
    {
        self.scan_with_prefix(K::KEY_KIND, prefix)
    }

    fn scan_with_prefix(
        &self,
        key_kind: KeyKind,
        prefix: &[u8],
    ) -> Result<Option<PhysicalScan<Bytes>>> {
        if self.empty {
            return Ok(None);
        }
        let mut start = BytesMut::from(prefix);
        if !self.append_minimum(0, &mut start)? {
            return Ok(None);
        }
        let mut end = BytesMut::from(prefix);
        for field in &self.fields {
            let FieldBounds::Interval { lower, upper } = field.bounds(&self.literals) else {
                return Ok(None);
            };
            if let (Included(lo), Included(hi)) = (lower, upper)
                && lo == hi
            {
                end.extend_from_slice(hi);
                continue;
            }
            if matches!(upper, Unbounded) && start == end {
                return Ok(Some(PhysicalScan::Prefix(key_kind, end.freeze())));
            }
            match upper {
                Included(value) => {
                    end.extend_from_slice(value);
                    if !advance_prefix(&mut end, 0) {
                        return Err(StorageError::DataIntegrityError);
                    }
                }
                Excluded(value) => end.extend_from_slice(value),
                Unbounded => {
                    if !advance_prefix(&mut end, 0) {
                        return Err(StorageError::DataIntegrityError);
                    }
                }
            }
            if start >= end {
                return Ok(None);
            }
            return Ok(Some(PhysicalScan::RangeExclusive(
                key_kind,
                ScanMode::from_range(&start, &end),
                start.freeze(),
                end.freeze(),
            )));
        }
        Ok(Some(PhysicalScan::Prefix(key_kind, end.freeze())))
    }

    /// Binds this immutable selection to one physical scan and its navigation state.
    /// Uses the same fixed-prefix contract as [`Self::scan`]; `None` means empty.
    #[cfg(test)]
    pub(crate) fn into_cursor(self, prefix: &[u8]) -> Result<Option<KeySelectionCursor<K>>>
    where
        K: OrderedKeySchema,
    {
        Arc::new(self).fixed_cursor(K::KEY_KIND, prefix)
    }

    /// Creates independent navigation state while sharing the immutable predicates.
    fn fixed_cursor(
        self: Arc<Self>,
        key_kind: KeyKind,
        prefix: &[u8],
    ) -> Result<Option<KeySelectionCursor<K>>> {
        let Some(scan) = self.scan_with_prefix(key_kind, prefix)? else {
            return Ok(None);
        };
        // Share the fixed identity with the lower bound rather than copying it.
        let identity = match &scan {
            PhysicalScan::Prefix(_, start) | PhysicalScan::RangeExclusive(_, _, start, _) => {
                start.slice(..prefix.len())
            }
        };
        let scan = match &self.scope {
            Some(scope) => match scan.intersect(&scope.0) {
                Some(scan) => scan,
                None => return Ok(None),
            },
            None => scan,
        };
        Ok(Some(self.with_cursor(scan, CursorHeader::Fixed(identity))))
    }

    fn with_cursor(
        self: Arc<Self>,
        scan: PhysicalScan<Bytes>,
        header: CursorHeader,
    ) -> KeySelectionCursor<K> {
        let fields = self.fields.len();
        KeySelectionCursor {
            selection: self,
            live: None,
            position: Position {
                scan,
                header,
                offsets: Vec::with_capacity(if fields == 0 { 0 } else { fields + 1 }),
                target: BytesMut::new(),
            },
        }
    }

    /// Records field boundaries in `offsets` up to the first field this selection
    /// rejects, and returns that field's index.
    fn first_rejected(&self, payload: &[u8], offsets: &mut Vec<usize>) -> Result<Option<usize>> {
        if self.fields.is_empty() {
            return Ok(None);
        }
        let mut remaining = payload;
        offsets.clear();
        offsets.push(0);
        for (index, field) in self.fields.iter().enumerate() {
            let encoded = (field.schema.take)(&mut remaining)?;
            offsets.push(payload.len() - remaining.len());
            if !field.matches(&self.literals, encoded)? {
                return Ok(Some(index));
            }
        }
        if !remaining.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(None)
    }
}

impl<K: KeySchema> KeySelection<K> {
    /// Restricts the read to `scope`, such as one partition-key range. The store's
    /// own scope is intersected with it when a cursor is bound, so a restriction
    /// can only narrow a read, never widen it or a previous restriction.
    pub fn restrict_scope(mut self, scope: <K::Header as KeyHeader>::Scope) -> Self {
        if self.empty {
            return self;
        }
        let scan = K::Header::bounds(scope).and_then(|bounds| match &self.scope {
            Some(current) => bounds.0.intersect(&current.0),
            None => Some(bounds.0),
        });
        self.empty = scan.is_none();
        self.scope = scan.map(HeaderBounds);
        self
    }

    /// Binds this selection to one store, whose own keys are `scope` (its
    /// partition keys, or one index of one partition). The read never leaves that
    /// scope, whatever the selection says; `None` means the two cannot overlap.
    /// One selection, shared behind the `Arc`, serves every store a query touches.
    pub(crate) fn cursor(
        self: Arc<Self>,
        scope: <K::Header as KeyHeader>::Scope,
    ) -> Result<Option<KeySelectionCursor<K>>> {
        if self.empty {
            return Ok(None);
        }
        let Some(bounds) = K::Header::bounds(scope) else {
            return Ok(None);
        };
        if let PhysicalScan::Prefix(kind, prefix) = &bounds.0 {
            return self.fixed_cursor(*kind, prefix);
        }
        // A query that pins one complete header navigates like a fixed owner.
        // Headers are indivisible units of ownership: a pinned header inside this
        // owner's bounds lies entirely within them, and one outside is not owned.
        if let Some(HeaderBounds(PhysicalScan::Prefix(kind, prefix))) = &self.scope
            && prefix.len() == K::Header::LENGTH
        {
            if !bounds.0.contains_key(prefix) {
                return Ok(None);
            }
            let (kind, prefix) = (*kind, prefix.clone());
            return self.fixed_cursor(kind, &prefix);
        }
        let scan = match &self.scope {
            Some(scope) => match bounds.0.intersect(&scope.0) {
                Some(scan) => scan,
                None => return Ok(None),
            },
            None => bounds.0,
        };
        Ok(Some(self.with_cursor(
            scan,
            CursorHeader::Varying(K::Header::LENGTH),
        )))
    }
}

/// A [`KeySelection`] bound to one scan: the interval to iterate, and the state
/// that turns each rejected key into the next seek target.
///
/// Field offsets and seek storage are reused from key to key; nothing borrowed
/// from the iterator survives a call. A rejection carries into earlier fields of
/// the same key but never into the header: under one fixed header, exhausting the
/// fields ends the scan; under varying headers (a partition-key range), the cursor
/// seeks to the next header and starts over.
pub(crate) struct KeySelectionCursor<K> {
    selection: Arc<KeySelection<K>>,
    /// The latest live selection. Within the static scan plan, it rejects keys and
    /// navigates past the ranges it rejects, like the static selection.
    live: Option<KeySelection<K>>,
    position: Position,
}

impl<K> KeySelectionCursor<K> {
    /// The static bounds the caller must enforce while iterating and seeking.
    pub(crate) fn scan(&self) -> &PhysicalScan<Bytes> {
        &self.position.scan
    }

    /// Installs a tighter live selection. It replaces the previous live one rather
    /// than combining with it, so every live selection must be sound on its own:
    /// keys it rejects are skipped for good. The static selection and the scan
    /// interval stay as they are. See
    /// [`restate_storage_query_api::selection::LiveSelection`].
    #[allow(dead_code)]
    pub(crate) fn set_live(&mut self, selection: KeySelection<K>) {
        self.live = Some(selection);
    }

    /// Checks one key that lies inside [`Self::scan`] and says what to do next. At
    /// the first field the static or live selection rejects, it computes the next
    /// key that could match: a later value of that field, or a carry into the
    /// field before it with the suffix reset, always within the key's header.
    ///
    /// Keys are checked lazily: an unconstrained selection parses nothing, and a
    /// rejection stops at the field that failed. `Match` means the conditions
    /// passed, not that every field was decoded and validated. The caller keeps the
    /// iterator inside the scan bounds; this method only checks the header.
    pub(crate) fn evaluate(&mut self, key: &[u8]) -> Result<KeyMatch> {
        let payload = match &self.position.header {
            CursorHeader::Fixed(identity) => key.strip_prefix(identity.as_ref()),
            CursorHeader::Varying(length) => key.get(*length..),
        }
        .ok_or(StorageError::DataIntegrityError)?;
        if self
            .live
            .as_ref()
            .and_then(|live| live.scope.as_ref())
            .is_some_and(|scope| !scope.0.contains_key(key))
        {
            return Ok(KeyMatch::Skip);
        }
        for selection in std::iter::once(self.selection.as_ref()).chain(self.live.as_ref()) {
            if selection.empty {
                return Ok(KeyMatch::Done);
            }
            if let Some(rejected) = selection.first_rejected(payload, &mut self.position.offsets)? {
                return self.position.advance(selection, key, payload, rejected);
            }
        }
        Ok(KeyMatch::Match)
    }
}

/// The scan's bounds and reusable seek storage, shared by the static and live selections.
struct Position {
    scan: PhysicalScan<Bytes>,
    header: CursorHeader,
    offsets: Vec<usize>,
    target: BytesMut,
}

enum CursorHeader {
    Fixed(Bytes),
    Varying(usize),
}

impl Position {
    /// Finds the next key that `selection` may accept after `key`, which it rejected
    /// at field `rejected`. Every skipped key is rejected by `selection`.
    fn advance<K>(
        &mut self,
        selection: &KeySelection<K>,
        key: &[u8],
        payload: &[u8],
        rejected: usize,
    ) -> Result<KeyMatch> {
        let identity = match &self.header {
            CursorHeader::Fixed(identity) => identity.as_ref(),
            CursorHeader::Varying(length) => &key[..*length],
        };
        for index in (0..=rejected).rev() {
            let start = self.offsets[index];
            let current = &payload[start..self.offsets[index + 1]];
            let next = selection.fields[index].advance(&selection.literals, current)?;
            self.target.clear();
            self.target.extend_from_slice(identity);
            self.target.extend_from_slice(&payload[..start]);
            match next {
                FieldAdvance::Exhausted => continue,
                FieldAdvance::Value(value) => {
                    self.target.extend_from_slice(value);
                    if !selection.append_minimum(index + 1, &mut self.target)? {
                        return Ok(KeyMatch::Done);
                    }
                }
                FieldAdvance::After(value) => {
                    let field_start = self.target.len();
                    self.target.extend_from_slice(value);
                    if !advance_prefix(&mut self.target, field_start) {
                        continue;
                    }
                }
            }
            if self.target.as_ref() <= key {
                return Err(StorageError::DataIntegrityError);
            }
            if !self.scan.contains_key(&self.target) {
                return Ok(KeyMatch::Done);
            }
            return Ok(KeyMatch::Seek(self.target.split().freeze()));
        }
        // The suffix is exhausted within this header. Seek to the first key after
        // it; carries may shorten the header, so no suffix bound is appended.
        let CursorHeader::Varying(length) = self.header else {
            return Ok(KeyMatch::Done);
        };
        self.target.clear();
        self.target.extend_from_slice(&key[..length]);
        if !advance_prefix(&mut self.target, KeyKind::SERIALIZED_LENGTH)
            || !self.scan.contains_key(&self.target)
        {
            return Ok(KeyMatch::Done);
        }
        Ok(KeyMatch::Seek(self.target.split().freeze()))
    }
}

/// Moves past a complete prefix, truncating trailing 0xff bytes. Restricting the
/// carry to `start..` prevents a field boundary from modifying its parent.
fn advance_prefix(bytes: &mut BytesMut, start: usize) -> bool {
    let Some(last) = bytes[start..].iter().rposition(|byte| *byte != 0xff) else {
        return false;
    };
    let last = start + last;
    bytes[last] += 1;
    bytes.truncate(last + 1);
    true
}
