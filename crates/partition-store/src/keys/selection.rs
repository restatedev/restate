// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prepared physical key restrictions, including implicit owner-local bounds.
//!
//! [`KeySelection`] binds clauses by tag, then stores them in physical field
//! order. All clauses are ANDed, including multiple clauses on the same field.
//! [`codec::FieldPredicateCodec`] translates literals before scanning; evaluation
//! borrows encoded fields without constructing their owned values.
//! Composite headers supply exact ownership bounds independently of suffix
//! ordering. Legacy keys can use `all` and `restrict_scope` without implementing
//! `OrderedKeySchema`. Ordered suffix predicates support seeks under a fixed
//! header and per-key matching when ownership spans multiple header values.
//!
//! A scan has two parts: an enclosing [`PhysicalScan`] for the iterator and a
//! [`KeySelectionCursor`] for exact matching within it. The interval can contain
//! holes, such as excluded IN-list values or rejected combinations of fields.
//! The caller must evaluate each visited key and honor [`KeyMatch`] by processing
//! a match, seeking forward, or ending the scan. A seek target is a byte boundary
//! and need not decode as a complete key.
//!
//! A live selection can refine navigation inside the original interval. Each update
//! replaces the previous live snapshot; it is checked in addition to the static
//! selection and does not rebuild the iterator's bounds.

// Physical scans and concrete selection bindings are introduced separately.
#![allow(dead_code)]

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
use super::{KeyKind, OrderedFieldCodec};

pub(crate) mod codec;

#[cfg(test)]
mod tests;

/// Physical field order and codecs, independent of the logical column enumeration.
/// Every field must use the ordered-field encoding contract, even if unconstrained.
/// A legacy table layout cannot be used merely because its decoded value types
/// also have ordered encodings; the stored representation must match these codecs.
pub trait OrderedKeySchema {
    /// The physical key kind containing these keys.
    const KEY_KIND: KeyKind;
    /// Fields after the fixed key prefix, in their encoded order.
    const FIELDS: &'static [OrderedKeyField];
}

/// Prepares a logical selection using this key's declared physical codecs.
/// The schema parameter keeps preparation usable through `Stat::OwnedKey`
/// without repeating the concrete key type in scan implementations.
pub trait PrepareKeySelection<T: SelectionSchema>: OrderedKeySchema + Sized {
    /// Unsupported acceleration widens the candidates; conversion errors propagate.
    /// Inspect the returned precision relative to the input native selection.
    fn prepare_candidates(selection: &Selection<T>) -> Result<KeySelection<Self>>;
}

/// A physical field's tag and the codec used to find its encoded boundary.
/// The logical target resolves this tag to its own column enumeration.
pub struct OrderedKeyField {
    tag: &'static str,
    /// Borrows one field and advances the input to the next field.
    take: for<'a> fn(&mut &'a [u8]) -> Result<&'a [u8]>,
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
        }
    }
}

/// Takes one encoded field without constructing an owned value.
fn take_field<'a, C: OrderedFieldCodec>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
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

/// All conditions on one physical field, combined with AND.
struct PreparedField {
    schema: &'static OrderedKeyField,
    predicates: Vec<PreparedFieldPredicate>,
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

    /// Intersects the field's constraints to find an enclosing scan interval.
    fn bounds<'a>(&'a self, literals: &'a [u8]) -> Result<FieldBounds<'a>> {
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

        let FieldBounds::Interval { lower, upper } = self.bounds(literals)? else {
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

/// Owns an AND of prepared conditions in physical key order.
///
/// Construction resolves tags, encodes literals, and detects impossible field
/// constraints where possible. Evaluation reuses this preparation. Scan bounds
/// enclose possible matches; the cursor still checks every visited key.
pub struct KeySelection<K> {
    fields: Vec<PreparedField>,
    /// Shared storage for equality, IN-list, and range literals. Predicates
    /// contain offsets into this buffer rather than individually owned values.
    literals: Box<[u8]>,
    /// Explicitly empty, or a field's conditions were proven impossible.
    empty: bool,
    precision: SelectionPrecision,
    /// Optional caller restriction; the local owner's bounds are always applied
    /// independently when binding a cursor, including unconstrained scans.
    scope: Option<HeaderBounds>,
    _key: PhantomData<fn() -> K>,
}

/// What to do after checking a key inside the compiled scan interval.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KeyMatch {
    /// The key matches; its value can now be processed.
    Match,
    /// The key does not match; advance normally. Used when a varying header
    /// prevents suffix-only navigation from proving a safe cross-header seek.
    Skip,
    /// A strictly forward, in-bounds absolute byte target. Seek to the first key
    /// at or after it and evaluate that key again; the target need not exist.
    Seek(Bytes),
    /// No later key in this scan interval can match.
    Done,
}

impl<K> KeySelection<K> {
    /// An unconstrained physical read. Local ownership is still enforced at execution.
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

    /// Prepares necessary key restrictions. Missing columns and explicitly
    /// unsupported operations widen the result and mark it inexact. Other errors
    /// propagate. The consumer applies residuals after row materialization.
    /// A returned predicate must be exact on valid stored field values. Returning
    /// `None` must preserve the literal bytes belonging to earlier clauses.
    pub(crate) fn for_key_fields<T: SelectionSchema>(
        selection: &Selection<T>,
        mut prepare: impl FnMut(&T::Clause, &mut Vec<u8>) -> Result<Option<PreparedFieldPredicate>>,
    ) -> Result<Self>
    where
        K: OrderedKeySchema,
    {
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
        // Payload-only predicates need neither field preparation nor per-key
        // boundary parsing. The materializer still validates the decoded record.
        if !has_key_clauses {
            return Ok(result);
        }
        result.fields.reserve(K::FIELDS.len());
        let mut literals = Vec::new();
        // Key order comes from the schema, not from the logical column enum.
        for schema in K::FIELDS {
            let mut prepared = Vec::new();
            if let Some(column) = T::column_from_tag(schema.tag()) {
                for clause in predicates.for_column(column) {
                    match prepare(clause, &mut literals)? {
                        Some(predicate) => prepared.push(predicate),
                        None => result.precision = SelectionPrecision::Inexact,
                    }
                }
            }
            result.fields.push(PreparedField {
                schema,
                predicates: prepared,
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
        result.literals = literals.into_boxed_slice();
        // Check every field, even beyond an unbounded field that stops positioning.
        for field in &result.fields {
            if matches!(field.bounds(&result.literals)?, FieldBounds::Empty) {
                result.empty = true;
                // A necessary restriction proving emptiness also proves the
                // complete selection empty, even if other clauses were skipped.
                result.precision = SelectionPrecision::Exact;
            }
        }
        Ok(result)
    }

    /// Appends a safe lower suffix. Included endpoints here are complete field
    /// values (a string prefix uses the encoding of its shortest match), so
    /// construction can continue. An exclusive or unbounded endpoint requires
    /// the iterator to discover the next actual value before we constrain its suffix.
    /// Used for both initial positioning and every reseek.
    fn append_minimum(&self, first: usize, target: &mut BytesMut) -> Result<bool> {
        for field in &self.fields[first..] {
            let FieldBounds::Interval { lower, .. } = field.bounds(&self.literals)? else {
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
            let FieldBounds::Interval { lower, upper } = field.bounds(&self.literals)? else {
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
    /// Adds an exact physical ownership restriction. Execution intersects it with
    /// the store's implicit scope; it can never widen that scope or a prior restriction.
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

    /// Specializes a shared query request for one owner. The caller supplies the
    /// local scope through the header's declared domain, not through SQL columns.
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

/// Per-scan navigation state for a forward iterator.
///
/// Field boundaries and seek storage are reused; no borrowed iterator bytes
/// survive a call. Under a fixed header, navigation can carry into preceding
/// suffix fields but never across the header. Under varying headers, predicates
/// use per-key matching; suffix exhaustion cannot terminate later header groups.
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

    /// Replaces the previous live snapshot while retaining the static selection and
    /// scan bounds. This is a complete replacement, not an accumulated conjunction
    /// with older live snapshots.
    ///
    /// Every snapshot must be sound on its own: navigation may permanently skip
    /// keys it rejects. See [`restate_storage_query_api::selection::LiveSelection`].
    #[allow(dead_code)]
    pub(crate) fn set_live(&mut self, selection: KeySelection<K>) {
        self.live = Some(selection);
    }

    /// Checks an in-bounds full key. At the first field rejected by the static or
    /// live selection, advance within that field's domain or carry to a preceding
    /// field and reset the suffix. Varying headers conservatively return `Skip`.
    ///
    /// The caller must enforce [`Self::scan`]'s bounds; this method checks the
    /// fixed identity or header length but does not recheck the whole interval. A match means the
    /// predicates passed, not that all values were decoded or validated. Rejection
    /// can stop before later fields are parsed; an unconstrained selection skips
    /// payload parsing entirely.
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
        let CursorHeader::Fixed(identity) = &self.header else {
            return Ok(KeyMatch::Skip);
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
        Ok(KeyMatch::Done)
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
