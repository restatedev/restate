// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use bytes::{Bytes, BytesMut};

use restate_storage_api::StorageError;
use restate_storage_api::filter::{Filter, FilterTarget};
use restate_util_string::{EncodedMemCmpStr, MemCmpPrefix};

use crate::scan::PhysicalScan;
use crate::{Result, ScanMode, TableKind};

use super::IndexFieldDecode;
use super::predicate::{PreparedIndexPredicate, PreparedIndexRanges};

pub(crate) mod codec;

#[cfg(test)]
mod tests;

/// Physical field order and codecs, independent of the logical field enumeration.
pub(crate) trait IndexKeySchema {
    /// The table containing these keys.
    const TABLE: TableKind;
    /// Fields after the fixed key prefix, in their encoded order.
    const FIELDS: &'static [IndexKeyField];
}

/// A field's logical tag and the codec used to find its encoded boundary.
pub(crate) struct IndexKeyField {
    tag: &'static str,
    /// Borrows one field and advances the input to the next field.
    take: for<'a> fn(&mut &'a [u8]) -> Result<&'a [u8]>,
}

impl IndexKeyField {
    /// Associates a logical field tag with its physical codec.
    pub(crate) const fn new<C: IndexFieldDecode + ?Sized>(tag: &'static str) -> Self {
        Self {
            tag,
            take: take_field::<C>,
        }
    }
}

/// Takes one encoded field without constructing an owned value.
fn take_field<'a, C: IndexFieldDecode + ?Sized>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    Ok(C::take_encoded(input)?.as_ref())
}

/// A prepared condition on a complete encoded field. Prefix conditions retain
/// their exact matching semantics alongside their encoded bounds.
pub(crate) enum PreparedFieldPredicate {
    /// Equality, membership, or a range over encoded field values.
    Value(PreparedIndexPredicate),
    /// A union of inclusive intervals in encoded field order.
    Ranges(PreparedIndexRanges),
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

    /// Checks one field, borrowing value literals from the owning filter.
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
    schema: &'static IndexKeyField,
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
    fn finite_predicate(&self, literals: &[u8]) -> Option<&PreparedIndexPredicate> {
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

/// Owns prepared conditions in physical key order. No tag lookups or literal
/// encodings occur while evaluating keys.
pub(crate) struct PreparedKeyFilter<K> {
    fields: Vec<PreparedField>,
    /// Shared storage for equality, IN-list, and range literals. Predicates
    /// contain offsets into this buffer rather than individually owned values.
    literals: Box<[u8]>,
    /// Explicitly empty, or a field's conditions were proven impossible.
    empty: bool,
    _key: PhantomData<fn() -> K>,
}

/// What to do after checking a key inside the compiled scan interval.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KeyMatch {
    /// The key matches; its value can now be processed.
    Match,
    /// A forward, in-bounds absolute seek target.
    Seek(Bytes),
    /// No later key in this scan interval can match.
    Done,
}

impl<K: IndexKeySchema> PreparedKeyFilter<K> {
    /// Binds logical clauses to physical fields and collects their encoded literals.
    pub(crate) fn new<T: FilterTarget>(
        filter: &Filter<T>,
        mut prepare: impl FnMut(&T::Clause, &mut Vec<u8>) -> Result<PreparedFieldPredicate>,
    ) -> Result<Self> {
        let mut result = Self {
            fields: Vec::new(),
            literals: Box::default(),
            empty: matches!(filter, Filter::Empty),
            _key: PhantomData,
        };
        let Filter::Predicates(filter) = filter else {
            return Ok(result);
        };
        if filter.is_unconstrained() {
            return Ok(result);
        }
        result.fields.reserve(K::FIELDS.len());
        // Silently dropping a requested field would weaken the filter.
        for (field, clauses) in filter.iter() {
            if !clauses.is_empty()
                && !K::FIELDS
                    .iter()
                    .any(|schema| T::field_from_tag(schema.tag) == Some(field))
            {
                return Err(StorageError::Conversion(anyhow::anyhow!(
                    "filter field {} is not available in this key",
                    T::tag(field)
                )));
            }
        }
        let mut literals = Vec::new();
        // Key order comes from the schema, not from the logical field enum.
        for schema in K::FIELDS {
            let predicates = match T::field_from_tag(schema.tag) {
                Some(field) => filter
                    .for_field(field)
                    .iter()
                    .map(|clause| prepare(clause, &mut literals))
                    .collect::<Result<Vec<_>>>()?,
                None => Vec::new(),
            };
            result.fields.push(PreparedField { schema, predicates });
        }
        result.literals = literals.into_boxed_slice();
        // Check every field, even beyond an unbounded field that stops positioning.
        for field in &result.fields {
            if matches!(field.bounds(&result.literals)?, FieldBounds::Empty) {
                result.empty = true;
            }
        }
        Ok(result)
    }

    /// Appends a safe lower suffix. Included endpoints are complete field values,
    /// so construction can continue. An exclusive or unbounded endpoint requires
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

    /// Builds a half-open interval scoped to the caller's fixed physical prefix.
    /// Lower bounds use the same suffix construction as reseeks. The upper bound
    /// is an envelope: only singleton fields allow it to extend to another field.
    pub(crate) fn scan(&self, prefix: &[u8]) -> Result<Option<PhysicalScan<Bytes>>> {
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
                return Ok(Some(PhysicalScan::Prefix(K::TABLE, end.freeze())));
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
                K::TABLE,
                ScanMode::from_range(&start, &end),
                start.freeze(),
                end.freeze(),
            )));
        }
        Ok(Some(PhysicalScan::Prefix(K::TABLE, end.freeze())))
    }

    /// Binds this immutable filter to one physical scan and its navigation state.
    pub(crate) fn into_cursor(self, prefix: &[u8]) -> Result<Option<KeyFilterCursor<K>>> {
        let Some(scan) = self.scan(prefix)? else {
            return Ok(None);
        };
        // Share the fixed identity with the lower bound rather than copying it.
        let identity = match &scan {
            PhysicalScan::Prefix(_, start) | PhysicalScan::RangeExclusive(_, _, start, _) => {
                start.slice(..prefix.len())
            }
        };
        let offsets = Vec::with_capacity(self.fields.len() + 1);
        Ok(Some(KeyFilterCursor {
            filter: self,
            scan,
            identity,
            offsets,
            target: BytesMut::new(),
        }))
    }
}

/// Per-scan navigation state. Field boundaries and seek storage are reused; no
/// borrowed iterator bytes survive a call. Carry always stays under `identity`.
pub(crate) struct KeyFilterCursor<K> {
    filter: PreparedKeyFilter<K>,
    scan: PhysicalScan<Bytes>,
    identity: Bytes,
    offsets: Vec<usize>,
    target: BytesMut,
}

impl<K: IndexKeySchema> KeyFilterCursor<K> {
    pub(crate) fn scan(&self) -> &PhysicalScan<Bytes> {
        &self.scan
    }

    /// Checks an in-bounds full key. At the first rejected field, advance within
    /// that field's domain or carry to a preceding field and reset the suffix.
    pub(crate) fn evaluate(&mut self, key: &[u8]) -> Result<KeyMatch> {
        let payload = key
            .strip_prefix(self.identity.as_ref())
            .ok_or(StorageError::DataIntegrityError)?;
        if self.filter.fields.is_empty() {
            return Ok(KeyMatch::Match);
        }
        let mut remaining = payload;
        self.offsets.clear();
        self.offsets.push(0);
        for (index, field) in self.filter.fields.iter().enumerate() {
            let encoded = (field.schema.take)(&mut remaining)?;
            self.offsets.push(payload.len() - remaining.len());
            if !field.matches(&self.filter.literals, encoded)? {
                return self.advance(key, payload, index);
            }
        }
        if !remaining.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(KeyMatch::Match)
    }

    fn advance(&mut self, key: &[u8], payload: &[u8], rejected: usize) -> Result<KeyMatch> {
        for index in (0..=rejected).rev() {
            let start = self.offsets[index];
            let current = &payload[start..self.offsets[index + 1]];
            let next = self.filter.fields[index].advance(&self.filter.literals, current)?;
            self.target.clear();
            self.target.extend_from_slice(&self.identity);
            self.target.extend_from_slice(&payload[..start]);
            match next {
                FieldAdvance::Exhausted => continue,
                FieldAdvance::Value(value) => {
                    self.target.extend_from_slice(value);
                    if !self.filter.append_minimum(index + 1, &mut self.target)? {
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
