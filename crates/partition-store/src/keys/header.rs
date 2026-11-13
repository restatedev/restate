// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Key headers: the fixed part in front of an ordered key's fields, and the scope
//! of keys one read covers.
//!
//! # Concepts
//!
//! - **Header**: the bytes that identify a key's table and owner, before any
//!   field: `key kind | partition key` for the existing partition-keyed tables, or
//!   `key kind | partition | index` for the keys of one index. A key's fields come
//!   right after it, and every key of a table has a header of the same length.
//! - **Scope**: the owners one read covers: a range of partition keys, or one
//!   index of one partition ([`IndexScope`]). It is a value of the header's
//!   [`KeyScope`] type, never a SQL column.
//! - **Header bounds** ([`HeaderBounds`]): the scope turned into exact byte bounds
//!   on the header, in the table's existing on-disk layout. Declaring a scope never
//!   changes how a header is encoded.
//!
//! For example, a partition-keyed table's header is `kind | partition key`, so the
//! scope `KeyRange 100..=200` becomes the half-open byte interval
//!
//! ```text
//! [ kind | 100 ,  kind | 201 )        partition keys as big-endian u64
//! ```
//!
//! and a read over it visits every key of those partition keys, whatever their
//! fields. A [`KeyHeader`] knows its length and builds these bounds; a
//! [`KeySchema`] ties a key type to its header, so a cursor can skip the header
//! and hand the fields to the key's decoder.

use std::marker::PhantomData;
use std::ops::Bound;

use bytes::Bytes;

use restate_storage_api::StorageError;
use restate_storage_query_api::SelectionSchema;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision};
use restate_types::identifiers::PartitionKey;
use restate_types::sharding::{KeyRange, PartitionId};

use crate::scan::{PhysicalScan, TableScan};
use crate::table::ReadMetadata;
use crate::{Result, ScanMode};

use super::selection::{KeySelection, OrderedKeySchema, PreparedFieldPredicate};
use super::{EncodeTableKeyPrefix, KeyDecode, KeyDecoder, KeyKind};

/// What a read covers: a range of partition keys, one partition, one index of one
/// partition, or `()` for a table whose keys need no owner bound.
pub trait KeyScope: private::Sealed + Copy + Send + Sync + 'static {}

impl KeyScope for PartitionId {}
impl KeyScope for KeyRange {}
impl KeyScope for () {}
impl KeyScope for IndexScope {}

/// The keys of one index in one partition. Index keys put the index's ID, issued
/// when the index is registered (a later commit), right after the partition in
/// their header, so one scope selects exactly that index's keys:
///
/// ```text
/// index key:   kind | pad | partition | index | field | … | field
///                          └── IndexScope { partition, index } ──┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexScope {
    pub partition: PartitionId,
    pub index: u32,
}

mod private {
    pub trait Sealed {}
    impl Sealed for restate_types::sharding::PartitionId {}
    impl Sealed for restate_types::sharding::KeyRange {}
    impl Sealed for () {}
    impl Sealed for super::IndexScope {}
}

/// A header layout: its key kind, its length, and how a scope becomes byte bounds
/// on it. The bounds must select exactly the keys of that kind owned by the scope,
/// no more and no fewer. Nothing here says anything about the bytes after the
/// header; whether the fields are ordered is the key's business.
pub trait KeyHeader: 'static {
    type Scope: KeyScope;

    const KEY_KIND: KeyKind;
    const LENGTH: usize;

    /// The scope a read covers on its partition. Layouts keyed by a registered
    /// index resolve it here and fail when the read must not use the index.
    fn scope(metadata: &ReadMetadata<'_>) -> Result<Self::Scope>;

    /// The byte bounds of `scope`, in the header's on-disk layout; `None` means
    /// the scope is empty. A prefix fixes one complete header of exactly `LENGTH`
    /// bytes; a range lets the header vary, as over several partition keys, and
    /// its endpoints need not be complete headers.
    fn bounds(scope: Self::Scope) -> Option<HeaderBounds>;
}

/// Byte bounds on the header alone: one complete header, or a half-open interval
/// of headers. Conditions on the fields apply separately, inside these bounds.
pub struct HeaderBounds(pub(crate) PhysicalScan<Bytes>);

impl HeaderBounds {
    /// Fixes one complete header, ending immediately before the key's suffix.
    pub fn prefix(key_kind: KeyKind, prefix: Bytes) -> Self {
        Self(PhysicalScan::Prefix(key_kind, prefix))
    }

    /// A half-open byte interval. Endpoints need not decode as complete headers.
    pub fn range(key_kind: KeyKind, start: Bytes, end: Bytes) -> Option<Self> {
        (start < end).then(|| {
            Self(PhysicalScan::RangeExclusive(
                key_kind,
                ScanMode::from_range(&start, &end),
                start,
                end,
            ))
        })
    }
}

/// A key type's header, so a read can bound it by scope and skip the header
/// before decoding. Independent of logical schemas and ordered fields; if the key
/// also implements `OrderedKeySchema`, its fields are exactly the bytes after the
/// header, and both must name the same key kind.
pub trait KeySchema: KeyDecode + 'static {
    type Header: KeyHeader;

    /// A decoder over `key`, positioned after the header, which the cursor has
    /// already checked. Legacy keys whose owned type includes header components
    /// override this to keep the header.
    fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
        let payload = key
            .get(Self::Header::LENGTH..)
            .ok_or(StorageError::DataIntegrityError)?;
        Ok(KeyDecoder::from_payload(payload))
    }
}

/// The one interval covering every range of `domain`, for headers that take a
/// single partition-key range. `None` means no partition key qualifies. Gaps
/// between the ranges are not excluded here; the residual filter rejects records
/// in them.
pub(crate) fn partition_key_envelope(domain: &Domain<PartitionKey>) -> Option<KeyRange> {
    let mut bounds = domain.ranges().filter_map(|(lower, upper)| {
        let start = match lower {
            Bound::Unbounded => 0,
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v.checked_add(1)?,
        };
        let end = match upper {
            Bound::Unbounded => PartitionKey::MAX,
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v.checked_sub(1)?,
        };
        (start <= end).then(|| KeyRange::new(start, end))
    });
    let first = bounds.next()?;
    Some(KeyRange::new(
        first.start(),
        bounds.last().unwrap_or(first).end(),
    ))
}

/// Keys with a `kind | partition key` header followed by ordered fields.
pub(crate) trait PartitionKeyed:
    OrderedKeySchema + KeySchema<Header = PartitionKeyHeader<Self>> + EncodeTableKeyPrefix
{
}

impl<K> PartitionKeyed for K where
    K: OrderedKeySchema + KeySchema<Header = PartitionKeyHeader<K>> + EncodeTableKeyPrefix
{
}

/// Prepares a selection over a [`PartitionKeyed`] key. `field` translates clauses
/// into conditions on the fields, as [`KeySelection::for_key_fields`] does.
/// Clauses on `header_columns` constrain the header instead: `header` turns each
/// into a set of partition keys, and the read is restricted to the one interval
/// covering their intersection.
///
/// A header-derived restriction makes the result inexact. For example,
/// `id IN (A, B)` with `A` on partition key 10 and `B` on 30 restricts the header
/// to `10..=30`, while `field` constrains the rest of the ID; a record on
/// partition key 20 whose field matches `A` lies inside both bounds and must be
/// rejected by the residual filter.
pub(crate) fn prepare_partition_keyed<S: SelectionSchema, K: PartitionKeyed>(
    selection: &Selection<S>,
    header_columns: &[S::Column],
    header: impl Fn(&S::Clause) -> Option<Domain<PartitionKey>>,
    field: impl FnMut(&S::Clause, &mut Vec<u8>) -> Result<Option<PreparedFieldPredicate>>,
) -> Result<(KeySelection<K>, SelectionPrecision)> {
    prepare_partition_keyed_with(selection, header_columns, header, field, |_, _| Ok(None))
}

/// [`prepare_partition_keyed`] with field restrictions derived from other columns
/// (see `KeySelection::for_key_fields_with`).
pub(crate) fn prepare_partition_keyed_with<S: SelectionSchema, K: PartitionKeyed>(
    selection: &Selection<S>,
    header_columns: &[S::Column],
    header: impl Fn(&S::Clause) -> Option<Domain<PartitionKey>>,
    field: impl FnMut(&S::Clause, &mut Vec<u8>) -> Result<Option<PreparedFieldPredicate>>,
    derive: impl FnMut(&'static str, &mut Vec<u8>) -> Result<Option<PreparedFieldPredicate>>,
) -> Result<(KeySelection<K>, SelectionPrecision)> {
    let request = KeySelection::for_key_fields_with(selection, field, derive)?;
    let Selection::Predicates(columns) = selection else {
        let precision = request.precision();
        return Ok((request, precision));
    };
    let clauses = || {
        header_columns
            .iter()
            .flat_map(|column| columns.for_column(*column))
    };
    let keys = clauses()
        .filter_map(header)
        .fold(Domain::all(), Domain::intersect);
    let precision = if clauses().next().is_none() {
        request.precision()
    } else {
        SelectionPrecision::Inexact
    };
    Ok(match partition_key_envelope(&keys) {
        Some(range) => (request.restrict_scope(range), precision),
        None => (KeySelection::empty(), SelectionPrecision::Exact),
    })
}

/// The header of the existing partition-keyed tables, `key kind | partition key`,
/// usable with ordered and legacy suffixes alike. `K`'s table declaration supplies
/// the key kind.
pub struct PartitionKeyHeader<K>(PhantomData<fn() -> K>);

impl<K: EncodeTableKeyPrefix + 'static> KeyHeader for PartitionKeyHeader<K> {
    type Scope = KeyRange;

    const KEY_KIND: KeyKind = K::KEY_KIND;
    const LENGTH: usize = KeyKind::SERIALIZED_LENGTH + size_of::<u64>();

    fn scope(metadata: &ReadMetadata<'_>) -> Result<KeyRange> {
        Ok(metadata.partition.key_range)
    }

    fn bounds(scope: KeyRange) -> Option<HeaderBounds> {
        (scope.start() <= scope.end())
            .then(|| HeaderBounds(TableScan::<K>::ScanPartitionKeyRange(scope).into()))
    }
}
