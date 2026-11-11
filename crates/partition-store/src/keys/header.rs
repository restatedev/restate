// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Composite physical headers and their ownership semantics. A header retains its
//! existing codec; declaring ownership does not flatten or re-encode its fields.

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

/// The ownership domain one read covers. `()` denotes a layout whose namespace
/// needs no additional ownership bound.
pub trait KeyScope: private::Sealed + Copy + Send + Sync + 'static {}

impl KeyScope for PartitionId {}
impl KeyScope for KeyRange {}
impl KeyScope for () {}
impl KeyScope for InstanceScope {}

/// One registered instance in one physical partition: the scope of layouts keyed
/// by a registry-issued physical ID, which each read resolves (see
/// `ReadMetadata::ready_instance`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceScope {
    pub partition: PartitionId,
    pub id: u32,
}

mod private {
    pub trait Sealed {}
    impl Sealed for restate_types::sharding::PartitionId {}
    impl Sealed for restate_types::sharding::KeyRange {}
    impl Sealed for () {}
    impl Sealed for super::InstanceScope {}
}

/// A composite header's physical identity and typed ownership restriction.
/// Bounds must select exactly the requested ownership domain and table identity.
/// This contract makes no ordering claim about the suffix after `LENGTH` bytes.
pub trait KeyHeader: 'static {
    type Scope: KeyScope;

    const KEY_KIND: KeyKind;
    const LENGTH: usize;

    /// The scope a read covers on its partition. Layouts keyed by a registered
    /// instance resolve it here and fail when the read must not use the instance.
    fn scope(metadata: &ReadMetadata<'_>) -> Result<Self::Scope>;

    /// Encodes the scope using the existing on-disk layout; `None` means empty.
    /// A prefix fixes the entire header and has exactly `LENGTH` bytes. A range
    /// permits varying headers; its endpoints need not be complete headers.
    fn bounds(scope: Self::Scope) -> Option<HeaderBounds>;
}

/// Exact encoded header restrictions, independent of suffix predicates.
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

/// Physical key metadata, independent of logical schemas and ordered-field support.
/// If the key also implements `OrderedKeySchema`, its fields describe exactly the
/// suffix after this header and its key kind must agree with the header's codec.
pub trait KeySchema: KeyDecode + 'static {
    type Header: KeyHeader;

    /// Borrows the key for the consumer after the cursor has checked its header.
    /// The default decoder reads the suffix. Legacy codecs may retain the header
    /// when their existing owned key includes its components.
    fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
        let payload = key
            .get(Self::Header::LENGTH..)
            .ok_or(StorageError::DataIntegrityError)?;
        Ok(KeyDecoder::from_payload(payload))
    }
}

/// The single interval covering `domain`, for headers that accept one partition-key
/// interval. `None` means no candidate key. Residuals retain any interior gaps.
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

/// Prepares a selection over a [`PartitionKeyed`] key. `field` binds clauses to the
/// ordered fields, and the header takes the interval covering the partition keys
/// that `header` derives from the clauses of `header_columns`.
///
/// A header-derived clause makes the result inexact: an ordered field value
/// crossed with the header interval can admit another partition key's record that
/// carries the same value (e.g. two IDs on different partition keys); residual
/// filters reject it.
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

/// Existing `key kind | partition key` headers, including tables with unordered
/// suffixes. The key's existing declaration supplies its physical key kind.
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
