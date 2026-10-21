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

use bytes::Bytes;

use restate_storage_api::StorageError;
use restate_types::sharding::{KeyRange, PartitionId};

use crate::scan::{PhysicalScan, TableScan};
use crate::{PartitionStore, Result, ScanMode};

use super::{EncodeTableKeyPrefix, KeyDecode, KeyDecoder, KeyKind};

/// The ownership domain supplied implicitly by the local storage backend.
/// `()` denotes a layout whose namespace needs no additional ownership bound.
pub trait KeyScope: private::Sealed + Copy + Send + Sync + 'static {
    #[doc(hidden)]
    fn for_store(store: &PartitionStore) -> Self;
}

impl KeyScope for PartitionId {
    fn for_store(store: &PartitionStore) -> Self {
        store.partition_id()
    }
}

impl KeyScope for KeyRange {
    fn for_store(store: &PartitionStore) -> Self {
        store.partition_key_range()
    }
}

impl KeyScope for () {
    fn for_store(_store: &PartitionStore) -> Self {}
}

mod private {
    pub trait Sealed {}
    impl Sealed for restate_types::sharding::PartitionId {}
    impl Sealed for restate_types::sharding::KeyRange {}
    impl Sealed for () {}
}

/// A composite header's physical identity and typed ownership restriction.
/// Bounds must select exactly the requested ownership domain and table identity.
/// This contract makes no ordering claim about the suffix after `LENGTH` bytes.
pub trait KeyHeader: 'static {
    type Scope: KeyScope;

    const KEY_KIND: KeyKind;
    const LENGTH: usize;

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

/// Existing `key kind | partition key` headers, including tables with unordered
/// suffixes. The key's existing declaration supplies its physical key kind.
pub struct PartitionKeyHeader<K>(PhantomData<fn() -> K>);

impl<K: EncodeTableKeyPrefix + 'static> KeyHeader for PartitionKeyHeader<K> {
    type Scope = KeyRange;

    const KEY_KIND: KeyKind = K::KEY_KIND;
    const LENGTH: usize = KeyKind::SERIALIZED_LENGTH + size_of::<u64>();

    fn bounds(scope: KeyRange) -> Option<HeaderBounds> {
        (scope.start() <= scope.end())
            .then(|| HeaderBounds(TableScan::<K>::ScanPartitionKeyRange(scope).into()))
    }
}
