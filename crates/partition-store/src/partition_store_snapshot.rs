// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Bytes, BytesMut};
use rocksdb::{DBPinnableSlice, ReadOptions};

use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::WithPartitionKey;
use restate_types::sharding::KeyRange;

use crate::features::StorageFeatures;
use crate::keys::{EncodeTableKeyPrefix, KeyKind};
use crate::partition_store::{StorageRead, assert_partition_key_or_err};
use crate::scan::{PhysicalScan, TableScan};
use crate::{PartitionReadSnapshot, PartitionSnapshotReader, PartitionSnapshotScan};

/// Read-only access to the tables of a partition as they were when the snapshot was taken,
/// created with [`PartitionStore::snapshot`](crate::PartitionStore::snapshot).
///
/// Unlike a [`PartitionStoreTransaction`](crate::PartitionStoreTransaction), it doesn't see
/// writes made after it was taken, including uncommitted ones.
pub struct PartitionStoreSnapshot<'a> {
    snapshot: PartitionReadSnapshot,
    // Point lookups. Each lookup chooses the column family of its key kind.
    reader: PartitionSnapshotReader<'static>,
    key_buffer: &'a mut BytesMut,
    storage_features: StorageFeatures,
    key_range: KeyRange,
}

impl<'a> PartitionStoreSnapshot<'a> {
    pub(crate) fn new(
        snapshot: PartitionReadSnapshot,
        key_buffer: &'a mut BytesMut,
        storage_features: StorageFeatures,
        key_range: KeyRange,
    ) -> Self {
        Self {
            reader: snapshot.owned_reader(),
            snapshot,
            key_buffer,
            storage_features,
            key_range,
        }
    }

    pub(crate) fn storage_features(&self) -> StorageFeatures {
        self.storage_features
    }

    pub(crate) fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) -> Result<()> {
        assert_partition_key_or_err(self.key_range, partition_key)
    }
}

impl StorageRead for PartitionStoreSnapshot<'_> {
    type Iterator<'b>
        = PartitionSnapshotScan<'b>
    where
        Self: 'b;

    fn iterator_from<K: EncodeTableKeyPrefix>(
        &self,
        scan: TableScan<K>,
    ) -> Result<Self::Iterator<'_>> {
        let mut opts = ReadOptions::default();
        opts.set_async_io(true);
        Ok(self.snapshot.scan(PhysicalScan::<Bytes>::from(scan), opts))
    }

    #[inline]
    fn get<K: AsRef<[u8]>>(
        &self,
        key_kind: KeyKind,
        key: K,
    ) -> Result<Option<DBPinnableSlice<'_>>> {
        self.reader
            .get_pinned(key_kind, key.as_ref())
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn cleared_key_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut {
        self.key_buffer.clear();
        self.key_buffer.reserve(min_size);
        self.key_buffer
    }
}
