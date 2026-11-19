// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use rocksdb::{DBPinnableSlice, ReadOptions};

use restate_core::io::Gate;
use restate_memory::{ClaimedMemoryPool, LocalMemoryLease};
use restate_rocksdb::GlobalExecutor;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::WithPartitionKey;
use restate_types::sharding::KeyRange;

use crate::budgeted_reads::BudgetedScan;
use crate::features::StorageFeatures;
use crate::keys::{EncodeTableKeyPrefix, KeyKind};
use crate::partition_store::{StorageRead, assert_partition_key_or_err};
use crate::scan::{PhysicalScan, TableScan};
use crate::{
    PartitionReadSnapshot, PartitionSnapshotReader, PartitionSnapshotScan,
    TableScanIterationDecision,
};

/// Read-only access to the tables of a partition as they were when the snapshot was taken,
/// created with [`PartitionStore::snapshot`](crate::PartitionStore::snapshot).
///
/// Unlike a [`PartitionStoreTransaction`](crate::PartitionStoreTransaction), it doesn't see
/// writes made after it was taken, including uncommitted ones.
pub struct PartitionStoreSnapshot<'a> {
    // The gate of the session the snapshot was taken in, which its scans count
    // in.
    gate: &'a Arc<Gate>,
    // The storage workers its scans may run on.
    executor: &'a GlobalExecutor,
    snapshot: PartitionReadSnapshot,
    // Point lookups. Each lookup chooses the column family of its key kind.
    reader: PartitionSnapshotReader<'static>,
    key_buffer: &'a mut BytesMut,
    storage_features: StorageFeatures,
    key_range: KeyRange,
}

impl<'a> PartitionStoreSnapshot<'a> {
    pub(crate) fn new(
        gate: &'a Arc<Gate>,
        executor: &'a GlobalExecutor,
        snapshot: PartitionReadSnapshot,
        key_buffer: &'a mut BytesMut,
        storage_features: StorageFeatures,
        key_range: KeyRange,
    ) -> Self {
        Self {
            gate,
            executor,
            reader: snapshot.owned_reader(),
            snapshot,
            key_buffer,
            storage_features,
            key_range,
        }
    }

    /// The storage workers that a scan of this snapshot may continue on.
    pub(crate) fn executor(&self) -> &'a GlobalExecutor {
        self.executor
    }

    pub(crate) fn storage_features(&self) -> StorageFeatures {
        self.storage_features
    }

    pub(crate) fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) -> Result<()> {
        assert_partition_key_or_err(self.key_range, partition_key)
    }

    /// Prepares a scan of `scan` that streams the rows `decode` builds.
    /// Nothing is read until the returned scan is run, which says where its
    /// rows are read.
    ///
    /// Each row's `cost` is reserved from `budget` before `decode` gets the row
    /// with its lease. `decode` builds the row to stream and decides whether
    /// the scan goes on.
    ///
    /// `name` labels the scan's RocksDB perf metrics.
    pub(crate) fn prepare_budgeted_scan<'s, T, C, D>(
        &'s self,
        name: &'static str,
        scan: PhysicalScan<Bytes>,
        budget: ClaimedMemoryPool<'s>,
        cost: C,
        decode: D,
    ) -> BudgetedScan<'s, C, D>
    where
        C: Fn(&[u8], &[u8]) -> usize + Send + 'static,
        D: FnMut(&[u8], &[u8], LocalMemoryLease) -> TableScanIterationDecision<T> + Send + 'static,
    {
        BudgetedScan {
            gate: self.gate,
            snapshot: &self.snapshot,
            name,
            scan,
            budget,
            cost,
            decode,
        }
    }

    /// A reader for point lookups that holds its own clone of the snapshot, so it can
    /// be kept by a scan that outlives this borrow.
    pub(crate) fn owned_reader(&self) -> PartitionSnapshotReader<'static> {
        self.snapshot.owned_reader()
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
        Ok(self
            .snapshot
            .scan(PhysicalScan::<Bytes>::from(scan), ReadOptions::default()))
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
