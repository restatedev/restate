// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use rocksdb::ReadOptions;

use restate_core::io::{AdmissionClosed, Gate};
use restate_rocksdb::reads::{
    Get, InlineVisitor, Iteration, Lookup, OwnedPinnedValue, ReadIterator, ReadSnapshot, Reader,
};
use restate_rocksdb::{Cooperative, Gated, IterAction, StorageOp};
use restate_types::partitions::Partition;

use crate::keys::KeyKind;
use crate::partition_db::{PartitionDb, PhysicalPartition};
use crate::scan::PhysicalScan;

/// A read snapshot of one partition, taken with [`PartitionDb::read_snapshot`].
/// Reads through it see all of the partition's data as it was when the snapshot
/// was taken.
///
/// ```no_run
/// # use restate_partition_store::scan::PhysicalScan;
/// # use restate_partition_store::{PartitionDb, PartitionSnapshotScan};
/// # fn read(db: &PartitionDb, scan: PhysicalScan<Vec<u8>>) -> anyhow::Result<()> {
/// let snapshot = db.read_snapshot()?;
///
/// // An owned scan holds its own clone of the snapshot, so a query cursor can
/// // keep it between batches, and move it between threads.
/// let mut rows: PartitionSnapshotScan<'static> = snapshot.owned_scan(scan, Default::default());
/// while let Some((key, value)) = rows.item()? {
///     // ... use `key` and `value` ...
///     rows.next();
/// }
/// # Ok(())
/// # }
/// ```
///
/// Scans are described with a [`PhysicalScan`], which also sets up the iterator
/// the way the partition's column families need: with the right bounds, and
/// using total order where a seek can't rely on the prefix extractor. That's why
/// this type offers scans rather than plain iterators.
///
/// The snapshot holds a lease from the partition's session. Closing the
/// partition, or deleting its column families, waits until the snapshot, and
/// every reader and scan created from it, has been dropped.
///
/// Clones are cheap and share the same snapshot.
#[derive(Clone)]
pub struct PartitionReadSnapshot {
    inner: ReadSnapshot<PhysicalPartition>,
}

/// A reader of a [`PartitionReadSnapshot`]. A borrowed one, from
/// [`PartitionReadSnapshot::reader`], can't outlive the snapshot. An owned one,
/// `PartitionSnapshotReader<'static>`, holds its own clone of the snapshot, so
/// it can be kept across pauses and moved between threads.
pub type PartitionSnapshotReader<'a> = Reader<'a, ReadSnapshot<PhysicalPartition>>;

/// The value of a key, read through a [`PartitionReadSnapshot`] without
/// copying it. It holds its own clone of the snapshot.
pub(crate) type PartitionSnapshotValue = OwnedPinnedValue<ReadSnapshot<PhysicalPartition>>;

/// A scan of a [`PartitionReadSnapshot`]. A borrowed one, from
/// [`PartitionReadSnapshot::scan`], can't outlive the snapshot. An owned one,
/// `PartitionSnapshotScan<'static>`, holds its own clone of the snapshot, so it
/// can be kept across pauses and moved between threads.
pub type PartitionSnapshotScan<'a> = ReadIterator<'a, ReadSnapshot<PhysicalPartition>>;

impl PartitionReadSnapshot {
    pub(crate) fn new(db: &PartitionDb) -> Result<Self, AdmissionClosed> {
        Ok(Self {
            inner: ReadSnapshot::new(db.physical_scope(), db.gate())?,
        })
    }

    /// The partition this snapshot was taken of.
    pub fn partition(&self) -> &Arc<Partition> {
        self.inner.scope().payload().partition()
    }

    /// A reader for point lookups. Each lookup takes the [`KeyKind`](crate::keys::KeyKind) of its key,
    /// which chooses the column family it reads. See [`ReadSnapshot::reader`].
    pub fn reader(&self) -> PartitionSnapshotReader<'_> {
        self.inner.reader()
    }

    /// Like [`Self::reader`], but the reader holds its own clone of the snapshot.
    /// See [`ReadSnapshot::owned_reader`].
    pub fn owned_reader(&self) -> PartitionSnapshotReader<'static> {
        self.inner.owned_reader()
    }

    /// An iterator over the keys that `scan` describes, positioned at its first key.
    ///
    /// The scan sets the iteration bounds and decides whether seeks use the prefix
    /// extractor or total order, replacing what `options` says about either. The
    /// scan's bytes are copied, so the iterator doesn't borrow them.
    pub fn scan<B: AsRef<[u8]>>(
        &self,
        scan: PhysicalScan<B>,
        options: ReadOptions,
    ) -> PartitionSnapshotScan<'_> {
        let mut iterator = self.unpositioned_scan(&scan, options);
        iterator.seek(scan.start().as_ref());
        iterator
    }

    /// Like [`Self::scan`], but the iterator holds its own clone of the snapshot,
    /// so it can be kept across pauses and moved between threads.
    pub fn owned_scan<B: AsRef<[u8]>>(
        &self,
        scan: PhysicalScan<B>,
        options: ReadOptions,
    ) -> PartitionSnapshotScan<'static> {
        self.scan(scan, options).into_owned()
    }

    /// Prepares a lookup of `key`, whose kind is `key_kind`, counted in `gate`.
    /// Nothing is read yet. See [`ReadSnapshot::prepare_get`].
    pub(crate) fn prepare_get<'a, 'g>(
        &'a self,
        gate: &'g Arc<Gate>,
        key_kind: KeyKind,
        key: &'a [u8],
    ) -> StorageOp<Lookup<'a, ReadSnapshot<PhysicalPartition>>, Get<KeyKind>, Gated<'g>> {
        self.inner.prepare_get(gate, key_kind, key)
    }

    /// Like [`Self::scan`], but the iterator isn't positioned yet: seeking is
    /// left to the caller, so this doesn't read anything.
    pub(crate) fn unpositioned_scan<B: AsRef<[u8]>>(
        &self,
        scan: &PhysicalScan<B>,
        mut options: ReadOptions,
    ) -> PartitionSnapshotScan<'_> {
        scan.configure(&mut options);
        self.inner.iterator(scan.key_kind(), options)
    }

    /// Prepares a scan of the keys that `scan` describes, which hands each row
    /// to `visitor`, counted in `gate`. Nothing is read yet: run it on the
    /// calling thread while its rows are in memory
    /// ([`call_cached`](StorageOp::call_cached)), submit it to an executor, or
    /// both. See [`ReadSnapshot::prepare_iteration`].
    #[allow(clippy::type_complexity)]
    pub(crate) fn prepare_scan<'g, V: InlineVisitor>(
        &self,
        gate: &'g Arc<Gate>,
        scan: PhysicalScan<Bytes>,
        visitor: V,
    ) -> StorageOp<
        Cooperative<&ReadSnapshot<PhysicalPartition>>,
        Iteration<KeyKind, impl Fn() -> ReadOptions + Send + 'static + use<V>, V>,
        Gated<'g>,
    > {
        let key_kind = scan.key_kind();
        let start = IterAction::Seek(scan.start().clone());
        let options = move || {
            let mut options = ReadOptions::default();
            scan.configure(&mut options);
            options
        };
        self.inner
            .prepare_iteration(gate, key_kind, options, start, visitor)
    }
}
