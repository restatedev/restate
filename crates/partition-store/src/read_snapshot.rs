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

use rocksdb::ReadOptions;

use restate_core::io::AdmissionClosed;
use restate_rocksdb::reads::{OwnedReadIterator, OwnedReader, ReadIterator, ReadSnapshot, Reader};
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
/// let mut rows: PartitionSnapshotScan = snapshot.owned_scan(scan, Default::default());
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

/// A reader of a [`PartitionReadSnapshot`] that holds its own clone of the
/// snapshot, so it can be kept across pauses and moved between threads.
pub type PartitionSnapshotReader = OwnedReader<ReadSnapshot<PhysicalPartition>>;

/// A scan of a [`PartitionReadSnapshot`] that holds its own clone of the
/// snapshot, so it can be kept across pauses and moved between threads.
pub type PartitionSnapshotScan = OwnedReadIterator<ReadSnapshot<PhysicalPartition>>;

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

    /// A reader for point lookups of keys of `key_kind`. See
    /// [`ReadSnapshot::reader`].
    pub fn reader(&self, key_kind: KeyKind, options: ReadOptions) -> Reader<'_> {
        self.inner.reader(key_kind, options)
    }

    /// Like [`Self::reader`], but the reader holds its own clone of the snapshot.
    /// See [`ReadSnapshot::owned_reader`].
    pub fn owned_reader(&self, key_kind: KeyKind, options: ReadOptions) -> PartitionSnapshotReader {
        self.inner.owned_reader(key_kind, options)
    }

    /// An iterator over the keys that `scan` describes, positioned at its first key.
    ///
    /// The scan sets the iteration bounds and decides whether seeks use the prefix
    /// extractor or total order, replacing what `options` says about either. The
    /// scan's bytes are copied, so the iterator doesn't borrow them.
    pub fn scan<B: AsRef<[u8]>>(
        &self,
        scan: PhysicalScan<B>,
        mut options: ReadOptions,
    ) -> ReadIterator<'_> {
        scan.configure(&mut options);
        let mut iterator = self.inner.iterator(scan.key_kind(), options);
        iterator.seek(scan.start().as_ref());
        iterator
    }

    /// Like [`Self::scan`], but the iterator holds its own clone of the snapshot,
    /// so it can be kept across pauses and moved between threads.
    pub fn owned_scan<B: AsRef<[u8]>>(
        &self,
        scan: PhysicalScan<B>,
        mut options: ReadOptions,
    ) -> PartitionSnapshotScan {
        scan.configure(&mut options);
        let mut iterator = self.inner.owned_iterator(scan.key_kind(), options);
        iterator.seek(scan.start().as_ref());
        iterator
    }
}
