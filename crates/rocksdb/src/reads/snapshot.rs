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

use rocksdb::{ReadOptions, SnapshotWithThreadMode};

use restate_core::io::{AdmissionClosed, Gate, OwnedLease};
use restate_util_lifecycle::Scope;

use super::{
    ColumnFamilies, OwnedReadIterator, OwnedReader, ReadIterator, Reader, iterator_options,
    reader_options,
};
use crate::RawRocksDb;

/// A snapshot of the column families of a scope, taken with [`Self::new`].
/// Every read through it sees the data as it was when the snapshot was taken.
/// See the [module documentation](super) for an example.
///
/// Clones are cheap and share the same snapshot; cloning doesn't take a new one.
///
/// The snapshot keeps the scope alive, and holds a lease from a gate inside it.
/// Draining that gate, or the database, waits while the snapshot, any of its
/// clones, or anything created from it is still alive. Nothing in this module
/// cancels a snapshot, so callers must drop snapshots they no longer need.
/// Otherwise, closing the gate or shutting down the database never finishes.
pub struct ReadSnapshot<C: ColumnFamilies> {
    inner: Arc<SnapshotInner<C>>,
}

struct SnapshotInner<C: ColumnFamilies> {
    // Fields are dropped in this order, and the order matters:
    // 1. The native snapshot is released while the database is still open.
    // 2. The scope is dropped, which may release the database and column families.
    // 3. The lease is released last, so a drain finishes only once both are gone.
    native: SnapshotWithThreadMode<'static, RawRocksDb>,
    scope: Scope<C>,
    _lease: OwnedLease,
}

impl<C: ColumnFamilies> Clone for ReadSnapshot<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C: ColumnFamilies> ReadSnapshot<C> {
    /// Takes a snapshot of `scope`'s column families right now, on the calling
    /// thread. The snapshot counts as work in `gate`, which is `scope`'s own gate
    /// or one below it. For an open partition, that's the gate of the session,
    /// so that closing the partition waits for the snapshot.
    ///
    /// Fails if `gate` has been sealed, for example because the partition was
    /// closed or the database is shutting down. Sealing later doesn't affect
    /// snapshots that already exist: reads through them keep working.
    ///
    /// # Panics
    /// In debug builds, panics if `gate` isn't inside `scope`.
    pub fn new(scope: &Scope<C>, gate: &Arc<Gate>) -> Result<Self, AdmissionClosed> {
        debug_assert!(
            gate.is_inside(scope.gate()),
            "the gate isn't inside the scope"
        );
        let lease = gate.try_acquire().ok_or(AdmissionClosed)?.into_owned();
        let native = scope
            .payload()
            .handles()
            .database()
            .inner()
            .as_raw_db()
            .snapshot();
        // SAFETY: the snapshot borrows the scope's database. We treat it as
        // 'static so that it can be stored next to the scope. This is sound
        // because:
        // - the scope's handles keep the same database open until the scope is
        //   dropped (see the safety contract of `ColumnFamilies`), and the
        //   snapshot is dropped before the scope;
        // - the snapshot is private, and everything created from it below either
        //   borrows `self` or holds its own clone of this snapshot.
        let native = unsafe {
            std::mem::transmute::<
                SnapshotWithThreadMode<'_, RawRocksDb>,
                SnapshotWithThreadMode<'static, RawRocksDb>,
            >(native)
        };
        Ok(Self {
            inner: Arc::new(SnapshotInner {
                native,
                scope: scope.clone(),
                _lease: lease,
            }),
        })
    }

    /// The scope this snapshot was taken of.
    pub fn scope(&self) -> &Scope<C> {
        &self.inner.scope
    }

    /// A reader for point lookups on one column family. The reader uses `options`
    /// for all of its lookups. Any snapshot already set in `options` is replaced
    /// with this one.
    ///
    /// Asynchronous reads are always requested; multi-gets use them when they can.
    /// RocksDB turns them off by itself when the build or the filesystem doesn't
    /// support them, so callers don't need to set this.
    pub fn reader(&self, cf: C::Cf, mut options: ReadOptions) -> Reader<'_> {
        options.set_snapshot(&self.inner.native);
        reader_options(&mut options);
        let handles = self.inner.scope.payload().handles();
        Reader::new(
            options,
            handles.database().inner().as_raw_db(),
            handles.get(cf),
        )
    }

    /// Like [`Self::reader`], but the reader holds its own clone of this snapshot.
    /// A cursor can keep it across pauses, move it between threads, and look up
    /// one batch after another with it.
    pub fn owned_reader(&self, cf: C::Cf, options: ReadOptions) -> OwnedReader<Self> {
        OwnedReader::retaining(self.clone(), |snapshot| snapshot.reader(cf, options))
    }

    /// An iterator over one column family that borrows this snapshot.
    ///
    /// Any snapshot already set in `options` is replaced with this one. Tailing is
    /// always turned off, because a tailing iterator ignores its snapshot and
    /// reads the latest data instead.
    ///
    /// Asynchronous prefetching is always requested. RocksDB turns it off by
    /// itself when the build or the filesystem doesn't support it, so callers
    /// don't need to set this.
    pub fn iterator(&self, cf: C::Cf, mut options: ReadOptions) -> ReadIterator<'_> {
        iterator_options(&mut options);
        ReadIterator::new(
            self.inner
                .native
                .raw_iterator_cf_opt(self.inner.scope.payload().handles().get(cf), options),
        )
    }

    /// Like [`Self::iterator`], but the iterator holds its own clone of this
    /// snapshot, so it can be kept across pauses and moved between threads.
    pub fn owned_iterator(&self, cf: C::Cf, options: ReadOptions) -> OwnedReadIterator<Self> {
        OwnedReadIterator::retaining(self.clone(), |snapshot| snapshot.iterator(cf, options))
    }
}
