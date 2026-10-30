// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Consistent reads from a snapshot of a database.
//!
//! A snapshot freezes the database at the moment it is taken: reads through it
//! don't see any later writes. Take one with [`OwnedReadSnapshot::capture`]. One
//! snapshot covers all of the owner's column families; each reader or iterator
//! created from it reads one of them.
//!
//! Each snapshot counts as an operation in its owner's [`DatabaseScope`]. Closing
//! that scope (for example, closing a partition) or shutting down the database
//! waits until the snapshot, and everything created from it, has been dropped.
//!
//! Reads block the calling thread. This module doesn't schedule or cancel reads;
//! that is up to the caller.
//!
//! Readers, iterators, and multi-get batches come in two versions:
//! - The borrowed version holds a reference to the snapshot, so it can't outlive
//!   the snapshot.
//! - The owned version holds its own clone of the snapshot. It can be kept across
//!   pauses and moved to another thread, which is what resumable query cursors
//!   need.
//!
//! The compiler stops borrowed values from outliving the snapshot or reader they
//! came from:
//!
//! ```compile_fail,E0597
//! # use restate_rocksdb::{OwnedReadSnapshot, ReadSnapshotOwner};
//! fn pinned_value<O: ReadSnapshotOwner>(owner: O, cf: O::Cf) {
//!     let value = {
//!         let snapshot = OwnedReadSnapshot::capture(owner).unwrap();
//!         let reader = snapshot.reader(cf, Default::default());
//!         reader.get_pinned(b"key").unwrap()
//!     };
//!     drop(value);
//! }
//! ```
//!
//! ```compile_fail,E0597
//! # use restate_rocksdb::{OwnedReadSnapshot, ReadSnapshotOwner};
//! fn batch<O: ReadSnapshotOwner>(owner: O, cf: O::Cf) {
//!     let snapshot = OwnedReadSnapshot::capture(owner).unwrap();
//!     let batch = {
//!         let reader = snapshot.reader(cf, Default::default());
//!         reader.multi_get_pinned([b"key"], false).unwrap()
//!     };
//!     drop(batch);
//! }
//! ```
//!
//! ```compile_fail,E0597
//! # use restate_rocksdb::{OwnedReadSnapshot, ReadSnapshotOwner};
//! fn iterator<O: ReadSnapshotOwner>(owner: O, cf: O::Cf) {
//!     let iterator = {
//!         let snapshot = OwnedReadSnapshot::capture(owner).unwrap();
//!         snapshot.iterator(cf, Default::default())
//!     };
//!     drop(iterator);
//! }
//! ```
//!
//! Data read through an owned iterator, reader, or batch borrows that value, so it
//! can't outlive it either, even while the snapshot itself is still alive:
//!
//! ```compile_fail,E0505
//! # use restate_rocksdb::{OwnedReadSnapshot, ReadSnapshotOwner};
//! fn owned_item<O: ReadSnapshotOwner>(snapshot: &OwnedReadSnapshot<O>, cf: O::Cf) -> Vec<u8> {
//!     let mut iterator = snapshot.owned_iterator(cf, Default::default());
//!     iterator.seek(b"");
//!     let key = iterator.item().unwrap().unwrap().0;
//!     drop(iterator);
//!     key.to_vec()
//! }
//! ```
//!
//! ```compile_fail,E0505
//! # use restate_rocksdb::{OwnedReadSnapshot, ReadSnapshotOwner};
//! fn owned_reader_value<O: ReadSnapshotOwner>(
//!     snapshot: &OwnedReadSnapshot<O>,
//!     cf: O::Cf,
//! ) -> Vec<u8> {
//!     let reader = snapshot.owned_reader(cf, Default::default());
//!     let value = reader.get_pinned(b"key").unwrap().unwrap();
//!     drop(reader);
//!     value.to_vec()
//! }
//! ```
//!
//! ```compile_fail,E0505
//! # use restate_rocksdb::{OwnedReadSnapshot, ReadSnapshotOwner};
//! fn owned_batch_value<O: ReadSnapshotOwner>(
//!     snapshot: &OwnedReadSnapshot<O>,
//!     cf: O::Cf,
//! ) -> Vec<u8> {
//!     let reader = snapshot.owned_reader(cf, Default::default());
//!     let batch = reader.owned_multi_get_pinned([b"key"], false).unwrap();
//!     let value = batch.get(0).unwrap().unwrap();
//!     drop(batch);
//!     value.to_vec()
//! }
//! ```

use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, DBPinnableBatch, DBPinnableSlice, DBRawIteratorWithThreadMode, ReadOptions,
    SnapshotWithThreadMode,
};

use restate_core::io::{AdmissionClosed, OwnedLease};

use crate::{DatabaseScope, RawRocksDb, RocksDb, RocksError};

/// Keeps the database and its column families alive while snapshots of it exist.
/// For the partition store, this is the partition's database handle.
///
/// The owner looks up its column families once, when it is created, and hands out
/// those same handles for every read. Reads never look up a column family by
/// name, so they can't pick up a column family that was dropped and recreated
/// under the same name.
///
/// # Safety
///
/// A snapshot keeps references into the owner's database that the compiler
/// doesn't check. Implementations must guarantee that:
/// - [`Self::rocksdb`] always returns the same database, and keeps it open until
///   the owner is dropped. Moving the owner must not move the database, and the
///   owner must never swap or close it.
/// - Every column family returned by [`Self::cf`] belongs to that database.
///   RocksDB doesn't check this itself.
///
/// [`Self::admission`] doesn't affect memory safety, but it decides what waits
/// for the owner's snapshots. It must return a scope of the same database, which
/// is checked when a snapshot is taken. An owner with its own lifecycle, such as
/// a partition, should return its own scope, so that closing it waits for its
/// snapshots.
pub unsafe trait ReadSnapshotOwner: Send + Sync + 'static {
    /// Chooses which of the owner's column families a reader or iterator reads.
    ///
    /// A snapshot always covers all of the owner's column families at once. This
    /// type only says which one a particular reader or iterator uses, because
    /// RocksDB reads from one column family at a time.
    ///
    /// The owner decides what the selector means. A partition, for example, can
    /// use the key kind: each key kind is stored in exactly one column family, so
    /// the key kind tells the partition which column family holds those keys.
    /// This doesn't limit a reader to keys of that kind. It can read any key in
    /// that column family, which may also hold keys of other kinds.
    type Cf: Copy;

    /// The database that snapshots read from.
    fn rocksdb(&self) -> &Arc<RocksDb>;

    /// The scope in which the owner's snapshots count as operations.
    fn admission(&self) -> &DatabaseScope;

    /// One of the column families the owner looked up when it was created.
    fn cf(&self, cf: Self::Cf) -> &Arc<BoundColumnFamily<'_>>;
}

/// A snapshot of the owner's database, taken with [`Self::capture`].
///
/// Clones are cheap and share the same snapshot; cloning doesn't take a new one.
///
/// Draining the owner's scope or the database waits while the snapshot, any of
/// its clones, or anything created from it is still alive. Nothing in this module
/// cancels a snapshot, so callers must drop snapshots they no longer need.
/// Otherwise, closing the partition or shutting down the database never finishes.
pub struct OwnedReadSnapshot<O: ReadSnapshotOwner> {
    inner: Arc<SnapshotInner<O>>,
}

struct SnapshotInner<O: ReadSnapshotOwner> {
    // Fields are dropped in this order, and the order matters:
    // 1. The native snapshot is released while the database is still open.
    // 2. The owner is dropped, which may release the database and column families.
    // 3. The lease is released last, so a drain finishes only once both are gone.
    native: SnapshotWithThreadMode<'static, RawRocksDb>,
    owner: O,
    _lease: OwnedLease,
}

impl<O: ReadSnapshotOwner> Clone for OwnedReadSnapshot<O> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<O: ReadSnapshotOwner> OwnedReadSnapshot<O> {
    /// Takes a snapshot of the owner's database right now, on the calling thread.
    ///
    /// Fails if the owner's scope or the database has been sealed. Sealing later
    /// doesn't affect snapshots that already exist: reads through them keep
    /// working.
    ///
    /// # Panics
    /// Panics if the owner's scope belongs to a different database.
    pub fn capture(owner: O) -> Result<Self, AdmissionClosed> {
        let lease = owner
            .admission()
            .try_acquire()
            .ok_or(AdmissionClosed)?
            .into_lease(owner.rocksdb())
            .into_owned();
        // Keep the owner and the lease together from here on. If taking the
        // snapshot panics, the owner is then still dropped before the lease.
        struct Capture<O> {
            owner: O,
            lease: OwnedLease,
        }
        let capture = Capture { owner, lease };
        let native = capture.owner.rocksdb().inner().as_raw_db().snapshot();
        // SAFETY: the snapshot borrows the owner's database. We treat it as
        // 'static so that it can be stored next to the owner. This is sound
        // because:
        // - the owner keeps the same database open until it is dropped (see the
        //   trait's safety contract), and the snapshot is dropped before the owner;
        // - the snapshot is private, and everything created from it below either
        //   borrows `self` or holds its own clone of this snapshot;
        // - nothing here gives out mutable access to the owner.
        let native = unsafe {
            std::mem::transmute::<
                SnapshotWithThreadMode<'_, RawRocksDb>,
                SnapshotWithThreadMode<'static, RawRocksDb>,
            >(native)
        };
        Ok(Self {
            inner: Arc::new(SnapshotInner {
                native,
                owner: capture.owner,
                _lease: capture.lease,
            }),
        })
    }

    /// The owner this snapshot was taken from.
    pub fn owner(&self) -> &O {
        &self.inner.owner
    }

    /// A reader for point lookups on one column family. The reader uses `options`
    /// for all of its lookups. Any snapshot already set in `options` is replaced
    /// with this one.
    ///
    /// Asynchronous reads are always requested; multi-gets use them when they can.
    /// RocksDB turns them off by itself when the build or the filesystem doesn't
    /// support them, so callers don't need to set this.
    pub fn reader(&self, cf: O::Cf, mut options: ReadOptions) -> SnapshotReader<'_> {
        options.set_snapshot(&self.inner.native);
        options.set_async_io(true);
        SnapshotReader {
            options,
            db: self.inner.owner.rocksdb().inner().as_raw_db(),
            cf: self.inner.owner.cf(cf),
            retained: (),
        }
    }

    /// Like [`Self::reader`], but the reader holds its own clone of this snapshot.
    /// A cursor can keep it across pauses, move it between threads, and look up
    /// one batch after another with it.
    pub fn owned_reader(&self, cf: O::Cf, options: ReadOptions) -> OwnedSnapshotReader<O> {
        let snapshot = self.clone();
        let reader = snapshot.reader(cf, options);
        // SAFETY: the reader borrows the database, a column family, and (through
        // its options) the native snapshot. All of them live as long as the
        // snapshot, and the reader stores `snapshot`, so they outlive the reader.
        // The reader's fields are private. Its methods only return values that
        // borrow the reader, except `owned_multi_get_pinned`, whose batch holds
        // its own clone of the snapshot.
        let reader =
            unsafe { std::mem::transmute::<SnapshotReader<'_>, SnapshotReader<'static>>(reader) };
        SnapshotReader {
            options: reader.options,
            db: reader.db,
            cf: reader.cf,
            retained: snapshot,
        }
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
    pub fn iterator(&self, cf: O::Cf, mut options: ReadOptions) -> SnapshotIterator<'_> {
        options.set_tailing(false);
        options.set_async_io(true);
        // Load each value when the iterator moves to it: this wrapper offers no
        // way to load it later.
        options.set_allow_unprepared_value(false);
        SnapshotIterator {
            iterator: self
                .inner
                .native
                .raw_iterator_cf_opt(self.inner.owner.cf(cf), options),
            retained: (),
        }
    }

    /// Like [`Self::iterator`], but the iterator holds its own clone of this
    /// snapshot, so it can be kept across pauses and moved between threads.
    pub fn owned_iterator(&self, cf: O::Cf, options: ReadOptions) -> OwnedSnapshotIterator<O> {
        let snapshot = self.clone();
        let iterator = snapshot.iterator(cf, options);
        // SAFETY: the iterator borrows the native snapshot and a column family,
        // which both live as long as the snapshot. The iterator stores `snapshot`
        // and drops the native iterator first. Its fields are private, and it only
        // gives out data that borrows the iterator itself.
        let iterator = unsafe {
            std::mem::transmute::<SnapshotIterator<'_>, SnapshotIterator<'static>>(iterator)
        };
        SnapshotIterator {
            iterator: iterator.iterator,
            retained: snapshot,
        }
    }
}

/// Point lookups on one column family of a snapshot.
///
/// `R` is what the reader keeps alive: nothing for a reader that borrows the
/// snapshot, or a clone of the snapshot for an [`OwnedSnapshotReader`].
pub struct SnapshotReader<'a, R = ()> {
    // These options have the snapshot set, so they must never leave this type.
    // They are dropped before `retained`.
    options: ReadOptions,
    db: &'a RawRocksDb,
    cf: &'a Arc<BoundColumnFamily<'a>>,
    retained: R,
}

/// A reader that holds its own clone of the snapshot, so it can be kept across
/// pauses and moved between threads.
pub type OwnedSnapshotReader<O> = SnapshotReader<'static, OwnedReadSnapshot<O>>;

// Every method here returns values that borrow `self`, never values that live for
// `'a`. For an owned reader, `'a` is `'static`, so a value living for `'a` could
// outlive the snapshot.
impl<R> SnapshotReader<'_, R> {
    /// Reads the value of `key`, copied into a new vector.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        Ok(self.db.get_cf_opt(self.cf, key, &self.options)?)
    }

    /// Reads the value of `key` without copying it. The returned value points
    /// into RocksDB's memory and keeps that memory in place until it is dropped.
    pub fn get_pinned(&self, key: &[u8]) -> Result<Option<DBPinnableSlice<'_>>, RocksError> {
        Ok(self.db.get_pinned_cf_opt(self.cf, key, &self.options)?)
    }

    /// Reads the values of all `keys` in one batched call, without copying them.
    ///
    /// The results are in the same order as `keys`. Set `sorted_input` only if
    /// `keys` are already sorted in the column family's key order, so that
    /// RocksDB can skip sorting them. If they aren't sorted, the results can be
    /// wrong.
    ///
    /// An error returned here means the whole lookup failed. An error for a
    /// single key is returned by [`PinnedBatch::get`] for that key.
    pub fn multi_get_pinned<K, I>(
        &self,
        keys: I,
        sorted_input: bool,
    ) -> Result<PinnedBatch<'_>, RocksError>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        Ok(PinnedBatch {
            batch: self.db.batched_multi_get_pinned_batch_cf_opt(
                self.cf,
                keys,
                sorted_input,
                &self.options,
            )?,
            _retained: (),
        })
    }
}

impl<O: ReadSnapshotOwner> OwnedSnapshotReader<O> {
    /// The snapshot this reader reads from.
    pub fn snapshot(&self) -> &OwnedReadSnapshot<O> {
        &self.retained
    }

    /// Like [`SnapshotReader::multi_get_pinned`], but the batch holds its own
    /// clone of the snapshot, so a cursor can keep it next to this reader across
    /// pauses.
    pub fn owned_multi_get_pinned<K, I>(
        &self,
        keys: I,
        sorted_input: bool,
    ) -> Result<OwnedPinnedBatch<O>, RocksError>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        // The batch borrows the database for 'static. That is sound because the
        // batch also holds a clone of the snapshot, which keeps the database open
        // (see `OwnedReadSnapshot::owned_reader`).
        Ok(PinnedBatch {
            batch: self.db.batched_multi_get_pinned_batch_cf_opt(
                self.cf,
                keys,
                sorted_input,
                &self.options,
            )?,
            _retained: self.retained.clone(),
        })
    }
}

/// The values found by one multi-get, in the same order as the keys.
///
/// The values point into RocksDB's memory and keep that memory in place until the
/// batch is dropped, so callers limit memory use by limiting how many keys they
/// look up per batch. All values are held by one native object, rather than one
/// object per key.
///
/// `R` is what the batch keeps alive: nothing for a batch that borrows its
/// reader, or a clone of the snapshot for an [`OwnedPinnedBatch`].
pub struct PinnedBatch<'a, R = ()> {
    // Dropped first, while `_retained` still keeps the database open.
    batch: DBPinnableBatch<'a>,
    _retained: R,
}

/// A batch that holds its own clone of the snapshot, so it can be kept across
/// pauses and moved between threads.
pub type OwnedPinnedBatch<O> = PinnedBatch<'static, OwnedReadSnapshot<O>>;

impl<R> PinnedBatch<'_, R> {
    /// The number of keys that were looked up.
    pub fn len(&self) -> usize {
        self.batch.len()
    }

    /// Whether no keys were looked up.
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// The value for the key at `index`: `Some` if the key exists, `None` if it
    /// doesn't, or the error that occurred while reading that key.
    ///
    /// # Panics
    /// Panics if `index` is out of bounds.
    pub fn get(&self, index: usize) -> Result<Option<&[u8]>, RocksError> {
        let Some(result) = self.batch.get(index) else {
            panic!(
                "index {index} out of bounds for a batch of {}",
                self.batch.len()
            );
        };
        Ok(result?)
    }
}

/// An iterator over one column family of a snapshot.
///
/// `R` is what the iterator keeps alive: nothing for an iterator that borrows the
/// snapshot, or a clone of the snapshot for an [`OwnedSnapshotIterator`].
///
/// The underlying RocksDB iterator is deliberately not exposed:
/// - Its `refresh` method would silently switch it from the snapshot to the
///   latest data.
/// - Swapping the underlying iterators of two owned iterators would leave each
///   one reading from a snapshot that the other one keeps alive.
pub struct SnapshotIterator<'a, R = ()> {
    // Dropped first, while `retained` still keeps the snapshot alive.
    iterator: DBRawIteratorWithThreadMode<'a, RawRocksDb>,
    retained: R,
}

/// An iterator that holds its own clone of the snapshot, so it can be kept
/// across pauses and moved between threads.
pub type OwnedSnapshotIterator<O> = SnapshotIterator<'static, OwnedReadSnapshot<O>>;

impl<O: ReadSnapshotOwner> OwnedSnapshotIterator<O> {
    /// The snapshot this iterator reads from.
    pub fn snapshot(&self) -> &OwnedReadSnapshot<O> {
        &self.retained
    }
}

impl<R> SnapshotIterator<'_, R> {
    /// Whether the iterator is positioned on an entry. A valid iterator has no
    /// error, but an invalid iterator may be exhausted or have failed; use
    /// [`Self::status`] to distinguish those cases. Prefer [`Self::item`] when
    /// reading entries, since it checks errors automatically.
    pub fn valid(&self) -> bool {
        self.iterator.valid()
    }

    /// Checks for an iterator error without reading the current entry. An OK
    /// status doesn't mean the iterator is positioned on an entry.
    ///
    /// Call this after navigation to check for errors, or after a loop driven by
    /// [`Self::valid`]. A subsequent seek clears the previous error status.
    pub fn status(&self) -> Result<(), RocksError> {
        Ok(self.iterator.status()?)
    }

    /// Moves to the first key at or after `key`.
    pub fn seek(&mut self, key: &[u8]) {
        self.iterator.seek(key);
    }

    /// Moves to the last key at or before `key`.
    pub fn seek_for_prev(&mut self, key: &[u8]) {
        self.iterator.seek_for_prev(key);
    }

    /// Moves to the next key.
    pub fn next(&mut self) {
        self.iterator.next();
    }

    /// Moves to the previous key.
    pub fn prev(&mut self) {
        self.iterator.prev();
    }

    /// The key and value at the current position. Returns `None` once the
    /// iterator has moved past the last key, or the error that stopped it.
    #[allow(clippy::type_complexity)]
    pub fn item(&self) -> Result<Option<(&[u8], &[u8])>, RocksError> {
        match self.iterator.item() {
            // RocksDB guarantees that an iterator positioned on an entry has no
            // error, so we only need to check for one when there is no entry.
            Some(entry) => Ok(Some(entry)),
            None => Ok(self.iterator.status().map(|()| None)?),
        }
    }
}
