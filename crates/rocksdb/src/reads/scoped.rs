// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::ReadOptions;

use restate_util_lifecycle::Scope;

use super::{
    ColumnFamilies, OwnedReadIterator, OwnedReader, ReadIterator, ReadSource, Reader,
    iterator_options, reader_options,
};

/// Reads of a scope's latest data, without a snapshot.
///
/// A point lookup sees the database as it is at that moment. An iterator sees
/// the database as it was when the iterator was created, for its whole life,
/// because RocksDB gives every iterator its own implicit snapshot. Separate reads
/// therefore aren't consistent with each other; use a
/// [`ReadSnapshot`](super::ReadSnapshot) when they need to be.
///
/// The reads keep the scope, and with it the database and column families,
/// alive, but they hold no lease from the scope's gate, so closing the scope
/// doesn't wait for them. Work that reads on a background executor should take
/// its own lease from the gate, like other background reads.
///
/// Only [`Scope`]s implement this trait.
pub trait ScopedReads: ReadSource {
    /// Chooses which of the scope's column families to read. See
    /// [`ColumnFamilies::Cf`].
    type Cf: Copy;

    /// A reader for point lookups on one column family. The reader uses `options`
    /// for all of its lookups, and each lookup sees the latest data.
    ///
    /// Don't set a snapshot in `options`. The reader keeps the options, so a
    /// snapshot released while the reader still exists would leave them pointing
    /// to freed memory. Use a [`ReadSnapshot`](super::ReadSnapshot) to read
    /// through a snapshot.
    ///
    /// Asynchronous reads are always requested; multi-gets use them when they can.
    /// RocksDB turns them off by itself when the build or the filesystem doesn't
    /// support them, so callers don't need to set this.
    fn reader(&self, cf: Self::Cf, options: ReadOptions) -> Reader<'_>;

    /// Like [`Self::reader`], but the reader holds its own clone of the scope. A
    /// cursor can keep it across pauses, move it between threads, and look up one
    /// batch after another with it.
    fn owned_reader(&self, cf: Self::Cf, options: ReadOptions) -> OwnedReader<Self>;

    /// An iterator over one column family that borrows the scope. It sees the
    /// database as it was when the iterator was created.
    ///
    /// Don't set a snapshot in `options`, for the same reason as for
    /// [`Self::reader`]. Tailing is always turned off, so that the iterator keeps
    /// seeing the database as it was when it was created.
    ///
    /// Asynchronous prefetching is always requested. RocksDB turns it off by
    /// itself when the build or the filesystem doesn't support it, so callers
    /// don't need to set this.
    fn iterator(&self, cf: Self::Cf, options: ReadOptions) -> ReadIterator<'_>;

    /// Like [`Self::iterator`], but the iterator holds its own clone of the scope,
    /// so it can be kept across pauses and moved between threads.
    fn owned_iterator(&self, cf: Self::Cf, options: ReadOptions) -> OwnedReadIterator<Self>;
}

impl<C: ColumnFamilies> ScopedReads for Scope<C> {
    type Cf = C::Cf;

    fn reader(&self, cf: C::Cf, mut options: ReadOptions) -> Reader<'_> {
        // TODO: clear any snapshot set in `options` once the fork offers a way to;
        // today it can only set one.
        reader_options(&mut options);
        let handles = self.payload().handles();
        Reader::new(
            options,
            handles.database().inner().as_raw_db(),
            handles.get(cf),
        )
    }

    fn owned_reader(&self, cf: C::Cf, options: ReadOptions) -> OwnedReader<Self> {
        OwnedReader::retaining(self.clone(), |scope| scope.reader(cf, options))
    }

    fn iterator(&self, cf: C::Cf, mut options: ReadOptions) -> ReadIterator<'_> {
        iterator_options(&mut options);
        let handles = self.payload().handles();
        ReadIterator::new(
            handles
                .database()
                .inner()
                .as_raw_db()
                .raw_iterator_cf_opt(handles.get(cf), options),
        )
    }

    fn owned_iterator(&self, cf: C::Cf, options: ReadOptions) -> OwnedReadIterator<Self> {
        OwnedReadIterator::retaining(self.clone(), |scope| scope.iterator(cf, options))
    }
}
