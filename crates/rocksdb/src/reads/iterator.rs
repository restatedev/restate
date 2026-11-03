// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::{DBRawIteratorWithThreadMode, ReadOptions};

use super::{ReadSource, Source, TOKEN, iterator_options};
use crate::{RawRocksDb, RocksError};

/// An iterator over one column family, through a snapshot or of the data as it
/// was when the iterator was created.
///
/// `H` is what the iterator reads through: a [`ReadSnapshot`](super::ReadSnapshot)
/// or a [`Scope`](restate_util_lifecycle::Scope). The iterator either borrows it,
/// or holds its own clone as an [`OwnedReadIterator`]. [`Self::into_owned`] turns
/// a borrowed iterator into an owned one.
///
/// The underlying RocksDB iterator is deliberately not exposed:
/// - Its `refresh` method would silently switch it to the latest data, away from
///   its snapshot or the moment it was created.
/// - Swapping the underlying iterators of two owned iterators would leave each
///   one reading from a database that the other one keeps alive.
pub struct ReadIterator<'a, H> {
    // Dropped first, while `source` still keeps the database alive.
    iterator: DBRawIteratorWithThreadMode<'a, RawRocksDb>,
    source: Source<'a, H>,
}

/// An iterator that holds its own clone of its snapshot or scope, so it can be
/// kept across pauses and moved between threads.
pub type OwnedReadIterator<H> = ReadIterator<'static, H>;

impl<'a, H: ReadSource> ReadIterator<'a, H> {
    // An iterator over `source`'s column family `cf`. The source creates the
    // native iterator itself, so it borrows only from the source, which
    // `into_owned` relies on.
    pub(super) fn new(source: &'a H, cf: H::CfKey, mut options: ReadOptions) -> Self {
        iterator_options(&mut options);
        Self {
            iterator: source.raw_iterator(TOKEN, cf, options),
            source: Source::Borrowed(source),
        }
    }

    // The snapshot or scope this iterator reads through, for tests. Not public:
    // through it, a caller could read past whatever its owner allows, such as a
    // partition's scans.
    #[cfg(test)]
    pub(super) fn source(&self) -> &H {
        self.source.get()
    }

    /// Turns this into an iterator that holds its own clone of its snapshot or
    /// scope, so it can be kept across pauses and moved between threads. It keeps
    /// its position, so reading continues where it left off. An iterator that is
    /// already owned stays as it is.
    pub fn into_owned(self) -> OwnedReadIterator<H> {
        let Self { iterator, source } = self;
        let source = source.into_owned();
        // SAFETY: the source created the native iterator (see `new`). It borrows
        // only from what every clone of the source shares: the database, the
        // column family, and the snapshot, if any, which its options point to. The
        // native iterator keeps those options itself, so they move with it. The
        // clone keeps all of that alive, in the same place, and the owned iterator
        // drops it after the native iterator. The fields are private, and the
        // iterator only gives out data that borrows the iterator itself.
        let iterator = unsafe {
            std::mem::transmute::<
                DBRawIteratorWithThreadMode<'a, RawRocksDb>,
                DBRawIteratorWithThreadMode<'static, RawRocksDb>,
            >(iterator)
        };
        ReadIterator {
            iterator,
            source: Source::Owned(source),
        }
    }
}

impl<H> ReadIterator<'_, H> {
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

    /// Moves to the first key.
    pub fn seek_to_first(&mut self) {
        self.iterator.seek_to_first();
    }

    /// Moves to the last key.
    pub fn seek_to_last(&mut self) {
        self.iterator.seek_to_last();
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
