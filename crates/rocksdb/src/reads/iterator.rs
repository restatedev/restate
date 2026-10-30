// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::DBRawIteratorWithThreadMode;

use super::ReadSource;
use crate::{RawRocksDb, RocksError};

/// An iterator over one column family, through a snapshot or of the data as it
/// was when the iterator was created.
///
/// `R` is what the iterator keeps alive: nothing for an iterator that borrows its
/// snapshot or scope, or a clone of it for an [`OwnedReadIterator`].
///
/// The underlying RocksDB iterator is deliberately not exposed:
/// - Its `refresh` method would silently switch it to the latest data, away from
///   its snapshot or the moment it was created.
/// - Swapping the underlying iterators of two owned iterators would leave each
///   one reading from a database that the other one keeps alive.
pub struct ReadIterator<'a, R = ()> {
    // Dropped first, while `retained` still keeps the database alive.
    iterator: DBRawIteratorWithThreadMode<'a, RawRocksDb>,
    retained: R,
}

/// An iterator that holds its own clone of its snapshot or scope, so it can be
/// kept across pauses and moved between threads.
pub type OwnedReadIterator<H> = ReadIterator<'static, H>;

impl<'a> ReadIterator<'a> {
    pub(super) fn new(iterator: DBRawIteratorWithThreadMode<'a, RawRocksDb>) -> Self {
        Self {
            iterator,
            retained: (),
        }
    }
}

impl<H: ReadSource> OwnedReadIterator<H> {
    /// Builds an iterator from `source` with `build`, and stores `source` in it.
    pub(super) fn retaining(source: H, build: impl FnOnce(&H) -> ReadIterator<'_>) -> Self {
        let iterator = build(&source);
        // SAFETY: the iterator borrows only from `source`. Every `ReadSource` is a
        // shared handle, so what the iterator borrows stays where it is when
        // `source` moves, and lives as long as `source`, which the owned iterator
        // stores and drops after the native iterator. The fields are private, and
        // the iterator only gives out data that borrows the iterator itself.
        let iterator =
            unsafe { std::mem::transmute::<ReadIterator<'_>, ReadIterator<'static>>(iterator) };
        ReadIterator {
            iterator: iterator.iterator,
            retained: source,
        }
    }

    /// The snapshot or scope this iterator reads through.
    pub fn source(&self) -> &H {
        &self.retained
    }
}

impl<R> ReadIterator<'_, R> {
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
