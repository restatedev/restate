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

use rocksdb::{BoundColumnFamily, DBPinnableBatch, DBPinnableSlice, ReadOptions};

use super::ReadSource;
use crate::{RawRocksDb, RocksError};

/// Point lookups on one column family, through a snapshot or of the latest data.
///
/// `R` is what the reader keeps alive: nothing for a reader that borrows its
/// snapshot or scope, or a clone of it for an [`OwnedReader`].
pub struct Reader<'a, R = ()> {
    // These options may have our snapshot set, so they must never leave this
    // type. They are dropped before `retained`.
    options: ReadOptions,
    db: &'a RawRocksDb,
    cf: &'a Arc<BoundColumnFamily<'a>>,
    retained: R,
}

/// A reader that holds its own clone of its snapshot or scope, so it can be
/// kept across pauses and moved between threads.
pub type OwnedReader<H> = Reader<'static, H>;

impl<'a> Reader<'a> {
    pub(super) fn new(
        options: ReadOptions,
        db: &'a RawRocksDb,
        cf: &'a Arc<BoundColumnFamily<'a>>,
    ) -> Self {
        Self {
            options,
            db,
            cf,
            retained: (),
        }
    }
}

// Every method here returns values that borrow `self`, never values that live for
// `'a`. For an owned reader, `'a` is `'static`, so a value living for `'a` could
// outlive the snapshot or scope that keeps the database open.
impl<R> Reader<'_, R> {
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

impl<H: ReadSource> OwnedReader<H> {
    /// Builds a reader from `source` with `build`, and stores `source` in it.
    pub(super) fn retaining(source: H, build: impl FnOnce(&H) -> Reader<'_>) -> Self {
        let reader = build(&source);
        // SAFETY: the reader borrows only from `source`. Every `ReadSource` is a
        // shared handle, so what the reader borrows stays where it is when
        // `source` moves, and lives as long as `source`, which the owned reader
        // stores. The reader's fields are private. Its methods only return values
        // that borrow the reader, except `owned_multi_get_pinned`, whose batch
        // holds its own clone of `source`.
        let reader = unsafe { std::mem::transmute::<Reader<'_>, Reader<'static>>(reader) };
        Reader {
            options: reader.options,
            db: reader.db,
            cf: reader.cf,
            retained: source,
        }
    }

    /// The snapshot or scope this reader reads through.
    pub fn source(&self) -> &H {
        &self.retained
    }

    /// Like [`Reader::multi_get_pinned`], but the batch holds its own clone of the
    /// snapshot or scope, so a cursor can keep it next to this reader across
    /// pauses.
    pub fn owned_multi_get_pinned<K, I>(
        &self,
        keys: I,
        sorted_input: bool,
    ) -> Result<OwnedPinnedBatch<H>, RocksError>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        // The batch borrows the database for 'static. That is sound because the
        // batch also holds a clone of the source, which keeps the database open.
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
/// reader, or a clone of the snapshot or scope for an [`OwnedPinnedBatch`].
pub struct PinnedBatch<'a, R = ()> {
    // Dropped first, while `_retained` still keeps the database open.
    batch: DBPinnableBatch<'a>,
    _retained: R,
}

/// A batch that holds its own clone of its snapshot or scope, so it can be kept
/// across pauses and moved between threads.
pub type OwnedPinnedBatch<H> = PinnedBatch<'static, H>;

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
