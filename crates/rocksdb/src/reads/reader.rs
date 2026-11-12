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

use rocksdb::{BoundColumnFamily, DBPinnableBatch, DBPinnableSlice, ReadOptions, ReadTier};

use super::{ReadSource, Source, TOKEN};
use crate::{RawRocksDb, RocksError};

/// Point lookups, through a snapshot or of the latest data. Each lookup chooses
/// the column family it reads.
///
/// `H` is what the reader reads through: a [`ReadSnapshot`](super::ReadSnapshot)
/// or a [`Scope`](restate_util_lifecycle::Scope). The reader either borrows it,
/// or holds its own clone as an [`OwnedReader`]. [`Self::into_owned`] turns a
/// borrowed reader into an owned one.
///
/// A new reader reads through its snapshot, if it has one, and requests
/// asynchronous reads where RocksDB supports them. [`Self::with_read_tier`]
/// limits where its lookups may read from.
pub struct Reader<'a, H> {
    // These options may point to our snapshot, so they must never leave this
    // type. They are dropped before `source`.
    options: ReadOptions,
    // The database to read, looked up once, when the reader was created. It points
    // into the source.
    db: &'a RawRocksDb,
    source: Source<'a, H>,
}

/// A reader that holds its own clone of its snapshot or scope, so it can be
/// kept across pauses and moved between threads.
pub type OwnedReader<H> = Reader<'static, H>;

impl<'a, H: ReadSource> Reader<'a, H> {
    // A reader of `source`. The database and the options both come from `source`
    // itself, so they point into it and nowhere else, which `into_owned` relies on.
    pub(super) fn new(source: &'a H) -> Self {
        Self {
            options: source.reader_options(TOKEN),
            db: source.handles(TOKEN).database().inner().as_raw_db(),
            source: Source::Borrowed(source),
        }
    }

    /// Limits where lookups may read from. For example, with
    /// [`ReadTier::BlockCache`], [`Self::get`] and [`Self::get_pinned`] only read
    /// what is already in memory, and fail with an `Incomplete` error instead of
    /// reading from disk.
    ///
    /// Don't rely on it for [`Self::multi_get_pinned`]: in a test without a block
    /// cache, a multi-get with [`ReadTier::BlockCache`] still returned a key that
    /// had been flushed to disk.
    pub fn with_read_tier(mut self, tier: ReadTier) -> Self {
        self.options.set_read_tier(tier);
        self
    }

    // Lets tests set read options that readers don't offer.
    #[cfg(test)]
    pub(super) fn with_options(mut self, set: impl FnOnce(&mut ReadOptions)) -> Self {
        set(&mut self.options);
        self
    }

    /// Turns this into a reader that holds its own clone of its snapshot or
    /// scope, so it can be kept across pauses and moved between threads. It keeps
    /// its settings. A reader that is already owned stays as it is.
    pub fn into_owned(self) -> OwnedReader<H> {
        let Self {
            options,
            db,
            source,
        } = self;
        let source = source.into_owned();
        // SAFETY: `new` takes `db` from the source's `CfHandles`, and it points into
        // the `RocksDb` that the handles keep open. That `RocksDb` lives behind an
        // `Arc` in shared, heap-allocated state, so it isn't moved while that state
        // is alive. `source` is a clone of the same scope or snapshot, so it keeps
        // that same state alive for as long as the owned reader exists. The fields
        // are private, and the methods only return values that borrow the reader,
        // so nothing uses `db` once the reader is gone.
        let db = unsafe { std::mem::transmute::<&'a RawRocksDb, &'static RawRocksDb>(db) };
        Reader {
            options,
            db,
            source: Source::Owned(source),
        }
    }
}

// Every method here returns values that borrow `self`, never values that live for
// `'a`. For an owned reader, `'a` is `'static`, so a value living for `'a` could
// outlive the snapshot or scope that keeps the database open.
impl<H: ReadSource> Reader<'_, H> {
    // The column family `cf` of the source. It points into the source, which the
    // reader keeps alive.
    fn cf(&self, cf: H::CfKey) -> &Arc<BoundColumnFamily<'_>> {
        self.source.get().handles(TOKEN).get(cf)
    }

    /// Reads the value of `key` in column family `cf`, copied into a new vector.
    pub fn get(&self, cf: H::CfKey, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        Ok(self.db.get_cf_opt(self.cf(cf), key, &self.options)?)
    }

    /// Reads the value of `key` in column family `cf` without copying it. The
    /// returned value points into RocksDB's memory and keeps that memory in place
    /// until it is dropped.
    pub fn get_pinned(
        &self,
        cf: H::CfKey,
        key: &[u8],
    ) -> Result<Option<DBPinnableSlice<'_>>, RocksError> {
        Ok(self.db.get_pinned_cf_opt(self.cf(cf), key, &self.options)?)
    }

    /// Reads the values of all `keys` in column family `cf` in one batched call,
    /// without copying them.
    ///
    /// The results are in the same order as `keys`. Set `sorted_input` only if
    /// `keys` are already sorted in the column family's key order, so that
    /// RocksDB can skip sorting them. If they aren't sorted, the results can be
    /// wrong.
    ///
    /// An error returned here means the whole lookup failed. An error for a
    /// single key is returned by [`PinnedBatch::get`] for that key.
    ///
    /// The batch borrows this reader. [`PinnedBatch::into_owned`] turns it into
    /// one that can outlive the reader.
    pub fn multi_get_pinned<K, I>(
        &self,
        cf: H::CfKey,
        keys: I,
        sorted_input: bool,
    ) -> Result<PinnedBatch<'_, H>, RocksError>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        Ok(PinnedBatch {
            batch: self.db.batched_multi_get_pinned_batch_cf_opt(
                self.cf(cf),
                keys,
                sorted_input,
                &self.options,
            )?,
            source: Source::Borrowed(self.source.get()),
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
/// `H` is the snapshot or scope the values were read through. A batch from
/// [`Reader::multi_get_pinned`] borrows the reader, and through it the snapshot
/// or scope. An [`OwnedPinnedBatch`] holds its own clone of the snapshot or
/// scope instead. [`Self::into_owned`] turns a borrowed batch into an owned one.
pub struct PinnedBatch<'a, H> {
    // Dropped first, while `source` still keeps the database open.
    batch: DBPinnableBatch<'a>,
    source: Source<'a, H>,
}

/// A batch that holds its own clone of its snapshot or scope, so it can be kept
/// across pauses and moved between threads.
pub type OwnedPinnedBatch<H> = PinnedBatch<'static, H>;

impl<'a, H: ReadSource> PinnedBatch<'a, H> {
    /// Turns this into a batch that holds its own clone of its snapshot or
    /// scope, so it can outlive the reader that looked it up. For example, a
    /// cursor can keep it across pauses. A batch that is already owned stays as
    /// it is.
    pub fn into_owned(self) -> OwnedPinnedBatch<H> {
        let Self { batch, source } = self;
        let source = source.into_owned();
        // SAFETY: the batch only borrows the database: once the lookup has
        // returned, the values it pins need neither the reader nor its options.
        // The source keeps that database open. Every `ReadSource` is a shared
        // handle, so the clone keeps the same database open, in the same place,
        // and the owned batch drops it after the native batch. The fields are
        // private, and the batch only gives out values that borrow it.
        let batch =
            unsafe { std::mem::transmute::<DBPinnableBatch<'a>, DBPinnableBatch<'static>>(batch) };
        PinnedBatch {
            batch,
            source: Source::Owned(source),
        }
    }
}

impl<H> PinnedBatch<'_, H> {
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
