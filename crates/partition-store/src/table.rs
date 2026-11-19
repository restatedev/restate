// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Executing prepared reads on a partition's snapshot.
//!
//! # Concepts
//!
//! - **Physical table** ([`PhysicalTable`]): a table as storage lays it out: its
//!   key schema, which names the key's header and decodes the rest of the key,
//!   and whether the layout is still in use on this store
//!   ([`PhysicalTable::is_active`]).
//! - **Request**: a [`KeySelection`] over the table's key, prepared by a schema
//!   binding from a logical selection. On keys whose fields are declared ordered
//!   it restricts fields and seeks; on the existing state keys, equality and
//!   membership seek while ranges and prefixes stay residual.
//! - **Read metadata** ([`ReadMetadata`]): what a read may touch: the partition's
//!   key range, the storage features in force and the registry as the snapshot
//!   saw it, so that an index is only read in a state the snapshot saw as Complete.
//! - **Record**: one key, handed to the caller as a decoder positioned after the
//!   header plus the value bytes, undecoded.
//! - **Multi-get cursor** ([`MultiGetCursor`]): the records of a finite ID set,
//!   fetched by key in sorted batches instead of iterated ([`MultiGetTable`]).
//!
//! # The flow
//!
//! ```text
//! KeySelection ──scan_cursor(snapshot)──▶ TableCursor
//!                                           │ advance(): at most one candidate per step
//!                                           ├─ Record   current() = (key decoder, value)
//!                                           ├─ Skipped  a key the selection or the ownership check rejected
//!                                           └─ Done
//! ```
//!
//! [`BlockingScan`] runs that loop to completion on the caller's thread.
//! [`CursorScan`] hands out the [`TableCursor`] instead, so the caller can
//! interleave steps with other work, pause, or move it between threads.

use std::collections::{BTreeSet, btree_set};
use std::num::NonZeroUsize;
use std::ops::{ControlFlow, RangeBounds};
use std::sync::Arc;

use bytes::BytesMut;

use restate_rocksdb::reads::{OwnedPinnedBatch, ReadSnapshot as RocksReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_query_api::scan::{BlockingScan, CursorScan, CursorStep, ReadCursor, Table};
use restate_types::identifiers::PartitionKey;
use restate_types::partitions::Partition;

use crate::features::StorageFeatures;
use crate::keys::header::{KeyHeader, KeySchema};
use crate::keys::selection::{KeyMatch, KeySelection, KeySelectionCursor};
use crate::keys::{EncodeTableKey, KeyDecoder, KeyKind};
use crate::partition_db::PhysicalPartition;
use crate::query_read::SnapshotCursor;
use crate::registry::{IndexKind, IndexNotComplete, PublishedRegistry};
use crate::{ReadSnapshot, Result};

#[cfg(test)]
mod tests;

/// What a read may touch: the partition, the storage features in force and where
/// the IDs of registered indexes are resolved. A snapshot read uses the features
/// and the registry captured when the snapshot was admitted; a store read uses
/// the features in force and the store's installed index set.
pub struct ReadMetadata<'a> {
    pub(crate) partition: &'a Partition,
    pub(crate) features: StorageFeatures,
    pub(crate) indexes: IndexSource<'a>,
}

/// Where a read resolves the index IDs of registered indexes.
#[derive(Clone, Copy)]
pub(crate) enum IndexSource<'a> {
    /// The registry as of the read's snapshot, so the index's ID and status
    /// match the data it reads.
    Snapshot(&'a ReadSnapshot),
    /// The store's installed indexes, for reads that are not snapshot-bound.
    #[allow(dead_code)] // Used by reads that are not snapshot-bound.
    Store(&'a PublishedRegistry),
}

impl ReadMetadata<'_> {
    /// The index ID of the Complete index of `definition` this read may use.
    /// Anything else fails with [`crate::registry::IndexNotComplete`]: an incomplete index is never
    /// read as complete.
    pub(crate) fn complete_index(&self, definition: &str, kind: IndexKind) -> Result<u32> {
        match self.indexes {
            IndexSource::Snapshot(snapshot) => snapshot.complete_index(definition, kind),
            IndexSource::Store(active) => {
                active.complete_index(definition, kind).map_err(|reason| {
                    StorageError::Generic(
                        IndexNotComplete {
                            definition: definition.into(),
                            partition: self.partition.partition_id,
                            reason,
                        }
                        .into(),
                    )
                })
            }
        }
    }
}

/// A table as storage lays it out. Its key schema declares the header and the
/// scope a read covers; whether its fields are ordered, and so support
/// restrictions and seeks, is the key's own declaration.
pub trait PhysicalTable:
    for<'a> Table<Scan = KeySelection<Self::Key>, Record<'a> = (KeyDecoder<'a, Self::Key>, &'a [u8])>
{
    type Key: KeySchema;

    /// Whether this layout still holds records on this store. For example, the
    /// unscoped state table is inactive once the store has migrated to scoped
    /// state. Checked when the read executes, not when it is prepared, so a
    /// request prepared earlier stays valid.
    fn is_active(_metadata: &ReadMetadata<'_>) -> bool {
        true
    }

    /// An extra ownership check for layouts whose header does not carry the
    /// record's partition key, such as secondary indexes: a record outside the
    /// partition is skipped.
    fn owns_record(_key: KeyDecoder<'_, Self::Key>, _metadata: &ReadMetadata<'_>) -> Result<bool> {
        Ok(true)
    }
}

/// A table whose records a query fetches by ID: the IDs' order is the order of
/// the keys they encode to, so a batch of IDs is one sorted multi-get.
pub trait MultiGetTable: PhysicalTable<Key: EncodeTableKey> {
    /// The ID of a record. Its `Ord` is the stored key order, which the sorted
    /// multi-get relies on; the keys of the table have one fixed length.
    type Id: Clone + Ord;

    /// IDs per multi-get.
    const BATCH_SIZE: NonZeroUsize;

    /// The partition key `id` belongs to: an ID outside the snapshot's partition
    /// has no record there and is not fetched.
    fn partition_key(id: &Self::Id) -> PartitionKey;

    /// The stored key of `id`'s record.
    fn key(id: &Self::Id) -> Self::Key;
}

/// A key a [`KeyBatchCursor`] fetches: its partition key, so keys outside the
/// snapshot's partition are skipped, and its stored bytes.
pub(crate) trait BatchKey {
    fn partition_key(&self) -> PartitionKey;
    fn encode(&self, target: &mut BytesMut);
}

/// Fetches the records of a sequence of keys, in stored-byte order, a bounded
/// batch per multi-get: a step hands out the next key that has a record
/// ([`CursorStep::Record`]) or skips one that has none, and a batch is fetched
/// when the steps reach it.
///
/// Each refill examines at most `batch_size` keys, owned or not, so a step does
/// bounded work even when a whole batch lies outside the partition.
pub(crate) struct KeyBatchCursor<I: BatchKey, It: ExactSizeIterator<Item = I>> {
    keys: It,
    batch_size: NonZeroUsize,
    key_kind: KeyKind,
    /// The keys of the fetched batch, in order; `key_buf` holds their bytes, and
    /// `ends` where each one ends in it.
    batch: Vec<I>,
    key_buf: BytesMut,
    ends: Vec<usize>,
    results: Option<OwnedPinnedBatch<RocksReadSnapshot<PhysicalPartition>>>,
    /// The batch index the next step examines.
    next: usize,
    /// The batch index of the current record, if the last step produced one.
    current: Option<usize>,
    /// Last: the fetched batch above is dropped before the query snapshot, so
    /// the read's completion is signalled only once the batch released its
    /// storage, however the cursor ends.
    snapshot: ReadSnapshot,
}

impl<I: BatchKey, It: ExactSizeIterator<Item = I>> KeyBatchCursor<I, It> {
    /// `keys` must be in stored-byte order: a batch is one sorted multi-get.
    pub(crate) fn new(
        snapshot: ReadSnapshot,
        keys: It,
        batch_size: NonZeroUsize,
        key_kind: KeyKind,
    ) -> Self {
        Self {
            batch: Vec::with_capacity(keys.len().min(batch_size.get())),
            keys,
            batch_size,
            key_kind,
            key_buf: BytesMut::new(),
            ends: Vec::new(),
            results: None,
            next: 0,
            current: None,
            snapshot,
        }
    }

    pub(crate) fn advance(&mut self) -> Result<CursorStep> {
        self.snapshot.check_active()?;
        self.current = None;
        if self.next == self.batch.len() {
            if self.keys.len() == 0 {
                return Ok(CursorStep::Done);
            }
            self.refill()?;
        }
        if self.next == self.batch.len() {
            // Every key of the batch lies outside the partition.
            return Ok(CursorStep::Skipped);
        }
        let index = self.next;
        self.next += 1;
        let present = self
            .results()?
            .get(index)
            .map_err(|error| StorageError::Generic(error.into()))?
            .is_some();
        self.current = present.then_some(index);
        Ok(if present {
            CursorStep::Record
        } else {
            CursorStep::Skipped
        })
    }

    /// The snapshot the cursor reads.
    pub(crate) fn snapshot(&self) -> &ReadSnapshot {
        &self.snapshot
    }

    /// The current record's key, its stored bytes and its stored value.
    pub(crate) fn current(&self) -> Result<(&I, &[u8], &[u8])> {
        self.snapshot.check_active()?;
        let index = self.current.ok_or(StorageError::DataIntegrityError)?;
        let value = self
            .results()?
            .get(index)
            .map_err(|error| StorageError::Generic(error.into()))?
            .ok_or(StorageError::DataIntegrityError)?;
        let start = index
            .checked_sub(1)
            .map_or(0, |previous| self.ends[previous]);
        Ok((
            &self.batch[index],
            &self.key_buf[start..self.ends[index]],
            value,
        ))
    }

    fn results(&self) -> Result<&OwnedPinnedBatch<RocksReadSnapshot<PhysicalPartition>>> {
        self.results
            .as_ref()
            .ok_or(StorageError::DataIntegrityError)
    }

    /// Fetches the next batch: the next `batch_size` keys, minus those outside
    /// the partition.
    fn refill(&mut self) -> Result<()> {
        self.key_buf.clear();
        self.ends.clear();
        self.batch.clear();
        self.results = None;
        self.next = 0;
        let partition = self.snapshot.partition_key_range();
        for key in self.keys.by_ref().take(self.batch_size.get()) {
            if partition.contains(&key.partition_key()) {
                key.encode(&mut self.key_buf);
                self.ends.push(self.key_buf.len());
                self.batch.push(key);
            }
        }
        if self.batch.is_empty() {
            return Ok(());
        }
        let key_buf = &self.key_buf;
        let keys = std::iter::once(0)
            .chain(self.ends.iter().copied())
            .zip(self.ends.iter().copied())
            .map(|(start, end)| &key_buf[start..end]);
        let reader = self.snapshot.storage_snapshot().reader();
        self.results = Some(
            reader
                .multi_get_pinned(self.key_kind, keys, true)
                .map_err(|error| StorageError::Generic(error.into()))?
                .into_owned(),
        );
        Ok(())
    }
}

/// An ID of a [`MultiGetTable`], fetched through its stored key.
pub(crate) struct IdKey<T: MultiGetTable>(T::Id);

impl<T: MultiGetTable> BatchKey for IdKey<T> {
    fn partition_key(&self) -> PartitionKey {
        T::partition_key(&self.0)
    }

    fn encode(&self, target: &mut BytesMut) {
        T::key(&self.0).serialize_to(target);
    }
}

type IdKeys<T> = std::iter::Map<
    btree_set::IntoIter<<T as MultiGetTable>::Id>,
    fn(<T as MultiGetTable>::Id) -> IdKey<T>,
>;

/// Fetches the records of an ID set in ID order, which is their stored-key order,
/// [`MultiGetTable::BATCH_SIZE`] IDs per multi-get ([`KeyBatchCursor`]).
pub(crate) struct MultiGetCursor<T: MultiGetTable>(KeyBatchCursor<IdKey<T>, IdKeys<T>>);

impl<T: MultiGetTable> MultiGetCursor<T> {
    pub(crate) fn new(snapshot: ReadSnapshot, ids: BTreeSet<T::Id>) -> Self {
        let keys: IdKeys<T> = ids.into_iter().map(IdKey);
        Self(KeyBatchCursor::new(
            snapshot,
            keys,
            T::BATCH_SIZE,
            T::Key::KEY_KIND,
        ))
    }

    pub(crate) fn advance(&mut self) -> Result<CursorStep> {
        self.0.advance()
    }

    /// The current record: its ID and its stored value.
    pub(crate) fn current(&self) -> Result<(T::Id, &[u8])> {
        let (id, _, value) = self.0.current()?;
        Ok((id.0.clone(), value))
    }
}

/// Visits sorted full keys as the storage iterator does: in-bounds keys only, and
/// a seek repositions at the first key at or after its target. Returns the indices
/// of matching keys and the number of evaluated keys.
#[cfg(test)]
pub(crate) fn navigate<K: KeySchema>(
    request: impl Into<Arc<KeySelection<K>>>,
    scope: <K::Header as KeyHeader>::Scope,
    keys: &[impl AsRef<[u8]>],
) -> (Vec<usize>, usize) {
    navigate_live(request, None, scope, keys)
}

/// [`navigate`] with an independently prepared live selection.
#[cfg(test)]
pub(crate) fn navigate_live<K: KeySchema>(
    request: impl Into<Arc<KeySelection<K>>>,
    live: Option<KeySelection<K>>,
    scope: <K::Header as KeyHeader>::Scope,
    keys: &[impl AsRef<[u8]>],
) -> (Vec<usize>, usize) {
    let Some(mut cursor) = request.into().cursor(scope).unwrap() else {
        return (Vec::new(), 0);
    };
    if let Some(live) = live {
        cursor.set_live(live);
    }
    let (mut matches, mut visited) = (Vec::new(), 0);
    let mut position = keys
        .iter()
        .position(|key| cursor.scan().contains_key(key))
        .unwrap_or(keys.len());
    while let Some(key) = keys.get(position).map(AsRef::as_ref)
        && cursor.scan().contains_key(key)
    {
        visited += 1;
        match cursor.evaluate(key).unwrap() {
            KeyMatch::Match => {
                matches.push(position);
                position += 1;
            }
            KeyMatch::Seek => {
                let target = cursor.seek_target();
                assert!(target > key);
                position = keys.partition_point(|key| key.as_ref() < target);
            }
            KeyMatch::Skip => position += 1,
            KeyMatch::Done => break,
        }
    }
    (matches, visited)
}

impl<T: PhysicalTable> BlockingScan<T> for ReadSnapshot {
    fn scan_blocking(
        &self,
        request: Arc<KeySelection<T::Key>>,
        mut emit: impl for<'a> FnMut((KeyDecoder<'a, T::Key>, &'a [u8])) -> ControlFlow<Result<()>>,
    ) -> Result<()> {
        let mut cursor = <Self as CursorScan<T>>::scan_cursor(self, request)?;
        loop {
            match cursor.advance()? {
                CursorStep::Done => return Ok(()),
                CursorStep::Skipped => {}
                CursorStep::Record => {
                    if let ControlFlow::Break(result) = emit(cursor.current()?) {
                        return result;
                    }
                }
            }
        }
    }
}

/// A request bound to one snapshot, with its own position. Each
/// [`ReadCursor::advance`] visits at most one candidate key, counting the ones
/// the selection or the ownership check rejects, so a step is bounded work.
pub struct TableCursor<T: PhysicalTable> {
    inner: Option<(SnapshotCursor, KeySelectionCursor<T::Key>)>,
    /// The selection asked to seek to its target before the next key.
    seek: bool,
    first: bool,
    positioned: bool,
    evaluated: u64,
}

impl<T: PhysicalTable> TableCursor<T> {
    /// Keys evaluated by navigation so far, including rejected keys and a final
    /// key that proves the selection exhausted.
    pub fn evaluated(&self) -> u64 {
        self.evaluated
    }
}

impl<T: PhysicalTable> CursorScan<T> for ReadSnapshot {
    type Cursor = TableCursor<T>;

    fn scan_cursor(&self, request: Arc<KeySelection<T::Key>>) -> Result<Self::Cursor> {
        self.check_active()?;
        let metadata = self.metadata();
        let selection = if T::is_active(&metadata) {
            request.cursor(<T::Key as KeySchema>::Header::scope(&metadata)?)?
        } else {
            None
        };
        Ok(TableCursor {
            inner: selection
                .map(|selection| {
                    self.physical_cursor(selection.scan().clone())
                        .map(|cursor| (cursor, selection))
                })
                .transpose()?,
            seek: false,
            first: true,
            positioned: false,
            evaluated: 0,
        })
    }
}

impl<T: PhysicalTable> ReadCursor<T> for TableCursor<T> {
    fn advance(&mut self) -> Result<CursorStep> {
        self.positioned = false;
        let Some((cursor, selection)) = &mut self.inner else {
            return Ok(CursorStep::Done);
        };
        if !std::mem::take(&mut self.first) {
            if std::mem::take(&mut self.seek) {
                cursor.seek_forward(selection.seek_target())?;
            } else {
                cursor.advance()?;
            }
        }
        let Some((key, _)) = cursor.peek()? else {
            self.inner = None;
            return Ok(CursorStep::Done);
        };
        self.evaluated += 1;
        match selection.evaluate(key)? {
            KeyMatch::Seek => self.seek = true,
            KeyMatch::Done => {
                self.inner = None;
                return Ok(CursorStep::Done);
            }
            KeyMatch::Skip => {}
            KeyMatch::Match => {
                self.positioned =
                    T::owns_record(T::Key::decoder(key)?, &cursor.snapshot().metadata())?;
            }
        }
        Ok(if self.positioned {
            CursorStep::Record
        } else {
            CursorStep::Skipped
        })
    }

    fn current(&self) -> Result<T::Record<'_>> {
        let Some((cursor, _)) = self.inner.as_ref().filter(|_| self.positioned) else {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "cursor is not positioned on a record"
            )));
        };
        let (key, value) = cursor.peek()?.ok_or(StorageError::DataIntegrityError)?;
        Ok((T::Key::decoder(key)?, value))
    }
}
