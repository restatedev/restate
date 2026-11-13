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
//!   saw it, so that an index is only read in a state the snapshot saw as Ready.
//! - **Record**: one key, handed to the caller as a decoder positioned after the
//!   header plus the value bytes, undecoded.
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

use std::ops::ControlFlow;
use std::sync::Arc;

use restate_storage_api::StorageError;
use restate_storage_query_api::scan::{BlockingScan, CursorScan, CursorStep, ReadCursor, Table};
use restate_types::partitions::Partition;

use crate::features::StorageFeatures;
use crate::keys::KeyDecoder;
use crate::keys::header::{KeyHeader, KeySchema};
use crate::keys::selection::{KeyMatch, KeySelection, KeySelectionCursor};
use crate::query_read::SnapshotCursor;
use crate::registry::{IndexKind, IndexNotReady, MaintainedIndexes};
use crate::{ReadSnapshot, Result};

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
    /// The registry as of the read's snapshot, so the ID and readiness match the
    /// data it reads.
    Snapshot(&'a ReadSnapshot),
    /// The store's installed indexes, for reads that are not snapshot-bound.
    #[allow(dead_code)] // Used by reads that are not snapshot-bound.
    Store(&'a MaintainedIndexes),
}

impl ReadMetadata<'_> {
    /// The index ID of the Ready index of `definition` this read may use.
    /// Anything else fails with [`crate::registry::IndexNotReady`]: an incomplete index is never
    /// read as complete.
    pub(crate) fn ready_index(&self, definition: &str, kind: IndexKind) -> Result<u32> {
        match self.indexes {
            IndexSource::Snapshot(snapshot) => snapshot.ready_index(definition, kind),
            IndexSource::Store(active) => active.ready_index(definition, kind).map_err(|reason| {
                StorageError::Generic(
                    IndexNotReady {
                        definition: definition.into(),
                        partition: self.partition.partition_id,
                        reason,
                    }
                    .into(),
                )
            }),
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
            KeyMatch::Seek(target) => {
                assert!(target.as_ref() > key);
                position = keys.partition_point(|key| key.as_ref() < target.as_ref());
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
    seek: Option<bytes::Bytes>,
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
            seek: None,
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
            match self.seek.take() {
                Some(target) => cursor.seek_forward(target)?,
                None => cursor.advance()?,
            }
        }
        let Some((key, _)) = cursor.peek()? else {
            self.inner = None;
            return Ok(CursorStep::Done);
        };
        self.evaluated += 1;
        match selection.evaluate(key)? {
            KeyMatch::Seek(target) => self.seek = Some(target),
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
