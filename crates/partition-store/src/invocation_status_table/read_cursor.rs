// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Invocation statuses read for queries: a cursor over the statuses a selection
//! names, on the caller's thread, through one admitted snapshot.
//!
//! A read is either an **ID set**, fetched with multi-gets in bounded batches in
//! key order, or a **key range** navigated with seeks ([`InvocationStatusKeys`]),
//! where an ID restriction positions the iterator at each requested invocation
//! instead of reading every status of the partition key ([`InvocationStatusScan`]).
//! The cursor ([`InvocationStatusCursor`]) does bounded work per step and hands
//! out the current invocation's ID and its status, decoded lazily.
//!
//! A read of [`InvocationStatusEntryRecords`] also hands out each invocation's
//! vqueue **entry**: its stage and stored status, read in step with the status
//! through the same snapshot, with no JOIN. Within a partition key, statuses and
//! invocation entries are stored in the same order, by UUID:
//!
//! ```text
//! statuses   is | pkey | uuid               is|7|u1   is|7|u4   is|8|u2
//! entries    qs | pkey | kind | uuid        qs|7|inv|u1  qs|7|inv|u4  qs|7|sm|…  qs|8|inv|u2
//! ```
//!
//! So a key scan moves a second iterator over the entries forward with the
//! statuses: one step usually reaches the next invocation's entry, and a seek
//! skips state mutations and invocations without a status. An ID set fetches the
//! entries of its next IDs with one multi-get, as it does their statuses, and an
//! entry-index read with its window.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::ops::RangeBounds;
use std::sync::Arc;

use restate_storage_api::StorageError;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_api::vqueue_table::{RawStatusHeaderRef, Stage, Status};
use restate_storage_query_api::ordering::{NativeOrder, OrderFunction, OrderInput, OrderTerm};
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor, Table};
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::invocation_status::{
    InvocationStatus, InvocationStatusClause,
};
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey};
use restate_types::sharding::KeyRange;

use crate::keys::codecs::InvocationUuidCodec;
use crate::keys::header::{KeyHeader, KeySchema, PartitionKeyHeader};
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyDecode, KeyDecoder, KeyKind};
use crate::query_read::SnapshotCursor;
use crate::table::{MultiGetCursor, MultiGetTable, PhysicalTable, TableCursor};
use crate::vqueue_table::EntryStatusKey;
use crate::{ReadSnapshot, Result};

use super::entry_index_read::{EntryIndexCursor, EntryIndexExecution, EntryIndexRead};
use super::{
    INVOCATION_STATUS_MULTI_GET_BATCH_SIZE, InvocationStatusKey, create_invocation_status_key,
};

/// Stored invocation-status keys: `kind | partition_key | invocation_uuid`. The UUID
/// is raw, fixed-width bytes, so it is declared as an ordered field: an ID
/// restriction positions the iterator at each requested invocation instead of
/// reading every invocation that shares its partition key (e.g. all invocations of
/// one scope or virtual-object key).
pub enum InvocationStatusKeys {}

impl Table for InvocationStatusKeys {
    type Record<'a> = (KeyDecoder<'a, InvocationStatusKey>, &'a [u8]);
    type Scan = KeySelection<InvocationStatusKey>;
}

impl PhysicalTable for InvocationStatusKeys {
    type Key = InvocationStatusKey;
}

impl MultiGetTable for InvocationStatusKeys {
    type Id = InvocationId;
    const BATCH_SIZE: NonZeroUsize = INVOCATION_STATUS_MULTI_GET_BATCH_SIZE;

    fn partition_key(id: &InvocationId) -> PartitionKey {
        id.partition_key()
    }

    fn key(id: &InvocationId) -> InvocationStatusKey {
        create_invocation_status_key(id)
    }
}

impl InvocationStatusKeys {
    /// Prepares a key iteration over the `header` partition keys, which the caller
    /// derives from the selection's sharding rules. Selected IDs position the
    /// iterator at their UUIDs under each header partition key: another partition
    /// key's invocation with a requested UUID is a candidate, so callers retain the
    /// ID filter as a residual, like every unsupported predicate.
    pub fn prepare(
        selection: &Selection<InvocationStatus>,
        header: Option<KeyRange>,
    ) -> Result<KeySelection<InvocationStatusKey>> {
        let request = KeySelection::for_key_fields(selection, |clause, literals| match clause {
            InvocationStatusClause::Id(predicate) => {
                InvocationUuidCodec::try_prepare_value(predicate, literals)
            }
            _ => Ok(None),
        })?;
        Ok(match header {
            Some(range) => request.restrict_scope(range),
            None => KeySelection::empty(),
        })
    }
}

impl KeySchema for InvocationStatusKey {
    type Header = PartitionKeyHeader<Self>;

    fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
        // The invocation ID needs the partition key. Retain the full key.
        Ok(KeyDecoder::from_payload(key))
    }
}

impl KeyDecode for InvocationStatusKey {
    fn decode(source: &mut &[u8]) -> Result<Self> {
        Self::deserialize_from(source)
    }
}

impl OrderedKeySchema for InvocationStatusKey {
    const KEY_KIND: KeyKind = KeyKind::InvocationStatus;
    const FIELDS: &'static [OrderedKeyField] = &[OrderedKeyField::new::<InvocationUuidCodec>("id")];
}

/// A prepared invocation-status read.
#[derive(Clone)]
pub enum InvocationStatusScan {
    /// Point reads of a finite ID set, batched through multi-get.
    Ids(BTreeSet<InvocationId>),
    /// Iteration over stored keys, navigated by the prepared selection.
    Keys(Arc<KeySelection<InvocationStatusKey>>),
    /// The invocations the entry index finds, newest created first (or oldest
    /// first, as the [`EntryIndexRead`] says), or the
    /// `fallback` iteration if the index can't stand in for it on the snapshot.
    /// `execution` is charged for the index read and learns which path it took.
    /// It serves one cursor: a request holds the execution of one partition
    /// read, and opening a second cursor from it, or from a clone, fails.
    EntryIndex {
        index: Arc<EntryIndexRead>,
        fallback: Arc<KeySelection<InvocationStatusKey>>,
        execution: Arc<EntryIndexExecution>,
    },
}

/// Invocation-status records. A caller can step through them with a cursor, or
/// a query can scan them on the query IO pool through [`CursorScan`]. Both read
/// the same keys in the same way.
pub enum InvocationStatusRecords {}

impl Table for InvocationStatusRecords {
    type Record<'a> = (InvocationId, InvocationStatusV2Lazy<'a>);
    type Scan = InvocationStatusScan;
    /// Keys are `partition key | uuid`, and an ID set is fetched in ID order, so
    /// both read in partition key order and in invocation ID order; with the entry
    /// kind fixed, that is also base entry ID order.
    const NATIVE_ORDERS: &'static [NativeOrder] = &[
        &[OrderTerm::Column("partition_key")],
        &[OrderTerm::Function(OrderFunction::InvocationId)],
        &[OrderTerm::Function(OrderFunction::BaseEntryId)],
    ];

    fn order_key<R>(
        (id, _): &Self::Record<'_>,
        function: OrderFunction,
        with: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        id.order_key(function, with)
    }
}

/// The stage and stored status of an invocation's vqueue entry, read with its
/// status ([`InvocationStatusEntryRecords`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvocationEntry {
    pub stage: Stage,
    pub status: Status,
}

/// [`InvocationStatusRecords`] with each invocation's vqueue entry, `None` for
/// an invocation without one. The same requests read the same statuses in the
/// same order; the entries come from the same snapshot (see the module docs).
pub enum InvocationStatusEntryRecords {}

impl Table for InvocationStatusEntryRecords {
    type Record<'a> = (
        InvocationId,
        InvocationStatusV2Lazy<'a>,
        Option<InvocationEntry>,
    );
    type Scan = InvocationStatusScan;
    const NATIVE_ORDERS: &'static [NativeOrder] = InvocationStatusRecords::NATIVE_ORDERS;

    fn order_key<R>(
        (id, ..): &Self::Record<'_>,
        function: OrderFunction,
        with: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        id.order_key(function, with)
    }
}

/// An [`InvocationStatusCursor`] that also hands out each invocation's entry.
pub struct InvocationStatusEntryCursor(InvocationStatusCursor);

/// A resumable scan or batched lookup of invocation statuses over one admitted
/// snapshot.
pub struct InvocationStatusCursor {
    mode: Mode,
    /// How the read finds each invocation's entry, if it reads them.
    entries: Option<Entries>,
    /// The entry of the record the last step produced.
    entry: Option<InvocationEntry>,
}

/// How a read with entries finds them, by the shape of its status read.
enum Entries {
    Scan(EntryScan),
    Ids(EntryLookups),
    /// The entry-index cursor fetches them with its window.
    Index,
}

/// The entries of a key scan's invocations: an iterator over the snapshot's
/// entries, moved forward with the statuses. Opened on the first status.
struct EntryScan {
    snapshot: ReadSnapshot,
    cursor: Option<SnapshotCursor>,
    target: Vec<u8>,
}

impl EntryScan {
    fn new(snapshot: ReadSnapshot) -> Self {
        Self {
            snapshot,
            cursor: None,
            target: Vec::with_capacity(EntryStatusKey::serialized_length_fixed()),
        }
    }

    /// The entry of `id`, whose status comes after every status asked for
    /// before: the iterator only moves forward.
    fn find(&mut self, id: &InvocationId) -> Result<Option<InvocationEntry>> {
        self.target.clear();
        EntryStatusKey::from(*id).serialize_to(&mut self.target);
        let cursor = match &mut self.cursor {
            Some(cursor) => cursor,
            None => {
                let bounds = PartitionKeyHeader::<EntryStatusKey>::bounds(
                    self.snapshot.partition_key_range(),
                )
                .ok_or(StorageError::DataIntegrityError)?;
                self.cursor.insert(self.snapshot.physical_cursor(bounds.0)?)
            }
        };
        // The next invocation's entry usually follows the last one: one step
        // first, then a seek past state mutations and statuses without entries.
        let mut stepped = false;
        loop {
            let ordering = match cursor.peek()? {
                None => return Ok(None),
                Some((key, value)) => match key.cmp(self.target.as_slice()) {
                    Ordering::Equal => return decode_entry(value).map(Some),
                    ordering => ordering,
                },
            };
            match ordering {
                Ordering::Greater => return Ok(None),
                _ if !stepped => {
                    cursor.advance()?;
                    stepped = true;
                }
                _ => cursor.seek_forward(&self.target)?,
            }
        }
    }
}

/// The entries of an ID set's invocations, fetched in ID order with one
/// multi-get per [`INVOCATION_STATUS_MULTI_GET_BATCH_SIZE`] IDs, from the first
/// ID whose entry is asked for.
struct EntryLookups {
    snapshot: ReadSnapshot,
    /// The read's IDs on the snapshot's partition, in order.
    ids: Vec<InvocationId>,
    /// The fetched IDs are `ids[start..start + fetched.len()]`.
    start: usize,
    fetched: Vec<Option<InvocationEntry>>,
    key_buf: Vec<u8>,
}

impl EntryLookups {
    fn new(snapshot: ReadSnapshot, ids: &BTreeSet<InvocationId>) -> Self {
        let partition = snapshot.partition_key_range();
        Self {
            ids: ids
                .iter()
                .filter(|id| partition.contains(&id.partition_key()))
                .copied()
                .collect(),
            snapshot,
            start: 0,
            fetched: Vec::new(),
            key_buf: Vec::new(),
        }
    }

    /// The entry of `id`, one of the read's IDs, asked for in ID order.
    fn find(&mut self, id: &InvocationId) -> Result<Option<InvocationEntry>> {
        let end = self.start + self.fetched.len();
        if let Ok(at) = self.ids[self.start..end].binary_search(id) {
            return Ok(self.fetched[at]);
        }
        let from = end + self.ids[end..].partition_point(|other| other < id);
        if self.ids.get(from) != Some(id) {
            return Err(StorageError::DataIntegrityError);
        }
        self.fetch(from)?;
        Ok(self.fetched[0])
    }

    fn fetch(&mut self, from: usize) -> Result<()> {
        const KEY_LEN: usize = EntryStatusKey::serialized_length_fixed();
        let to = self
            .ids
            .len()
            .min(from + INVOCATION_STATUS_MULTI_GET_BATCH_SIZE.get());
        self.key_buf.clear();
        for id in &self.ids[from..to] {
            EntryStatusKey::from(*id).serialize_to(&mut self.key_buf);
        }
        let (keys, remainder) = self.key_buf.as_chunks::<KEY_LEN>();
        debug_assert!(remainder.is_empty());
        let reader = self.snapshot.storage_snapshot().reader();
        let values = reader
            .multi_get_pinned(KeyKind::VQueueEntryStatus, keys, true)
            .map_err(|error| StorageError::Generic(error.into()))?;
        self.fetched.clear();
        for slot in 0..to - from {
            let value = values
                .get(slot)
                .map_err(|error| StorageError::Generic(error.into()))?;
            self.fetched.push(value.map(decode_entry).transpose()?);
        }
        self.start = from;
        Ok(())
    }
}

/// The stage and stored status of a stored entry status.
pub(super) fn decode_entry(mut value: &[u8]) -> Result<InvocationEntry> {
    let header = RawStatusHeaderRef::decode_stored(&mut value)?;
    Ok(InvocationEntry {
        stage: header.stage,
        status: header.status,
    })
}

enum Mode {
    Empty,
    Scan(Box<TableCursor<InvocationStatusKeys>>),
    Keys(Box<MultiGetCursor<InvocationStatusKeys>>),
    EntryIndex(Box<EntryIndexCursor>),
}

impl ReadSnapshot {
    pub fn invocation_status_cursor(
        &self,
        scan: InvocationStatusScan,
    ) -> Result<InvocationStatusCursor> {
        self.open_invocation_status_cursor(scan, false)
    }

    /// [`Self::invocation_status_cursor`], also reading each invocation's entry
    /// ([`InvocationStatusEntryRecords`]).
    pub fn invocation_status_entry_cursor(
        &self,
        scan: InvocationStatusScan,
    ) -> Result<InvocationStatusEntryCursor> {
        self.open_invocation_status_cursor(scan, true)
            .map(InvocationStatusEntryCursor)
    }

    fn open_invocation_status_cursor(
        &self,
        scan: InvocationStatusScan,
        entries: bool,
    ) -> Result<InvocationStatusCursor> {
        self.check_active()?;
        let scan_entries = || entries.then(|| Entries::Scan(EntryScan::new(self.clone())));
        let (mode, entries) = match scan {
            InvocationStatusScan::Ids(ids) => {
                let entries = entries.then(|| Entries::Ids(EntryLookups::new(self.clone(), &ids)));
                (
                    Mode::Keys(Box::new(MultiGetCursor::new(self.clone(), ids))),
                    entries,
                )
            }
            InvocationStatusScan::Keys(request) => (
                Mode::Scan(Box::new(
                    <Self as CursorScan<InvocationStatusKeys>>::scan_cursor(self, request)?,
                )),
                scan_entries(),
            ),
            InvocationStatusScan::EntryIndex {
                index,
                fallback,
                execution,
            } => match EntryIndexCursor::open_with(self, &index, &execution, entries)? {
                Some(cursor) => (
                    Mode::EntryIndex(Box::new(cursor)),
                    entries.then_some(Entries::Index),
                ),
                None => (
                    Mode::Scan(Box::new(
                        <Self as CursorScan<InvocationStatusKeys>>::scan_cursor(self, fallback)?,
                    )),
                    scan_entries(),
                ),
            },
        };
        Ok(InvocationStatusCursor {
            mode,
            entries,
            entry: None,
        })
    }
}

impl CursorScan<InvocationStatusRecords> for ReadSnapshot {
    type Cursor = InvocationStatusCursor;

    fn scan_cursor(&self, request: Arc<InvocationStatusScan>) -> Result<Self::Cursor> {
        // If the caller hands over the last reference to the request, the cursor
        // takes its ID list; otherwise it copies the list. Cursors opened from
        // one request don't affect each other, except an entry-index request's,
        // whose execution serves one cursor: a second fails to open.
        self.invocation_status_cursor(Arc::unwrap_or_clone(request))
    }
}

impl CursorScan<InvocationStatusEntryRecords> for ReadSnapshot {
    type Cursor = InvocationStatusEntryCursor;

    /// As for [`InvocationStatusRecords`].
    fn scan_cursor(&self, request: Arc<InvocationStatusScan>) -> Result<Self::Cursor> {
        self.invocation_status_entry_cursor(Arc::unwrap_or_clone(request))
    }
}

impl InvocationStatusCursor {
    /// A step, which also finds the entry of the record it produces when the
    /// read asked for entries.
    fn step(&mut self) -> Result<CursorStep> {
        self.entry = None;
        let step = match &mut self.mode {
            Mode::Empty => CursorStep::Done,
            Mode::Scan(cursor) => cursor.advance()?,
            Mode::Keys(cursor) => cursor.advance()?,
            Mode::EntryIndex(cursor) => cursor.advance()?,
        };
        match step {
            CursorStep::Done => {
                self.mode = Mode::Empty;
                self.entries = None;
            }
            CursorStep::Record if self.entries.is_some() => {
                let id = self.status()?.0;
                self.entry = match (&mut self.entries, &self.mode) {
                    (Some(Entries::Scan(entries)), _) => entries.find(&id)?,
                    (Some(Entries::Ids(entries)), _) => entries.find(&id)?,
                    (Some(Entries::Index), Mode::EntryIndex(cursor)) => cursor.entry()?,
                    _ => return Err(StorageError::DataIntegrityError),
                };
            }
            CursorStep::Record => {}
            CursorStep::Skipped => {}
        }
        Ok(step)
    }

    fn status(&self) -> Result<(InvocationId, &[u8])> {
        Ok(match &self.mode {
            Mode::Scan(cursor) => {
                let (key, value) = cursor.current()?;
                let (partition, uuid) = key.decode_all()?.split();
                (InvocationId::from_parts(partition, uuid), value)
            }
            Mode::Keys(cursor) => cursor.current()?,
            Mode::EntryIndex(cursor) => cursor.current_raw()?,
            Mode::Empty => return Err(StorageError::DataIntegrityError),
        })
    }
}

impl ReadCursor<InvocationStatusRecords> for InvocationStatusCursor {
    fn advance(&mut self) -> Result<CursorStep> {
        self.step()
    }

    fn current(&self) -> Result<(InvocationId, InvocationStatusV2Lazy<'_>)> {
        let (id, value) = self.status()?;
        Ok((id, super::decode_invocation_status_lazy(value)?))
    }
}

impl ReadCursor<InvocationStatusEntryRecords> for InvocationStatusEntryCursor {
    fn advance(&mut self) -> Result<CursorStep> {
        self.0.step()
    }

    fn current(
        &self,
    ) -> Result<(
        InvocationId,
        InvocationStatusV2Lazy<'_>,
        Option<InvocationEntry>,
    )> {
        let (id, value) = self.0.status()?;
        Ok((
            id,
            super::decode_invocation_status_lazy(value)?,
            self.0.entry,
        ))
    }
}

#[cfg(test)]
mod tests {
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InvocationStatus as StoredStatus, WriteInvocationStatusTable,
    };
    use restate_storage_query_api::selection::ValuePredicate;
    use restate_types::identifiers::{InvocationUuid, PartitionId, PartitionKey};
    use restate_types::partition_table::Partition;
    use tokio_util::sync::CancellationToken;

    use crate::PartitionStoreManager;

    use super::*;

    /// `order`'s sort key for the record with ID `InvocationId` text `id`.
    fn order_key(order: NativeOrder, id: &str) -> Vec<Vec<u8>> {
        order
            .iter()
            .map(|term| match term {
                OrderTerm::Column("partition_key") => id
                    .parse::<InvocationId>()
                    .unwrap()
                    .partition_key()
                    .to_be_bytes()
                    .to_vec(),
                OrderTerm::Function(function) => function.encode(id, <[u8]>::to_vec).unwrap(),
                term => panic!("no {term:?} in an ID"),
            })
            .collect()
    }

    #[restate_core::test]
    async fn ids_position_invocation_status_scans() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        // 128 invocations under each partition key, as for one scope or object key.
        // Equal UUIDs across keys model the candidates a header interval admits.
        let id = |key: PartitionKey, n: u128| {
            InvocationId::from_parts(key, InvocationUuid::from_u128(n))
        };
        let mut tx = store.transaction();
        for key in [0, 7, 8, PartitionKey::MAX] {
            for n in 1..=128 {
                let status = StoredStatus::Completed(CompletedInvocation::mock_neo());
                tx.put_invocation_status(&id(key, n), &status).unwrap();
            }
        }
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        // Returns the records read and the cursor steps taken to read them.
        let read = |selection: Selection<InvocationStatus>, header: KeyRange| {
            let request = InvocationStatusKeys::prepare(&selection, Some(header)).unwrap();
            let mut cursor = <ReadSnapshot as CursorScan<InvocationStatusRecords>>::scan_cursor(
                &snapshot,
                Arc::new(InvocationStatusScan::Keys(Arc::new(request))),
            )
            .unwrap();
            let (mut ids, mut steps) = (Vec::new(), 0);
            loop {
                match cursor.advance().unwrap() {
                    CursorStep::Done => return (ids, steps),
                    CursorStep::Skipped => steps += 1,
                    CursorStep::Record => {
                        steps += 1;
                        ids.push(cursor.current().unwrap().0);
                    }
                }
            }
        };
        let ids = |ids: &[InvocationId]| {
            Selection::All.and(InvocationStatusClause::Id(ValuePredicate::In(ids.to_vec())))
        };

        assert_eq!(
            read(ids(&[id(7, 64)]), KeyRange::new(7, 7)),
            (vec![id(7, 64)], 1),
            "an_id_reads_one_of_its_neighbors"
        );
        assert_eq!(
            read(ids(&[id(7, 5), id(7, 90)]), KeyRange::new(7, 7)),
            (vec![id(7, 5), id(7, 90)], 3),
            "same_header_ids_seek_between_each_other"
        );
        assert_eq!(
            read(ids(&[id(7, 1000)]), KeyRange::new(7, 7)),
            (vec![], 0),
            "a_missing_id_reads_nothing"
        );
        let (found, steps) = read(ids(&[id(7, 5), id(8, 90)]), KeyRange::new(7, 8));
        assert_eq!(found, [id(7, 5), id(7, 90), id(8, 5), id(8, 90)]);
        assert_eq!(steps, 9, "cross_header_ids_seek_within_and_between_headers");
        let (found, steps) = read(ids(&[id(0, 1), id(PartitionKey::MAX, 128)]), KeyRange::FULL);
        assert_eq!(found.len(), 8, "boundary_ids_cross_every_header: {found:?}");
        assert_eq!(steps, 12, "boundary_ids_seek_within_and_between_headers");
        assert_eq!(
            read(Selection::All, KeyRange::new(7, 7)).1,
            128,
            "header_only_reads_every_neighbor"
        );

        // Two cursors from one ID request must not affect each other: open both,
        // read one to the end, then check that the other still returns every ID.
        let expected = [id(7, 5), id(8, 90)];
        let request = Arc::new(InvocationStatusScan::Ids(expected.into()));
        let cursors = [Arc::clone(&request), request].map(|request| {
            <ReadSnapshot as CursorScan<InvocationStatusRecords>>::scan_cursor(&snapshot, request)
                .unwrap()
        });
        for mut cursor in cursors {
            let mut found = Vec::new();
            loop {
                match cursor.advance().unwrap() {
                    CursorStep::Record => found.push(cursor.current().unwrap().0),
                    CursorStep::Skipped => {}
                    CursorStep::Done => break,
                }
            }
            assert_eq!(found, expected, "shared_requests_have_independent_cursors");
        }
    }

    /// Every read shape hands out each invocation's entry with its status: a
    /// key scan steps an entry iterator past state mutations and entries
    /// without statuses, lookups by ID fetch the entries of their IDs, an
    /// `entry_by_created/1` read fetches its window's, and a status lane read
    /// takes them from its keys. An invocation without an entry has none.
    #[restate_core::test]
    async fn entries_are_read_with_their_statuses() {
        use std::collections::BTreeMap;

        use restate_clock::UniqueTimestamp;
        use restate_storage_api::fsm_table::WriteFsmTable;
        use restate_storage_api::invocation_status_table::StatusTimestamps;
        use restate_storage_api::vqueue_table::stats::EntryStatistics;
        use restate_storage_api::vqueue_table::{
            EntryContext, EntryKey, EntryMetadata, EntryStateRef, WriteVQueueTable,
        };
        use restate_storage_query_api::scan::ReadDirection;
        use restate_storage_query_api::tables::index::{
            EntryByServiceCreatedClause, EntryByStatusCreatedClause,
        };
        use restate_types::config::Configuration;
        use restate_types::invocation::InvocationTarget;
        use restate_types::partitions::features::PersistedFeatures;
        use restate_types::time::MillisSinceEpoch;
        use restate_types::vqueues::{
            EntryId, EntryKind, EntryTargetExt, EntryTargetRef, HandlerRef, Seq, VQueueId,
        };

        use super::super::entry_index_read::{EntryIndexExecution, EntryIndexRead};

        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let mut config = Configuration::default();
        config.common.experimental.set_track_entry_indexes(true);
        store
            .verify_and_run_migrations(CancellationToken::new(), &config)
            .await
            .unwrap();

        const BASE: u64 = 1_744_010_000_000;
        let stages = [
            Some((Stage::Inbox, Status::New)),
            Some((Stage::Running, Status::BackingOff)),
            None,
            Some((Stage::Finished, Status::Failed)),
            Some((Stage::Paused, Status::Started)),
            Some((Stage::Suspended, Status::Started)),
        ];
        let service = InvocationTarget::service("S", "h");
        let entry = |key: u64, seq: u64, id: EntryId| {
            let created =
                UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(BASE + seq));
            let entry_key = EntryKey::new(false, created.to_unix_millis(), seq, id);
            let stats = EntryStatistics::new(created, entry_key.run_at());
            (VQueueId::custom(key, "q"), entry_key, stats)
        };
        let mut expected = BTreeMap::new();
        let mut tx = store.transaction();
        for key in [7, 8] {
            for (n, stage) in stages.iter().enumerate() {
                let seq = key * 100 + n as u64;
                let id = InvocationId::from_parts(key, InvocationUuid::from_u128(n as u128 + 1));
                let mut status = CompletedInvocation::mock_neo();
                status.invocation_target = service.clone();
                status.timestamps = StatusTimestamps::init(MillisSinceEpoch::new(BASE + seq));
                status.creation_seq = Some(Seq::new(seq));
                tx.put_invocation_status(&id, &StoredStatus::Completed(status))
                    .unwrap();
                let entry_of = stage.map(|(stage, status)| InvocationEntry { stage, status });
                expected.insert(id, entry_of);
                let Some((stage, status)) = *stage else {
                    continue;
                };
                let (qid, entry_key, stats) = entry(key, seq, EntryId::from(id));
                tx.create_vqueue_entry_status(
                    &EntryContext {
                        qid: &qid,
                        target: &service.entry_target_ref(),
                    },
                    EntryStateRef {
                        stage,
                        status,
                        entry_key: &entry_key,
                        metadata: &EntryMetadata::default(),
                        stats: &stats,
                    },
                );
            }
        }
        // Entries without a status in between: a state mutation, and an
        // invocation of another service, which the index reads skip.
        for (n, id) in [
            EntryId::new(EntryKind::StateMutation, [3; 16]),
            EntryId::from(InvocationId::from_parts(
                7,
                InvocationUuid::from_u128(4 << 64),
            )),
        ]
        .into_iter()
        .enumerate()
        {
            let (qid, entry_key, stats) = entry(7, 900 + n as u64, id);
            let target = if id.kind() == EntryKind::StateMutation {
                EntryTargetRef::VirtualObject {
                    scope: None,
                    service: "O",
                    key: "k",
                    handler: HandlerRef::StateMutation,
                }
            } else {
                EntryTargetRef::Service {
                    scope: None,
                    service: "O",
                    handler: "h",
                }
            };
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target,
                },
                EntryStateRef {
                    stage: Stage::Inbox,
                    status: Status::New,
                    entry_key: &entry_key,
                    metadata: &EntryMetadata::default(),
                    stats: &stats,
                },
            );
        }
        tx.put_state_machine_features(&PersistedFeatures {
            vqueues: true,
            ..Default::default()
        })
        .unwrap();
        tx.commit().await.unwrap();
        drop(tx);

        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let read = |request: InvocationStatusScan| {
            let mut cursor =
                <ReadSnapshot as CursorScan<InvocationStatusEntryRecords>>::scan_cursor(
                    &snapshot,
                    Arc::new(request),
                )
                .unwrap();
            let mut found = BTreeMap::new();
            loop {
                match cursor.advance().unwrap() {
                    CursorStep::Record => {
                        let (id, _, entry) = cursor.current().unwrap();
                        assert!(found.insert(id, entry).is_none());
                    }
                    CursorStep::Skipped => {}
                    CursorStep::Done => return found,
                }
            }
        };
        let execution = || {
            Arc::new(EntryIndexExecution::new(
                Box::new(super::super::entry_index_read::counting::Counting::default()),
                |_| {},
                |_, _| {},
            ))
        };
        let all =
            Arc::new(InvocationStatusKeys::prepare(&Selection::All, Some(KeyRange::FULL)).unwrap());
        assert_eq!(
            read(InvocationStatusScan::Keys(Arc::clone(&all))),
            expected,
            "scan"
        );
        let some: BTreeSet<_> = expected.keys().copied().skip(1).step_by(2).collect();
        let some_expected: BTreeMap<_, _> = expected
            .iter()
            .filter(|(id, _)| some.contains(id))
            .map(|(id, entry)| (*id, *entry))
            .collect();
        assert_eq!(read(InvocationStatusScan::Ids(some)), some_expected, "ids");
        // The index reads find the invocations that have entries.
        let indexed: BTreeMap<_, _> = expected
            .iter()
            .filter(|(_, entry)| entry.is_some())
            .map(|(id, entry)| (*id, *entry))
            .collect();
        let by_service = Selection::All.and(EntryByServiceCreatedClause::TargetServiceName(
            ValuePredicate::Equal("S".into()),
        ));
        let created = EntryIndexRead::prepare(&by_service, ReadDirection::Forward).unwrap();
        let by_service_lanes = Selection::All.and(EntryByStatusCreatedClause::TargetServiceName(
            ValuePredicate::Equal("S".into()),
        ));
        let lanes =
            EntryIndexRead::prepare_by_status(&by_service_lanes, ReadDirection::Forward).unwrap();
        for (name, index) in [("created", created), ("lanes", lanes)] {
            let found = read(InvocationStatusScan::EntryIndex {
                index: Arc::new(index),
                fallback: Arc::clone(&all),
                execution: execution(),
            });
            assert_eq!(found, indexed, "{name}");
        }
    }

    /// A key scan and lookups by ID both read in each of the table's native
    /// orders, which the IDs' text doesn't sort like.
    #[restate_core::test]
    async fn key_ordered_reads_deliver_native_orders() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let uuids = [1, 2, 256, 1 << 64, (1 << 64) + 1, u128::MAX - 1];
        let ids: BTreeSet<_> = [0, 7, 8, PartitionKey::MAX]
            .into_iter()
            .flat_map(|key| {
                uuids.map(|uuid| InvocationId::from_parts(key, InvocationUuid::from_u128(uuid)))
            })
            .collect();
        let mut tx = store.transaction();
        for id in &ids {
            let status = StoredStatus::Completed(CompletedInvocation::mock_neo());
            tx.put_invocation_status(id, &status).unwrap();
        }
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let read = |request: InvocationStatusScan| {
            let mut cursor = <ReadSnapshot as CursorScan<InvocationStatusRecords>>::scan_cursor(
                &snapshot,
                Arc::new(request),
            )
            .unwrap();
            let mut found = Vec::new();
            loop {
                match cursor.advance().unwrap() {
                    CursorStep::Record => found.push(cursor.current().unwrap().0.to_string()),
                    CursorStep::Skipped => {}
                    CursorStep::Done => return found,
                }
            }
        };
        let all = InvocationStatusKeys::prepare(&Selection::All, Some(KeyRange::FULL)).unwrap();
        let some: BTreeSet<_> = ids.iter().copied().step_by(3).collect();
        for (read, expected) in [
            (read(InvocationStatusScan::Keys(Arc::new(all))), ids.len()),
            (read(InvocationStatusScan::Ids(ids.clone())), ids.len()),
            (read(InvocationStatusScan::Ids(some.clone())), some.len()),
        ] {
            assert_eq!(read.len(), expected);
            assert!(!read.is_sorted(), "text_order_differs");
            for order in InvocationStatusRecords::NATIVE_ORDERS {
                let values: Vec<_> = read.iter().map(|id| order_key(order, id)).collect();
                assert!(values.is_sorted(), "{order:?}");
            }
        }
    }
}
