// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Entries newest first, or oldest first ([`ReadDirection::Backward`]), through
//! an entry index whose keys each name one entry status:
//! `entry_by_status_created/1` and `entry_by_status_transitioned/1` hold one
//! key per entry in a known stage, by stage and status, a **lane** each, then
//! by the hour and time of creation or of the last transition ([`LanesRead`]).
//!
//! A query on those entries, perhaps of a few statuses or stages, ordered by
//! that time, merges the matching runs into one order and fetches each entry's
//! current status, instead of reading every entry status of the partition:
//!
//! ```text
//! index runs                                entry statuses, one multi-get per window
//! (failed,    "Utils", "call"): t9 t4   ─┐
//!                                        ├─▶  t9 t8 t5 t4 …, each checked
//! (succeeded, "Utils", "call"): t8 t5   ─┘
//! ```
//!
//! The index finds every Finished entry once it is Complete on the query's own
//! snapshot; otherwise, or if finding the runs takes more than one read allows,
//! the read scans the entry statuses instead, before it returns anything. It
//! reports which it did ([`EntryIndexPath`]), so that a caller relying on the
//! order sorts a scan.
//!
//! A key is used only if its entry's current status is present on the snapshot
//! and is still that key's: the key's stage, the key's status where the key
//! holds one, the key's time and incarnation. A missing or stale reference is
//! skipped and counted, as the stage reads do; a malformed status, or a storage
//! error, fails the read.
//! Everything the read holds is charged to the caller's memory, and released
//! as soon as it ends or fails.
//!
//! A status index's key holds everything some queries ask of an entry: its
//! stage, status, the index's time and its incarnation. A keys-only read hands
//! out the keys alone ([`EntryKeyRow`]), fetching no status, so its keys aren't
//! checked against the statuses: a Complete index has exactly one key per
//! entry of the stages it holds, maintained with the entry.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use restate_clock::UniqueTimestamp;
use restate_rocksdb::reads::{OwnedPinnedBatch, ReadSnapshot as RocksReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{RawStatusHeaderRef, Stage, Status};
use restate_storage_query_api::scan::{CursorStep, ReadDirection};
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::index::{
    EntryStatusByStatusCreated as StatusCreatedIndex,
    EntryStatusByStatusTransitioned as StatusTransitionedIndex,
};
use restate_types::identifiers::{BaseEntryId, CanonicalEntryId};

use crate::index::{PartitionKeyFilter, RegisteredIndex};
use crate::invocation_status_table::entry_index_read::{
    EntryIndexExecution, EntryIndexPath, Execution, Held, ScanReason, SharedMemory,
};
use crate::keys::selection::OrderedKeySchema;
use crate::keys::{EncodeTableKey, KeyDecoder, KeyKind};
use crate::ordered_runs::OrderedRunsLimits;
use crate::partition_db::PhysicalPartition;
use crate::registry::IndexKind;
use crate::status_lanes::{Lanes, StatusLanes};
use crate::table::PhysicalTable;
use crate::{ReadSnapshot, Result};

use super::read_cursor::EntryKeyRow;
use super::{
    ENTRY_STATUS_MULTI_GET_BATCH_SIZE, EntryByStatusCreated, EntryByStatusCreatedKey,
    EntryByStatusTransitioned, EntryByStatusTransitionedKey, EntryStatusKey,
};

/// An entry index whose keys each name one entry status, read newest first or
/// backwards: one of the two status indexes, read as lanes, each the entries of
/// one stage and status, whose runs are its hours and services
/// ([`crate::status_lanes`]).
pub(crate) trait EntryIndex: RegisteredIndex + PhysicalTable<Key: OrderedKeySchema> {
    /// What `key` says its entry's current status must still say.
    fn reference(key: KeyDecoder<'_, Self::Key>) -> Result<Reference>;

    /// The time the index orders by, as the status records it.
    fn time(header: &RawStatusHeaderRef<'_>) -> UniqueTimestamp;
}

impl EntryIndex for EntryByStatusCreated {
    fn reference(key: KeyDecoder<'_, EntryByStatusCreatedKey>) -> Result<Reference> {
        let key = key.decode_all()?;
        Ok(Reference {
            id: key.canonical_id,
            stage: key.stage,
            status: key.status,
            at: key.created_at.0,
        })
    }

    fn time(header: &RawStatusHeaderRef<'_>) -> UniqueTimestamp {
        header.stats.created_at
    }
}

impl EntryIndex for EntryByStatusTransitioned {
    fn reference(key: KeyDecoder<'_, EntryByStatusTransitionedKey>) -> Result<Reference> {
        let key = key.decode_all()?;
        Ok(Reference {
            id: key.canonical_id,
            stage: key.stage,
            status: key.status,
            at: key.transitioned_at.0,
        })
    }

    fn time(header: &RawStatusHeaderRef<'_>) -> UniqueTimestamp {
        header.stats.transitioned_at
    }
}

/// The same as the invocation entry-index read's: a few statuses of a few
/// hundred handlers stay within one read.
const LIMITS: OrderedRunsLimits = OrderedRunsLimits {
    max_runs: 256,
    max_discovery_keys: 4096,
    buffer: NonZeroUsize::new(8).unwrap(),
    step_budget: NonZeroUsize::new(256).unwrap(),
    bucket_fields: 0,
};

const WINDOW: usize = ENTRY_STATUS_MULTI_GET_BATCH_SIZE.get();
const KEY_LEN: usize = EntryStatusKey::serialized_length_fixed();

/// A read of the entries of some stages and statuses through one of the two
/// status indexes, a lane per stage and status
/// ([`crate::status_lanes`]), in the order of the index's time: newest first
/// or, backwards, oldest first. The lanes may admit more entries than the query
/// matches: the caller keeps its predicate.
pub struct LanesRead<K> {
    lanes: StatusLanes<K>,
    direction: ReadDirection,
    partition_keys: PartitionKeyFilter,
}

impl<K> LanesRead<K> {
    /// Restricts the read to entries under the partition keys `keys` admits,
    /// such as those of the queried entry or queue IDs: an index key of another
    /// partition key is skipped before its entry status is fetched.
    pub fn with_partition_keys(mut self, keys: PartitionKeyFilter) -> Self {
        self.partition_keys = keys;
        self
    }
}

/// Entries by stage and status, newest created first:
/// `entry_by_status_created/1`.
pub type StatusCreatedRead = LanesRead<EntryByStatusCreatedKey>;
/// Entries by stage and status, newest transitioned first:
/// `entry_by_status_transitioned/1`.
pub type StatusTransitionedRead = LanesRead<EntryByStatusTransitionedKey>;

impl StatusCreatedRead {
    pub fn prepare(
        selection: &Selection<StatusCreatedIndex>,
        direction: ReadDirection,
    ) -> Result<Self> {
        let lanes = StatusLanes::new(
            EntryByStatusCreatedKey::entry_lanes(selection),
            |stages, statuses| {
                EntryByStatusCreatedKey::prepare_entry_lanes(selection, stages, statuses)
            },
        )?;
        Ok(Self {
            lanes,
            direction,
            partition_keys: PartitionKeyFilter::ALL,
        })
    }
}

impl StatusTransitionedRead {
    pub fn prepare(
        selection: &Selection<StatusTransitionedIndex>,
        direction: ReadDirection,
    ) -> Result<Self> {
        let lanes = StatusLanes::new(
            EntryByStatusTransitionedKey::entry_lanes(selection),
            |stages, statuses| {
                EntryByStatusTransitionedKey::prepare_entry_lanes(selection, stages, statuses)
            },
        )?;
        Ok(Self {
            lanes,
            direction,
            partition_keys: PartitionKeyFilter::ALL,
        })
    }
}

/// Why a read of `I` on `snapshot` can't go through it, if it can't: the index
/// isn't Complete on it.
fn unusable<I: EntryIndex>(snapshot: &ReadSnapshot) -> Result<Option<ScanReason>> {
    Ok(snapshot
        .complete_index(I::DEFINITION, IndexKind::Secondary)
        .is_err()
        .then_some(ScanReason::NotComplete))
}

/// An index key's reference to an entry status, with what the status must
/// still say: its stage and status, the time the index orders by and the
/// incarnation.
pub(crate) struct Reference {
    id: CanonicalEntryId,
    stage: Stage,
    status: Status,
    at: UniqueTimestamp,
}

/// How a read that holds nothing ended.
#[derive(Clone, Copy)]
enum Ended {
    Done,
    Failed,
}

/// Merges the index's lanes, collects references into a window, fetches the
/// window's entry statuses with one multi-get and hands out the ones that are
/// still the keys', in index order.
pub(super) struct IndexedCursor<I: EntryIndex> {
    /// `None` once the read ended: it holds nothing then.
    runs: Option<Lanes<I>>,
    runs_done: bool,
    /// The most references a window holds.
    window_size: usize,
    window: Vec<Reference>,
    /// Next window position to check, once the window is fetched.
    next: Option<usize>,
    key_buf: Vec<u8>,
    results: Option<OwnedPinnedBatch<RocksReadSnapshot<PhysicalPartition>>>,
    results_bytes: usize,
    /// Window position the cursor is on.
    current: Option<usize>,
    held: Held,
    fetched: Box<dyn Fn(usize, u64) + Send>,
    skipped: Box<dyn Fn(u64) + Send>,
    /// Index keys examined that `fetched` has learned of.
    reported: u64,
    ended: Option<Ended>,
    /// Whether the read hands out its keys alone, fetching no status.
    keys_only: bool,
    /// The partition keys whose index keys the read uses; others are skipped.
    partition_keys: PartitionKeyFilter,
    /// Last: the runs and the fetched window are dropped before the snapshot.
    snapshot: Option<ReadSnapshot>,
}

impl<I: EntryIndex> Drop for IndexedCursor<I> {
    /// A read dropped part-way, say past a query's LIMIT, still tells what it
    /// examined.
    fn drop(&mut self) {
        self.report(0);
    }
}

impl IndexedCursor<EntryByStatusCreated> {
    /// A read of the statuses the lanes' keys name or, `keys_only`, of the keys
    /// alone ([`KeySteps`]).
    pub(super) fn status_created(
        snapshot: &ReadSnapshot,
        read: &StatusCreatedRead,
        execution: &EntryIndexExecution,
        window: NonZeroUsize,
        keys_only: bool,
    ) -> Result<Option<Self>> {
        Self::open(snapshot, read, execution, window, keys_only)
    }
}

impl IndexedCursor<EntryByStatusTransitioned> {
    /// As [`IndexedCursor::status_created`].
    pub(super) fn status_transitioned(
        snapshot: &ReadSnapshot,
        read: &StatusTransitionedRead,
        execution: &EntryIndexExecution,
        window: NonZeroUsize,
        keys_only: bool,
    ) -> Result<Option<Self>> {
        Self::open(snapshot, read, execution, window, keys_only)
    }
}

impl<I: EntryIndex> IndexedCursor<I> {
    /// The cursor, or `None` if the read scans instead. Nothing is held then.
    /// Either way, `execution` learns the path before this returns.
    /// `window` bounds a window's references, at most [`WINDOW`]; a keys-only
    /// read holds one.
    fn open(
        snapshot: &ReadSnapshot,
        read: &LanesRead<I::Key>,
        execution: &EntryIndexExecution,
        window: NonZeroUsize,
        keys_only: bool,
    ) -> Result<Option<Self>> {
        let window_size = if keys_only {
            1
        } else {
            window.get().min(WINDOW)
        };
        let Execution {
            memory,
            chosen,
            fetched,
            skipped,
        } = execution.take()?;
        let memory = SharedMemory(Arc::new(Mutex::new(memory)));
        // Discovery's work counts even if it fails: the error stays the read's.
        let mut discovered = 0;
        let opened = match Self::try_open(snapshot, read, memory, window_size, &mut discovered) {
            Ok(opened) => opened,
            Err(error) => {
                if discovered > 0 {
                    fetched(0, discovered);
                }
                return Err(error);
            }
        };
        let cursor = match opened {
            Ok((runs, held)) => Ok(Self {
                runs: Some(runs),
                runs_done: false,
                window_size,
                window: Vec::with_capacity(window_size),
                next: None,
                key_buf: Vec::with_capacity(window_size * KEY_LEN),
                results: None,
                results_bytes: 0,
                current: None,
                held,
                fetched,
                skipped,
                reported: 0,
                ended: None,
                keys_only,
                partition_keys: read.partition_keys.clone(),
                snapshot: Some(snapshot.clone()),
            }),
            Err((reason, examined)) => {
                if examined > 0 {
                    fetched(0, examined);
                }
                Err(reason)
            }
        };
        chosen(match &cursor {
            Ok(cursor) => EntryIndexPath::Index {
                runs: cursor.runs.as_ref().map_or(0, Lanes::runs),
            },
            Err(reason) => EntryIndexPath::Scan(*reason),
        });
        Ok(cursor.ok())
    }

    #[allow(clippy::type_complexity)]
    fn try_open(
        snapshot: &ReadSnapshot,
        read: &LanesRead<I::Key>,
        memory: SharedMemory,
        window_size: usize,
        discovered: &mut u64,
    ) -> Result<std::result::Result<(Lanes<I>, Held), (ScanReason, u64)>> {
        if let Some(reason) = unusable::<I>(snapshot)? {
            return Ok(Err((reason, 0)));
        }
        let mut held = Held {
            memory: memory.clone(),
            bytes: 0,
        };
        held.grow(
            read.lanes.retained_bytes()
                + read.partition_keys.heap_bytes()
                + window_size * std::mem::size_of::<Reference>()
                + window_size * KEY_LEN,
        )?;
        Ok(
            match Lanes::open(
                snapshot,
                &read.lanes,
                read.direction,
                LIMITS,
                (&memory, &mut held),
                discovered,
            )? {
                Some(lanes) => Ok((lanes, held)),
                None => Err((ScanReason::TooManyRuns, *discovered)),
            },
        )
    }

    /// Tells `fetched` of a multi-get of `statuses`, if any, and of the index
    /// keys examined since it last learned.
    fn report(&mut self, statuses: usize) {
        let examined = self.runs.as_ref().map_or(self.reported, Lanes::examined);
        let new = examined.saturating_sub(self.reported);
        if statuses > 0 || new > 0 {
            (self.fetched)(statuses, new);
        }
        self.reported = examined;
    }

    /// The next step. A step that fails, or finds the read done, releases
    /// everything the read holds, its snapshot included.
    pub(super) fn advance(&mut self) -> Result<CursorStep> {
        let step = self.step();
        match step {
            Err(_) => self.release(Ended::Failed),
            Ok(CursorStep::Done) => self.release(Ended::Done),
            Ok(_) => {}
        }
        step
    }

    fn release(&mut self, ended: Ended) {
        self.report(0);
        self.runs = None;
        self.results = None;
        self.results_bytes = 0;
        self.window = Vec::new();
        self.key_buf = Vec::new();
        self.next = None;
        self.current = None;
        let held = self.held.bytes;
        self.held.shrink(held);
        self.snapshot = None;
        self.ended = Some(ended);
    }

    fn step(&mut self) -> Result<CursorStep> {
        match self.ended {
            Some(Ended::Done) => return Ok(CursorStep::Done),
            Some(Ended::Failed) => {
                return Err(StorageError::Generic(anyhow::anyhow!(
                    "the entry index read failed earlier"
                )));
            }
            None => {}
        }
        let (Some(runs), Some(snapshot)) = (&mut self.runs, &self.snapshot) else {
            return Err(StorageError::DataIntegrityError);
        };
        snapshot.check_active()?;
        self.current = None;
        if self.keys_only {
            // Each key is a row: there is no status to fetch.
            if self.runs_done {
                return Ok(CursorStep::Done);
            }
            return Ok(match runs.advance()? {
                CursorStep::Done => {
                    self.runs_done = true;
                    CursorStep::Done
                }
                CursorStep::Skipped => CursorStep::Skipped,
                CursorStep::Record => {
                    let reference = I::reference(runs.current()?.0)?;
                    if !self.partition_keys.admits(reference.id.partition_key()) {
                        return Ok(CursorStep::Skipped);
                    }
                    self.window.clear();
                    self.window.push(reference);
                    self.current = Some(0);
                    CursorStep::Record
                }
            });
        }
        if let Some(next) = self.next {
            if next < self.window.len() {
                self.next = Some(next + 1);
                return self.check(next);
            }
            self.next = None;
            self.window.clear();
            self.results = None;
            self.held.shrink(std::mem::take(&mut self.results_bytes));
        }
        if self.window.len() == self.window_size || (self.runs_done && !self.window.is_empty()) {
            self.fetch()?;
            self.next = Some(0);
            return Ok(CursorStep::Skipped);
        }
        if self.runs_done {
            return Ok(CursorStep::Done);
        }
        match runs.advance()? {
            CursorStep::Done => self.runs_done = true,
            CursorStep::Skipped => {}
            CursorStep::Record => {
                let reference = I::reference(runs.current()?.0)?;
                if self.partition_keys.admits(reference.id.partition_key()) {
                    self.window.push(reference);
                }
            }
        }
        Ok(CursorStep::Skipped)
    }

    /// Requests the window's entry statuses with one multi-get, in index order,
    /// and charges them before the read keeps them.
    fn fetch(&mut self) -> Result<()> {
        self.key_buf.clear();
        for reference in &self.window {
            EntryStatusKey::from(reference.id.to_base_entry_id()).serialize_to(&mut self.key_buf);
        }
        let (keys, remainder) = self.key_buf.as_chunks::<KEY_LEN>();
        debug_assert!(remainder.is_empty());
        let snapshot = self
            .snapshot
            .as_ref()
            .ok_or(StorageError::DataIntegrityError)?;
        let reader = snapshot.storage_snapshot().reader();
        let fetched = reader
            .multi_get_pinned(KeyKind::VQueueEntryStatus, keys, false)
            .map_err(|error| StorageError::Generic(error.into()))?
            .into_owned();
        self.report(self.window.len());
        let mut bytes = 0;
        for slot in 0..self.window.len() {
            let value = fetched
                .get(slot)
                .map_err(|error| StorageError::Generic(error.into()))?;
            bytes += value.map_or(0, <[u8]>::len);
        }
        self.held.grow(bytes)?;
        self.results = Some(fetched);
        self.results_bytes = bytes;
        Ok(())
    }

    /// Positions the read on `slot` if its entry's status is still its key's;
    /// otherwise skips and counts it.
    fn check(&mut self, slot: usize) -> Result<CursorStep> {
        let reference = &self.window[slot];
        let Some(mut value) = self.value(slot)? else {
            (self.skipped)(1);
            return Ok(CursorStep::Skipped);
        };
        let header = RawStatusHeaderRef::decode_stored(&mut value)?;
        if header.stage == reference.stage
            && header.status == reference.status
            && I::time(&header) == reference.at
            && header.seq == reference.id.seq()
        {
            self.current = Some(slot);
            Ok(CursorStep::Record)
        } else {
            (self.skipped)(1);
            Ok(CursorStep::Skipped)
        }
    }

    fn value(&self, slot: usize) -> Result<Option<&[u8]>> {
        self.results
            .as_ref()
            .ok_or(StorageError::DataIntegrityError)?
            .get(slot)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    /// The key a keys-only read's step positioned on.
    fn key(&self) -> Result<&Reference> {
        self.snapshot
            .as_ref()
            .ok_or(StorageError::DataIntegrityError)?
            .check_active()?;
        let slot = self.current.ok_or(StorageError::DataIntegrityError)?;
        self.window
            .get(slot)
            .ok_or(StorageError::DataIntegrityError)
    }

    /// The entry status the step positioned on, with its base entry ID.
    pub(super) fn current(&self) -> Result<(BaseEntryId, &[u8])> {
        self.snapshot
            .as_ref()
            .ok_or(StorageError::DataIntegrityError)?
            .check_active()?;
        let slot = self.current.ok_or(StorageError::DataIntegrityError)?;
        let value = self.value(slot)?.ok_or(StorageError::DataIntegrityError)?;
        Ok((self.window[slot].id.to_base_entry_id(), value))
    }
}

/// The steps of a keys-only read of a status index.
pub(super) trait KeySteps: Send {
    fn advance(&mut self) -> Result<CursorStep>;
    fn current(&self) -> Result<EntryKeyRow>;
}

impl KeySteps for IndexedCursor<EntryByStatusCreated> {
    fn advance(&mut self) -> Result<CursorStep> {
        IndexedCursor::advance(self)
    }

    fn current(&self) -> Result<EntryKeyRow> {
        let key = self.key()?;
        Ok(EntryKeyRow {
            id: key.id,
            stage: key.stage,
            status: key.status,
            created_at: Some(key.at),
            transitioned_at: None,
        })
    }
}

impl KeySteps for IndexedCursor<EntryByStatusTransitioned> {
    fn advance(&mut self) -> Result<CursorStep> {
        IndexedCursor::advance(self)
    }

    fn current(&self) -> Result<EntryKeyRow> {
        let key = self.key()?;
        Ok(EntryKeyRow {
            id: key.id,
            stage: key.stage,
            status: key.status,
            created_at: None,
            transitioned_at: Some(key.at),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use bilrost::{Message, OwnedMessage};
    use futures::FutureExt;
    use tokio_util::sync::CancellationToken;

    use restate_clock::time::MillisSinceEpoch;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::stats::EntryStatistics;
    use restate_storage_api::vqueue_table::{
        EntryContext, EntryKey, EntryMetadata, EntryStateRef, RawStatusHeader, StoredEntryRef,
        WriteVQueueTable,
    };
    use restate_storage_query_api::scan::CursorScan;
    use restate_storage_query_api::scan::ReadCursor;
    use restate_storage_query_api::selection::ValuePredicate;
    use restate_storage_query_api::tables::index::EntryStatusByStatusTransitionedClause;
    use restate_types::config::Configuration;
    use restate_types::identifiers::PartitionId;
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::{
        EffectiveStatus, EntryId, EntryKind, EntryTargetRef, Seq, VQueueId,
    };

    use crate::invocation_status_table::entry_index_read::counting::{Charge, Counting};
    use crate::keys::selection::KeySelection;
    use crate::vqueue_table::{EntryKeyRecords, EntryKeyScan, EntryStatusKeys, EntryStatusScan};
    use crate::{PartitionStore, PartitionStoreManager};

    use super::*;

    /// A finished entry as read: its ID, status and transition millisecond.
    type Row = (BaseEntryId, Status, u64);

    /// How far a read goes: to its end, or until it has handed out some
    /// records, as past a query's LIMIT.
    #[derive(Clone, Copy)]
    enum Stop {
        End,
        After(usize),
    }

    /// A read of `selection` through `entry_by_status_transitioned/1`, on a snapshot of its
    /// own whose last outside reference is dropped once the read opens, and
    /// what it showed at each point it promises something. Everything is
    /// collected before anything is asserted, so an error can't skip a
    /// cleanup check.
    #[derive(Debug, Default)]
    struct Run {
        /// The open's error, if it failed.
        open_error: Option<String>,
        path: Option<EntryIndexPath>,
        rows: Vec<Row>,
        /// Each row's stage and full creation time.
        created: Vec<(Stage, UniqueTimestamp)>,
        skipped: u64,
        /// The last step: none if stopped before it, `Ok` for Done.
        ended: Option<std::result::Result<(), String>>,
        /// Two more steps after the last, and whether `current()` failed then.
        again: Vec<std::result::Result<CursorStep, String>>,
        current_fails: bool,
        /// `(statuses, keys)` reported up to the last step, and in all.
        reported_at_end: Vec<(usize, u64)>,
        reported: Vec<(usize, u64)>,
        /// The charge, and whether the snapshot completed, after the last step
        /// with the cursor still alive, and once it's dropped.
        held_at_end: usize,
        closed_at_end: bool,
        held: usize,
        closed: bool,
        refused: Option<usize>,
        /// The largest charge until the read opened.
        opened_peak: usize,
    }

    impl Run {
        fn keys(reported: &[(usize, u64)]) -> u64 {
            reported.iter().map(|(_, keys)| keys).sum()
        }

        fn statuses(&self) -> usize {
            self.reported.iter().map(|(statuses, _)| statuses).sum()
        }

        /// The read ended with an error, released everything at once, and
        /// fails the same way afterwards, without reporting more.
        fn failed_and_released(&self, error: &str) {
            assert!(
                matches!(&self.ended, Some(Err(failed)) if failed.contains(error)),
                "{self:#?}"
            );
            assert_eq!(
                (self.held_at_end, self.closed_at_end),
                (0, true),
                "{self:#?}"
            );
            assert!(
                self.again.iter().all(|step| matches!(step, Err(failed)
                    if failed.contains("failed earlier"))),
                "{self:#?}"
            );
            assert!(self.current_fails);
            assert_eq!(self.reported, self.reported_at_end);
            assert_eq!((self.held, self.closed), (0, true));
        }
    }

    /// A read of `selection` through `entry_by_status_transitioned/1`:
    /// [`run_scan`].
    fn run(
        store: &PartitionStore,
        selection: Selection<StatusTransitionedIndex>,
        charge: Charge,
        stop: Stop,
    ) -> Run {
        run_in(store, selection, ReadDirection::Forward, charge, stop)
    }

    /// [`run`] in `direction`.
    fn run_in(
        store: &PartitionStore,
        selection: Selection<StatusTransitionedIndex>,
        direction: ReadDirection,
        charge: Charge,
        stop: Stop,
    ) -> Run {
        run_scan(
            store,
            |execution| EntryStatusScan::StatusTransitioned {
                read: Arc::new(StatusTransitionedRead::prepare(&selection, direction).unwrap()),
                fallback: scan_all(),
                execution,
            },
            charge,
            stop,
        )
    }

    /// The Finished entries: the index's Finished lanes.
    fn finished() -> Selection<StatusTransitionedIndex> {
        Selection::All.and(EntryStatusByStatusTransitionedClause::Stage(
            ValuePredicate::Equal(Stage::Finished),
        ))
    }

    fn scan_all() -> Arc<KeySelection<EntryStatusKey>> {
        Arc::new(EntryStatusKeys::prepare(&Selection::All, Some(KeyRange::FULL)).unwrap())
    }

    fn run_scan(
        store: &PartitionStore,
        scan: impl FnOnce(Arc<EntryIndexExecution>) -> EntryStatusScan,
        charge: Charge,
        stop: Stop,
    ) -> Run {
        let memory = Counting::with(charge);
        let (chosen, path) = std::sync::mpsc::channel();
        let reported = Arc::new(Mutex::new(Vec::new()));
        let skipped = Arc::new(AtomicU64::new(0));
        let execution = EntryIndexExecution::new(
            Box::new(memory.clone()),
            move |taken| {
                let _ = chosen.send(taken);
            },
            {
                let reported = Arc::clone(&reported);
                move |statuses, keys| reported.lock().unwrap().push((statuses, keys))
            },
        )
        .with_skipped({
            let skipped = Arc::clone(&skipped);
            move |references| {
                skipped.fetch_add(references, Ordering::Relaxed);
            }
        });
        let (own, closed) = store.query_read(CancellationToken::new()).unwrap();
        let mut closed = Box::pin(closed);
        let mut completed = false;
        let mut closed_now = move || {
            completed = completed || closed.as_mut().now_or_never().is_some();
            completed
        };
        let opened = own.entry_status_cursor(scan(Arc::new(execution)));
        drop(own);
        let mut run = Run {
            path: path.try_recv().ok(),
            opened_peak: memory.peak(),
            ..Default::default()
        };
        match opened {
            Err(error) => run.open_error = Some(error.to_string()),
            Ok(mut cursor) => {
                loop {
                    match cursor.advance() {
                        Ok(CursorStep::Record) => match cursor.current() {
                            Ok((id, header)) => {
                                run.created.push((header.stage, header.stats.created_at));
                                run.rows.push((
                                    id,
                                    header.status,
                                    header.stats.transitioned_at.to_unix_millis().as_u64(),
                                ));
                            }
                            Err(error) => {
                                run.ended = Some(Err(error.to_string()));
                                break;
                            }
                        },
                        Ok(CursorStep::Skipped) => {}
                        Ok(CursorStep::Done) => {
                            run.ended = Some(Ok(()));
                            break;
                        }
                        Err(error) => {
                            run.ended = Some(Err(error.to_string()));
                            break;
                        }
                    }
                    if matches!(stop, Stop::After(records) if run.rows.len() == records) {
                        break;
                    }
                }
                run.held_at_end = memory.now();
                run.closed_at_end = closed_now();
                run.reported_at_end = reported.lock().unwrap().clone();
                if run.ended.is_some() {
                    run.again = (0..2)
                        .map(|_| cursor.advance().map_err(|error| error.to_string()))
                        .collect();
                    run.current_fails = cursor.current().is_err();
                }
                drop(cursor);
            }
        }
        run.reported = reported.lock().unwrap().clone();
        run.skipped = skipped.load(Ordering::Relaxed);
        run.held = memory.now();
        run.closed = closed_now();
        run.refused = memory.refused();
        run
    }

    fn key_of(id: &BaseEntryId) -> Vec<u8> {
        let mut key = Vec::new();
        EntryStatusKey::from(*id).serialize_to(&mut key);
        key
    }

    /// The stored status of `id`.
    fn stored(store: &PartitionStore, id: &BaseEntryId) -> Vec<u8> {
        let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
        snapshot
            .storage_snapshot()
            .reader()
            .get(KeyKind::VQueueEntryStatus, &key_of(id))
            .unwrap()
            .unwrap()
            .to_vec()
    }

    /// A stored status with its header edited, without its index keys.
    fn rewrite(value: &[u8], edit: &dyn Fn(&mut RawStatusHeader)) -> Vec<u8> {
        let mut rest = value;
        let mut header = RawStatusHeader::decode_length_delimited(&mut rest).unwrap();
        edit(&mut header);
        let mut bytes = header.encode_length_delimited_to_vec();
        bytes.extend_from_slice(rest);
        bytes
    }

    /// What a read of `selection` to its end returns, newest finished first,
    /// with the path it took and the references it skipped. However it ends,
    /// it holds nothing once dropped, and its snapshot completes.
    fn read(
        store: &PartitionStore,
        selection: Selection<StatusTransitionedIndex>,
    ) -> std::result::Result<(Vec<Row>, Option<EntryIndexPath>, u64), String> {
        let run = run(store, selection, Charge::default(), Stop::End);
        assert_eq!((run.held, run.closed), (0, true), "{run:#?}");
        match (run.open_error, run.ended) {
            (Some(error), _) | (None, Some(Err(error))) => Err(error),
            _ => Ok((run.rows, run.path, run.skipped)),
        }
    }

    /// A read of the Finished lanes returns Finished entries newest finished
    /// first, narrowed by status, and only those whose current status is still their key's: a
    /// stale, missing or reincarnated reference is skipped and counted, a
    /// malformed status fails the read. Without a Complete index, it scans.
    ///
    /// However it ends, at its end, on an error or a refused charge, or dropped
    /// part-way, it releases every charge and its snapshot, and tells its work
    /// once. A refusal fails the read: it never scans instead.
    #[restate_core::test]
    async fn lane_reads_check_each_status_and_skip_stale_references() {
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
        let qid = VQueueId::custom(7, "q");
        let mut ids = Vec::new();
        let mut tx = store.transaction();
        for n in 0..14u64 {
            let (stage, status) = match n % 7 {
                0..=2 => (Stage::Finished, Status::Succeeded),
                3 | 4 => (Stage::Finished, Status::Failed),
                5 => (Stage::Running, Status::Started),
                _ => (Stage::Inbox, Status::New),
            };
            let created =
                UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(BASE + n));
            let entry = EntryId::new(EntryKind::Invocation, [n as u8 + 1; 16]);
            let key = EntryKey::new(false, created.to_unix_millis(), n + 1, entry);
            let mut stats = EntryStatistics::new(created, key.run_at());
            stats.transitioned_at = UniqueTimestamp::from_unix_millis_unchecked(
                MillisSinceEpoch::new(BASE + 100 + n * 10),
            );
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &EntryTargetRef::Service {
                        scope: None,
                        service: ["A", "B"][(n % 2) as usize],
                        handler: "h",
                    },
                },
                EntryStateRef {
                    stage,
                    status,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &stats,
                },
            );
            if stage == Stage::Finished {
                ids.push((
                    entry.to_base_id(qid.partition_key()),
                    status,
                    BASE + 100 + n * 10,
                ));
            }
        }
        tx.commit().await.unwrap();
        drop(tx);
        ids.reverse();

        // Newest finished first, over the (Finished, status) lanes, a run per
        // service in each. At its last
        // step the read releases everything, its snapshot included, with the
        // cursor still alive, and stays done. Its one multi-get and every key
        // it examined are told once.
        let all = run(&store, finished(), Charge::default(), Stop::End);
        assert_eq!(all.rows, ids);
        assert_eq!(
            (all.path, all.skipped, all.ended.clone()),
            (Some(EntryIndexPath::Index { runs: 4 }), 0, Some(Ok(())))
        );
        assert_eq!((all.held_at_end, all.closed_at_end), (0, true));
        assert!(all.again.iter().all(|step| *step == Ok(CursorStep::Done)));
        let examined = Run::keys(&all.reported);
        assert!(examined >= ids.len() as u64);
        assert_eq!(all.statuses(), ids.len());
        assert_eq!(all.reported, all.reported_at_end);
        assert_eq!((all.held, all.closed), (0, true));
        // Backwards, the exact reverse, oldest finished first, and released at
        // its end the same way.
        let oldest = run_in(
            &store,
            finished(),
            ReadDirection::Backward,
            Charge::default(),
            Stop::End,
        );
        assert_eq!(oldest.rows, ids.iter().rev().cloned().collect::<Vec<_>>());
        assert_eq!(
            (oldest.path, oldest.ended.clone(), oldest.held_at_end),
            (Some(EntryIndexPath::Index { runs: 4 }), Some(Ok(())), 0)
        );
        assert_eq!(oldest.statuses(), ids.len());
        // Stopped after one record, as past a LIMIT, it holds its window until
        // dropped, then releases everything and tells its work once. Here the
        // merge read every key ahead of the first record.
        let part = run(&store, finished(), Charge::default(), Stop::After(1));
        assert_eq!((part.rows.len(), part.ended.clone()), (1, None));
        assert!(part.held_at_end > 0 && !part.closed_at_end);
        assert_eq!((part.held, part.closed), (0, true));
        assert_eq!(
            (part.statuses(), Run::keys(&part.reported)),
            (ids.len(), examined)
        );
        // A status narrows the lanes.
        let failed = finished().and(EntryStatusByStatusTransitionedClause::Status(
            ValuePredicate::Equal(EffectiveStatus::new(Stage::Finished, Status::Failed)),
        ));
        let (rows, path, _) = read(&store, failed).unwrap();
        let expected: Vec<_> = ids
            .iter()
            .filter(|row| row.1 == Status::Failed)
            .cloned()
            .collect();
        assert_eq!(rows, expected);
        assert_eq!(path, Some(EntryIndexPath::Index { runs: 2 }));
        // Partition keys are checked on each index key before its status is
        // fetched: every entry here is under partition key 7, so a read of
        // another key walks the lanes but fetches no status.
        let restricted = |keys: PartitionKeyFilter| {
            run_scan(
                &store,
                |execution| EntryStatusScan::StatusTransitioned {
                    read: Arc::new(
                        StatusTransitionedRead::prepare(&finished(), ReadDirection::Forward)
                            .unwrap()
                            .with_partition_keys(keys),
                    ),
                    fallback: scan_all(),
                    execution,
                },
                Charge::default(),
                Stop::End,
            )
        };
        assert_eq!(
            restricted(PartitionKeyFilter::new([KeyRange::new(7, 7)])).rows,
            ids
        );
        let other = restricted(PartitionKeyFilter::new([KeyRange::new(8, 8)]));
        assert_eq!((other.rows.len(), other.statuses()), (0, 0));
        assert!(Run::keys(&other.reported) >= ids.len() as u64);

        // Edit three statuses without their index keys: one now says another
        // status, one is gone, one is another incarnation.
        // A refused charge fails the read with the refusal, never a scan in
        // its place; every charge is released and the snapshot completes.
        // Refused for the read's own state, as it opens: no work, no path.
        let fixed = StatusTransitionedRead::prepare(&finished(), ReadDirection::Forward)
            .unwrap()
            .lanes
            .retained_bytes()
            + WINDOW * std::mem::size_of::<Reference>()
            + WINDOW * KEY_LEN;
        let refused = run(
            &store,
            finished(),
            Charge {
                limit: Some(fixed - 1),
                ..Default::default()
            },
            Stop::End,
        );
        assert!(
            matches!(&refused.open_error, Some(error) if error.contains("refused")),
            "{refused:#?}"
        );
        assert_eq!(
            (refused.refused, refused.path, refused.reported.len()),
            (Some(fixed), None, 0)
        );
        assert_eq!((refused.held, refused.closed), (0, true));
        // Refused at discovery's largest charge, once it examined keys: told
        // exactly once.
        let refused = run(
            &store,
            finished(),
            Charge {
                limit: Some(all.opened_peak - 1),
                ..Default::default()
            },
            Stop::End,
        );
        assert!(
            matches!(&refused.open_error, Some(error) if error.contains("refused")),
            "{refused:#?}"
        );
        assert!(
            matches!(refused.reported.as_slice(), [(0, keys)] if *keys > 0),
            "{refused:#?}"
        );
        assert_eq!(
            (refused.path, refused.held, refused.closed),
            (None, 0, true)
        );
        // Refused for the fetched statuses, measured directly, after their
        // multi-get: the read fails at once, its multi-get and keys told once.
        let window = ids
            .iter()
            .map(|(id, ..)| stored(&store, id).len())
            .sum::<usize>();
        let refused = run(
            &store,
            finished(),
            Charge {
                refuse: Some(window),
                ..Default::default()
            },
            Stop::End,
        );
        assert_eq!(
            (refused.path, refused.refused),
            (Some(EntryIndexPath::Index { runs: 4 }), Some(window))
        );
        refused.failed_and_released("refused");
        assert_eq!(
            (refused.statuses(), Run::keys(&refused.reported)),
            (ids.len(), examined)
        );

        let (relabelled, deleted, reincarnated) = (ids[0].0, ids[1].0, ids[2].0);
        let relabel = rewrite(&stored(&store, &relabelled), &|header| {
            header.status = Status::Killed
        });
        let reincarnate = rewrite(&stored(&store, &reincarnated), &|header| {
            header.seq = Seq::new(999)
        });
        let mut tx = store.transaction();
        tx.raw_put_cf(KeyKind::VQueueEntryStatus, key_of(&relabelled), relabel);
        tx.raw_delete_cf(KeyKind::VQueueEntryStatus, key_of(&deleted));
        tx.raw_put_cf(
            KeyKind::VQueueEntryStatus,
            key_of(&reincarnated),
            reincarnate,
        );
        tx.commit().await.unwrap();
        drop(tx);
        let (rows, _, skipped) = read(&store, finished()).unwrap();
        assert_eq!(rows, ids[3..]);
        assert_eq!(skipped, 3);

        // A malformed status fails the step that reaches it, which releases
        // everything at once; the read fails the same way afterwards.
        let mut tx = store.transaction();
        tx.raw_put_cf(
            KeyKind::VQueueEntryStatus,
            key_of(&ids[3].0),
            b"\xff\xff\xff",
        );
        tx.commit().await.unwrap();
        drop(tx);
        run(&store, finished(), Charge::default(), Stop::End).failed_and_released("cannot decode");

        // Not Complete: a partition that doesn't maintain the index scans.
        let mut plain = manager
            .open(&Partition::new(PartitionId::from(1), KeyRange::FULL), None)
            .await
            .unwrap();
        let mut tx = plain.transaction();
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &EntryTargetRef::Service {
                    scope: None,
                    service: "A",
                    handler: "h",
                },
            },
            EntryStateRef {
                stage: Stage::Finished,
                status: Status::Succeeded,
                entry_key: &EntryKey::new(
                    false,
                    MillisSinceEpoch::new(BASE),
                    1,
                    EntryId::new(EntryKind::Invocation, [99; 16]),
                ),
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(
                    UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(BASE)),
                    MillisSinceEpoch::new(BASE).into(),
                ),
            },
        );
        tx.commit().await.unwrap();
        drop(tx);
        let (rows, path, _) = read(&plain, finished()).unwrap();
        assert_eq!(path, Some(EntryIndexPath::Scan(ScanReason::NotComplete)));
        assert_eq!(rows.len(), 1);
    }

    #[restate_core::test]
    async fn keys_only_reads_fetch_no_status() {
        use restate_storage_api::vqueue_table::ReadVQueueTable;
        use restate_storage_query_api::tables::index::EntryStatusByStatusCreatedClause;
        use restate_types::vqueues::HandlerRef;

        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut tracked = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let mut config = Configuration::default();
        config.common.experimental.set_track_entry_indexes(true);
        tracked
            .verify_and_run_migrations(CancellationToken::new(), &config)
            .await
            .unwrap();
        let mut plain = manager
            .open(&Partition::new(PartitionId::from(1), KeyRange::FULL), None)
            .await
            .unwrap();

        const BASE: u64 = 1_744_010_000_000;
        const HOUR: u64 = 3_600_000;
        let at = |ms| UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(ms));
        let qid = VQueueId::custom(7, "q");
        let kinds = [
            (Stage::Finished, Status::Succeeded),
            (Stage::Finished, Status::Killed),
            (Stage::Running, Status::Started),
        ];
        // One entry an hour, each a stage and status in turn; every fourth a
        // state mutation. (id, stage, status, created, transitioned, seq)
        let mut entries: Vec<_> = (0..12u64)
            .map(|n| {
                let (stage, status) = kinds[(n % 3) as usize];
                let kind = if n % 4 == 3 {
                    EntryKind::StateMutation
                } else {
                    EntryKind::Invocation
                };
                let id = EntryId::new(kind, [n as u8 + 1; 16]);
                (id, stage, status, at(BASE + n), at(BASE + n * HOUR + 7), 1)
            })
            .collect();
        let context = |id: &EntryId| {
            if id.kind() == EntryKind::StateMutation {
                EntryTargetRef::VirtualObject {
                    scope: None,
                    service: "A",
                    key: "k",
                    handler: HandlerRef::StateMutation,
                }
            } else {
                EntryTargetRef::Service {
                    scope: None,
                    service: "A",
                    handler: "h",
                }
            }
        };
        type Entry = (
            EntryId,
            Stage,
            Status,
            UniqueTimestamp,
            UniqueTimestamp,
            u64,
        );
        let state = |&(id, _, _, created, transitioned, seq): &Entry| {
            let key = EntryKey::new(false, created.to_unix_millis(), seq, id);
            let mut stats = EntryStatistics::new(created, key.run_at());
            stats.transitioned_at = transitioned;
            (key, stats)
        };
        for store in [&mut tracked, &mut plain] {
            let mut tx = store.transaction();
            for entry in &entries {
                let (key, stats) = state(entry);
                tx.create_vqueue_entry_status(
                    &EntryContext {
                        qid: &qid,
                        target: &context(&entry.0),
                    },
                    EntryStateRef {
                        stage: entry.1,
                        status: entry.2,
                        entry_key: &key,
                        metadata: &EntryMetadata::default(),
                        stats: &stats,
                    },
                );
            }
            tx.commit().await.unwrap();
        }
        // A finished state mutation comes back as its second incarnation: its
        // keys move with it.
        let again = 7;
        entries[again].5 = 2;
        for store in [&mut tracked, &mut plain] {
            let mut tx = store.transaction();
            let base = entries[again].0.to_base_id(qid.partition_key());
            let header = tx.get_vqueue_entry_status(&base).await.unwrap().unwrap();
            let (key, stats) = state(&entries[again]);
            tx.update_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &context(&entries[again].0),
                },
                StoredEntryRef::from_header(&header),
                EntryStateRef {
                    stage: entries[again].1,
                    status: entries[again].2,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &stats,
                },
            );
            tx.commit().await.unwrap();
        }
        assert_eq!(entries[again].0.kind(), EntryKind::StateMutation);
        assert_eq!(entries[again].1, Stage::Finished);

        // Finished entries: through the creation index, all of them, newest
        // created first; through the transition index, those that transitioned
        // in the last six hours, newest transitioned first.
        let since = BASE + 6 * HOUR;
        let finished = |stage: &Stage| *stage == Stage::Finished;
        let by_created = Selection::All.and(EntryStatusByStatusCreatedClause::Stage(
            ValuePredicate::Equal(Stage::Finished),
        ));
        let by_transitioned = Selection::All
            .and(EntryStatusByStatusTransitionedClause::Stage(
                ValuePredicate::Equal(Stage::Finished),
            ))
            .and(EntryStatusByStatusTransitionedClause::TransitionedAt(
                ValuePredicate::Range {
                    lower: std::ops::Bound::Included(MillisSinceEpoch::new(since)),
                    upper: std::ops::Bound::Unbounded,
                },
            ));
        let created = |execution| EntryKeyScan::StatusCreated {
            read: Arc::new(
                StatusCreatedRead::prepare(&by_created, ReadDirection::Forward).unwrap(),
            ),
            fallback: scan_all(),
            execution,
        };
        let transitioned = |execution| EntryKeyScan::StatusTransitioned {
            read: Arc::new(
                StatusTransitionedRead::prepare(&by_transitioned, ReadDirection::Forward).unwrap(),
            ),
            fallback: scan_all(),
            execution,
        };
        // The rows read, up to `stop`, the path, what was reported and what
        // the read holds once dropped.
        let read = |store: &PartitionStore,
                    scan: &dyn Fn(Arc<EntryIndexExecution>) -> EntryKeyScan,
                    stop: Option<usize>| {
            let memory = Counting::default();
            let (chosen, path) = std::sync::mpsc::channel();
            let reported = Arc::new(Mutex::new((0, 0)));
            let execution = EntryIndexExecution::new(
                Box::new(memory.clone()),
                move |taken| {
                    let _ = chosen.send(taken);
                },
                {
                    let reported = Arc::clone(&reported);
                    move |statuses, keys| {
                        let mut reported = reported.lock().unwrap();
                        reported.0 += statuses;
                        reported.1 += keys;
                    }
                },
            );
            let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
            let mut cursor = <ReadSnapshot as CursorScan<EntryKeyRecords>>::scan_cursor(
                &snapshot,
                Arc::new(scan(Arc::new(execution))),
            )
            .unwrap();
            let mut rows = Vec::new();
            while stop.is_none_or(|stop| rows.len() < stop) {
                match cursor.advance().unwrap() {
                    CursorStep::Record => rows.push(cursor.current().unwrap()),
                    CursorStep::Skipped => {}
                    CursorStep::Done => break,
                }
            }
            drop(cursor);
            let reported = *reported.lock().unwrap();
            (rows, path.try_recv().ok(), reported, memory.now())
        };
        let row = |&(id, stage, status, created, transitioned, seq): &Entry, by_created: bool| {
            EntryKeyRow {
                id: id
                    .to_base_id(qid.partition_key())
                    .canonicalize(Seq::new(seq)),
                stage,
                status,
                created_at: by_created.then_some(created),
                transitioned_at: (!by_created).then_some(transitioned),
            }
        };

        let (rows, path, (statuses, keys), held) = read(&tracked, &created, None);
        let expected: Vec<_> = entries
            .iter()
            .rev()
            .filter(|entry| finished(&entry.1))
            .map(|entry| row(entry, true))
            .collect();
        assert_eq!(rows, expected);
        assert!(
            matches!(path, Some(EntryIndexPath::Index { .. })),
            "{path:?}"
        );
        // Keys only, discovery's included: no status.
        assert!(
            statuses == 0 && keys > 0 && held == 0,
            "{statuses} {keys} {held}"
        );

        let (rows, path, (statuses, keys), held) = read(&tracked, &transitioned, None);
        let expected: Vec<_> = entries
            .iter()
            .rev()
            .filter(|entry| finished(&entry.1) && entry.4.to_unix_millis().as_u64() >= since)
            .map(|entry| row(entry, false))
            .collect();
        assert_eq!(rows, expected);
        assert!(expected.contains(&row(&entries[again], false)));
        assert!(
            matches!(path, Some(EntryIndexPath::Index { runs: 2 })),
            "{path:?}"
        );
        assert!(
            statuses == 0 && keys > 0 && held == 0,
            "{statuses} {keys} {held}"
        );

        // Dropped after its first row, as past a LIMIT.
        let (rows, _, (statuses, keys), held) = read(&tracked, &transitioned, Some(1));
        assert_eq!(rows, expected[..1]);
        assert!(
            statuses == 0 && keys > 0 && held == 0,
            "{statuses} {keys} {held}"
        );

        let (rows, path, (statuses, _), held) = read(&plain, &transitioned, None);
        assert_eq!(path, Some(EntryIndexPath::Scan(ScanReason::NotComplete)));
        assert_eq!((rows.len(), statuses, held), (entries.len(), 0, 0));
        assert!(rows.iter().all(|row| {
            entries
                .iter()
                .any(|&(id, stage, status, created, transitioned, seq)| {
                    *row == EntryKeyRow {
                        id: id
                            .to_base_id(qid.partition_key())
                            .canonicalize(Seq::new(seq)),
                        stage,
                        status,
                        created_at: Some(created),
                        transitioned_at: Some(transitioned),
                    }
                })
        }));
    }
}
