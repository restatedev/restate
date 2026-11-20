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
//!
//! - `finished_entry_by_outcome/1` holds one key per entry whose stored stage
//!   is Finished, sorted by stored status, service and handler, then by the
//!   time the entry entered Finished, newest first ([`FinishedRead`]);
//! - `active_entry_by_stage/1` holds one key per entry whose stored stage is
//!   Inbox, Running, Suspended or Paused, sorted by stage, service and handler,
//!   then by creation time, newest first ([`ActiveRead`]).
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

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use restate_clock::UniqueTimestamp;
use restate_rocksdb::reads::{OwnedPinnedBatch, ReadSnapshot as RocksReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{RawStatusHeaderRef, Stage, Status};
use restate_storage_query_api::scan::{CursorStep, ReadDirection};
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::index::{
    ActiveEntryByStage as ActiveIndex, EntryStatusByStatusCreated as StatusCreatedIndex,
    EntryStatusByStatusTransitioned as StatusTransitionedIndex,
    FinishedEntryByOutcome as FinishedIndex,
};
use restate_types::identifiers::{BaseEntryId, CanonicalEntryId};

use crate::index::RegisteredIndex;
use crate::invocation_status_table::entry_index_read::{
    EntryIndexExecution, EntryIndexPath, Execution, Held, ScanReason, SharedMemory,
};
use crate::keys::selection::{KeySelection, OrderedKeySchema, PrepareKeySelection};
use crate::keys::{EncodeTableKey, KeyDecoder, KeyKind};
use crate::ordered_runs::{OrderedRuns, OrderedRunsDiscovery, OrderedRunsLimits};
use crate::partition_db::PhysicalPartition;
use crate::registry::IndexKind;
use crate::status_lanes::{Lanes, StatusLanes};
use crate::table::PhysicalTable;
use crate::{ReadSnapshot, Result};

use super::{
    ActiveEntryByStage, ActiveEntryByStageKey, ENTRY_STATUS_MULTI_GET_BATCH_SIZE,
    EntryByStatusCreated, EntryByStatusCreatedKey, EntryByStatusTransitioned,
    EntryByStatusTransitionedKey, EntryStatusKey, FinishedEntryByOutcome,
    FinishedEntryByOutcomeKey,
};

/// An entry index whose keys each name one entry status, read newest first or
/// backwards. The finished and active indexes are read as runs
/// ([`Keys::Runs`]), each the entries of one value of their three leading
/// fields: a stage or status, a service and a handler. The two status indexes
/// are read as lanes ([`Keys::Lanes`]), each the entries of one stage and
/// status, whose runs are its hours and services ([`crate::status_lanes`]).
pub(crate) trait EntryIndex: RegisteredIndex + PhysicalTable<Key: OrderedKeySchema> {
    /// What `key` says its entry's current status must still say.
    fn reference(key: KeyDecoder<'_, Self::Key>) -> Result<Reference>;

    /// The time the index orders by, as the status records it.
    fn time(header: &RawStatusHeaderRef<'_>) -> UniqueTimestamp;
}

impl EntryIndex for FinishedEntryByOutcome {
    fn reference(key: KeyDecoder<'_, FinishedEntryByOutcomeKey>) -> Result<Reference> {
        let key = key.decode_all()?;
        Ok(Reference {
            id: key.canonical_id,
            stage: Stage::Finished,
            status: Some(key.status),
            at: key.transitioned_at.0,
        })
    }

    fn time(header: &RawStatusHeaderRef<'_>) -> UniqueTimestamp {
        header.stats.transitioned_at
    }
}

impl EntryIndex for ActiveEntryByStage {
    fn reference(key: KeyDecoder<'_, ActiveEntryByStageKey>) -> Result<Reference> {
        let key = key.decode_all()?;
        Ok(Reference {
            id: key.canonical_id,
            stage: key.stage,
            status: None,
            at: key.created_at.0,
        })
    }

    fn time(header: &RawStatusHeaderRef<'_>) -> UniqueTimestamp {
        header.stats.created_at
    }
}

impl EntryIndex for EntryByStatusCreated {
    fn reference(key: KeyDecoder<'_, EntryByStatusCreatedKey>) -> Result<Reference> {
        let key = key.decode_all()?;
        Ok(Reference {
            id: key.canonical_id,
            stage: key.stage,
            status: Some(key.status),
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
            status: Some(key.status),
            at: key.transitioned_at.0,
        })
    }

    fn time(header: &RawStatusHeaderRef<'_>) -> UniqueTimestamp {
        header.stats.transitioned_at
    }
}

/// A run is the entries of one stage or status, service and handler.
const RUN_FIELDS: usize = 3;

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

/// A read of finished entries through the index, restricted by the index's
/// columns. The restriction may admit more entries than the query matches: the
/// caller keeps its predicate.
pub struct FinishedRead {
    index: Arc<KeySelection<FinishedEntryByOutcomeKey>>,
    direction: ReadDirection,
}

impl FinishedRead {
    pub fn prepare(selection: &Selection<FinishedIndex>, direction: ReadDirection) -> Result<Self> {
        Ok(Self {
            index: Arc::new(FinishedEntryByOutcomeKey::prepare_candidates(selection)?),
            direction,
        })
    }
}

/// A read of active entries, those in Inbox, Running, Suspended or Paused,
/// through `active_entry_by_stage/1`, newest created first, restricted by the
/// index's columns. The restriction may admit more entries than the query
/// matches: the caller keeps its predicate.
pub struct ActiveRead {
    index: Arc<KeySelection<ActiveEntryByStageKey>>,
    direction: ReadDirection,
}

impl ActiveRead {
    pub fn prepare(selection: &Selection<ActiveIndex>, direction: ReadDirection) -> Result<Self> {
        Ok(Self {
            index: Arc::new(ActiveEntryByStageKey::prepare_candidates(selection)?),
            direction,
        })
    }
}

/// A read of the entries of some stages and statuses through one of the two
/// status indexes, a lane per stage and status
/// ([`crate::status_lanes`]), in the order of the index's time: newest first
/// or, backwards, oldest first. The lanes may admit more entries than the query
/// matches: the caller keeps its predicate.
pub struct LanesRead<K> {
    lanes: StatusLanes<K>,
    direction: ReadDirection,
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
        Ok(Self { lanes, direction })
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
        Ok(Self { lanes, direction })
    }
}

/// What an indexed read merges: one read of the index's runs, or a status
/// index's lanes.
enum Keys<I: EntryIndex> {
    Runs(OrderedRuns<I>),
    Lanes(Lanes<I>),
}

impl<I: EntryIndex> Keys<I> {
    fn advance(&mut self) -> Result<CursorStep> {
        match self {
            Self::Runs(runs) => runs.advance(),
            Self::Lanes(lanes) => lanes.advance(),
        }
    }

    /// The reference of the key the last step produced.
    fn reference(&self) -> Result<Reference> {
        match self {
            Self::Runs(runs) => I::reference(runs.current()?.0),
            Self::Lanes(lanes) => I::reference(lanes.current()?.0),
        }
    }

    fn examined(&self) -> u64 {
        match self {
            Self::Runs(runs) => runs.examined(),
            Self::Lanes(lanes) => lanes.examined(),
        }
    }

    fn runs(&self) -> usize {
        match self {
            Self::Runs(runs) => runs.runs(),
            Self::Lanes(lanes) => lanes.runs(),
        }
    }
}

/// How to open an indexed read: its selection, or its lanes.
enum Opening<'a, I: EntryIndex> {
    Runs(&'a Arc<KeySelection<I::Key>>),
    Lanes(&'a StatusLanes<I::Key>),
}

impl<I: EntryIndex> Opening<'_, I> {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Runs(selection) => selection.retained_bytes(),
            Self::Lanes(lanes) => lanes.retained_bytes(),
        }
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
/// still say: its stage, its status if the key holds one, the time the index
/// orders by and the incarnation.
pub(crate) struct Reference {
    id: CanonicalEntryId,
    stage: Stage,
    status: Option<Status>,
    at: UniqueTimestamp,
}

/// How a read that holds nothing ended.
#[derive(Clone, Copy)]
enum Ended {
    Done,
    Failed,
}

/// Merges the index runs, collects references into a window, fetches the
/// window's entry statuses with one multi-get and hands out the ones that are
/// still the keys', in index order.
pub(super) struct IndexedCursor<I: EntryIndex> {
    /// `None` once the read ended: it holds nothing then.
    runs: Option<Keys<I>>,
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

impl IndexedCursor<FinishedEntryByOutcome> {
    pub(super) fn finished(
        snapshot: &ReadSnapshot,
        read: &FinishedRead,
        execution: &EntryIndexExecution,
        window: NonZeroUsize,
    ) -> Result<Option<Self>> {
        Self::open(
            snapshot,
            Opening::Runs(&read.index),
            read.direction,
            execution,
            window,
        )
    }
}

impl IndexedCursor<ActiveEntryByStage> {
    pub(super) fn active(
        snapshot: &ReadSnapshot,
        read: &ActiveRead,
        execution: &EntryIndexExecution,
        window: NonZeroUsize,
    ) -> Result<Option<Self>> {
        Self::open(
            snapshot,
            Opening::Runs(&read.index),
            read.direction,
            execution,
            window,
        )
    }
}

impl IndexedCursor<EntryByStatusCreated> {
    pub(super) fn status_created(
        snapshot: &ReadSnapshot,
        read: &StatusCreatedRead,
        execution: &EntryIndexExecution,
        window: NonZeroUsize,
    ) -> Result<Option<Self>> {
        Self::open(
            snapshot,
            Opening::Lanes(&read.lanes),
            read.direction,
            execution,
            window,
        )
    }
}

impl IndexedCursor<EntryByStatusTransitioned> {
    pub(super) fn status_transitioned(
        snapshot: &ReadSnapshot,
        read: &StatusTransitionedRead,
        execution: &EntryIndexExecution,
        window: NonZeroUsize,
    ) -> Result<Option<Self>> {
        Self::open(
            snapshot,
            Opening::Lanes(&read.lanes),
            read.direction,
            execution,
            window,
        )
    }
}

impl<I: EntryIndex> IndexedCursor<I> {
    /// The cursor, or `None` if the read scans instead. Nothing is held then.
    /// Either way, `execution` learns the path before this returns.
    /// `window` bounds a window's references, at most [`WINDOW`].
    fn open(
        snapshot: &ReadSnapshot,
        index: Opening<'_, I>,
        direction: ReadDirection,
        execution: &EntryIndexExecution,
        window: NonZeroUsize,
    ) -> Result<Option<Self>> {
        let window_size = window.get().min(WINDOW);
        let Execution {
            memory,
            chosen,
            fetched,
            skipped,
        } = execution.take()?;
        let memory = SharedMemory(Arc::new(Mutex::new(memory)));
        // Discovery's work counts even if it fails: the error stays the read's.
        let mut discovered = 0;
        let opened = match Self::try_open(
            snapshot,
            index,
            direction,
            memory,
            window_size,
            &mut discovered,
        ) {
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
                runs: cursor.runs.as_ref().map_or(0, Keys::runs),
            },
            Err(reason) => EntryIndexPath::Scan(*reason),
        });
        Ok(cursor.ok())
    }

    #[allow(clippy::type_complexity)]
    fn try_open(
        snapshot: &ReadSnapshot,
        index: Opening<'_, I>,
        direction: ReadDirection,
        memory: SharedMemory,
        window_size: usize,
        discovered: &mut u64,
    ) -> Result<std::result::Result<(Keys<I>, Held), (ScanReason, u64)>> {
        if let Some(reason) = unusable::<I>(snapshot)? {
            return Ok(Err((reason, 0)));
        }
        let mut held = Held {
            memory: memory.clone(),
            bytes: 0,
        };
        held.grow(
            index.retained_bytes()
                + window_size * std::mem::size_of::<Reference>()
                + window_size * KEY_LEN,
        )?;
        let selection = match index {
            Opening::Runs(selection) => selection,
            Opening::Lanes(lanes) => {
                return Ok(
                    match Lanes::open(
                        snapshot,
                        lanes,
                        direction,
                        LIMITS,
                        (&memory, &mut held),
                        discovered,
                    )? {
                        Some(lanes) => Ok((Keys::Lanes(lanes), held)),
                        None => Err((ScanReason::TooManyRuns, *discovered)),
                    },
                );
            }
        };
        Ok(
            match snapshot.ordered_runs_counted::<I>(
                Arc::clone(selection),
                RUN_FIELDS,
                direction,
                LIMITS,
                Box::new(memory),
                discovered,
            )? {
                OrderedRunsDiscovery::Ready(runs) => Ok((Keys::Runs(runs), held)),
                OrderedRunsDiscovery::TooManyRuns { examined } => {
                    Err((ScanReason::TooManyRuns, examined))
                }
            },
        )
    }

    /// Tells `fetched` of a multi-get of `statuses`, if any, and of the index
    /// keys examined since it last learned.
    fn report(&mut self, statuses: usize) {
        let examined = self.runs.as_ref().map_or(self.reported, Keys::examined);
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
            CursorStep::Record => self.window.push(runs.reference()?),
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
            && reference
                .status
                .is_none_or(|status| header.status == status)
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
        EntryContext, EntryKey, EntryMetadata, EntryStateRef, RawStatusHeader, ReadVQueueTable,
        StoredEntryRef, WriteVQueueTable,
    };
    use restate_storage_query_api::scan::ReadCursor;
    use restate_storage_query_api::selection::ValuePredicate;
    use restate_storage_query_api::tables::index::{
        ActiveEntryByStageClause, FinishedEntryByOutcomeClause,
    };
    use restate_types::config::Configuration;
    use restate_types::identifiers::PartitionId;
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, Seq, VQueueId};

    use crate::invocation_status_table::entry_index_read::counting::{Charge, Counting};
    use crate::vqueue_table::{EntryStatusKeys, EntryStatusScan};
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

    /// A read of `selection` through the Finished path, on a snapshot of its
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

    /// A finished-entry read of `selection`: [`run_scan`].
    fn run(
        store: &PartitionStore,
        selection: Selection<FinishedIndex>,
        charge: Charge,
        stop: Stop,
    ) -> Run {
        run_in(store, selection, ReadDirection::Forward, charge, stop)
    }

    /// [`run`] in `direction`.
    fn run_in(
        store: &PartitionStore,
        selection: Selection<FinishedIndex>,
        direction: ReadDirection,
        charge: Charge,
        stop: Stop,
    ) -> Run {
        run_scan(
            store,
            |execution| EntryStatusScan::Finished {
                read: Arc::new(FinishedRead::prepare(&selection, direction).unwrap()),
                fallback: scan_all(),
                execution,
            },
            charge,
            stop,
        )
    }

    /// An active-entry read of `selection`: [`run_scan`].
    fn active(
        store: &PartitionStore,
        selection: Selection<ActiveIndex>,
        charge: Charge,
        stop: Stop,
    ) -> Run {
        active_in(store, selection, ReadDirection::Forward, charge, stop)
    }

    /// [`active`] in `direction`.
    fn active_in(
        store: &PartitionStore,
        selection: Selection<ActiveIndex>,
        direction: ReadDirection,
        charge: Charge,
        stop: Stop,
    ) -> Run {
        run_scan(
            store,
            |execution| EntryStatusScan::Active {
                read: Arc::new(ActiveRead::prepare(&selection, direction).unwrap()),
                fallback: scan_all(),
                execution,
            },
            charge,
            stop,
        )
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
        selection: Selection<FinishedIndex>,
    ) -> std::result::Result<(Vec<Row>, Option<EntryIndexPath>, u64), String> {
        let run = run(store, selection, Charge::default(), Stop::End);
        assert_eq!((run.held, run.closed), (0, true), "{run:#?}");
        match (run.open_error, run.ended) {
            (Some(error), _) | (None, Some(Err(error))) => Err(error),
            _ => Ok((run.rows, run.path, run.skipped)),
        }
    }

    /// The read returns Finished entries newest finished first, narrowed by
    /// status, and only those whose current status is still their key's: a
    /// stale, missing or reincarnated reference is skipped and counted, a
    /// malformed status fails the read. Without a Complete index, it scans.
    ///
    /// However it ends, at its end, on an error or a refused charge, or dropped
    /// part-way, it releases every charge and its snapshot, and tells its work
    /// once. A refusal fails the read: it never scans instead.
    #[restate_core::test]
    async fn finished_reads_check_each_status_and_skip_stale_references() {
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

        // Newest finished first, over (status, service) runs. At its last
        // step the read releases everything, its snapshot included, with the
        // cursor still alive, and stays done. Its one multi-get and every key
        // it examined are told once.
        let all = run(&store, Selection::All, Charge::default(), Stop::End);
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
            Selection::All,
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
        let part = run(&store, Selection::All, Charge::default(), Stop::After(1));
        assert_eq!((part.rows.len(), part.ended.clone()), (1, None));
        assert!(part.held_at_end > 0 && !part.closed_at_end);
        assert_eq!((part.held, part.closed), (0, true));
        assert_eq!(
            (part.statuses(), Run::keys(&part.reported)),
            (ids.len(), examined)
        );
        // A status narrows the runs.
        let failed = Selection::All.and(FinishedEntryByOutcomeClause::Status(
            ValuePredicate::Equal(Status::Failed),
        ));
        let (rows, path, _) = read(&store, failed).unwrap();
        let expected: Vec<_> = ids
            .iter()
            .filter(|row| row.1 == Status::Failed)
            .cloned()
            .collect();
        assert_eq!(rows, expected);
        assert_eq!(path, Some(EntryIndexPath::Index { runs: 2 }));

        // Edit three statuses without their index keys: one now says another
        // status, one is gone, one is another incarnation.
        // A refused charge fails the read with the refusal, never a scan in
        // its place; every charge is released and the snapshot completes.
        // Refused for the read's own state, as it opens: no work, no path.
        let fixed = FinishedRead::prepare(&Selection::All, ReadDirection::Forward)
            .unwrap()
            .index
            .retained_bytes()
            + WINDOW * std::mem::size_of::<Reference>()
            + WINDOW * KEY_LEN;
        let refused = run(
            &store,
            Selection::All,
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
            Selection::All,
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
            Selection::All,
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
        let (rows, _, skipped) = read(&store, Selection::All).unwrap();
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
        run(&store, Selection::All, Charge::default(), Stop::End)
            .failed_and_released("cannot decode");

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
        let (rows, path, _) = read(&plain, Selection::All).unwrap();
        assert_eq!(path, Some(EntryIndexPath::Scan(ScanReason::NotComplete)));
        assert_eq!(rows.len(), 1);
    }

    /// Active entries come newest created first, the full timestamp breaking
    /// ties within a millisecond, narrowed by stage. Each entry follows its
    /// stage as it moves, and leaves when it finishes; a status change that
    /// leaves the key alone keeps it. A reference whose status no longer says
    /// its key's stage, creation time or incarnation, or that is gone, is
    /// skipped and counted; a malformed status fails the read and releases
    /// everything. Without a Complete index, it scans.
    #[restate_core::test]
    async fn active_reads_follow_each_entry_across_stages() {
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
        let services = ["A", "B"];
        let target = |n: u64| EntryTargetRef::Service {
            scope: None,
            service: services[(n % 2) as usize],
            handler: "h",
        };
        // Pairs share a millisecond, apart by their logical clock.
        let entries: Vec<_> = (0..10u64)
            .map(|n| {
                let (stage, status) = match n % 5 {
                    0 => (Stage::Inbox, Status::New),
                    1 => (Stage::Running, Status::Started),
                    2 => (Stage::Suspended, Status::Yielded),
                    3 => (Stage::Paused, Status::BackingOff),
                    _ => (Stage::Finished, Status::Succeeded),
                };
                let created = UniqueTimestamp::from_parts_unchecked(BASE + n / 2, n % 2);
                let entry = EntryId::new(EntryKind::Invocation, [n as u8 + 1; 16]);
                let key = EntryKey::new(false, created.to_unix_millis(), n + 1, entry);
                let stats = EntryStatistics::new(created, key.run_at());
                (
                    stage,
                    status,
                    key,
                    stats,
                    entry.to_base_id(qid.partition_key()),
                )
            })
            .collect();
        let mut tx = store.transaction();
        for (n, (stage, status, key, stats, _)) in entries.iter().enumerate() {
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target(n as u64),
                },
                EntryStateRef {
                    stage: *stage,
                    status: *status,
                    entry_key: key,
                    metadata: &EntryMetadata::default(),
                    stats,
                },
            );
        }
        tx.commit().await.unwrap();
        drop(tx);
        // What an active read returns: each row's ID, stage and creation time.
        let shown = |run: &Run| {
            run.rows
                .iter()
                .zip(&run.created)
                .map(|(row, (stage, created))| (row.0, *stage, *created))
                .collect::<Vec<_>>()
        };
        let expected = |entries: &[(Stage, Status, EntryKey, EntryStatistics, BaseEntryId)],
                        keep: &dyn Fn(Stage) -> bool| {
            let mut rows: Vec<_> = entries
                .iter()
                .filter(|entry| entry.0 != Stage::Finished && keep(entry.0))
                .map(|entry| (entry.4, entry.0, entry.3.created_at))
                .collect();
            rows.sort_by_key(|row| std::cmp::Reverse(row.2));
            rows
        };

        // Every active entry, over (stage, service) runs, released at its end.
        let all = active(&store, Selection::All, Charge::default(), Stop::End);
        assert_eq!(shown(&all), expected(&entries, &|_| true));
        assert_eq!(
            (all.path, all.skipped, all.ended.clone()),
            (Some(EntryIndexPath::Index { runs: 8 }), 0, Some(Ok(())))
        );
        assert_eq!((all.held_at_end, all.closed_at_end), (0, true));
        assert_eq!((all.held, all.closed), (0, true));
        // Backwards, oldest created first: the exact reverse, ties included,
        // as the logical clock orders entries created in one millisecond.
        let oldest = active_in(
            &store,
            Selection::All,
            ReadDirection::Backward,
            Charge::default(),
            Stop::End,
        );
        let mut reversed = expected(&entries, &|_| true);
        reversed.reverse();
        assert_eq!(shown(&oldest), reversed);
        assert_eq!(
            (oldest.path, oldest.held_at_end, oldest.closed_at_end),
            (Some(EntryIndexPath::Index { runs: 8 }), 0, true)
        );
        // A stage set narrows the runs.
        let some = Selection::All.and(ActiveEntryByStageClause::Stage(ValuePredicate::In(vec![
            Stage::Inbox,
            Stage::Paused,
        ])));
        let narrowed = active(&store, some, Charge::default(), Stop::End);
        assert_eq!(
            shown(&narrowed),
            expected(&entries, &|stage| matches!(
                stage,
                Stage::Inbox | Stage::Paused
            ))
        );
        assert_eq!(narrowed.path, Some(EntryIndexPath::Index { runs: 4 }));

        // Maintained moves: Inbox → Running, Running → Finished, and a status
        // change in Suspended, which leaves the key alone.
        let mut moved = entries.clone();
        moved[0].0 = Stage::Running;
        moved[1].0 = Stage::Finished;
        moved[2].1 = Status::BackingOff;
        let mut tx = store.transaction();
        for n in 0..3 {
            let header = tx
                .get_vqueue_entry_status(&entries[n].4)
                .await
                .unwrap()
                .unwrap();
            let (stage, status, key, stats, _) = &moved[n];
            tx.update_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target(n as u64),
                },
                StoredEntryRef::from_header(&header),
                EntryStateRef {
                    stage: *stage,
                    status: *status,
                    entry_key: key,
                    metadata: &EntryMetadata::default(),
                    stats,
                },
            );
        }
        tx.commit().await.unwrap();
        drop(tx);
        let after = active(&store, Selection::All, Charge::default(), Stop::End);
        assert_eq!(shown(&after), expected(&moved, &|_| true));
        assert_eq!(after.skipped, 0);

        // Edits without their index keys: another stage, gone, another
        // incarnation, another creation time. Each is skipped and counted.
        let stale = [3, 5, 6, 7].map(|n| moved[n].4);
        let relabelled = rewrite(&stored(&store, &stale[0]), &|header| {
            header.stage = Stage::Inbox
        });
        let reincarnated = rewrite(&stored(&store, &stale[2]), &|header| {
            header.seq = Seq::new(999)
        });
        let recreated = rewrite(&stored(&store, &stale[3]), &|header| {
            header.stats.created_at = UniqueTimestamp::from_parts_unchecked(BASE, 9)
        });
        let mut tx = store.transaction();
        tx.raw_put_cf(KeyKind::VQueueEntryStatus, key_of(&stale[0]), relabelled);
        tx.raw_delete_cf(KeyKind::VQueueEntryStatus, key_of(&stale[1]));
        tx.raw_put_cf(KeyKind::VQueueEntryStatus, key_of(&stale[2]), reincarnated);
        tx.raw_put_cf(KeyKind::VQueueEntryStatus, key_of(&stale[3]), recreated);
        tx.commit().await.unwrap();
        drop(tx);
        let edited = active(&store, Selection::All, Charge::default(), Stop::End);
        let mut left = expected(&moved, &|_| true);
        left.retain(|row| !stale.contains(&row.0));
        assert_eq!((shown(&edited), edited.skipped), (left, 4));

        // A malformed status fails the step that reaches it, which releases
        // everything at once.
        let mut tx = store.transaction();
        tx.raw_put_cf(
            KeyKind::VQueueEntryStatus,
            key_of(&moved[8].4),
            b"\xff\xff\xff",
        );
        tx.commit().await.unwrap();
        drop(tx);
        active(&store, Selection::All, Charge::default(), Stop::End)
            .failed_and_released("cannot decode");

        // Not Complete: a partition that doesn't maintain the index scans.
        let plain = manager
            .open(&Partition::new(PartitionId::from(1), KeyRange::FULL), None)
            .await
            .unwrap();
        let scanned = active(&plain, Selection::All, Charge::default(), Stop::End);
        assert_eq!(
            (scanned.path, scanned.held, scanned.closed),
            (Some(EntryIndexPath::Scan(ScanReason::NotComplete)), 0, true)
        );
    }
}
