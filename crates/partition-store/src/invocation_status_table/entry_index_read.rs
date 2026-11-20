// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Invocations found through the entry index, newest created first or,
//! backwards ([`ReadDirection::Backward`]), oldest first.
//!
//! The entry index `entry_by_created/1` holds one key per vqueue entry, sorted
//! by creation hour, newest first, then service and creation time, newest
//! first; the handler is in the key's value. A query reads one hour at a time:
//! the hour's runs, one per service it names (or every service with an
//! invocation in that hour), merged into creation order, and fetches the
//! invocation of each entry, instead of reading every invocation status of the
//! partition. The query's conditions on the handler are checked on each key's
//! value, so an entry of another handler is never fetched; every condition,
//! the handler's included, is checked again on the fetched status. For a query
//! on `target_handler_name = 'run'`:
//!
//! ```text
//! entry index runs: time, handler     invocation statuses, 25 per multi-get
//! hour h9: (h9, A): t9 run, t7 run  ─┐
//!          (h9, B): t8 run, t6 get  ─┴─▶  t9 t8 t7, each checked; t6 not fetched
//! hour h8: (h8, A): t5 run, t4 run  ─┬─▶  t5 t4 …
//! ```
//!
//! The index finds every invocation only once the partition has fully migrated
//! to vqueues and the index is Complete ([`EntryIndexRead::unusable`]). Both are
//! read through the query's own snapshot when the read opens. If either doesn't
//! hold, or finding the runs takes more than one read allows, the read scans the
//! invocation statuses instead, before it returns anything: the result is
//! complete either way. It reports which it did and why ([`EntryIndexPath`]), so
//! that a caller relying on creation order sorts a scan.
//!
//! Everything the index read holds is charged to the caller's memory
//! ([`EntryIndexExecution`]): the merge of the runs (see [`OrderedRuns`]), the
//! caller's selection, the window with its keys, and the fetched statuses.
//! RocksDB pins the statuses while one multi-get reads them, before they can be
//! measured; the read charges them before it keeps them in its state, and drops
//! them at once if the charge is refused. A read that fails, or ends, releases
//! everything it holds right away, its snapshot included, without waiting to be
//! dropped.
//!
//! State mutations in the index are skipped. Every invocation entry must have
//! its invocation's status, of the same incarnation (created by the same
//! record) and created at the same millisecond: anything else fails the read
//! instead of returning a wrong row.

use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};

use restate_rocksdb::reads::{OwnedPinnedBatch, ReadSnapshot as RocksReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_query_api::scan::{CursorStep, ReadDirection};
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::{
    EntryByServiceCreated as EntryIndex, EntryByServiceCreatedClause as EntryIndexClause,
    EntryByServiceCreatedColumn as EntryIndexColumn,
};
use restate_types::identifiers::InvocationId;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::Seq;
use restate_util_string::ReString;

use crate::index::{IndexValueRef, RegisteredIndex};
use crate::keys::selection::{KeySelection, PrepareKeySelection};
use crate::keys::{EncodeTableKey, KeyKind};
use crate::ordered_runs::{OrderedRuns, OrderedRunsDiscovery, OrderedRunsLimits, RetainedMemory};
use crate::partition_db::PhysicalPartition;
use crate::registry::IndexKind;
use crate::vqueue_table::{CreatedEntryValueRef, EntryByCreated, EntryByCreatedKey};
use crate::{ReadSnapshot, Result};

use super::{
    INVOCATION_STATUS_MULTI_GET_BATCH_SIZE, InvocationStatusKey, create_invocation_status_key,
};

/// A run is the entries of one creation hour and service
/// (`entry_by_created/1`).
const RUN_FIELDS: usize = 2;

/// Not measured. The read merges one hour's runs at a time, one per service
/// with an invocation created in that hour. If the first hour it reads has more
/// than `max_runs`, it scans instead, before returning anything; a later hour
/// merges all of its runs, however many services were active in it.
const LIMITS: OrderedRunsLimits = OrderedRunsLimits {
    max_runs: 256,
    max_discovery_keys: 4096,
    buffer: NonZeroUsize::new(8).unwrap(),
    step_budget: NonZeroUsize::new(256).unwrap(),
    // Runs of an hour and a service, merged one hour at a time.
    bucket_fields: 1,
};

/// Statuses fetched per multi-get, and the window that holds their invocations.
const WINDOW: usize = INVOCATION_STATUS_MULTI_GET_BATCH_SIZE.get();
const KEY_LEN: usize = InvocationStatusKey::serialized_length_fixed();

/// How a read of the entry index went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryIndexPath {
    /// Through the index, merging `runs` runs: the invocations come in creation
    /// order, newest first or, read backwards, oldest first.
    Index { runs: usize },
    /// A scan of the invocation statuses, in key order.
    Scan(ScanReason),
}

/// Why an entry-index read scans instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanReason {
    /// The partition hasn't fully migrated to vqueues: an invocation may have no
    /// entry.
    NotMigrated,
    /// The index isn't Complete on the read's snapshot.
    NotComplete,
    /// Finding the runs takes more than one read allows.
    TooManyRuns,
}

/// What one execution of an entry-index read charges and reports. The read takes
/// it when it opens.
pub struct EntryIndexExecution(Mutex<Option<Execution>>);

pub(crate) struct Execution {
    pub(crate) memory: Box<dyn RetainedMemory>,
    pub(crate) chosen: Box<dyn FnOnce(EntryIndexPath) + Send>,
    pub(crate) fetched: Box<dyn Fn(usize, u64) + Send>,
    /// Learns how many references the read skipped: missing or stale.
    pub(crate) skipped: Box<dyn Fn(u64) + Send>,
}

impl EntryIndexExecution {
    /// `memory` is charged for what the index read holds. `chosen` learns which
    /// path the read takes before it returns any row. `fetched` learns how many
    /// statuses each multi-get requests, once it returns: the window's slots,
    /// before any is inspected or charged; and how many index keys the read
    /// examined since it last learned, the rest of them once the read ends or is
    /// dropped.
    pub fn new(
        memory: Box<dyn RetainedMemory>,
        chosen: impl FnOnce(EntryIndexPath) + Send + 'static,
        fetched: impl Fn(usize, u64) + Send + 'static,
    ) -> Self {
        Self(Mutex::new(Some(Execution {
            memory,
            chosen: Box::new(chosen),
            fetched: Box::new(fetched),
            skipped: Box::new(|_| {}),
        })))
    }

    /// Also tells `skipped` how many references the read skips, for a read that
    /// skips stale ones rather than failing on them.
    pub fn with_skipped(self, skipped: impl Fn(u64) + Send + 'static) -> Self {
        if let Some(execution) = self.0.lock().expect("not poisoned").as_mut() {
            execution.skipped = Box::new(skipped);
        }
        self
    }

    /// What the read charges and reports; a read takes it once, as it opens.
    pub(crate) fn take(&self) -> Result<Execution> {
        self.0
            .lock()
            .expect("not poisoned")
            .take()
            .ok_or_else(|| StorageError::Generic(anyhow::anyhow!("an index read opens once")))
    }
}

/// One charge shared by the merge of the runs and the cursor's own state.
#[derive(Clone)]
pub(crate) struct SharedMemory(pub(crate) Arc<Mutex<Box<dyn RetainedMemory>>>);

impl RetainedMemory for SharedMemory {
    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.0.lock().expect("not poisoned").grow(bytes)
    }

    fn shrink(&mut self, bytes: usize) {
        self.0.lock().expect("not poisoned").shrink(bytes)
    }
}

/// What the cursor itself holds, released when it is dropped.
pub(crate) struct Held {
    pub(crate) memory: SharedMemory,
    pub(crate) bytes: usize,
}

impl Held {
    pub(crate) fn grow(&mut self, bytes: usize) -> Result<()> {
        self.memory.grow(bytes)?;
        self.bytes += bytes;
        Ok(())
    }

    pub(crate) fn shrink(&mut self, bytes: usize) {
        self.memory.shrink(bytes);
        self.bytes -= bytes;
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.memory.shrink(self.bytes);
    }
}

/// A read of invocations through the entry index, restricted by the index's
/// columns, newest created first or, backwards, oldest first. The restriction
/// may admit more entries than the query matches: the caller keeps its
/// predicate.
pub struct EntryIndexRead {
    index: Arc<KeySelection<EntryByCreatedKey>>,
    handlers: Arc<HandlerConditions>,
    direction: ReadDirection,
}

impl EntryIndexRead {
    pub fn prepare(selection: &Selection<EntryIndex>, direction: ReadDirection) -> Result<Self> {
        Ok(Self {
            index: Arc::new(EntryByCreatedKey::prepare_candidates(selection)?),
            handlers: Arc::new(HandlerConditions::of(selection)),
            direction,
        })
    }

    /// Why a read on `snapshot` can't go through the index, if it can't. It can,
    /// and so return the invocations in creation order, once the partition
    /// has fully migrated to vqueues, so every invocation has its entry, and the
    /// index is Complete.
    pub fn unusable(snapshot: &ReadSnapshot) -> Result<Option<ScanReason>> {
        Ok(if !snapshot.fully_migrated_to_vqueues()? {
            Some(ScanReason::NotMigrated)
        } else if snapshot
            .complete_index(EntryByCreated::DEFINITION, IndexKind::Secondary)
            .is_err()
        {
            Some(ScanReason::NotComplete)
        } else {
            None
        })
    }
}

/// The selection's conditions on the handler, which the index keeps in each
/// key's value: an entry that fails one is skipped before its status is
/// fetched. The caller's predicate still checks the fetched status.
struct HandlerConditions(Box<[ValuePredicate<ReString>]>);

impl HandlerConditions {
    fn of(selection: &Selection<EntryIndex>) -> Self {
        let Selection::Predicates(predicates) = selection else {
            return Self(Box::default());
        };
        Self(
            predicates
                .for_column(EntryIndexColumn::TargetHandlerName)
                .iter()
                .filter_map(|clause| match clause {
                    EntryIndexClause::TargetHandlerName(predicate) => Some(predicate.clone()),
                    _ => None,
                })
                .collect(),
        )
    }

    /// Whether the invocation entry whose key holds `value` meets every
    /// condition. An invocation's entry always holds its handler.
    fn admit(&self, value: &[u8]) -> Result<bool> {
        if self.0.is_empty() {
            return Ok(true);
        }
        let handler = CreatedEntryValueRef::decode(value)?
            .handler
            .ok_or(StorageError::DataIntegrityError)?;
        Ok(self.0.iter().all(|predicate| match predicate {
            ValuePredicate::Equal(expected) => expected.as_str() == handler,
            ValuePredicate::In(values) => values.iter().any(|value| value.as_str() == handler),
            ValuePredicate::Range { lower, upper } => RangeBounds::<str>::contains(
                &(
                    lower.as_ref().map(ReString::as_str),
                    upper.as_ref().map(ReString::as_str),
                ),
                handler,
            ),
        }))
    }

    fn retained_bytes(&self) -> usize {
        let string = |value: &ReString| value.len();
        let heap = |predicate: &ValuePredicate<ReString>| match predicate {
            ValuePredicate::Equal(value) => string(value),
            ValuePredicate::In(values) => {
                values.capacity() * std::mem::size_of::<ReString>()
                    + values.iter().map(string).sum::<usize>()
            }
            ValuePredicate::Range { lower, upper } => [lower, upper]
                .into_iter()
                .map(|bound| match bound {
                    Bound::Included(value) | Bound::Excluded(value) => string(value),
                    Bound::Unbounded => 0,
                })
                .sum(),
        };
        std::mem::size_of_val(&*self.0) + self.0.iter().map(heap).sum::<usize>()
    }
}

/// Merges the index runs one step at a time, collecting invocations into a
/// window, then fetches the window's statuses with one multi-get and hands them
/// out in index order.
pub(super) struct EntryIndexCursor {
    /// `None` once the read failed: it holds nothing then.
    runs: Option<OrderedRuns<EntryByCreated>>,
    runs_done: bool,
    handlers: Arc<HandlerConditions>,
    /// The window's invocations in index order, with their entries' creation
    /// records and times.
    window: Vec<(InvocationId, Seq, MillisSinceEpoch)>,
    /// Next window position to hand out, once the window is fetched.
    next: Option<usize>,
    key_buf: Vec<u8>,
    results: Option<OwnedPinnedBatch<RocksReadSnapshot<PhysicalPartition>>>,
    /// The bytes of the fetched statuses, charged while they're held.
    results_bytes: usize,
    /// Window position the cursor is on.
    current: Option<usize>,
    held: Held,
    /// Learns how many statuses each multi-get requests, and how many index
    /// keys the read examined.
    fetched: Box<dyn Fn(usize, u64) + Send>,
    /// Index keys examined that `fetched` has learned of.
    reported: u64,
    /// How the read ended, once it holds nothing.
    ended: Option<Ended>,
    /// Last: the runs and the fetched window are dropped before the snapshot.
    /// `None` once the read ended.
    snapshot: Option<ReadSnapshot>,
}

impl Drop for EntryIndexCursor {
    /// A read dropped part-way, say past a query's LIMIT, still tells what it
    /// examined.
    fn drop(&mut self) {
        self.report(0);
    }
}

/// How opening went: the runs to merge and what the read holds, or the scan's
/// reason and the index keys examined before it was chosen.
enum Opened {
    Index(OrderedRuns<EntryByCreated>, Held),
    Scan(ScanReason, u64),
}

/// How a read that holds nothing ended.
#[derive(Clone, Copy)]
enum Ended {
    Done,
    Failed,
}

impl EntryIndexCursor {
    /// The cursor, or `None` if the read scans instead: the index can't stand in
    /// for a scan on this snapshot ([`EntryIndexRead::unusable`]), or finding its
    /// runs takes more than one read allows. Nothing is held then. Either way,
    /// `execution` learns the path before this returns.
    pub(super) fn open(
        snapshot: &ReadSnapshot,
        read: &EntryIndexRead,
        execution: &EntryIndexExecution,
    ) -> Result<Option<Self>> {
        let Execution {
            memory,
            chosen,
            fetched,
            ..
        } = execution.take()?;
        let memory = SharedMemory(Arc::new(Mutex::new(memory)));
        // Discovery's work counts even if it fails: the error stays the read's.
        let mut discovered = 0;
        let opened = Self::try_open(snapshot, read, memory, &mut discovered);
        let opened = match opened {
            Ok(opened) => opened,
            Err(error) => {
                if discovered > 0 {
                    fetched(0, discovered);
                }
                return Err(error);
            }
        };
        let cursor = match opened {
            Opened::Index(runs, held) => Ok(Self::new(
                snapshot,
                runs,
                Arc::clone(&read.handlers),
                held,
                fetched,
            )),
            Opened::Scan(reason, examined) => {
                // What discovery examined before it gave up is work done too.
                if examined > 0 {
                    fetched(0, examined);
                }
                Err(reason)
            }
        };
        chosen(match &cursor {
            Ok(cursor) => EntryIndexPath::Index {
                runs: cursor.runs.as_ref().map_or(0, OrderedRuns::runs),
            },
            Err(reason) => EntryIndexPath::Scan(*reason),
        });
        Ok(cursor.ok())
    }

    fn try_open(
        snapshot: &ReadSnapshot,
        read: &EntryIndexRead,
        memory: SharedMemory,
        discovered: &mut u64,
    ) -> Result<Opened> {
        if let Some(reason) = EntryIndexRead::unusable(snapshot)? {
            return Ok(Opened::Scan(reason, 0));
        }
        // The caller's selection, its handler conditions included, and the
        // window with its keys, before the window is allocated.
        let mut held = Held {
            memory: memory.clone(),
            bytes: 0,
        };
        held.grow(
            read.index.retained_bytes()
                + read.handlers.retained_bytes()
                + WINDOW * std::mem::size_of::<(InvocationId, Seq, MillisSinceEpoch)>()
                + WINDOW * KEY_LEN,
        )?;
        let discovery = snapshot.ordered_runs_counted::<EntryByCreated>(
            Arc::clone(&read.index),
            RUN_FIELDS,
            read.direction,
            LIMITS,
            Box::new(memory),
            discovered,
        )?;
        Ok(match discovery {
            OrderedRunsDiscovery::Ready(runs) => Opened::Index(runs, held),
            OrderedRunsDiscovery::TooManyRuns { examined } => {
                Opened::Scan(ScanReason::TooManyRuns, examined)
            }
        })
    }

    fn new(
        snapshot: &ReadSnapshot,
        runs: OrderedRuns<EntryByCreated>,
        handlers: Arc<HandlerConditions>,
        held: Held,
        fetched: Box<dyn Fn(usize, u64) + Send>,
    ) -> Self {
        Self {
            runs: Some(runs),
            runs_done: false,
            handlers,
            window: Vec::with_capacity(WINDOW),
            next: None,
            key_buf: Vec::with_capacity(WINDOW * KEY_LEN),
            results: None,
            results_bytes: 0,
            current: None,
            held,
            fetched,
            reported: 0,
            ended: None,
            snapshot: Some(snapshot.clone()),
        }
    }

    /// Tells `fetched` of a multi-get of `statuses`, if any, and of the index
    /// keys examined since it last learned.
    fn report(&mut self, statuses: usize) {
        let examined = self
            .runs
            .as_ref()
            .map_or(self.reported, OrderedRuns::examined);
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

    /// Drops what the read holds and its charge: the merge of the runs, the
    /// window with its keys, the fetched statuses and the snapshot.
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
                    "the entry-index read failed earlier"
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
        if self.window.len() == WINDOW || (self.runs_done && !self.window.is_empty()) {
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
                let (key, value) = runs.current()?;
                let key = key.decode_all()?;
                if let Some(id) = key.canonical_id.to_base_entry_id().to_invocation_id()
                    && self.handlers.admit(value)?
                {
                    let created_at = key.created_at.0.to_unix_millis();
                    self.window.push((id, key.canonical_id.seq(), created_at));
                }
            }
        }
        Ok(CursorStep::Skipped)
    }

    /// Requests the window's statuses with one multi-get, in index order, and
    /// charges them before the read keeps them. If the charge is refused, they
    /// are dropped here.
    fn fetch(&mut self) -> Result<()> {
        self.key_buf.clear();
        for (id, ..) in &self.window {
            create_invocation_status_key(id).serialize_to(&mut self.key_buf);
        }
        let (keys, remainder) = self.key_buf.as_chunks::<KEY_LEN>();
        debug_assert!(remainder.is_empty());
        let snapshot = self
            .snapshot
            .as_ref()
            .ok_or(StorageError::DataIntegrityError)?;
        let reader = snapshot.storage_snapshot().reader();
        let fetched = reader
            .multi_get_pinned(KeyKind::InvocationStatus, keys, false)
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

    /// Positions the read on `slot` once its status corresponds to its entry,
    /// within the step, so that a mismatch fails the step.
    fn check(&mut self, slot: usize) -> Result<CursorStep> {
        let (id, seq, created_at) = self.window[slot];
        let Some(value) = self.value(slot)? else {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "invocation {id} has a vqueue entry but no status, although its partition \
                 has fully migrated to vqueues"
            )));
        };
        let status = super::decode_invocation_status_lazy(value)?;
        // The entry names one incarnation: an invocation with the same ID, created
        // by another record, isn't the entry's. An invocation created before
        // statuses recorded their creation record has none, and is the entry's:
        // the migration doesn't revisit it, and it gets one only on its next
        // transition. Its creation time is still checked below.
        if status
            .inner
            .creation_seq
            .is_some_and(|creation_seq| creation_seq != seq.as_u64())
        {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "invocation {id} was created by record {:?} but its vqueue entry by record {}",
                status.inner.creation_seq,
                seq.as_u64()
            )));
        }
        // The index order is the creation order of SQL's `created_at` only if
        // each entry was created with its invocation.
        if status.inner.creation_time != created_at.as_u64() {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "invocation {id} was created at {} ms but its vqueue entry at {} ms",
                status.inner.creation_time,
                created_at.as_u64()
            )));
        }
        self.current = Some(slot);
        Ok(CursorStep::Record)
    }

    fn value(&self, slot: usize) -> Result<Option<&[u8]>> {
        self.results
            .as_ref()
            .ok_or(StorageError::DataIntegrityError)?
            .get(slot)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    /// The status the step positioned on, already checked against its entry.
    pub(super) fn current(&self) -> Result<(InvocationId, InvocationStatusV2Lazy<'_>)> {
        self.snapshot
            .as_ref()
            .ok_or(StorageError::DataIntegrityError)?
            .check_active()?;
        let slot = self.current.ok_or(StorageError::DataIntegrityError)?;
        let (id, ..) = self.window[slot];
        let value = self.value(slot)?.ok_or(StorageError::DataIntegrityError)?;
        Ok((id, super::decode_invocation_status_lazy(value)?))
    }
}

/// A charge for the entry-index reads' tests.
#[cfg(test)]
pub(crate) mod counting {
    use std::sync::{Arc, Mutex};

    use restate_storage_api::StorageError;

    use crate::Result;
    use crate::ordered_runs::RetainedMemory;

    /// Counts what a read charges. It refuses a charge that would pass
    /// `limit`, or one of exactly `refuse` bytes, and records the refusal.
    #[derive(Clone, Default)]
    pub(crate) struct Counting(Arc<Mutex<Charge>>);

    #[derive(Default)]
    pub(crate) struct Charge {
        pub(crate) now: usize,
        pub(crate) peak: usize,
        pub(crate) limit: Option<usize>,
        pub(crate) refuse: Option<usize>,
        pub(crate) refused: Option<usize>,
    }

    impl Counting {
        pub(crate) fn with(charge: Charge) -> Self {
            Self(Arc::new(Mutex::new(charge)))
        }

        pub(crate) fn now(&self) -> usize {
            self.0.lock().unwrap().now
        }

        pub(crate) fn refused(&self) -> Option<usize> {
            self.0.lock().unwrap().refused
        }

        pub(crate) fn peak(&self) -> usize {
            self.0.lock().unwrap().peak
        }
    }

    impl RetainedMemory for Counting {
        fn grow(&mut self, bytes: usize) -> Result<()> {
            let mut charge = self.0.lock().unwrap();
            if charge.limit.is_some_and(|limit| charge.now + bytes > limit)
                || charge.refuse == Some(bytes)
            {
                charge.refused = Some(bytes);
                return Err(StorageError::Generic(anyhow::anyhow!("refused")));
            }
            charge.now += bytes;
            charge.peak = charge.peak.max(charge.now);
            Ok(())
        }

        fn shrink(&mut self, bytes: usize) {
            self.0.lock().unwrap().now -= bytes;
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use restate_clock::UniqueTimestamp;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::fsm_table::WriteFsmTable;
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InvocationStatus, StatusTimestamps, WriteInvocationStatusTable,
    };
    use restate_storage_api::vqueue_table::stats::EntryStatistics;
    use restate_storage_api::vqueue_table::{
        EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
    };
    use restate_storage_query_api::selection::ValuePredicate;
    use restate_storage_query_api::tables::index::EntryByServiceCreatedClause;
    use restate_types::config::Configuration;
    use restate_types::identifiers::{InvocationUuid, PartitionId, WithPartitionKey};
    use restate_types::invocation::InvocationTarget;
    use restate_types::partitions::Partition;
    use restate_types::partitions::features::PersistedFeatures;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::{EntryId, EntryTargetExt, VQueueId};
    use tokio_util::sync::CancellationToken;

    use crate::{PartitionStore, PartitionStoreManager};

    use super::counting::{Charge, Counting};
    use super::*;

    /// An execution charging `memory`, the path it takes, and the index keys it
    /// examines.
    fn execution(
        memory: &Counting,
    ) -> (
        EntryIndexExecution,
        std::sync::mpsc::Receiver<EntryIndexPath>,
        Arc<std::sync::atomic::AtomicU64>,
    ) {
        let (chosen, path) = std::sync::mpsc::channel();
        let examined = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let execution = EntryIndexExecution::new(
            Box::new(memory.clone()),
            move |taken| {
                let _ = chosen.send(taken);
            },
            {
                let examined = Arc::clone(&examined);
                move |_, keys| {
                    examined.fetch_add(keys, std::sync::atomic::Ordering::Relaxed);
                }
            },
        );
        (execution, path, examined)
    }

    /// Between steps, an entry-index read charges exactly what it holds: the
    /// merge of the runs, the caller's selection, the window with its keys and
    /// the fetched statuses, measured separately. It releases everything when
    /// it's done or dropped, a refused charge fails it, and it reports its path
    /// as it opens: the index, or a scan where the partition hasn't migrated.
    #[restate_core::test]
    async fn entry_index_reads_charge_what_they_hold_and_report_their_path() {
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
        // Two windows of statuses.
        const BASE: u64 = 1_744_010_000_000;
        let target = InvocationTarget::service("S", "h");
        let mut tx = store.transaction();
        for index in 0..30u64 {
            let id = InvocationId::from_parts(index * 997, InvocationUuid::from_u128(index.into()));
            let created = MillisSinceEpoch::new(BASE + index);
            let mut completed = CompletedInvocation::mock_neo();
            completed.invocation_target = target.clone();
            completed.timestamps = StatusTimestamps::init(created);
            completed.creation_seq = Some(Seq::new(index));
            tx.put_invocation_status(&id, &InvocationStatus::Completed(completed))
                .unwrap();
            let created = UniqueTimestamp::from_unix_millis_unchecked(created);
            let key = EntryKey::new(false, created.to_unix_millis(), index, EntryId::from(id));
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &VQueueId::custom(id.partition_key(), "q"),
                    target: &target.entry_target_ref(),
                },
                EntryStateRef {
                    stage: Stage::Finished,
                    status: Status::Succeeded,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &EntryStatistics::new(created, key.run_at()),
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
        let service = || {
            Selection::All.and(EntryByServiceCreatedClause::TargetServiceName(
                ValuePredicate::Equal("S".into()),
            ))
        };
        let read = EntryIndexRead::prepare(&service(), ReadDirection::Forward).unwrap();
        let fixed = read.index.retained_bytes()
            + WINDOW * std::mem::size_of::<(InvocationId, Seq, MillisSinceEpoch)>()
            + WINDOW * KEY_LEN;
        assert!(read.index.retained_bytes() > 0);

        // Newest first, and backwards, oldest first.
        let backward = EntryIndexRead::prepare(&service(), ReadDirection::Backward).unwrap();
        let mut all = 0;
        for read in [&read, &backward] {
            let memory = Counting::default();
            let (opening, path, examined) = execution(&memory);
            let mut cursor = EntryIndexCursor::open(&snapshot, read, &opening)
                .unwrap()
                .unwrap();
            // One service, one handler: one run.
            assert_eq!(path.try_recv(), Ok(EntryIndexPath::Index { runs: 1 }));
            let (mut created, mut fetched) = (Vec::new(), 0);
            loop {
                let statuses = (0..cursor.window.len())
                    .filter_map(|slot| cursor.results.as_ref().map(|_| slot))
                    .map(|slot| cursor.value(slot).unwrap().map_or(0, <[u8]>::len))
                    .sum::<usize>();
                assert_eq!(cursor.results_bytes, statuses);
                assert_eq!(
                    memory.now(),
                    fixed + statuses + cursor.runs.as_ref().unwrap().retained_bytes(),
                    "the charge is what the read holds"
                );
                fetched = fetched.max(statuses);
                match cursor.advance().unwrap() {
                    CursorStep::Record => {
                        created.push(cursor.current().unwrap().1.inner.creation_time)
                    }
                    CursorStep::Skipped => {}
                    CursorStep::Done => break,
                }
            }
            assert!(fetched > 0);
            assert_eq!(created.len(), 30);
            // Every entry's key, at least, once the read is done.
            all = examined.load(std::sync::atomic::Ordering::Relaxed);
            assert!(all >= 30, "{all}");
            if read.direction == ReadDirection::Forward {
                assert!(created.is_sorted_by(|newer, older| newer > older));
            } else {
                assert!(created.is_sorted_by(|older, newer| older < newer));
            }
            assert_eq!(memory.now(), 0, "everything is released at the end");
            assert!(cursor.snapshot.is_none() && cursor.advance().unwrap() == CursorStep::Done);
        }
        let memory = Counting::default();

        // Dropped part-way through a window, it releases everything too, and
        // tells what it examined until then.
        let (opening, _, examined) = execution(&memory);
        let mut cursor = EntryIndexCursor::open(&snapshot, &read, &opening)
            .unwrap()
            .unwrap();
        while cursor.advance().unwrap() != CursorStep::Record {}
        assert!(memory.now() > fixed);
        drop(cursor);
        // Here the merge read ahead every key before handing out a record.
        let part = examined.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(part, all);
        assert_eq!(memory.now(), 0);

        // A refused charge fails the read. Refused at the open, nothing opens.
        let refusing = Counting::with(Charge {
            limit: Some(fixed - 1),
            ..Default::default()
        });
        let (opening, _, _) = execution(&refusing);
        assert!(EntryIndexCursor::open(&snapshot, &read, &opening).is_err());
        assert_eq!((refusing.refused(), refusing.now()), (Some(fixed), 0));
        // Refused at discovery's largest charge, once it examined keys: the read
        // fails with the refusal, still tells those keys exactly once, and holds
        // nothing.
        let measuring = Counting::default();
        let (opening, _, _) = execution(&measuring);
        drop(EntryIndexCursor::open(&snapshot, &read, &opening).unwrap());
        let peak = measuring.peak();
        let refusing = Counting::with(Charge {
            limit: Some(peak - 1),
            ..Default::default()
        });
        let (chosen, path) = std::sync::mpsc::channel();
        let reports = Arc::new(Mutex::new(Vec::new()));
        let opening = EntryIndexExecution::new(
            Box::new(refusing.clone()),
            move |taken| {
                let _ = chosen.send(taken);
            },
            {
                let reports = Arc::clone(&reports);
                move |statuses, keys| reports.lock().unwrap().push((statuses, keys))
            },
        );
        let failed = EntryIndexCursor::open(&snapshot, &read, &opening)
            .err()
            .unwrap();
        assert!(failed.to_string().contains("refused"), "{failed}");
        let reports = reports.lock().unwrap().clone();
        assert!(
            matches!(reports.as_slice(), [(0, keys)] if *keys > 0),
            "{reports:?}"
        );
        assert!(path.try_recv().is_err(), "a failed open chooses no path");
        assert_eq!(refusing.now(), 0);
        // Refused for the first window of statuses, the newest 25, measured
        // directly: the cursor, still held, holds nothing and fails again.
        let status_len = |index: u64| {
            let id = InvocationId::from_parts(index * 997, InvocationUuid::from_u128(index.into()));
            let mut key = Vec::new();
            create_invocation_status_key(&id).serialize_to(&mut key);
            let reader = snapshot.storage_snapshot().reader();
            reader
                .get(KeyKind::InvocationStatus, &key)
                .unwrap()
                .unwrap()
                .len()
        };
        let window = (5..30).map(status_len).sum::<usize>();
        let refusing = Counting::with(Charge {
            refuse: Some(window),
            ..Default::default()
        });
        let (opening, path, _) = execution(&refusing);
        let (own, closed) = store.query_read(CancellationToken::new()).unwrap();
        let mut closed = std::pin::pin!(closed);
        let mut cursor = EntryIndexCursor::open(&own, &read, &opening)
            .unwrap()
            .unwrap();
        drop(own);
        assert_eq!(path.try_recv(), Ok(EntryIndexPath::Index { runs: 1 }));
        let failed = loop {
            match cursor.advance() {
                Ok(CursorStep::Done) => panic!("the read completes"),
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        assert_eq!(refusing.refused(), Some(window), "{failed}");
        assert_eq!(refusing.now(), 0, "a failed read holds nothing");
        assert!(cursor.runs.is_none() && cursor.results.is_none());
        assert_eq!(cursor.window.capacity() + cursor.key_buf.capacity(), 0);
        assert!(
            closed.as_mut().now_or_never().is_some(),
            "a failed read releases its snapshot"
        );
        assert!(cursor.advance().is_err());
        drop(cursor);

        // The newest invocation, with its creation record set as given.
        let id = InvocationId::from_parts(29 * 997, InvocationUuid::from_u128(29));
        let newest = async |store: &mut PartitionStore, creation_seq: Option<Seq>| {
            let mut completed = CompletedInvocation::mock_neo();
            completed.invocation_target = target.clone();
            completed.timestamps = StatusTimestamps::init(MillisSinceEpoch::new(BASE + 29));
            completed.creation_seq = creation_seq;
            let mut tx = store.transaction();
            tx.put_invocation_status(&id, &InvocationStatus::Completed(completed))
                .unwrap();
            tx.commit().await.unwrap();
        };

        // A status from before statuses recorded their creation record has none:
        // it is its entry's, and the read returns every invocation.
        newest(&mut store, None).await;
        let (own, _closed) = store.query_read(CancellationToken::new()).unwrap();
        let (opening, _, _) = execution(&memory);
        let mut cursor = EntryIndexCursor::open(&own, &read, &opening)
            .unwrap()
            .unwrap();
        let mut records = 0;
        while cursor.advance().unwrap() != CursorStep::Done {
            records += usize::from(cursor.current.is_some());
        }
        assert_eq!(records, 30, "a status without a creation record is read");
        drop(cursor);

        // A status of another incarnation than its entry fails the step that
        // reaches it, which releases everything too.
        newest(&mut store, Some(Seq::new(1_000))).await;
        let (own, closed) = store.query_read(CancellationToken::new()).unwrap();
        let mut closed = std::pin::pin!(closed);
        let (opening, _, _) = execution(&memory);
        let mut cursor = EntryIndexCursor::open(&own, &read, &opening)
            .unwrap()
            .unwrap();
        drop(own);
        let failed = loop {
            match cursor.advance() {
                Ok(CursorStep::Record) => panic!("the newest status is of another incarnation"),
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        assert!(failed.to_string().contains("created by record"), "{failed}");
        assert_eq!(memory.now(), 0);
        assert!(cursor.runs.is_none() && cursor.results.is_none());
        assert!(
            closed.as_mut().now_or_never().is_some(),
            "a failed read releases its snapshot"
        );
        drop(cursor);

        // A run longer than the merge reads ahead: dropped while it navigates
        // past what it last reported, the read still tells those keys.
        let big = InvocationTarget::service("Big", "h");
        let mut tx = store.transaction();
        for index in 0..400u64 {
            let n = 1_000 + index;
            let id = InvocationId::from_parts(n * 997, InvocationUuid::from_u128(n.into()));
            let created = MillisSinceEpoch::new(BASE + n);
            let mut completed = CompletedInvocation::mock_neo();
            completed.invocation_target = big.clone();
            completed.timestamps = StatusTimestamps::init(created);
            completed.creation_seq = Some(Seq::new(n));
            tx.put_invocation_status(&id, &InvocationStatus::Completed(completed))
                .unwrap();
            let created = UniqueTimestamp::from_unix_millis_unchecked(created);
            let key = EntryKey::new(false, created.to_unix_millis(), n, EntryId::from(id));
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &VQueueId::custom(id.partition_key(), "q"),
                    target: &big.entry_target_ref(),
                },
                EntryStateRef {
                    stage: Stage::Finished,
                    status: Status::Succeeded,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &EntryStatistics::new(created, key.run_at()),
                },
            );
        }
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let read_big = EntryIndexRead::prepare(
            &Selection::All.and(EntryByServiceCreatedClause::TargetServiceName(
                ValuePredicate::Equal("Big".into()),
            )),
            ReadDirection::Forward,
        )
        .unwrap();
        let (opening, _, examined) = execution(&memory);
        let mut cursor = EntryIndexCursor::open(&snapshot, &read_big, &opening)
            .unwrap()
            .unwrap();
        let unreported = loop {
            assert_ne!(cursor.advance().unwrap(), CursorStep::Done);
            let navigated = cursor.runs.as_ref().map_or(0, OrderedRuns::examined);
            if navigated > cursor.reported && cursor.reported > 0 {
                break navigated - cursor.reported;
            }
        };
        let before = examined.load(std::sync::atomic::Ordering::Relaxed);
        drop(cursor);
        assert_eq!(
            examined.load(std::sync::atomic::Ordering::Relaxed),
            before + unreported
        );
        drop(snapshot);

        // Not Complete: a migrated partition that doesn't maintain the index scans.
        let mut plain = manager
            .open(&Partition::new(PartitionId::from(1), KeyRange::FULL), None)
            .await
            .unwrap();
        let mut tx = plain.transaction();
        tx.put_state_machine_features(&PersistedFeatures {
            vqueues: true,
            ..Default::default()
        })
        .unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = plain.query_read(CancellationToken::new()).unwrap();
        let (opening, path, _) = execution(&memory);
        assert!(
            EntryIndexCursor::open(&snapshot, &read, &opening)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            path.try_recv(),
            Ok(EntryIndexPath::Scan(ScanReason::NotComplete))
        );
        drop(snapshot);

        // Not migrated: the read scans, and holds nothing of the index.
        let mut tx = store.transaction();
        tx.put_state_machine_features(&PersistedFeatures::default())
            .unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let (opening, path, _) = execution(&memory);
        assert!(
            EntryIndexCursor::open(&snapshot, &read, &opening)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            path.try_recv(),
            Ok(EntryIndexPath::Scan(ScanReason::NotMigrated))
        );
        assert_eq!(memory.now(), 0);
    }
}
