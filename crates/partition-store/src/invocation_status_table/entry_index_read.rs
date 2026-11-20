// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Invocations found through an entry index, newest created first or,
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
//! A query on invocation statuses or outcomes reads `entry_by_status_created/1`
//! instead, which leads with each entry's stored stage and status. Each stage
//! and status, a **lane**, is read like `entry_by_created/1`, its keys' values
//! holding the handler too, and the lanes are merged into creation order
//! again:
//!
//! ```text
//! lane (Finished, Failed):    t8 t3    ─┐
//! lane (Finished, Killed):    t9       ─┼─▶  t9 t8 t5 t3 …
//! lane (Finished, Cancelled): t5       ─┘
//! ```
//!
//! A query names the invocations' public `status` or `completion_result`,
//! which select the lanes their entries can be in
//! ([`EntryByStatusCreatedKey::lanes`]). One probe of the index finds which of
//! them have keys, and only those are read. The probe and the lanes' discovery
//! share the limits of one read; past them the read scans instead, before it
//! returns anything. Later, each step advances one lane, and every lane has its
//! next key at hand before the read hands out the earliest.
//!
//! An index finds every invocation only once the partition has fully migrated
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
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_storage_query_api::SelectionSchema;
use restate_storage_query_api::scan::{CursorStep, ReadDirection};
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::{
    EntryByServiceCreated as EntryIndex, EntryByServiceCreatedClause as EntryIndexClause,
    EntryByServiceCreatedColumn as EntryIndexColumn, EntryByStatusCreated as StatusIndex,
    EntryByStatusCreatedClause as StatusIndexClause,
    EntryByStatusCreatedColumn as StatusIndexColumn,
};
use restate_types::identifiers::InvocationId;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{CanonicalEntryId, Seq};
use restate_util_string::ReString;

use crate::index::{IndexValueRef, RegisteredIndex};
use crate::keys::codecs::{StageRankCodec, StatusCodec};
use crate::keys::header::{KeyHeader, KeySchema};
use crate::keys::selection::{KeySelection, OrderedKeySchema, PrepareKeySelection};
use crate::keys::{EncodeTableKey, FieldCodec, KeyKind};
use crate::ordered_runs::{
    Opening, OrderedRuns, OrderedRunsDiscovery, OrderedRunsLimits, RetainedMemory,
};
use crate::partition_db::PhysicalPartition;
use crate::registry::IndexKind;
use crate::vqueue_table::{
    EntryByCreated, EntryByCreatedKey, EntryByStatusCreated, EntryByStatusCreatedKey,
    EntryTargetValueRef,
};
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

/// A lane's run is the entries of one creation hour and service in its stage
/// and status (`entry_by_status_created/1`); the stage, status and hour are its
/// bucket.
const LANE_RUN_FIELDS: usize = 4;

/// [`LIMITS`] for each lane. The lanes' first hours share `LIMITS`' runs, and
/// their opening its discovery keys, the probe's included; a later hour of a
/// lane merges all of its runs.
const LANE_LIMITS: OrderedRunsLimits = OrderedRunsLimits {
    bucket_fields: 3,
    ..LIMITS
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

/// A read of invocations through an entry index, restricted by the index's
/// columns, newest created first or, backwards, oldest first. The restriction
/// may admit more entries than the query matches: the caller keeps its
/// predicate.
pub struct EntryIndexRead {
    index: IndexRead,
    handlers: Arc<HandlerConditions>,
    direction: ReadDirection,
}

enum IndexRead {
    /// `entry_by_created/1`.
    Created(Arc<KeySelection<EntryByCreatedKey>>),
    /// `entry_by_status_created/1`, a lane per stage and status.
    Status {
        /// Every lane at once, to find the ones with keys.
        probe: Arc<KeySelection<EntryByStatusCreatedKey>>,
        /// Each lane's stage, status and selection, in key order.
        lanes: Vec<(Stage, Status, Arc<KeySelection<EntryByStatusCreatedKey>>)>,
    },
}

impl EntryIndexRead {
    /// A read of `entry_by_created/1`.
    pub fn prepare(selection: &Selection<EntryIndex>, direction: ReadDirection) -> Result<Self> {
        Ok(Self {
            index: IndexRead::Created(Arc::new(EntryByCreatedKey::prepare_candidates(selection)?)),
            handlers: Arc::new(HandlerConditions::of(
                selection,
                EntryIndexColumn::TargetHandlerName,
                |clause| match clause {
                    EntryIndexClause::TargetHandlerName(predicate) => Some(predicate),
                    _ => None,
                },
            )),
            direction,
        })
    }

    /// A read of `entry_by_status_created/1`, of the lanes `selection`'s
    /// invocations can be in.
    pub fn prepare_by_status(
        selection: &Selection<StatusIndex>,
        direction: ReadDirection,
    ) -> Result<Self> {
        let pairs = EntryByStatusCreatedKey::lanes(selection).collect::<Vec<_>>();
        let (mut stages, mut statuses) = (Vec::new(), Vec::new());
        for (stage, status) in &pairs {
            if !stages.contains(stage) {
                stages.push(*stage);
            }
            if !statuses.contains(status) {
                statuses.push(*status);
            }
        }
        let probe = EntryByStatusCreatedKey::prepare_lanes(selection, &stages, &statuses)?;
        let lanes = pairs
            .into_iter()
            .map(|(stage, status)| {
                let lane = EntryByStatusCreatedKey::prepare_lanes(selection, &[stage], &[status])?;
                Ok((stage, status, Arc::new(lane)))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            index: IndexRead::Status {
                probe: Arc::new(probe),
                lanes,
            },
            handlers: Arc::new(HandlerConditions::of(
                selection,
                StatusIndexColumn::TargetHandlerName,
                |clause| match clause {
                    StatusIndexClause::TargetHandlerName(predicate) => Some(predicate),
                    _ => None,
                },
            )),
            direction,
        })
    }

    /// Why a read on `snapshot` can't go through the index, if it can't. It can,
    /// and so return the invocations in creation order, once the partition
    /// has fully migrated to vqueues, so every invocation has its entry, and the
    /// index is Complete.
    pub fn unusable(&self, snapshot: &ReadSnapshot) -> Result<Option<ScanReason>> {
        let definition = match self.index {
            IndexRead::Created(_) => EntryByCreated::DEFINITION,
            IndexRead::Status { .. } => EntryByStatusCreated::DEFINITION,
        };
        Ok(if !snapshot.fully_migrated_to_vqueues()? {
            Some(ScanReason::NotMigrated)
        } else if snapshot
            .complete_index(definition, IndexKind::Secondary)
            .is_err()
        {
            Some(ScanReason::NotComplete)
        } else {
            None
        })
    }

    /// The bytes its selections hold, its handler conditions included.
    fn retained_bytes(&self) -> usize {
        self.handlers.retained_bytes()
            + match &self.index {
                IndexRead::Created(selection) => selection.retained_bytes(),
                IndexRead::Status { probe, lanes } => {
                    probe.retained_bytes()
                        + lanes.capacity() * std::mem::size_of::<(Stage, Status, Arc<()>)>()
                        + lanes
                            .iter()
                            .map(|(.., lane)| lane.retained_bytes())
                            .sum::<usize>()
                }
            }
    }
}

/// The selection's conditions on the handler, which both indexes keep in each
/// key's value: an entry that fails one is skipped before its status is
/// fetched. The caller's predicate still checks the fetched status.
struct HandlerConditions(Box<[ValuePredicate<ReString>]>);

impl HandlerConditions {
    /// The conditions of `selection` on `column`, the handler, which
    /// `handler` takes out of their clauses.
    fn of<'a, T: SelectionSchema>(
        selection: &'a Selection<T>,
        column: T::Column,
        handler: impl Fn(&'a T::Clause) -> Option<&'a ValuePredicate<ReString>>,
    ) -> Self {
        let Selection::Predicates(predicates) = selection else {
            return Self(Box::default());
        };
        Self(
            predicates
                .for_column(column)
                .iter()
                .filter_map(handler)
                .cloned()
                .collect(),
        )
    }

    /// Whether the invocation entry whose key holds `value` meets every
    /// condition. An invocation's entry always holds its handler.
    fn admit(&self, value: &[u8]) -> Result<bool> {
        if self.0.is_empty() {
            return Ok(true);
        }
        let handler = EntryTargetValueRef::decode(value)?
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

/// The index read the cursor merges.
enum Source {
    Created(OrderedRuns<EntryByCreated>),
    Lanes(Lanes),
}

impl Source {
    fn advance(&mut self) -> Result<CursorStep> {
        match self {
            Self::Created(runs) => runs.advance(),
            Self::Lanes(lanes) => lanes.advance(),
        }
    }

    /// The entry of the key the last step produced: its canonical ID, when it
    /// was created, and the key's value.
    fn current(&self) -> Result<(CanonicalEntryId, MillisSinceEpoch, &[u8])> {
        Ok(match self {
            Self::Created(runs) => {
                let (key, value) = runs.current()?;
                let key = key.decode_all()?;
                (key.canonical_id, key.created_at.0.to_unix_millis(), value)
            }
            Self::Lanes(lanes) => {
                let (key, value) = lanes.current()?;
                let key = key.decode_all()?;
                (key.canonical_id, key.created_at.0.to_unix_millis(), value)
            }
        })
    }

    /// Keys examined so far.
    fn examined(&self) -> u64 {
        match self {
            Self::Created(runs) => runs.examined(),
            Self::Lanes(lanes) => lanes.examined(),
        }
    }

    /// Runs merged now.
    fn runs(&self) -> usize {
        match self {
            Self::Created(runs) => runs.runs(),
            Self::Lanes(lanes) => lanes.lanes.iter().map(|lane| lane.runs.runs()).sum(),
        }
    }

    #[cfg(test)]
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Created(runs) => runs.retained_bytes(),
            Self::Lanes(lanes) => lanes
                .lanes
                .iter()
                .map(|lane| lane.runs.retained_bytes())
                .sum(),
        }
    }
}

/// The lanes of `entry_by_status_created/1` with keys, merged into creation
/// order.
struct Lanes {
    lanes: Vec<Lane>,
    /// The lanes to advance before the read hands out a key, the next last:
    /// every lane at first, then the lane of each key handed out.
    unprimed: Vec<usize>,
    /// The lane of the key the last step produced.
    current: Option<usize>,
    /// Keys the probe and the lanes' discovery examined while the read opened.
    opening: u64,
    direction: ReadDirection,
}

struct Lane {
    runs: OrderedRuns<EntryByStatusCreated>,
    /// The keys `runs` had examined once it opened, counted in
    /// [`Lanes::opening`].
    opened: u64,
    /// While the lane has a key the read hasn't handed out: where the key's
    /// merge order starts in its payload, after the run's fields.
    head: Option<usize>,
}

impl Lanes {
    /// One step: advances the next lane without a key, if any, and hands out
    /// the earliest key once every lane has one or is exhausted.
    fn advance(&mut self) -> Result<CursorStep> {
        self.current = None;
        if let Some(&at) = self.unprimed.last() {
            let lane = &mut self.lanes[at];
            match lane.runs.advance()? {
                CursorStep::Skipped => return Ok(CursorStep::Skipped),
                CursorStep::Record => {
                    let payload = lane.runs.current()?.0.remaining;
                    let mut rest = payload;
                    for field in &EntryByStatusCreatedKey::FIELDS[..LANE_RUN_FIELDS] {
                        field.take_encoded(&mut rest)?;
                    }
                    lane.head = Some(payload.len() - rest.len());
                }
                CursorStep::Done => {}
            }
            self.unprimed.pop();
            if !self.unprimed.is_empty() {
                return Ok(CursorStep::Skipped);
            }
        }
        let mut earliest: Option<(usize, &[u8])> = None;
        for (at, lane) in self.lanes.iter().enumerate() {
            let Some(start) = lane.head else {
                continue;
            };
            let order = &lane.runs.current()?.0.remaining[start..];
            let earlier = earliest.is_none_or(|(_, other)| match self.direction {
                ReadDirection::Forward => order < other,
                ReadDirection::Backward => order > other,
            });
            if earlier {
                earliest = Some((at, order));
            }
        }
        let Some(at) = earliest.map(|(at, _)| at) else {
            return Ok(CursorStep::Done);
        };
        self.lanes[at].head = None;
        self.unprimed.push(at);
        self.current = Some(at);
        Ok(CursorStep::Record)
    }

    fn current(&self) -> Result<(crate::keys::KeyDecoder<'_, EntryByStatusCreatedKey>, &[u8])> {
        let at = self.current.ok_or(StorageError::DataIntegrityError)?;
        self.lanes[at].runs.current()
    }

    fn examined(&self) -> u64 {
        self.opening
            + self
                .lanes
                .iter()
                .map(|lane| lane.runs.examined() - lane.opened)
                .sum::<u64>()
    }
}

/// Merges the index runs one step at a time, collecting invocations into a
/// window, then fetches the window's statuses with one multi-get and hands them
/// out in index order.
pub(super) struct EntryIndexCursor {
    /// `None` once the read failed: it holds nothing then.
    source: Option<Source>,
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
    Index(Source, Held),
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
            Opened::Index(source, held) => Ok(Self::new(
                snapshot,
                source,
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
                runs: cursor.source.as_ref().map_or(0, Source::runs),
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
        if let Some(reason) = read.unusable(snapshot)? {
            return Ok(Opened::Scan(reason, 0));
        }
        // The caller's selections, their handler conditions included, and the
        // window with its keys, before the window is allocated.
        let mut held = Held {
            memory: memory.clone(),
            bytes: 0,
        };
        held.grow(
            read.retained_bytes()
                + WINDOW * std::mem::size_of::<(InvocationId, Seq, MillisSinceEpoch)>()
                + WINDOW * KEY_LEN,
        )?;
        let (probe, lanes) = match &read.index {
            IndexRead::Created(selection) => {
                let discovery = snapshot.ordered_runs_counted::<EntryByCreated>(
                    Arc::clone(selection),
                    RUN_FIELDS,
                    read.direction,
                    LIMITS,
                    Box::new(memory),
                    discovered,
                )?;
                return Ok(match discovery {
                    OrderedRunsDiscovery::Ready(runs) => Opened::Index(Source::Created(runs), held),
                    OrderedRunsDiscovery::TooManyRuns { examined } => {
                        Opened::Scan(ScanReason::TooManyRuns, examined)
                    }
                });
            }
            IndexRead::Status { probe, lanes } => (probe, lanes),
        };
        // The lanes with keys, in key order, charged while the read holds them.
        let Some((prefixes, charged)) = snapshot.distinct_prefixes::<EntryByStatusCreated>(
            Arc::clone(probe),
            2,
            (lanes.len(), LIMITS.max_discovery_keys),
            Box::new(memory.clone()),
            discovered,
        )?
        else {
            return Ok(Opened::Scan(ScanReason::TooManyRuns, *discovered));
        };
        held.bytes += charged;
        // The lanes, and the order to prime them in.
        held.grow(prefixes.len() * (std::mem::size_of::<Lane>() + std::mem::size_of::<usize>()))?;
        let header = <<EntryByStatusCreatedKey as KeySchema>::Header as KeyHeader>::LENGTH;
        let mut open = Vec::with_capacity(prefixes.len());
        // The runs the lanes' first hours may still have.
        let mut available = LIMITS.max_runs;
        for prefix in &prefixes {
            let mut fields = prefix
                .get(header..)
                .ok_or(StorageError::DataIntegrityError)?;
            let stage = StageRankCodec::decode_encoded(StageRankCodec::take_encoded(&mut fields)?)?;
            let status = StatusCodec::decode_encoded(StatusCodec::take_encoded(&mut fields)?)?;
            let (.., selection) = lanes
                .iter()
                .find(|(s, t, _)| (*s, *t) == (stage, status))
                .ok_or(StorageError::DataIntegrityError)?;
            let opening = Opening {
                runs: available,
                examined: discovered,
            };
            match snapshot.ordered_runs_within::<EntryByStatusCreated>(
                Arc::clone(selection),
                LANE_RUN_FIELDS,
                read.direction,
                LANE_LIMITS,
                Box::new(memory.clone()),
                opening,
            )? {
                OrderedRunsDiscovery::Ready(runs) => {
                    let opened = runs.examined();
                    available = available.saturating_sub(runs.runs());
                    if runs.runs() > 0 {
                        open.push(Lane {
                            runs,
                            opened,
                            head: None,
                        });
                    }
                }
                OrderedRunsDiscovery::TooManyRuns { .. } => {
                    return Ok(Opened::Scan(ScanReason::TooManyRuns, *discovered));
                }
            }
        }
        // The probe's prefixes are done with, and lanes without runs need no
        // place in the priming order.
        held.shrink(charged + (prefixes.len() - open.len()) * std::mem::size_of::<usize>());
        drop(prefixes);
        let mut unprimed = Vec::with_capacity(open.len());
        unprimed.extend((0..open.len()).rev());
        Ok(Opened::Index(
            Source::Lanes(Lanes {
                lanes: open,
                unprimed,
                current: None,
                opening: *discovered,
                direction: read.direction,
            }),
            held,
        ))
    }

    fn new(
        snapshot: &ReadSnapshot,
        source: Source,
        handlers: Arc<HandlerConditions>,
        held: Held,
        fetched: Box<dyn Fn(usize, u64) + Send>,
    ) -> Self {
        Self {
            source: Some(source),
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
        let examined = self.source.as_ref().map_or(self.reported, Source::examined);
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
        self.source = None;
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
        let (Some(source), Some(snapshot)) = (&mut self.source, &self.snapshot) else {
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
        match source.advance()? {
            CursorStep::Done => self.runs_done = true,
            CursorStep::Skipped => {}
            CursorStep::Record => {
                let (canonical_id, created_at, value) = source.current()?;
                if let Some(id) = canonical_id.to_base_entry_id().to_invocation_id()
                    && self.handlers.admit(value)?
                {
                    self.window.push((id, canonical_id.seq(), created_at));
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
        let fixed = read.retained_bytes()
            + WINDOW * std::mem::size_of::<(InvocationId, Seq, MillisSinceEpoch)>()
            + WINDOW * KEY_LEN;
        assert!(read.retained_bytes() > 0);

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
                    fixed + statuses + cursor.source.as_ref().unwrap().retained_bytes(),
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
        assert!(cursor.source.is_none() && cursor.results.is_none());
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
        assert!(cursor.source.is_none() && cursor.results.is_none());
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
            let navigated = cursor.source.as_ref().map_or(0, Source::examined);
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

    /// A read of invocation statuses or outcomes reads only the lanes their
    /// entries can be in, merged into creation order both ways: failed
    /// invocations skip every Succeeded key, and other stages every Finished
    /// one, and an entry of another handler is skipped on its key's value.
    /// The lanes' opening shares one read's limits, so lanes that each
    /// fit but together don't scan instead, before returning anything; later
    /// hours merge all of their runs, however many.
    #[restate_core::test]
    async fn status_lanes_merge_into_creation_order_within_shared_limits() {
        use restate_storage_query_api::tables::index::EntryByStatusCreatedClause;

        const HOUR: u64 = 3_600_000;
        const BASE: u64 = 1_744_009_200_000; // the start of an hour
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut config = Configuration::default();
        config.common.experimental.set_track_entry_indexes(true);
        let mut next = 0u64;
        // Stores an invocation of `service` and its entry in `stage` and `status`.
        let mut put = |tx: &mut crate::PartitionStoreTransaction<'_>,
                       service: &str,
                       stage: Stage,
                       status: Status,
                       created: u64| {
            next += 1;
            let id = InvocationId::from_parts(next * 7919, InvocationUuid::from_u128(next.into()));
            let target = InvocationTarget::service(service, "h");
            let created = MillisSinceEpoch::new(created);
            let mut completed = CompletedInvocation::mock_neo();
            completed.invocation_target = target.clone();
            completed.timestamps = StatusTimestamps::init(created);
            completed.creation_seq = Some(Seq::new(next));
            tx.put_invocation_status(&id, &InvocationStatus::Completed(completed))
                .unwrap();
            let created = UniqueTimestamp::from_unix_millis_unchecked(created);
            let key = EntryKey::new(false, created.to_unix_millis(), next, EntryId::from(id));
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &VQueueId::custom(id.partition_key(), "q"),
                    target: &target.entry_target_ref(),
                },
                EntryStateRef {
                    stage,
                    status,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &EntryStatistics::new(created, key.run_at()),
                },
            );
            (created.to_unix_millis().as_u64(), stage, status)
        };
        let open = async |id: u16| {
            let mut store = manager
                .open(&Partition::new(PartitionId::from(id), KeyRange::FULL), None)
                .await
                .unwrap();
            store
                .verify_and_run_migrations(CancellationToken::new(), &config)
                .await
                .unwrap();
            store
        };
        let migrated = |tx: &mut crate::PartitionStoreTransaction<'_>| {
            tx.put_state_machine_features(&PersistedFeatures {
                vqueues: true,
                ..Default::default()
            })
            .unwrap()
        };

        // Three hours of two services, in every kind of stage and status: the
        // statuses of each stage interleave in creation time.
        let mut store = open(10).await;
        let mut tx = store.transaction();
        let kinds = [
            (Stage::Finished, Status::Succeeded),
            (Stage::Finished, Status::Failed),
            (Stage::Finished, Status::Killed),
            (Stage::Finished, Status::Succeeded),
            (Stage::Running, Status::Started),
            (Stage::Inbox, Status::New),
            (Stage::Suspended, Status::Started),
            (Stage::Finished, Status::Succeeded),
        ];
        let mut stored = Vec::new();
        for n in 0..96u64 {
            let (stage, status) = kinds[(n % 8) as usize];
            let service = if n % 3 == 0 { "A" } else { "B" };
            stored.push(put(&mut tx, service, stage, status, BASE + n * (HOUR / 32)));
        }
        migrated(&mut tx);
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let read = |selection: Selection<StatusIndex>, direction| {
            EntryIndexRead::prepare_by_status(&selection, direction).unwrap()
        };
        let failure = || {
            Selection::All.and(EntryByStatusCreatedClause::CompletionResult(
                ValuePredicate::Equal(Some("failure".into())),
            ))
        };
        let active = || {
            Selection::All.and(EntryByStatusCreatedClause::Status(ValuePredicate::In(
                vec!["invoked".into(), "inboxed".into(), "suspended".into()],
            )))
        };
        let memory = Counting::default();
        // Every record's creation time, and the keys the read examined.
        let records = |read: &EntryIndexRead| {
            let (opening, path, examined) = execution(&memory);
            let mut cursor = EntryIndexCursor::open(&snapshot, read, &opening)
                .unwrap()
                .unwrap();
            let mut created = Vec::new();
            loop {
                match cursor.advance().unwrap() {
                    CursorStep::Record => {
                        created.push(cursor.current().unwrap().1.inner.creation_time)
                    }
                    CursorStep::Skipped => {}
                    CursorStep::Done => break,
                }
            }
            assert_eq!(memory.now(), 0, "everything is released at the end");
            (
                path.try_recv().unwrap(),
                created,
                examined.load(std::sync::atomic::Ordering::Relaxed),
            )
        };
        let expected = |keep: &dyn Fn(Stage, Status) -> bool, backward: bool| {
            let mut times = stored
                .iter()
                .filter(|(_, stage, status)| keep(*stage, *status))
                .map(|(created, ..)| *created)
                .collect::<Vec<_>>();
            times.sort_unstable();
            if !backward {
                times.reverse();
            }
            times
        };
        let failed = |stage, status| stage == Stage::Finished && status != Status::Succeeded;
        let (path, created, examined) = records(&read(failure(), ReadDirection::Forward));
        // Two lanes, Failed and Killed, each with a run per service and hour.
        assert_eq!(path, EntryIndexPath::Index { runs: 4 });
        assert_eq!(created, expected(&failed, false));
        // At most twice the 24 failed keys; the 48 Succeeded ones aren't read.
        assert!(examined <= 2 * 24, "{examined}");
        let (_, created, _) = records(&read(failure(), ReadDirection::Backward));
        assert_eq!(created, expected(&failed, true));
        // The handler is checked on the keys' values: every entry's is "h".
        let handler = |name: &str| {
            failure().and(EntryByStatusCreatedClause::TargetHandlerName(
                ValuePredicate::Equal(name.into()),
            ))
        };
        let (_, created, _) = records(&read(handler("h"), ReadDirection::Forward));
        assert_eq!(created, expected(&failed, false));
        let (_, created, _) = records(&read(handler("other"), ReadDirection::Forward));
        assert!(created.is_empty());
        // Nothing selected: every lane, each entry once.
        for backward in [false, true] {
            let direction = if backward {
                ReadDirection::Backward
            } else {
                ReadDirection::Forward
            };
            let (_, created, _) = records(&read(Selection::All, direction));
            assert_eq!(created, expected(&|_, _| true, backward));
        }
        let (_, created, examined) = records(&read(active(), ReadDirection::Forward));
        assert_eq!(
            created,
            expected(&|stage, _| stage != Stage::Finished, false)
        );
        // At most twice the 36 active keys, which discovery and reading ahead
        // each visit; none of the 60 Finished keys beyond the probe's seeks.
        assert!(examined <= 2 * 36, "{examined}");
        // A service and a time window narrow every lane.
        let windowed = failure()
            .and(EntryByStatusCreatedClause::TargetServiceName(
                ValuePredicate::Equal("B".into()),
            ))
            .and(EntryByStatusCreatedClause::CreatedAt(
                ValuePredicate::Range {
                    lower: std::ops::Bound::Included(MillisSinceEpoch::new(BASE + HOUR)),
                    upper: std::ops::Bound::Unbounded,
                },
            ));
        let (_, created, _) = records(&read(windowed, ReadDirection::Forward));
        let b = stored
            .iter()
            .enumerate()
            .filter(|(n, (created, stage, status))| {
                n % 3 != 0 && *created >= BASE + HOUR && failed(*stage, *status)
            })
            .map(|(_, (created, ..))| *created)
            .rev()
            .collect::<Vec<_>>();
        assert_eq!(created, b);
        drop(snapshot);

        // Two lanes with 130 services each in their first hour: each lane fits
        // one read's 256 runs, both don't, so the read scans.
        let mut store = open(11).await;
        let mut tx = store.transaction();
        for (n, status) in (0..260u64).zip([Status::Failed, Status::Killed].into_iter().cycle()) {
            put(
                &mut tx,
                &format!("s{}", n / 2),
                Stage::Finished,
                status,
                BASE + n,
            );
        }
        migrated(&mut tx);
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let (opening, path, examined) = execution(&memory);
        let wide = read(failure(), ReadDirection::Forward);
        assert!(
            EntryIndexCursor::open(&snapshot, &wide, &opening)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            path.try_recv(),
            Ok(EntryIndexPath::Scan(ScanReason::TooManyRuns))
        );
        assert!(examined.load(std::sync::atomic::Ordering::Relaxed) > 0);
        assert_eq!(memory.now(), 0);
        drop(snapshot);

        // Later hours merge all of their runs, however many: the read has
        // returned rows, so it can't scan instead. Two lanes, each with one run
        // in its newest hour; in the hour before, the Failed lane has 257, more
        // than one read's first hours may have, and the Killed lane 130.
        let mut store = open(12).await;
        let mut tx = store.transaction();
        let mut times = Vec::new();
        for (status, older) in [(Status::Failed, 257u64), (Status::Killed, 130)] {
            let newest = put(
                &mut tx,
                &format!("{status}"),
                Stage::Finished,
                status,
                BASE + HOUR,
            );
            times.push(newest.0);
            for n in 0..older {
                let entry = put(
                    &mut tx,
                    &format!("{status}{n}"),
                    Stage::Finished,
                    status,
                    BASE + n,
                );
                times.push(entry.0);
            }
        }
        migrated(&mut tx);
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let (opening, path, _) = execution(&memory);
        let mut cursor = EntryIndexCursor::open(&snapshot, &wide, &opening)
            .unwrap()
            .unwrap();
        assert_eq!(path.try_recv(), Ok(EntryIndexPath::Index { runs: 2 }));
        let mut created = Vec::new();
        while cursor.advance().unwrap() != CursorStep::Done {
            if let Ok((_, status)) = cursor.current() {
                created.push(status.inner.creation_time);
            }
        }
        times.sort_unstable();
        times.reverse();
        assert_eq!(created, times);
        assert_eq!(memory.now(), 0, "a finished read holds nothing");
        drop((cursor, snapshot));

        // Opened, the read charges exactly what it holds: its selections, the
        // window, each lane's slot, place in the priming order and own charge,
        // and none of the probe's prefixes any more. Refused at its largest
        // charge, nothing opens and nothing stays charged.
        let mut store = open(14).await;
        let mut tx = store.transaction();
        for (n, status) in (0..6u64).zip(
            [Status::Failed, Status::Killed, Status::Cancelled]
                .into_iter()
                .cycle(),
        ) {
            put(&mut tx, "s", Stage::Finished, status, BASE + n);
        }
        migrated(&mut tx);
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let measuring = Counting::default();
        let (opening, _, _) = execution(&measuring);
        let cursor = EntryIndexCursor::open(&snapshot, &wide, &opening)
            .unwrap()
            .unwrap();
        let Some(Source::Lanes(lanes)) = &cursor.source else {
            panic!("a status read merges lanes");
        };
        assert_eq!(lanes.lanes.len(), 3);
        assert_eq!(lanes.unprimed.capacity(), 3);
        assert_eq!(
            measuring.now(),
            wide.retained_bytes()
                + WINDOW * std::mem::size_of::<(InvocationId, Seq, MillisSinceEpoch)>()
                + WINDOW * KEY_LEN
                + 3 * (std::mem::size_of::<Lane>() + std::mem::size_of::<usize>())
                + cursor.source.as_ref().unwrap().retained_bytes(),
            "the charge is what the opened read holds"
        );
        let peak = measuring.peak();
        drop(cursor);
        assert_eq!(measuring.now(), 0);
        let refusing = Counting::with(Charge {
            limit: Some(peak - 1),
            ..Default::default()
        });
        let (opening, path, _) = execution(&refusing);
        assert!(EntryIndexCursor::open(&snapshot, &wide, &opening).is_err());
        assert!(path.try_recv().is_err(), "a failed open chooses no path");
        assert_eq!(refusing.now(), 0, "a refused open holds nothing");
    }
}
