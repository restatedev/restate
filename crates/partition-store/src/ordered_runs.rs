// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reads an index in the order of its trailing fields, across the runs its
//! leading fields split it into.
//!
//! An index keyed `service | handler | created_at ↓ | id` is sorted by service and
//! handler first. A query that names only a service still wants the service's
//! entries newest first, so it reads each handler's **run** (the keys sharing a
//! service and handler, already newest first) and merges the runs:
//!
//! ```text
//! run (Utils, NULL)  : t6
//! run (Utils, "call"): t9 t7 t2
//! run (Utils, "run") : t8 t3
//!         │ merge by the key after the run's prefix: created_at ↓, then id
//!         ▼
//! t9 t8 t7 t6 t3 t2
//! ```
//!
//! **Discovery** finds the runs before the read emits anything: it evaluates
//! the read's selection from the start of its bounds, takes the first matching
//! key of each new prefix as that run's head, and seeks past the whole prefix to
//! the next one. It works on the read's own snapshot, so a prefix written after
//! the query was planned is found and an emptied one is skipped. It stops, with
//! nothing read, once it has found more runs or visited more keys than the
//! caller allows ([`OrderedRunsDiscovery::TooManyRuns`]), so the caller can read
//! the table another way instead, without any duplicate or missing row.
//!
//! **Merging** uses one iterator for all runs. Each run reads a few keys ahead;
//! when they are consumed, the iterator moves back to where that run stopped
//! ([`SnapshotCursor::reposition`]). A heap picks the run whose next key sorts
//! first after its prefix. Keys of records the partition doesn't own, left over
//! from a split, are skipped like in any index read.
//!
//! **Buckets** ([`OrderedRunsLimits::bucket_fields`]). An index keyed
//! `hour ↓ | service | created_at ↓ | id` has a run per hour and service, too
//! many to discover at once over days of hours. But every key of a newer hour
//! sorts before every key of an older one, so the read discovers the runs of the
//! first hour only, stopping at the first key of the next, merges them, and
//! then discovers the next hour's runs from that key, on the same iterator:
//!
//! ```text
//! hour h9: run (h9, "Other"): t9 t6   ─┐ merge ─▶ t9 t8 t6
//!          run (h9, "Utils"): t8      ─┘
//! hour h8: run (h8, "Utils"): t7 t5   ──────────▶ t7 t5     (discovered next)
//! ```
//!
//! Backwards, the read starts at the last bucket: it steps back to the last key
//! of the read, discovers the runs of that key's bucket from the bucket's start,
//! and later steps back from that start to the bucket before. A bucket without
//! a match is skipped.
//!
//! The first bucket's discovery has the limits: past them, it ends as without
//! buckets, before the read returns anything, so the caller can still read
//! another way. A later bucket can't: the read has returned the earlier
//! buckets' rows. So it merges every run it has, however many, with what they
//! hold charged to the read's memory, which bounds them as it bounds the rest
//! of the query. Its discovery, which may pass many keys and buckets without a
//! match, debits the step's budget like reading ahead does, pausing where it
//! is spent and continuing in the next step, instead of having a limit of its
//! own. Later buckets reuse the runs' read-ahead buffers.
//!
//! **Backwards** ([`ReadDirection::Backward`]), the same merge yields the reverse
//! order, oldest first, for a query that asks for it:
//!
//! ```text
//! run (Utils, NULL)  : t6
//! run (Utils, "call"): t2 t7 t9    ← each run read from its end
//! run (Utils, "run") : t3 t8
//!         │ merge, largest key after the run's prefix first
//!         ▼
//! t2 t3 t6 t7 t8 t9
//! ```
//!
//! Discovery is the same. Each run then steps back with `prev`
//! ([`SnapshotCursor::reposition_back`], [`SnapshotCursor::retreat`]), checking
//! each key against the selection, between two bounds that leave out the keys
//! it can't match at either end of the run:
//!
//! - It starts at the end of what the selection may match in the run, from the
//!   bounds of the fields after the prefix ([`KeySelectionCursor::end_within`]):
//!   for a creation-time window, just past the window's oldest time.
//! - It stops at the run's first match, which discovery found: on the same
//!   snapshot, no key before it can match.
//!
//! ```text
//! run (Utils, "call") in key order, created_at in [t3, t7]:
//!
//!   t9 │ t7  t5  t3 │ t2
//!      │ ◄───────── │
//!      │            └ starts at the window's end, just past t3
//!      └ stops after t7, discovery's first match
//! ```
//!
//! Neither t9 nor t2 is examined.
//!
//! Between the two, seeks only move forward, so a key the selection rejects,
//! such as a creation time an IN list leaves out, is stepped over rather than
//! sought past; the step budget still bounds each step. The cursor seeks in
//! total order, without the prefix extractor, as a backward seek may target a
//! key past the run's prefix.
//!
//! **Bounded steps.** Every run is read ahead before the first key is emitted,
//! so that the heap can compare them all. Reading ahead may pass any number of
//! keys the partition doesn't own, so it examines at most
//! [`OrderedRunsLimits::step_budget`] keys per [`OrderedRuns::advance`] and
//! reports [`CursorStep::Skipped`] until it is done. The next step continues
//! where it stopped; nothing is cut short.
//!
//! **Memory.** What the read holds is charged to a [`RetainedMemory`] at the
//! capacity it allocated, in two ways:
//!
//! - What grows with the data is charged **before** it is allocated:
//!   - container slots (one per run, `buffer` records read ahead per run, one
//!     heap entry per run), kept until the read ends;
//!   - the bytes it copies (run prefixes, the first match a backward run stops
//!     at, keys to resume from, keys and values read ahead, the key that seeks
//!     past a run), each in its own allocation of exactly its length, released
//!     once dropped;
//!   - the snapshot cursor's copy of the bounds and the iterator's, charged as a
//!     bound of three times their length.
//! - Navigation state, about one key's worth per read, is charged at its
//!   measured capacity right **after** it is allocated or grows, in the same
//!   step: the selection's bounds and fixed header (exact, unshared copies) and
//!   its seek storage, which only grows. Seek targets are borrowed from it, not
//!   copied.
//!
//! A record is held in one place at a time: read ahead, then as its run's head,
//! then as the current record. Between steps the charge covers what the read
//! holds: exactly, except the copies of the bounds, which are charged as a
//! bound. Once the read is done, or a step fails (a refused charge, a
//! cancellation), it releases everything, its snapshot included; dropping it
//! does too. Not charged: the iterator's blocks, which RocksDB accounts for,
//! the selection the caller passes (with any list of headers it shares), and
//! the allocator's own overhead.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::marker::PhantomData;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;

use restate_storage_api::StorageError;
use restate_storage_query_api::scan::{CursorStep, ReadDirection};

use crate::keys::KeyDecoder;
use crate::keys::header::{KeyHeader, KeySchema};
use crate::keys::selection::{KeyMatch, KeySelection, KeySelectionCursor, OrderedKeySchema};
use crate::query_read::SnapshotCursor;
use crate::scan::PhysicalScan;
use crate::table::PhysicalTable;
use crate::{ReadSnapshot, Result, ScanMode};

#[cfg(test)]
mod tests;

/// How much work a read may do and how many keys each run reads ahead.
#[derive(Debug, Clone, Copy)]
pub struct OrderedRunsLimits {
    /// The most runs one read merges: in its first bucket, for a bucketed
    /// read. A later bucket merges all of its runs (see the module docs).
    pub max_runs: usize,
    /// The most keys discovery evaluates, including keys the selection rejects
    /// and runs without a key in the read's window: in its first bucket, for a
    /// bucketed read, whose later buckets' discovery debits the step budget.
    pub max_discovery_keys: usize,
    /// How many of the run fields form a **bucket**, `0` for none: leading
    /// fields whose every key sorts, in the read's order, before every key of
    /// the next bucket, such as a creation time floored to its hour in front of
    /// the creation time. Discovery then finds one bucket's runs at a time, and
    /// the read merges them before it discovers the next bucket's (see the
    /// module docs), backwards from the last bucket. Only a read of one key
    /// header, such as an index's, can have buckets: a bucket's keys under one
    /// header don't sort before the next bucket's under another, so a read of
    /// several headers, such as a partition key range, refuses them.
    pub bucket_fields: usize,
    /// Keys read ahead per run.
    pub buffer: NonZeroUsize,
    /// The most keys one step examines while reading ahead, including keys the
    /// partition doesn't own.
    pub step_budget: NonZeroUsize,
}

/// The memory a read holds, charged as it grows and released as it shrinks.
pub trait RetainedMemory: Send {
    /// Charges `bytes` more: before the read holds them, or right after
    /// navigation state grows, in the same step (see the module docs). An error
    /// stops the read.
    fn grow(&mut self, bytes: usize) -> Result<()>;
    /// Releases `bytes` the read no longer holds.
    fn shrink(&mut self, bytes: usize);
}

/// Charges nothing.
impl RetainedMemory for () {
    fn grow(&mut self, _: usize) -> Result<()> {
        Ok(())
    }

    fn shrink(&mut self, _: usize) {}
}

/// Distinct key prefixes, and the bytes charged for them
/// ([`ReadSnapshot::distinct_prefixes`]).
pub type Prefixes = (Vec<Box<[u8]>>, usize);

/// What a read shares with the other reads that open with it, such as the
/// lanes of one cursor: `runs`, the runs its first bucket may still have, what
/// the reads opened before it left of their shared
/// [`OrderedRunsLimits::max_runs`]; and `examined`, the keys their discoveries
/// examined so far, against [`OrderedRunsLimits::max_discovery_keys`]. Past
/// either, the read stops before reading anything. Later buckets merge all of
/// their runs (see the module docs).
pub struct Opening<'a> {
    pub runs: usize,
    pub examined: &'a mut u64,
}

/// What discovery found.
pub enum OrderedRunsDiscovery<T: PhysicalTable> {
    /// The runs, ready to be merged.
    Ready(OrderedRuns<T>),
    /// More runs or more discovery work than the limits allow. Nothing was read
    /// and nothing is held. The keys discovery examined before it stopped.
    TooManyRuns { examined: u64 },
}

/// The keys of one read, in the order of the fields after the runs' prefix.
pub struct OrderedRuns<T: PhysicalTable> {
    /// Dropped once the runs are exhausted, releasing the snapshot.
    inner: Option<Box<Merge<T::Key>>>,
    /// The record the last step produced.
    current: Option<Head>,
    /// Keys examined so far, by discovery and by reading ahead.
    examined: u64,
    /// Last, so that it releases its charge after the fields above are dropped.
    memory: Accounted,
    _table: PhantomData<fn() -> T>,
}

/// The charged bytes and where they are charged to.
struct Accounted {
    memory: Box<dyn RetainedMemory>,
    bytes: usize,
    /// The part of `bytes` charged for the selection's storage.
    selection: usize,
}

impl Accounted {
    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.memory.grow(bytes)?;
        self.bytes += bytes;
        Ok(())
    }

    fn shrink(&mut self, bytes: usize) {
        let bytes = bytes.min(self.bytes);
        self.bytes -= bytes;
        self.memory.shrink(bytes);
    }

    /// Charges the growth of the selection's storage, which never shrinks.
    fn selection(&mut self, selection: &KeySelectionCursor<impl Sized>) -> Result<()> {
        let bytes = selection.retained_bytes();
        if bytes > self.selection {
            self.grow(bytes - self.selection)?;
            self.selection = bytes;
        }
        Ok(())
    }

    fn release(&mut self) {
        self.shrink(self.bytes);
        self.selection = 0;
    }
}

impl Drop for Accounted {
    fn drop(&mut self) {
        self.memory.shrink(self.bytes);
    }
}

struct Merge<K> {
    cursor: SnapshotCursor,
    selection: KeySelectionCursor<K>,
    runs: Vec<Run>,
    /// The front record of each run that has one, except the current record's
    /// run. Never holds more than one head per run, so it never grows.
    heads: BinaryHeap<Reverse<Head>>,
    /// The run reading ahead, if a step stopped before it was done: during
    /// priming, the next run to read; later, the run whose records ran out.
    pending: Option<usize>,
    /// Whether every run has been read ahead once.
    primed: bool,
    buffer: usize,
    step_budget: usize,
    direction: ReadDirection,
    shape: Shape,
    /// A bucketed read's next bucket. Forward: its first key, where discovery
    /// stopped. Backward: the start of the current bucket, to step back from.
    next_bucket: Option<Box<[u8]>>,
    /// A later bucket's discovery that ran out of a step's budget: where the
    /// next step continues it.
    resume: Option<Resume>,
    /// Read-ahead buffers of exhausted runs, empty, for the next bucket's runs.
    spare: Vec<VecDeque<Record>>,
}

/// How a read's keys split into runs and buckets.
struct Shape {
    /// The length of the key's header.
    header: usize,
    run_fields: usize,
    /// `0` for a read without buckets.
    bucket_fields: usize,
    direction: ReadDirection,
}

/// The limits of the first bucket's discovery, or the whole read's without
/// buckets.
#[derive(Clone, Copy)]
struct Caps {
    max_runs: usize,
    max_discovery_keys: usize,
}

/// Where discovery stopped.
enum Discovered {
    /// At the end of the read's keys.
    End,
    /// Past its limits ([`Caps`]).
    TooManyRuns,
    /// At the first key of the next bucket, which it holds, charged.
    NextBucket(Box<[u8]>),
    /// Out of the step's budget, before evaluating a key: where it continues.
    Paused(Resume),
}

/// Where a paused discovery continues, with the keys it holds, charged.
enum Resume {
    /// At this key.
    At(Box<[u8]>),
    /// Backwards, at `at`, within the bucket that starts at `anchor`.
    Within { anchor: Box<[u8]>, at: Box<[u8]> },
}

/// A bucket's start and the key within it where its discovery continues.
type Anchored = (Box<[u8]>, Box<[u8]>);

/// What the next later bucket's discovery came to.
enum NextBucket {
    /// Its runs, ready to merge.
    Ready,
    /// Out of the step's budget: the next step continues it.
    Paused,
    /// No bucket is left.
    Done,
}

/// Discovery over a read's cursor and selection.
struct Discovery<'a, K> {
    cursor: &'a mut SnapshotCursor,
    selection: &'a mut KeySelectionCursor<K>,
    shape: &'a Shape,
    memory: &'a mut Accounted,
    /// Kept up to date key by key, so that a caller can report the work even
    /// when discovery fails.
    examined: &'a mut u64,
}

struct Run {
    /// The header and the leading fields every key of the run shares.
    prefix: Box<[u8]>,
    /// Records read ahead, in the read's direction, after the run's head.
    /// Reading ahead starts once the run's records are consumed and stops at
    /// `buffer`, so the deque never grows.
    records: VecDeque<Record>,
    /// Where the run continues, once its records read ahead are consumed: the
    /// next key to read, or backwards, the key to read back from (initially
    /// the end of what the selection may match in the run).
    resume: Option<Box<[u8]>>,
    /// Backwards, the run's first match, which discovery found: reading back
    /// stops before it, as no earlier key can match on the read's snapshot.
    /// `None` forward.
    floor: Option<Box<[u8]>>,
}

/// Owned copies, allocated at their exact length and never shared.
struct Record {
    key: Box<[u8]>,
    value: Box<[u8]>,
}

impl Record {
    /// The bytes it copied.
    fn bytes(&self) -> usize {
        self.key.len() + self.value.len()
    }
}

/// A run's front record, ordered by its key after the run's prefix, in the
/// read's direction: the head that orders first is emitted first.
struct Head {
    record: Record,
    prefix: usize,
    run: usize,
    backward: bool,
}

impl Head {
    fn order(&self) -> (&[u8], usize) {
        (&self.record.key[self.prefix..], self.run)
    }
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.order() == other.order()
    }
}

impl Eq for Head {}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let order = self.order().cmp(&other.order());
        if self.backward {
            order.reverse()
        } else {
            order
        }
    }
}

/// What reading ahead a run achieved within its step budget.
enum Fill {
    /// The run has records again, or none are left.
    Done,
    /// The budget ran out first; the run resumes where it stopped.
    Paused,
}

enum Step {
    Next,
    /// To the selection's seek target.
    Seek,
    /// Past the prefix of the run just found.
    PastRun,
    Stop,
}

impl ReadSnapshot {
    /// Discovers the runs of `request` whose prefix is its key's header and first
    /// `run_fields` fields, and prepares to merge them in `direction`. What the
    /// read holds is charged to `memory`.
    pub fn ordered_runs<T>(
        &self,
        request: Arc<KeySelection<T::Key>>,
        run_fields: usize,
        direction: ReadDirection,
        limits: OrderedRunsLimits,
        memory: Box<dyn RetainedMemory>,
    ) -> Result<OrderedRunsDiscovery<T>>
    where
        T: PhysicalTable,
        T::Key: OrderedKeySchema,
    {
        self.ordered_runs_counted(request, run_fields, direction, limits, memory, &mut 0)
    }

    /// [`Self::ordered_runs`], keeping in `examined` the keys discovery examined
    /// so far, so that a caller can tell that work even when discovery fails.
    pub fn ordered_runs_counted<T>(
        &self,
        request: Arc<KeySelection<T::Key>>,
        run_fields: usize,
        direction: ReadDirection,
        limits: OrderedRunsLimits,
        memory: Box<dyn RetainedMemory>,
        examined: &mut u64,
    ) -> Result<OrderedRunsDiscovery<T>>
    where
        T: PhysicalTable,
        T::Key: OrderedKeySchema,
    {
        let opening = Opening {
            runs: limits.max_runs,
            examined,
        };
        self.ordered_runs_within(request, run_fields, direction, limits, memory, opening)
    }

    /// [`Self::ordered_runs_counted`] as one of several reads that share the
    /// runs and the keys their openings may find and examine ([`Opening`]).
    pub fn ordered_runs_within<T>(
        &self,
        request: Arc<KeySelection<T::Key>>,
        run_fields: usize,
        direction: ReadDirection,
        limits: OrderedRunsLimits,
        memory: Box<dyn RetainedMemory>,
        Opening {
            runs: available,
            examined,
        }: Opening<'_>,
    ) -> Result<OrderedRunsDiscovery<T>>
    where
        T: PhysicalTable,
        T::Key: OrderedKeySchema,
    {
        self.check_active()?;
        let mut runs = OrderedRuns {
            inner: None,
            current: None,
            examined: 0,
            memory: Accounted {
                memory,
                bytes: 0,
                selection: 0,
            },
            _table: PhantomData,
        };
        let metadata = self.metadata();
        if !T::is_active(&metadata) {
            return Ok(OrderedRunsDiscovery::Ready(runs));
        }
        let Some(mut selection) = request.cursor(
            <<T::Key as KeySchema>::Header as KeyHeader>::scope(&metadata)?,
        )?
        else {
            return Ok(OrderedRunsDiscovery::Ready(runs));
        };
        if limits.bucket_fields > 0 && !selection.fixed_header() {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "a bucketed read must cover one key header"
            )));
        }
        runs.memory.selection(&selection)?;
        let scan = match direction {
            ReadDirection::Forward => selection.scan().to_exact(),
            ReadDirection::Backward => total_order(selection.scan())?,
        };
        // The cursor's own copy of the bounds, and the iterator's: at most twice
        // their length, for a prefix and the end of its range.
        runs.memory.grow(3 * scan.bound_bytes())?;
        let mut cursor = self.physical_cursor(scan)?;
        let shape = Shape {
            header: <<T::Key as KeySchema>::Header as KeyHeader>::LENGTH,
            run_fields,
            bucket_fields: limits.bucket_fields.min(run_fields),
            direction,
        };
        let caps = Caps {
            max_runs: available.min(limits.max_runs),
            max_discovery_keys: limits.max_discovery_keys,
        };
        let mut found = Vec::new();
        let mut discovery = Discovery {
            cursor: &mut cursor,
            selection: &mut selection,
            shape: &shape,
            memory: &mut runs.memory,
            examined,
        };
        let discovered = if shape.bucket_fields > 0 && direction == ReadDirection::Backward {
            discovery.bucket_before(Some(caps), &mut found, &mut Vec::new(), None, None, None)?
        } else {
            discovery.runs(Some(caps), &mut found, &mut Vec::new(), None, None)?
        };
        runs.examined = *examined;
        let next_bucket = match discovered {
            Discovered::TooManyRuns => {
                return Ok(OrderedRunsDiscovery::TooManyRuns {
                    examined: runs.examined,
                });
            }
            Discovered::End => None,
            Discovered::NextBucket(start) => Some(start),
            // Discovery of the first bucket has no budget.
            Discovered::Paused(_) => return Err(StorageError::DataIntegrityError),
        };

        if found.is_empty() {
            // Nothing to merge: release the selection, the cursor and their charge.
            drop((cursor, selection, next_bucket));
            runs.memory.release();
            return Ok(OrderedRunsDiscovery::Ready(runs));
        }
        runs.memory.grow(size_of::<Merge<T::Key>>())?;
        let mut merge = Box::new(Merge {
            cursor,
            selection,
            runs: found,
            heads: BinaryHeap::new(),
            pending: Some(0),
            primed: false,
            buffer: limits.buffer.get(),
            step_budget: limits.step_budget.get(),
            direction,
            shape,
            next_bucket,
            resume: None,
            spare: Vec::new(),
        });
        merge.reserve(&mut runs.memory)?;
        runs.inner = Some(merge);
        Ok(OrderedRunsDiscovery::Ready(runs))
    }
}

impl ReadSnapshot {
    /// The distinct prefixes, the header and first `fields` fields, of the keys
    /// `request` matches, in key order, found as discovery finds runs: the
    /// first match of each prefix, then a seek past it. `None` past `max`
    /// prefixes, or once `examined`, kept up to date key by key, passes
    /// `max_keys`. Charges `memory` while it works. The prefixes it returns
    /// stay charged to `memory`, by the bytes returned with them, until the
    /// caller releases them.
    pub fn distinct_prefixes<T>(
        &self,
        request: Arc<KeySelection<T::Key>>,
        fields: usize,
        (max, max_keys): (usize, usize),
        memory: Box<dyn RetainedMemory>,
        examined: &mut u64,
    ) -> Result<Option<Prefixes>>
    where
        T: PhysicalTable,
        T::Key: OrderedKeySchema,
    {
        self.check_active()?;
        let mut memory = Accounted {
            memory,
            bytes: 0,
            selection: 0,
        };
        let metadata = self.metadata();
        if !T::is_active(&metadata) {
            return Ok(Some((Vec::new(), 0)));
        }
        let Some(mut selection) = request.cursor(
            <<T::Key as KeySchema>::Header as KeyHeader>::scope(&metadata)?,
        )?
        else {
            return Ok(Some((Vec::new(), 0)));
        };
        memory.selection(&selection)?;
        let scan = selection.scan().to_exact();
        memory.grow(3 * scan.bound_bytes())?;
        let mut cursor = self.physical_cursor(scan)?;
        let shape = Shape {
            header: <<T::Key as KeySchema>::Header as KeyHeader>::LENGTH,
            run_fields: fields,
            bucket_fields: 0,
            direction: ReadDirection::Forward,
        };
        let mut found = Vec::new();
        let discovered = Discovery {
            cursor: &mut cursor,
            selection: &mut selection,
            shape: &shape,
            memory: &mut memory,
            examined,
        }
        .runs(
            Some(Caps {
                max_runs: max,
                max_discovery_keys: max_keys,
            }),
            &mut found,
            &mut Vec::new(),
            None,
            None,
        )?;
        match discovered {
            Discovered::End => {
                // Charged for the caller before the work's own charge ends.
                let kept = found.len() * size_of::<Box<[u8]>>()
                    + found.iter().map(|run| run.prefix.len()).sum::<usize>();
                memory.memory.grow(kept)?;
                let mut prefixes = Vec::with_capacity(found.len());
                prefixes.extend(found.into_iter().map(|run| run.prefix));
                Ok(Some((prefixes, kept)))
            }
            Discovered::TooManyRuns => Ok(None),
            // Without buckets or a budget, discovery ends or passes its caps.
            Discovered::NextBucket(_) | Discovered::Paused(_) => {
                Err(StorageError::DataIntegrityError)
            }
        }
    }
}

/// The length of the prefix of `key` that holds its bucket: the header and the
/// bucket fields.
fn bucket_len<K: OrderedKeySchema>(shape: &Shape, key: &[u8]) -> Result<usize> {
    let payload = key
        .get(shape.header..)
        .ok_or(StorageError::DataIntegrityError)?;
    let mut rest = payload;
    for field in &K::FIELDS[..shape.bucket_fields] {
        field.take_encoded(&mut rest)?;
    }
    Ok(shape.header + payload.len() - rest.len())
}

impl<K: OrderedKeySchema> Discovery<'_, K> {
    /// Backwards, the runs of the last bucket with a match before `before`, a
    /// bucket's start, or before the end of the read: steps back to the last
    /// key, discovers its bucket from the bucket's start, and steps back again
    /// past a bucket without a match. Returns the start of the bucket found as
    /// [`Discovered::NextBucket`], charged, to step back from next. A
    /// discovery paused within a bucket continues from `within`.
    fn bucket_before(
        &mut self,
        caps: Option<Caps>,
        found: &mut Vec<Run>,
        spare: &mut Vec<VecDeque<Record>>,
        mut budget: Option<&mut usize>,
        mut before: Option<Box<[u8]>>,
        mut within: Option<Anchored>,
    ) -> Result<Discovered> {
        loop {
            let bucket = match within.take() {
                Some((anchor, at)) => {
                    let positioned = self.cursor.reposition(&at)?;
                    self.memory.shrink(at.len());
                    if !positioned {
                        self.memory.shrink(anchor.len());
                        return Ok(Discovered::End);
                    }
                    anchor
                }
                None => {
                    // Past the read's last key, the cursor goes to its last key.
                    let target = before.as_deref().unwrap_or(&[u8::MAX]);
                    let bucket = if self.cursor.reposition_back(target)?
                        && let Some((key, _)) = self.cursor.peek()?
                    {
                        // Charged before it is copied.
                        let len = bucket_len::<K>(self.shape, key)?;
                        self.memory.grow(len)?;
                        Some(Box::<[u8]>::from(&key[..len]))
                    } else {
                        None
                    };
                    if let Some(before) = before.take() {
                        self.memory.shrink(before.len());
                    }
                    let Some(bucket) = bucket else {
                        return Ok(Discovered::End);
                    };
                    if !self.cursor.reposition_from(&bucket)? {
                        self.memory.shrink(bucket.len());
                        return Ok(Discovered::End);
                    }
                    bucket
                }
            };
            // Only this bucket: past it in key order are the buckets the read
            // returned, even where this one has no match.
            match self.runs(caps, found, spare, Some(&bucket), budget.as_deref_mut())? {
                Discovered::TooManyRuns => {
                    self.memory.shrink(bucket.len());
                    return Ok(Discovered::TooManyRuns);
                }
                Discovered::Paused(Resume::At(at)) => {
                    return Ok(Discovered::Paused(Resume::Within { anchor: bucket, at }));
                }
                Discovered::Paused(Resume::Within { .. }) => {
                    return Err(StorageError::DataIntegrityError);
                }
                Discovered::NextBucket(next) => self.memory.shrink(next.len()),
                Discovered::End => {}
            }
            if !found.is_empty() {
                return Ok(Discovered::NextBucket(bucket));
            }
            before = Some(bucket);
        }
    }

    /// Finds runs from the cursor's position on, into `found`: all of the
    /// read's, or for a bucketed read the runs of its first match's bucket,
    /// stopping at the first key of the next. With an `anchor`, a bucket's
    /// prefix, it stops at the first key outside it, matching or not. New runs
    /// take their read-ahead buffers from `spare`. Past `caps`, discovery ends
    /// with [`Discovered::TooManyRuns`]. With a `budget`, each key examined
    /// debits it, and discovery pauses when it is spent, continuing later with
    /// the runs already in `found`.
    fn runs(
        &mut self,
        caps: Option<Caps>,
        found: &mut Vec<Run>,
        spare: &mut Vec<VecDeque<Record>>,
        anchor: Option<&[u8]>,
        mut budget: Option<&mut usize>,
    ) -> Result<Discovered> {
        let Shape {
            header,
            run_fields,
            bucket_fields,
            direction,
        } = *self.shape;
        let fields = &K::FIELDS[..run_fields];
        // The length of the bucket's prefix, once a run is found.
        let mut bucket = match found.first() {
            Some(first) if bucket_fields > 0 => Some(bucket_len::<K>(self.shape, &first.prefix)?),
            _ => None,
        };
        loop {
            let step = {
                let Some((key, _)) = self.cursor.peek()? else {
                    break;
                };
                if anchor.is_some_and(|anchor| !key.starts_with(anchor)) {
                    break;
                }
                if let Some(budget) = budget.as_deref_mut() {
                    if *budget == 0 {
                        self.memory.grow(key.len())?;
                        return Ok(Discovered::Paused(Resume::At(Box::from(key))));
                    }
                    *budget -= 1;
                }
                *self.examined += 1;
                if caps.is_some_and(|caps| *self.examined > caps.max_discovery_keys as u64) {
                    return Ok(Discovered::TooManyRuns);
                }
                let matched = self.selection.evaluate(key)?;
                self.memory.selection(self.selection)?;
                match matched {
                    KeyMatch::Done => Step::Stop,
                    KeyMatch::Skip => Step::Next,
                    KeyMatch::Seek => Step::Seek,
                    KeyMatch::Match => {
                        let payload = key.get(header..).ok_or(StorageError::DataIntegrityError)?;
                        let mut rest = payload;
                        let mut bucket_len = header;
                        for (index, field) in fields.iter().enumerate() {
                            field.take_encoded(&mut rest)?;
                            if index + 1 == bucket_fields {
                                bucket_len = header + payload.len() - rest.len();
                            }
                        }
                        if bucket_fields > 0 {
                            match (bucket, found.first()) {
                                (None, _) => bucket = Some(bucket_len),
                                (Some(len), Some(first))
                                    if len != bucket_len || first.prefix[..len] != key[..len] =>
                                {
                                    self.memory.grow(key.len())?;
                                    return Ok(Discovered::NextBucket(Box::from(key)));
                                }
                                _ => {}
                            }
                        }
                        if caps.is_some_and(|caps| found.len() == caps.max_runs) {
                            return Ok(Discovered::TooManyRuns);
                        }
                        let max_runs = caps.map_or(usize::MAX, |caps| caps.max_runs);
                        reserve_run(found, max_runs, self.memory)?;
                        let prefix_len = header + payload.len() - rest.len();
                        let prefix = &key[..prefix_len];
                        // Forward, the run starts at this key, its first match.
                        // Backward, it reads back from the end of what the
                        // selection may match in the run, and stops at this key.
                        let (start, floor): (Box<[u8]>, _) = match direction {
                            ReadDirection::Forward => {
                                self.memory.grow(prefix_len + key.len())?;
                                (Box::from(key), None)
                            }
                            ReadDirection::Backward => {
                                let end = self.selection.end_within(prefix, run_fields)?;
                                if end <= key {
                                    return Err(StorageError::DataIntegrityError);
                                }
                                self.memory.grow(prefix_len + key.len() + end.len())?;
                                let end = Box::from(end);
                                self.memory.selection(self.selection)?;
                                (end, Some(Box::from(key)))
                            }
                        };
                        found.push(Run {
                            prefix: Box::from(prefix),
                            records: spare.pop().unwrap_or_default(),
                            resume: Some(start),
                            floor,
                        });
                        Step::PastRun
                    }
                }
            };
            match step {
                Step::Next => self.cursor.advance()?,
                Step::Seek => {
                    if !self.cursor.reposition(self.selection.seek_target())? {
                        break;
                    }
                }
                Step::PastRun => {
                    let prefix = &found.last().ok_or(StorageError::DataIntegrityError)?.prefix;
                    // The target is held only for the seek.
                    self.memory.grow(prefix.len())?;
                    let positioned = match successor(prefix) {
                        Some(next) => self.cursor.reposition(&next)?,
                        None => false,
                    };
                    self.memory.shrink(prefix.len());
                    if !positioned {
                        break;
                    }
                }
                Step::Stop => break,
            }
        }
        Ok(Discovered::End)
    }
}

impl<T: PhysicalTable> OrderedRuns<T>
where
    T::Key: OrderedKeySchema,
{
    /// Moves to the next key in merge order. While runs are being read ahead, a
    /// step that runs out of its budget reports [`CursorStep::Skipped`]. Once the
    /// read is done, or a step fails, it releases everything it holds, its
    /// snapshot included.
    pub fn advance(&mut self) -> Result<CursorStep> {
        let step = self.step();
        if !matches!(step, Ok(CursorStep::Record | CursorStep::Skipped)) {
            self.current = None;
            self.inner = None;
            self.memory.release();
        }
        step
    }

    fn step(&mut self) -> Result<CursorStep> {
        let Some(merge) = &mut self.inner else {
            return Ok(CursorStep::Done);
        };
        merge.cursor.snapshot().check_active()?;
        if let Some(head) = self.current.take() {
            let (run, bytes) = (head.run, head.record.bytes());
            drop(head);
            self.memory.shrink(bytes);
            if !merge.push_head(run) {
                merge.pending = Some(run);
            }
        }

        let mut budget = merge.step_budget;
        loop {
            while let Some(run) = merge.pending {
                match merge.fill::<T>(run, &mut budget, &mut self.memory, &mut self.examined)? {
                    Fill::Paused => return Ok(CursorStep::Skipped),
                    Fill::Done => {
                        merge.push_head(run);
                        merge.pending = if merge.primed || run + 1 == merge.runs.len() {
                            merge.primed = true;
                            None
                        } else {
                            Some(run + 1)
                        };
                    }
                }
            }

            match merge.heads.pop() {
                Some(Reverse(head)) => {
                    self.current = Some(head);
                    return Ok(CursorStep::Record);
                }
                None => {
                    match merge.next_bucket(&mut self.memory, &mut self.examined, &mut budget)? {
                        NextBucket::Ready => {}
                        NextBucket::Paused => return Ok(CursorStep::Skipped),
                        NextBucket::Done => return Ok(CursorStep::Done),
                    }
                }
            }
        }
    }

    /// The current key, decoded from its start, and its value.
    pub fn current(&self) -> Result<(KeyDecoder<'_, T::Key>, &[u8])> {
        let (Some(merge), Some(head)) = (&self.inner, &self.current) else {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "ordered runs are not positioned on a key"
            )));
        };
        merge.cursor.snapshot().check_active()?;
        let Record { key, value } = &head.record;
        Ok((<T::Key as KeySchema>::decoder(key)?, value))
    }

    /// The bytes charged for what the read holds now.
    pub fn retained_bytes(&self) -> usize {
        self.memory.bytes
    }

    /// Keys examined so far, by discovery and by reading ahead, including the
    /// ones the selection or the ownership check rejected.
    pub fn examined(&self) -> u64 {
        self.examined
    }

    /// How many runs the read merges; none once they're exhausted.
    pub fn runs(&self) -> usize {
        self.inner.as_ref().map_or(0, |merge| merge.runs.len())
    }
}

impl<K: OrderedKeySchema> Merge<K> {
    /// Gives every run a read-ahead buffer of `buffer` records and the heads
    /// room for one per run, charging what is allocated: before, and for any
    /// excess the allocator gives, right after.
    fn reserve(&mut self, memory: &mut Accounted) -> Result<()> {
        let buffer = self.buffer;
        for run in &mut self.runs {
            let had = run.records.capacity();
            if had < buffer {
                memory.grow((buffer - had) * size_of::<Record>())?;
                run.records.reserve_exact(buffer);
                memory.grow((run.records.capacity() - buffer) * size_of::<Record>())?;
            }
        }
        let had = self.heads.capacity();
        let runs = self.runs.len();
        if had < runs {
            memory.grow((runs - had) * size_of::<Reverse<Head>>())?;
            self.heads.reserve_exact(runs - self.heads.len());
            memory.grow((self.heads.capacity() - runs) * size_of::<Reverse<Head>>())?;
        }
        Ok(())
    }

    /// Discovers the runs of the next bucket once the current bucket's runs are
    /// exhausted, their read-ahead buffers, empty, serving the next ones; or
    /// continues the discovery a spent `budget` paused. The read has emitted
    /// earlier buckets' rows, so it can't read another way any more: it merges
    /// all of the bucket's runs, charged to its memory. Its discovery has no
    /// key limit; it pauses with the step's budget instead.
    fn next_bucket(
        &mut self,
        memory: &mut Accounted,
        examined: &mut u64,
        budget: &mut usize,
    ) -> Result<NextBucket> {
        let resume = self.resume.take();
        let start = match resume {
            Some(_) => None,
            None => {
                let Some(start) = self.next_bucket.take() else {
                    return Ok(NextBucket::Done);
                };
                let mut exhausted = std::mem::take(&mut self.runs);
                for run in exhausted.drain(..) {
                    let held = run.prefix.len()
                        + run.resume.as_ref().map_or(0, |resume| resume.len())
                        + run.floor.as_ref().map_or(0, |floor| floor.len());
                    memory.shrink(held);
                    if self.spare.len() == self.spare.capacity() {
                        let additional = self.spare.len().max(4);
                        memory.grow(additional * size_of::<VecDeque<Record>>())?;
                        self.spare.reserve_exact(additional);
                        let excess = self.spare.capacity() - self.spare.len() - additional;
                        memory.grow(excess * size_of::<VecDeque<Record>>())?;
                    }
                    self.spare.push(run.records);
                }
                self.runs = exhausted;
                Some(start)
            }
        };
        let mut runs = std::mem::take(&mut self.runs);
        let mut discovery = Discovery {
            cursor: &mut self.cursor,
            selection: &mut self.selection,
            shape: &self.shape,
            memory,
            examined,
        };
        let discovered = match (self.direction, resume, start) {
            (ReadDirection::Forward, Some(Resume::At(at)), None)
            | (ReadDirection::Forward, None, Some(at)) => {
                let positioned = discovery.cursor.reposition(&at)?;
                discovery.memory.shrink(at.len());
                drop(at);
                if positioned {
                    discovery.runs(None, &mut runs, &mut self.spare, None, Some(budget))
                } else {
                    Ok(Discovered::End)
                }
            }
            (ReadDirection::Backward, Some(Resume::Within { anchor, at }), None) => discovery
                .bucket_before(
                    None,
                    &mut runs,
                    &mut self.spare,
                    Some(budget),
                    None,
                    Some((anchor, at)),
                ),
            (ReadDirection::Backward, None, Some(start)) => discovery.bucket_before(
                None,
                &mut runs,
                &mut self.spare,
                Some(budget),
                Some(start),
                None,
            ),
            _ => Err(StorageError::DataIntegrityError),
        };
        self.runs = runs;
        match discovered? {
            Discovered::Paused(resume) => {
                self.resume = Some(resume);
                return Ok(NextBucket::Paused);
            }
            Discovered::NextBucket(start) => self.next_bucket = Some(start),
            Discovered::End => self.next_bucket = None,
            // Without caps, discovery finds every run.
            Discovered::TooManyRuns => return Err(StorageError::DataIntegrityError),
        }
        if self.runs.is_empty() {
            return Ok(NextBucket::Done);
        }
        self.reserve(memory)?;
        self.pending = Some(0);
        self.primed = false;
        Ok(NextBucket::Ready)
    }
}

impl<K: KeySchema> Merge<K> {
    /// Moves the front record of `run` to the heads, if it has one.
    fn push_head(&mut self, run: usize) -> bool {
        let Run {
            prefix, records, ..
        } = &mut self.runs[run];
        let Some(record) = records.pop_front() else {
            return false;
        };
        self.heads.push(Reverse(Head {
            record,
            prefix: prefix.len(),
            run,
            backward: self.direction == ReadDirection::Backward,
        }));
        true
    }

    /// Reads up to `buffer` more owned records of `run`, examining at most
    /// `budget` keys.
    fn fill<T: PhysicalTable<Key = K>>(
        &mut self,
        run: usize,
        budget: &mut usize,
        memory: &mut Accounted,
        examined: &mut u64,
    ) -> Result<Fill> {
        match self.direction {
            ReadDirection::Forward => self.fill_forward::<T>(run, budget, memory, examined),
            ReadDirection::Backward => self.fill_backward::<T>(run, budget, memory, examined),
        }
    }

    fn fill_forward<T: PhysicalTable<Key = K>>(
        &mut self,
        run: usize,
        budget: &mut usize,
        memory: &mut Accounted,
        examined: &mut u64,
    ) -> Result<Fill> {
        let Self {
            cursor,
            selection,
            runs,
            buffer,
            ..
        } = self;
        let run = &mut runs[run];
        let Some(resume) = run.resume.take() else {
            return Ok(Fill::Done);
        };
        let positioned = cursor.reposition(&resume)?;
        let resumed = resume.len();
        drop(resume);
        memory.shrink(resumed);
        if !positioned {
            return Ok(Fill::Done);
        }
        while run.records.len() < *buffer {
            let step = {
                let Some((key, value)) = cursor.peek()? else {
                    return Ok(Fill::Done);
                };
                if !key.starts_with(&run.prefix) {
                    return Ok(Fill::Done);
                }
                if *budget == 0 {
                    // Resume from this key in a later step.
                    memory.grow(key.len())?;
                    run.resume = Some(Box::from(key));
                    return Ok(Fill::Paused);
                }
                *budget -= 1;
                *examined += 1;
                let matched = selection.evaluate(key)?;
                memory.selection(selection)?;
                match matched {
                    KeyMatch::Done => Step::Stop,
                    KeyMatch::Skip => Step::Next,
                    KeyMatch::Seek => Step::Seek,
                    KeyMatch::Match => {
                        if T::owns_record(K::decoder(key)?, &cursor.snapshot().metadata())? {
                            memory.grow(key.len() + value.len())?;
                            run.records.push_back(Record {
                                key: Box::from(key),
                                value: Box::from(value),
                            });
                        }
                        Step::Next
                    }
                }
            };
            match step {
                Step::Next => cursor.advance()?,
                Step::Seek => {
                    let target = selection.seek_target();
                    if !target.starts_with(&run.prefix) || !cursor.reposition(target)? {
                        return Ok(Fill::Done);
                    }
                }
                Step::PastRun | Step::Stop => return Ok(Fill::Done),
            }
        }
        // Read ahead in full: continue from the next key, if it is in the run.
        if let Some((key, _)) = cursor.peek()?
            && key.starts_with(&run.prefix)
        {
            memory.grow(key.len())?;
            run.resume = Some(Box::from(key));
        }
        Ok(Fill::Done)
    }

    /// [`Self::fill`] backwards, from the key the run resumes at back to its
    /// first match. Seeks only move forward, so every key in between is checked
    /// against the selection, and the ones it doesn't match are stepped over.
    fn fill_backward<T: PhysicalTable<Key = K>>(
        &mut self,
        run: usize,
        budget: &mut usize,
        memory: &mut Accounted,
        examined: &mut u64,
    ) -> Result<Fill> {
        let Self {
            cursor,
            selection,
            runs,
            buffer,
            ..
        } = self;
        let run = &mut runs[run];
        let floor = run
            .floor
            .as_deref()
            .ok_or(StorageError::DataIntegrityError)?;
        let Some(resume) = run.resume.take() else {
            return Ok(Fill::Done);
        };
        let positioned = cursor.reposition_back(&resume)?;
        let resumed = resume.len();
        drop(resume);
        memory.shrink(resumed);
        if !positioned {
            return Ok(Fill::Done);
        }
        while run.records.len() < *buffer {
            {
                let Some((key, value)) = cursor.peek()? else {
                    return Ok(Fill::Done);
                };
                if key < floor {
                    // Before the run's first match, it's done.
                    return Ok(Fill::Done);
                }
                // Past the run's end only at the first key: the end it reads
                // back from may itself be a key.
                if key.starts_with(&run.prefix) {
                    if *budget == 0 {
                        // Resume from this key in a later step.
                        memory.grow(key.len())?;
                        run.resume = Some(Box::from(key));
                        return Ok(Fill::Paused);
                    }
                    *budget -= 1;
                    *examined += 1;
                    let matched = selection.evaluate(key)?;
                    memory.selection(selection)?;
                    if matches!(matched, KeyMatch::Match)
                        && T::owns_record(K::decoder(key)?, &cursor.snapshot().metadata())?
                    {
                        memory.grow(key.len() + value.len())?;
                        run.records.push_back(Record {
                            key: Box::from(key),
                            value: Box::from(value),
                        });
                    }
                }
            }
            cursor.retreat()?;
        }
        // Read ahead in full: continue from the previous key, if it isn't before
        // the run's first match.
        if let Some((key, _)) = cursor.peek()?
            && key >= floor
        {
            memory.grow(key.len())?;
            run.resume = Some(Box::from(key));
        }
        Ok(Fill::Done)
    }
}

/// Exact copies of `scan`'s bounds, read in total order: a backward seek may
/// target the key just past a run, outside the prefix that prefix seeks stay
/// within.
fn total_order(scan: &PhysicalScan<Bytes>) -> Result<PhysicalScan<Bytes>> {
    let (kind, start, end) = match scan {
        PhysicalScan::Prefix(kind, prefix) => (
            *kind,
            prefix,
            successor(prefix).ok_or(StorageError::DataIntegrityError)?,
        ),
        PhysicalScan::RangeExclusive(kind, _, start, end) => (*kind, start, end.to_vec()),
    };
    Ok(PhysicalScan::RangeExclusive(
        kind,
        ScanMode::TotalOrder,
        Bytes::copy_from_slice(start),
        Bytes::copy_from_slice(&end),
    ))
}

/// Makes room for one more run, growing `runs` geometrically up to `max_runs`,
/// and charges the slots it adds before allocating them.
fn reserve_run(runs: &mut Vec<Run>, max_runs: usize, memory: &mut Accounted) -> Result<()> {
    if runs.len() < runs.capacity() {
        return Ok(());
    }
    let additional = runs.len().max(4).min(max_runs - runs.len());
    memory.grow(additional * size_of::<Run>())?;
    let before = runs.capacity();
    runs.reserve_exact(additional);
    // In case the allocation is larger than requested.
    memory.grow((runs.capacity() - before - additional) * size_of::<Run>())
}

/// The smallest key after every key that starts with `prefix`, if there is one.
/// Allocates at most `prefix.len()` bytes.
fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    while let Some(last) = next.last_mut() {
        if *last < u8::MAX {
            *last += 1;
            return Some(next);
        }
        next.pop();
    }
    None
}
