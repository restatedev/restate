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
//!   - the bytes it copies (run prefixes, keys to resume from, keys and values
//!     read ahead, the key that seeks past a run), each in its own allocation
//!     of exactly its length, released once dropped;
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

use restate_storage_api::StorageError;
use restate_storage_query_api::scan::CursorStep;

use crate::keys::KeyDecoder;
use crate::keys::header::{KeyHeader, KeySchema};
use crate::keys::selection::{KeyMatch, KeySelection, KeySelectionCursor, OrderedKeySchema};
use crate::query_read::SnapshotCursor;
use crate::table::PhysicalTable;
use crate::{ReadSnapshot, Result};

#[cfg(test)]
mod tests;

/// How much work a read may do and how many keys each run reads ahead.
#[derive(Debug, Clone, Copy)]
pub struct OrderedRunsLimits {
    /// The most runs one read merges.
    pub max_runs: usize,
    /// The most keys discovery evaluates, including keys the selection rejects
    /// and runs without a key in the read's window.
    pub max_discovery_keys: usize,
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
}

struct Run {
    /// The header and the leading fields every key of the run shares.
    prefix: Box<[u8]>,
    /// Records read ahead, in key order, after the run's head. Reading ahead
    /// starts once the run's records are consumed and stops at `buffer`, so the
    /// deque never grows.
    records: VecDeque<Record>,
    /// Where the run continues, once its records read ahead are consumed.
    resume: Option<Box<[u8]>>,
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

/// A run's front record, ordered by its key after the run's prefix.
struct Head {
    record: Record,
    prefix: usize,
    run: usize,
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
        self.order().cmp(&other.order())
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
    /// `run_fields` fields, and prepares to merge them. What the read holds is
    /// charged to `memory`.
    pub fn ordered_runs<T>(
        &self,
        request: Arc<KeySelection<T::Key>>,
        run_fields: usize,
        limits: OrderedRunsLimits,
        memory: Box<dyn RetainedMemory>,
    ) -> Result<OrderedRunsDiscovery<T>>
    where
        T: PhysicalTable,
        T::Key: OrderedKeySchema,
    {
        self.ordered_runs_counted(request, run_fields, limits, memory, &mut 0)
    }

    /// [`Self::ordered_runs`], keeping in `examined` the keys discovery examined
    /// so far, so that a caller can tell that work even when discovery fails.
    pub fn ordered_runs_counted<T>(
        &self,
        request: Arc<KeySelection<T::Key>>,
        run_fields: usize,
        limits: OrderedRunsLimits,
        memory: Box<dyn RetainedMemory>,
        examined: &mut u64,
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
        runs.memory.selection(&selection)?;
        // The cursor's own copy of the bounds, and the iterator's: at most twice
        // their length, for a prefix and the end of its range.
        runs.memory.grow(3 * selection.scan().bound_bytes())?;
        let mut cursor = self.physical_cursor(selection.scan().to_exact())?;
        let header = <<T::Key as KeySchema>::Header as KeyHeader>::LENGTH;
        let fields = &<T::Key as OrderedKeySchema>::FIELDS[..run_fields];

        let mut found = Vec::new();
        loop {
            let step = {
                let Some((key, _)) = cursor.peek()? else {
                    break;
                };
                runs.examined += 1;
                *examined = runs.examined;
                if runs.examined > limits.max_discovery_keys as u64 {
                    return Ok(OrderedRunsDiscovery::TooManyRuns {
                        examined: runs.examined,
                    });
                }
                let matched = selection.evaluate(key)?;
                runs.memory.selection(&selection)?;
                match matched {
                    KeyMatch::Done => Step::Stop,
                    KeyMatch::Skip => Step::Next,
                    KeyMatch::Seek => Step::Seek,
                    KeyMatch::Match => {
                        if found.len() == limits.max_runs {
                            return Ok(OrderedRunsDiscovery::TooManyRuns {
                                examined: runs.examined,
                            });
                        }
                        reserve_run(&mut found, limits.max_runs, &mut runs.memory)?;
                        let payload = key.get(header..).ok_or(StorageError::DataIntegrityError)?;
                        let mut rest = payload;
                        for field in fields {
                            field.take_encoded(&mut rest)?;
                        }
                        let prefix_len = header + payload.len() - rest.len();
                        runs.memory.grow(prefix_len + key.len())?;
                        found.push(Run {
                            prefix: Box::from(&key[..prefix_len]),
                            records: VecDeque::new(),
                            resume: Some(Box::from(key)),
                        });
                        Step::PastRun
                    }
                }
            };
            match step {
                Step::Next => cursor.advance()?,
                Step::Seek => {
                    if !cursor.reposition(selection.seek_target())? {
                        break;
                    }
                }
                Step::PastRun => {
                    let prefix = &found.last().ok_or(StorageError::DataIntegrityError)?.prefix;
                    // The target is held only for the seek.
                    runs.memory.grow(prefix.len())?;
                    let positioned = match successor(prefix) {
                        Some(next) => cursor.reposition(&next)?,
                        None => false,
                    };
                    runs.memory.shrink(prefix.len());
                    if !positioned {
                        break;
                    }
                }
                Step::Stop => break,
            }
        }

        if found.is_empty() {
            // Nothing to merge: release the selection, the cursor and their charge.
            drop((cursor, selection));
            runs.memory.release();
            return Ok(OrderedRunsDiscovery::Ready(runs));
        }
        let buffer = limits.buffer.get();
        let slots = found.len() * buffer;
        runs.memory.grow(
            size_of::<Merge<T::Key>>()
                + slots * size_of::<Record>()
                + found.len() * size_of::<Reverse<Head>>(),
        )?;
        for run in &mut found {
            run.records.reserve_exact(buffer);
        }
        let heads = BinaryHeap::with_capacity(found.len());
        // In case the allocations are larger than requested.
        let records = found
            .iter()
            .map(|run| run.records.capacity())
            .sum::<usize>();
        runs.memory.grow(
            (records - slots) * size_of::<Record>()
                + (heads.capacity() - found.len()) * size_of::<Reverse<Head>>(),
        )?;
        runs.inner = Some(Box::new(Merge {
            cursor,
            selection,
            runs: found,
            heads,
            pending: Some(0),
            primed: false,
            buffer,
            step_budget: limits.step_budget.get(),
        }));
        Ok(OrderedRunsDiscovery::Ready(runs))
    }
}

impl<T: PhysicalTable> OrderedRuns<T> {
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
                Ok(CursorStep::Record)
            }
            None => Ok(CursorStep::Done),
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
