// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Finished entries through the entry index `finished_entry_by_outcome/1`,
//! newest finished first.
//!
//! The index holds one key per entry whose stored stage is Finished, sorted by
//! stored status, service and handler, then by the time the entry entered
//! Finished, newest first. A query on finished entries, perhaps of a few
//! statuses, ordered by that time, merges the matching runs into one order and
//! fetches each entry's current status, instead of reading every entry status
//! of the partition:
//!
//! ```text
//! index runs                                entry statuses, one multi-get per window
//! (failed,    "Utils", "call"): t9 t4   ─┐
//!                                        ├─▶  t9 t8 t5 t4 …, each checked
//! (succeeded, "Utils", "call"): t8 t5   ─┘
//! ```
//!
//! The index finds every Finished entry once it is Ready on the query's own
//! snapshot; otherwise, or if finding the runs takes more than one read allows,
//! the read scans the entry statuses instead, before it returns anything. It
//! reports which it did ([`EntryIndexPath`]), so that a caller relying on the
//! order sorts a scan.
//!
//! A key is used only if its entry's current status is present on the snapshot
//! and is still that key's: Finished, with the key's status, transition time
//! and incarnation. A missing or stale reference is skipped and counted, as the
//! stage reads do; a malformed status, or a storage error, fails the read.
//! Everything the read holds is charged to the caller's memory, and released
//! as soon as it ends or fails.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use restate_clock::UniqueTimestamp;
use restate_rocksdb::reads::{OwnedPinnedBatch, ReadSnapshot as RocksReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{RawStatusHeaderRef, Stage, Status};
use restate_storage_query_api::scan::CursorStep;
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::index::FinishedEntryByOutcome as FinishedIndex;
use restate_types::identifiers::{BaseEntryId, CanonicalEntryId};

use crate::index::RegisteredIndex;
use crate::invocation_status_table::entry_index_read::{
    EntryIndexExecution, EntryIndexPath, Execution, Held, ScanReason, SharedMemory,
};
use crate::keys::selection::{KeySelection, PrepareKeySelection};
use crate::keys::{EncodeTableKey, KeyKind};
use crate::ordered_runs::{OrderedRuns, OrderedRunsDiscovery, OrderedRunsLimits};
use crate::partition_db::PhysicalPartition;
use crate::registry::IndexKind;
use crate::{ReadSnapshot, Result};

use super::{
    ENTRY_STATUS_MULTI_GET_BATCH_SIZE, EntryStatusKey, FinishedEntryByOutcome,
    FinishedEntryByOutcomeKey,
};

/// A run is the finished entries of one status, service and handler.
const RUN_FIELDS: usize = 3;

/// The same as the invocation entry-index read's: a few statuses of a few
/// hundred handlers stay within one read.
const LIMITS: OrderedRunsLimits = OrderedRunsLimits {
    max_runs: 256,
    max_discovery_keys: 4096,
    buffer: NonZeroUsize::new(8).unwrap(),
    step_budget: NonZeroUsize::new(256).unwrap(),
};

const WINDOW: usize = ENTRY_STATUS_MULTI_GET_BATCH_SIZE.get();
const KEY_LEN: usize = EntryStatusKey::serialized_length_fixed();

/// A read of finished entries through the index, restricted by the index's
/// columns. The restriction may admit more entries than the query matches: the
/// caller keeps its predicate.
pub struct FinishedRead {
    index: Arc<KeySelection<FinishedEntryByOutcomeKey>>,
}

impl FinishedRead {
    pub fn prepare(selection: &Selection<FinishedIndex>) -> Result<Self> {
        Ok(Self {
            index: Arc::new(FinishedEntryByOutcomeKey::prepare_candidates(selection)?),
        })
    }

    /// Why a read on `snapshot` can't go through the index, if it can't: the
    /// index isn't Ready on it.
    pub fn unusable(snapshot: &ReadSnapshot) -> Result<Option<ScanReason>> {
        Ok(snapshot
            .ready_index(FinishedEntryByOutcome::DEFINITION, IndexKind::Secondary)
            .is_err()
            .then_some(ScanReason::NotReady))
    }
}

/// An index key's reference to an entry status, with what the status must
/// still say.
struct Reference {
    id: CanonicalEntryId,
    status: Status,
    transitioned_at: UniqueTimestamp,
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
pub(super) struct FinishedCursor {
    /// `None` once the read ended: it holds nothing then.
    runs: Option<OrderedRuns<FinishedEntryByOutcome>>,
    runs_done: bool,
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

impl Drop for FinishedCursor {
    /// A read dropped part-way, say past a query's LIMIT, still tells what it
    /// examined.
    fn drop(&mut self) {
        self.report(0);
    }
}

impl FinishedCursor {
    /// The cursor, or `None` if the read scans instead. Nothing is held then.
    /// Either way, `execution` learns the path before this returns.
    pub(super) fn open(
        snapshot: &ReadSnapshot,
        read: &FinishedRead,
        execution: &EntryIndexExecution,
    ) -> Result<Option<Self>> {
        let Execution {
            memory,
            chosen,
            fetched,
            skipped,
        } = execution.take()?;
        let memory = SharedMemory(Arc::new(Mutex::new(memory)));
        // Discovery's work counts even if it fails: the error stays the read's.
        let mut discovered = 0;
        let opened = match Self::try_open(snapshot, read, memory, &mut discovered) {
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
                window: Vec::with_capacity(WINDOW),
                next: None,
                key_buf: Vec::with_capacity(WINDOW * KEY_LEN),
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
                runs: cursor.runs.as_ref().map_or(0, OrderedRuns::runs),
            },
            Err(reason) => EntryIndexPath::Scan(*reason),
        });
        Ok(cursor.ok())
    }

    #[allow(clippy::type_complexity)]
    fn try_open(
        snapshot: &ReadSnapshot,
        read: &FinishedRead,
        memory: SharedMemory,
        discovered: &mut u64,
    ) -> Result<std::result::Result<(OrderedRuns<FinishedEntryByOutcome>, Held), (ScanReason, u64)>>
    {
        if let Some(reason) = FinishedRead::unusable(snapshot)? {
            return Ok(Err((reason, 0)));
        }
        let mut held = Held {
            memory: memory.clone(),
            bytes: 0,
        };
        held.grow(
            read.index.retained_bytes()
                + WINDOW * std::mem::size_of::<Reference>()
                + WINDOW * KEY_LEN,
        )?;
        Ok(
            match snapshot.ordered_runs_counted::<FinishedEntryByOutcome>(
                Arc::clone(&read.index),
                RUN_FIELDS,
                LIMITS,
                Box::new(memory),
                discovered,
            )? {
                OrderedRunsDiscovery::Ready(runs) => Ok((runs, held)),
                OrderedRunsDiscovery::TooManyRuns { examined } => {
                    Err((ScanReason::TooManyRuns, examined))
                }
            },
        )
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
                    "the finished-entry index read failed earlier"
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
                let key = runs.current()?.0.decode_all()?;
                self.window.push(Reference {
                    id: key.canonical_id,
                    status: key.status,
                    transitioned_at: key.transitioned_at.0,
                });
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
        if header.stage == Stage::Finished
            && header.status == reference.status
            && header.stats.transitioned_at == reference.transitioned_at
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
        EntryContext, EntryKey, EntryMetadata, EntryStateRef, RawStatusHeader, WriteVQueueTable,
    };
    use restate_storage_query_api::scan::ReadCursor;
    use restate_storage_query_api::selection::ValuePredicate;
    use restate_storage_query_api::tables::index::FinishedEntryByOutcomeClause;
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

    fn run(
        store: &PartitionStore,
        selection: Selection<FinishedIndex>,
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
        let opened = own.entry_status_cursor(EntryStatusScan::Finished {
            read: Arc::new(FinishedRead::prepare(&selection).unwrap()),
            fallback: Arc::new(
                EntryStatusKeys::prepare(&Selection::All, Some(KeyRange::FULL)).unwrap(),
            ),
            execution: Arc::new(execution),
        });
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
                            Ok((id, header)) => run.rows.push((
                                id,
                                header.status,
                                header.stats.transitioned_at.to_unix_millis().as_u64(),
                            )),
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
    /// malformed status fails the read. Without a Ready index, it scans.
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
        let key_of = |id: &BaseEntryId| {
            let mut key = Vec::new();
            EntryStatusKey::from(*id).serialize_to(&mut key);
            key
        };
        let stored = |store: &crate::PartitionStore, id: &BaseEntryId| {
            let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
            snapshot
                .storage_snapshot()
                .reader()
                .get(KeyKind::VQueueEntryStatus, &key_of(id))
                .unwrap()
                .unwrap()
                .to_vec()
        };
        let rewrite = |value: &[u8], edit: &dyn Fn(&mut RawStatusHeader)| {
            let mut rest = value;
            let mut header = RawStatusHeader::decode_length_delimited(&mut rest).unwrap();
            edit(&mut header);
            let mut bytes = header.encode_length_delimited_to_vec();
            bytes.extend_from_slice(rest);
            bytes
        };

        // A refused charge fails the read with the refusal, never a scan in
        // its place; every charge is released and the snapshot completes.
        // Refused for the read's own state, as it opens: no work, no path.
        let fixed = FinishedRead::prepare(&Selection::All)
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

        // Not Ready: a partition that doesn't maintain the index scans.
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
        assert_eq!(path, Some(EntryIndexPath::Scan(ScanReason::NotReady)));
        assert_eq!(rows.len(), 1);
    }
}
