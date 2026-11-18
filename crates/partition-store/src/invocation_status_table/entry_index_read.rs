// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Invocations found through the entry index, newest created first.
//!
//! The entry index `entry_by_service_created/1` holds one key per vqueue entry,
//! sorted by service, handler and creation time, newest first. A query that
//! names a service, and perhaps a handler, reads the matching runs merged into
//! creation order and fetches the invocation of each entry, instead of reading
//! every invocation status of the partition:
//!
//! ```text
//! entry index runs                     invocation statuses, 25 per multi-get
//! (Promises, "promise"): t9 t7 t4   ─┐
//!                                    ├─▶  t9 t8 t7 t5 t4 …, each checked
//! (Promises, "resolve"): t8 t5      ─┘
//! ```
//!
//! The index finds every invocation only once the partition has fully migrated
//! to vqueues and the index is Ready ([`EntryIndexRead::usable`]). Both are read
//! through the query's own snapshot when the read opens. If either doesn't
//! hold, or finding the runs takes more than one read allows, the read scans the
//! invocation statuses instead, before it returns anything: the result is
//! complete either way. It reports which it did ([`EntryIndexPath`]), so that a
//! caller relying on creation order sorts a scan.
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
use std::sync::{Arc, Mutex};

use restate_rocksdb::reads::{OwnedPinnedBatch, ReadSnapshot as RocksReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_query_api::scan::CursorStep;
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::index::EntryByServiceCreated as EntryIndex;
use restate_types::identifiers::InvocationId;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::Seq;

use crate::index::RegisteredIndex;
use crate::keys::selection::{KeySelection, PrepareKeySelection};
use crate::keys::{EncodeTableKey, KeyKind};
use crate::ordered_runs::{OrderedRuns, OrderedRunsDiscovery, OrderedRunsLimits, RetainedMemory};
use crate::partition_db::PhysicalPartition;
use crate::registry::IndexKind;
use crate::vqueue_table::{EntryByServiceCreated, EntryByServiceCreatedKey};
use crate::{ReadSnapshot, Result};

use super::{
    INVOCATION_STATUS_MULTI_GET_BATCH_SIZE, InvocationStatusKey, create_invocation_status_key,
};

/// A run is the entries of one service and handler.
const RUN_FIELDS: usize = 2;

/// Not measured. A service has few handlers, so a query naming a few services
/// stays well within `max_runs`.
const LIMITS: OrderedRunsLimits = OrderedRunsLimits {
    max_runs: 256,
    max_discovery_keys: 4096,
    buffer: NonZeroUsize::new(8).unwrap(),
    step_budget: NonZeroUsize::new(256).unwrap(),
};

/// Statuses fetched per multi-get, and the window that holds their invocations.
const WINDOW: usize = INVOCATION_STATUS_MULTI_GET_BATCH_SIZE.get();
const KEY_LEN: usize = InvocationStatusKey::serialized_length_fixed();

/// How a read of the entry index went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryIndexPath {
    /// Through the index: the invocations come newest created first.
    Index,
    /// A scan of the invocation statuses, in key order.
    Scan,
}

/// What one execution of an entry-index read charges and reports. The read takes
/// it when it opens.
pub struct EntryIndexExecution(Mutex<Option<Execution>>);

struct Execution {
    memory: Box<dyn RetainedMemory>,
    chosen: Box<dyn FnOnce(EntryIndexPath) + Send>,
    fetched: Box<dyn Fn(usize) + Send>,
}

impl EntryIndexExecution {
    /// `memory` is charged for what the index read holds. `chosen` learns which
    /// path the read takes before it returns any row. `fetched` learns how many
    /// statuses each multi-get requests, once it returns: the window's slots,
    /// before any is inspected or charged.
    pub fn new(
        memory: Box<dyn RetainedMemory>,
        chosen: impl FnOnce(EntryIndexPath) + Send + 'static,
        fetched: impl Fn(usize) + Send + 'static,
    ) -> Self {
        Self(Mutex::new(Some(Execution {
            memory,
            chosen: Box::new(chosen),
            fetched: Box::new(fetched),
        })))
    }
}

/// One charge shared by the merge of the runs and the cursor's own state.
#[derive(Clone)]
struct SharedMemory(Arc<Mutex<Box<dyn RetainedMemory>>>);

impl RetainedMemory for SharedMemory {
    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.0.lock().expect("not poisoned").grow(bytes)
    }

    fn shrink(&mut self, bytes: usize) {
        self.0.lock().expect("not poisoned").shrink(bytes)
    }
}

/// What the cursor itself holds, released when it is dropped.
struct Held {
    memory: SharedMemory,
    bytes: usize,
}

impl Held {
    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.memory.grow(bytes)?;
        self.bytes += bytes;
        Ok(())
    }

    fn shrink(&mut self, bytes: usize) {
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
/// columns. The restriction may admit more entries than the query matches: the
/// caller keeps its predicate.
pub struct EntryIndexRead {
    index: Arc<KeySelection<EntryByServiceCreatedKey>>,
}

impl EntryIndexRead {
    pub fn prepare(selection: &Selection<EntryIndex>) -> Result<Self> {
        Ok(Self {
            index: Arc::new(EntryByServiceCreatedKey::prepare_candidates(selection)?),
        })
    }

    /// Whether a read on `snapshot` goes through the index, and so returns the
    /// invocations newest created first: the partition has fully migrated to
    /// vqueues, so every invocation has its entry, and the index is Ready.
    pub fn usable(snapshot: &ReadSnapshot) -> Result<bool> {
        Ok(snapshot.fully_migrated_to_vqueues()?
            && snapshot
                .ready_index(EntryByServiceCreated::DEFINITION, IndexKind::Secondary)
                .is_ok())
    }
}

/// Merges the index runs one step at a time, collecting invocations into a
/// window, then fetches the window's statuses with one multi-get and hands them
/// out in index order.
pub(super) struct EntryIndexCursor {
    /// `None` once the read failed: it holds nothing then.
    runs: Option<OrderedRuns<EntryByServiceCreated>>,
    runs_done: bool,
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
    /// Learns how many statuses each multi-get requests.
    fetched: Box<dyn Fn(usize) + Send>,
    /// How the read ended, once it holds nothing.
    ended: Option<Ended>,
    /// Last: the runs and the fetched window are dropped before the snapshot.
    /// `None` once the read ended.
    snapshot: Option<ReadSnapshot>,
}

/// How a read that holds nothing ended.
#[derive(Clone, Copy)]
enum Ended {
    Done,
    Failed,
}

impl EntryIndexCursor {
    /// The cursor, or `None` if the read scans instead: the index can't stand in
    /// for a scan on this snapshot ([`EntryIndexRead::usable`]), or finding its
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
        } = execution
            .0
            .lock()
            .expect("not poisoned")
            .take()
            .ok_or_else(|| {
                StorageError::Generic(anyhow::anyhow!("an entry-index read opens once"))
            })?;
        let memory = SharedMemory(Arc::new(Mutex::new(memory)));
        let cursor = Self::try_open(snapshot, read, memory, fetched)?;
        chosen(match cursor {
            Some(_) => EntryIndexPath::Index,
            None => EntryIndexPath::Scan,
        });
        Ok(cursor)
    }

    fn try_open(
        snapshot: &ReadSnapshot,
        read: &EntryIndexRead,
        memory: SharedMemory,
        fetched: Box<dyn Fn(usize) + Send>,
    ) -> Result<Option<Self>> {
        if !EntryIndexRead::usable(snapshot)? {
            return Ok(None);
        }
        // The caller's selection, and the window with its keys, before the window
        // is allocated.
        let mut held = Held {
            memory: memory.clone(),
            bytes: 0,
        };
        held.grow(
            read.index.retained_bytes()
                + WINDOW * std::mem::size_of::<(InvocationId, Seq, MillisSinceEpoch)>()
                + WINDOW * KEY_LEN,
        )?;
        let discovery = snapshot.ordered_runs::<EntryByServiceCreated>(
            Arc::clone(&read.index),
            RUN_FIELDS,
            LIMITS,
            Box::new(memory),
        )?;
        let OrderedRunsDiscovery::Ready(runs) = discovery else {
            return Ok(None);
        };
        Ok(Some(Self {
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
            ended: None,
            snapshot: Some(snapshot.clone()),
        }))
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
                let key = runs.current()?.0.decode_all()?;
                if let Some(id) = key.canonical_id.to_base_entry_id().to_invocation_id() {
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
        (self.fetched)(self.window.len());
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

    use super::*;

    /// Counts what a read charges. It refuses a charge that would pass
    /// `limit`, or one of exactly `refuse` bytes, and records the refusal.
    #[derive(Clone, Default)]
    struct Counting(Arc<Mutex<Charge>>);

    #[derive(Default)]
    struct Charge {
        now: usize,
        limit: Option<usize>,
        refuse: Option<usize>,
        refused: Option<usize>,
    }

    impl Counting {
        fn with(charge: Charge) -> Self {
            Self(Arc::new(Mutex::new(charge)))
        }

        fn now(&self) -> usize {
            self.0.lock().unwrap().now
        }

        fn refused(&self) -> Option<usize> {
            self.0.lock().unwrap().refused
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
            Ok(())
        }

        fn shrink(&mut self, bytes: usize) {
            self.0.lock().unwrap().now -= bytes;
        }
    }

    fn execution(
        memory: &Counting,
    ) -> (
        EntryIndexExecution,
        std::sync::mpsc::Receiver<EntryIndexPath>,
    ) {
        let (chosen, path) = std::sync::mpsc::channel();
        let execution = EntryIndexExecution::new(
            Box::new(memory.clone()),
            move |taken| {
                let _ = chosen.send(taken);
            },
            |_| {},
        );
        (execution, path)
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
        let read = EntryIndexRead::prepare(&Selection::All.and(
            EntryByServiceCreatedClause::TargetServiceName(ValuePredicate::Equal("S".into())),
        ))
        .unwrap();
        let fixed = read.index.retained_bytes()
            + WINDOW * std::mem::size_of::<(InvocationId, Seq, MillisSinceEpoch)>()
            + WINDOW * KEY_LEN;
        assert!(read.index.retained_bytes() > 0);

        let memory = Counting::default();
        let (opening, path) = execution(&memory);
        let mut cursor = EntryIndexCursor::open(&snapshot, &read, &opening)
            .unwrap()
            .unwrap();
        assert_eq!(path.try_recv(), Ok(EntryIndexPath::Index));
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
                CursorStep::Record => created.push(cursor.current().unwrap().1.inner.creation_time),
                CursorStep::Skipped => {}
                CursorStep::Done => break,
            }
        }
        assert!(fetched > 0);
        assert_eq!(created.len(), 30);
        assert!(created.is_sorted_by(|newer, older| newer > older));
        assert_eq!(memory.now(), 0, "everything is released at the end");
        assert!(cursor.snapshot.is_none() && cursor.advance().unwrap() == CursorStep::Done);
        drop(cursor);

        // Dropped part-way through a window, it releases everything too.
        let (opening, _) = execution(&memory);
        let mut cursor = EntryIndexCursor::open(&snapshot, &read, &opening)
            .unwrap()
            .unwrap();
        while cursor.advance().unwrap() != CursorStep::Record {}
        assert!(memory.now() > fixed);
        drop(cursor);
        assert_eq!(memory.now(), 0);

        // A refused charge fails the read. Refused at the open, nothing opens.
        let refusing = Counting::with(Charge {
            limit: Some(fixed - 1),
            ..Default::default()
        });
        let (opening, _) = execution(&refusing);
        assert!(EntryIndexCursor::open(&snapshot, &read, &opening).is_err());
        assert_eq!((refusing.refused(), refusing.now()), (Some(fixed), 0));
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
        let (opening, path) = execution(&refusing);
        let (own, closed) = store.query_read(CancellationToken::new()).unwrap();
        let mut closed = std::pin::pin!(closed);
        let mut cursor = EntryIndexCursor::open(&own, &read, &opening)
            .unwrap()
            .unwrap();
        drop(own);
        assert_eq!(path.try_recv(), Ok(EntryIndexPath::Index));
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
        let (opening, _) = execution(&memory);
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
        let (opening, _) = execution(&memory);
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

        // Not migrated: the read scans, and holds nothing of the index.
        let mut tx = store.transaction();
        tx.put_state_machine_features(&PersistedFeatures::default())
            .unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let (opening, path) = execution(&memory);
        assert!(
            EntryIndexCursor::open(&snapshot, &read, &opening)
                .unwrap()
                .is_none()
        );
        assert_eq!(path.try_recv(), Ok(EntryIndexPath::Scan));
        assert_eq!(memory.now(), 0);
    }
}
