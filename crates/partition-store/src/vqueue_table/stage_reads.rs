// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Entry statuses read through the scheduler's stage tables.
//!
//! The stage tables hold exactly one key per current entry status, in the stage
//! and with the `(queue, has_lock, run_at, seq, entry_id)` tuple of that status
//! (the scheduler's stage/status correspondence). A query restricted to a few
//! queues or stages navigates those keys instead of every entry status, then
//! fetches the referenced statuses from the same snapshot in bounded windows.
//!
//! Each reference keeps its stage tuple. A fetched status is associated by its
//! base entry ID and emitted only if it names the same stage, tuple and
//! incarnation; a missing status or a different tuple is skipped and counted.
//! Malformed stage keys or fetched status values, and storage errors, are
//! errors; stage values are not read.

use std::sync::Arc;

use bytes::BytesMut;

use restate_clock::RoughTimestamp;
use restate_rocksdb::reads::{OwnedPinnedBatch, ReadSnapshot as RocksReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{RawStatusHeaderRef, Stage};
use restate_storage_query_api::SchemaBinding;
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor};
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::index::EntryByQueue;
use restate_types::identifiers::BaseEntryId;
use restate_types::vqueues::{Seq, VQueueId, VQueueIdRef};

use crate::keys::selection::KeySelection;
use crate::keys::{EncodeTableKey, EncodeTableKeyPrefix, KeyKind};
use crate::partition_db::PhysicalPartition;
use crate::table::TableCursor;
use crate::{ReadSnapshot, Result};

use super::inbox::{FinishedKey, InboxKey, PausedKey, RunningKey, SuspendedKey};
use super::stage_index::{
    FinishedIndexBinding, FinishedIndexTable, InboxIndexBinding, InboxIndexTable,
    PausedIndexBinding, PausedIndexTable, RunningIndexBinding, RunningIndexTable,
    SuspendedIndexBinding, SuspendedIndexTable, decode_stage_key,
};
use super::{ENTRY_STATUS_MULTI_GET_BATCH_SIZE, EntryStatusKey};

/// Prepared stage-table navigation for one entry-status query: one key selection
/// per stage. Stages excluded by the selection read nothing.
pub struct StageReads {
    inbox: Arc<KeySelection<InboxKey>>,
    running: Arc<KeySelection<RunningKey>>,
    suspended: Arc<KeySelection<SuspendedKey>>,
    paused: Arc<KeySelection<PausedKey>>,
    finished: Arc<KeySelection<FinishedKey>>,
}

impl StageReads {
    /// Prepares every stage with the stage index's key codecs. The selection is a
    /// necessary restriction; callers keep their original predicate as a residual.
    pub fn prepare(selection: &Selection<EntryByQueue>) -> Result<Self> {
        Ok(Self {
            inbox: Arc::clone(InboxIndexBinding::prepare(selection)?.request()),
            running: Arc::clone(RunningIndexBinding::prepare(selection)?.request()),
            suspended: Arc::clone(SuspendedIndexBinding::prepare(selection)?.request()),
            paused: Arc::clone(PausedIndexBinding::prepare(selection)?.request()),
            finished: Arc::clone(FinishedIndexBinding::prepare(selection)?.request()),
        })
    }
}

/// Work done so far by a stage-table read of entry statuses, for diagnostics and
/// bounded-work assertions. References are buffered before they are fetched and
/// checked, so `emitted = references - missing - mismatched` holds only once the
/// read is exhausted; an early stop can leave a window pending.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StageReadCounters {
    /// Stage keys evaluated by navigation, including keys its field predicates
    /// reject and a final key proving a stage exhausted.
    pub visited: u64,
    /// Stage keys accepted by navigation: the references to fetch.
    pub references: u64,
    /// Distinct entry-status keys requested by multi-gets.
    pub fetched: u64,
    /// References whose entry status does not exist.
    pub missing: u64,
    /// References whose entry status has another stage, tuple or incarnation.
    pub mismatched: u64,
    /// References whose entry status matched: the rows this read produced.
    pub emitted: u64,
}

/// A stage key's reference to an entry status, with the tuple it must match.
struct Reference {
    id: BaseEntryId,
    stage: Stage,
    qid: VQueueId,
    has_lock: bool,
    run_at: RoughTimestamp,
    seq: Seq,
}

impl Reference {
    fn matches(&self, header: &RawStatusHeaderRef<'_>) -> bool {
        header.stage == self.stage
            && header.qid == VQueueIdRef::from(&self.qid)
            && header.has_lock == self.has_lock
            && header.next_run_at == self.run_at
            && header.seq == self.seq
    }
}

macro_rules! stage_cursors {
    ($($variant:ident: $table:ty, $key:ty, $field:ident;)+) => {
        enum StageCursor {
            $($variant(TableCursor<$table>),)+
        }

        impl StageCursor {
            const STAGES: &[Stage] = &[$(Stage::$variant,)+];

            fn open(snapshot: &ReadSnapshot, reads: &StageReads, stage: Stage) -> Result<Self> {
                Ok(match stage {
                    $(Stage::$variant => Self::$variant(<ReadSnapshot as CursorScan<$table>>::scan_cursor(
                        snapshot,
                        Arc::clone(&reads.$field),
                    )?),)+
                    Stage::Unknown => unreachable!("only stored stages are opened"),
                })
            }

            fn advance(&mut self) -> Result<CursorStep> {
                match self {
                    $(Self::$variant(cursor) => cursor.advance(),)+
                }
            }

            fn evaluated(&self) -> u64 {
                match self {
                    $(Self::$variant(cursor) => cursor.evaluated(),)+
                }
            }

            fn reference(&self) -> Result<Reference> {
                let (key, kind, stage) = match self {
                    $(Self::$variant(cursor) => (
                        cursor.current()?.0.remaining,
                        <$key as EncodeTableKeyPrefix>::KEY_KIND,
                        Stage::$variant,
                    ),)+
                };
                let (qid, has_lock, run_at, seq, entry_id) = decode_stage_key(key, kind)?;
                Ok(Reference {
                    id: entry_id.to_base_id(qid.partition_key()),
                    stage,
                    qid,
                    has_lock,
                    run_at,
                    seq,
                })
            }
        }
    };
}

stage_cursors! {
    Inbox: InboxIndexTable, InboxKey, inbox;
    Running: RunningIndexTable, RunningKey, running;
    Suspended: SuspendedIndexTable, SuspendedKey, suspended;
    Paused: PausedIndexTable, PausedKey, paused;
    Finished: FinishedIndexTable, FinishedKey, finished;
}

/// Navigates the stage tables one key per step, then fetches and checks a window
/// of references. Windows hold at most [`ENTRY_STATUS_MULTI_GET_BATCH_SIZE`]
/// references and reuse their buffers.
pub(super) struct StagesCursor {
    reads: Arc<StageReads>,
    /// Index of the next stage to open in [`StageCursor::STAGES`].
    next_stage: usize,
    keys: Option<StageCursor>,
    /// References of the current window, sorted by base ID once fetched.
    window: Vec<Reference>,
    /// Next window position to check, while the window is fetched.
    next: Option<usize>,
    /// Distinct base IDs of the fetched window, in multi-get order.
    ids: Vec<BaseEntryId>,
    key_buf: BytesMut,
    results: Option<OwnedPinnedBatch<RocksReadSnapshot<PhysicalPartition>>>,
    /// Multi-get position of the matched status the cursor is positioned on.
    current: Option<usize>,
    counters: StageReadCounters,
    /// Last: the stage cursor and the fetched window above are dropped before
    /// the query snapshot, so the read's completion is signalled only once they
    /// released their storage, however the cursor ends.
    snapshot: ReadSnapshot,
}

impl StagesCursor {
    pub(super) fn new(snapshot: ReadSnapshot, reads: Arc<StageReads>) -> Self {
        Self {
            snapshot,
            reads,
            next_stage: 0,
            keys: None,
            window: Vec::new(),
            next: None,
            ids: Vec::new(),
            key_buf: BytesMut::new(),
            results: None,
            current: None,
            counters: StageReadCounters::default(),
        }
    }

    pub(super) fn counters(&self) -> StageReadCounters {
        StageReadCounters {
            visited: self.counters.visited + self.keys.as_ref().map_or(0, StageCursor::evaluated),
            ..self.counters
        }
    }

    pub(super) fn advance(&mut self) -> Result<CursorStep> {
        self.snapshot.check_active()?;
        self.current = None;
        if let Some(next) = self.next {
            if next < self.window.len() {
                self.next = Some(next + 1);
                return self.check(next);
            }
            self.next = None;
            self.window.clear();
            self.results = None;
        }
        loop {
            let exhausted = self.keys.is_none() && self.next_stage == StageCursor::STAGES.len();
            if self.window.len() == ENTRY_STATUS_MULTI_GET_BATCH_SIZE.get() || exhausted {
                if self.window.is_empty() {
                    return Ok(CursorStep::Done);
                }
                self.fetch()?;
                self.next = Some(0);
                return Ok(CursorStep::Skipped);
            }
            let Some(keys) = &mut self.keys else {
                let stage = StageCursor::STAGES[self.next_stage];
                self.next_stage += 1;
                self.keys = Some(StageCursor::open(&self.snapshot, &self.reads, stage)?);
                continue;
            };
            match keys.advance()? {
                CursorStep::Done => {
                    self.counters.visited += keys.evaluated();
                    self.keys = None;
                }
                CursorStep::Skipped => return Ok(CursorStep::Skipped),
                CursorStep::Record => {
                    self.counters.references += 1;
                    self.window.push(keys.reference()?);
                    return Ok(CursorStep::Skipped);
                }
            }
        }
    }

    /// Requests the window's distinct entry statuses with one sorted multi-get.
    /// References to one base ID stay separate: at most one can match its status.
    fn fetch(&mut self) -> Result<()> {
        const KEY_LEN: usize = EntryStatusKey::serialized_length_fixed();
        self.window.sort_unstable_by_key(|reference| reference.id);
        self.ids.clear();
        self.key_buf.clear();
        for reference in &self.window {
            if self.ids.last() != Some(&reference.id) {
                EncodeTableKey::serialize_to(
                    &EntryStatusKey::from(reference.id),
                    &mut self.key_buf,
                );
                self.ids.push(reference.id);
            }
        }
        let (keys, remainder) = self.key_buf.as_chunks::<KEY_LEN>();
        debug_assert!(remainder.is_empty());
        let reader = self.snapshot.storage_snapshot().reader();
        self.results = Some(
            reader
                .multi_get_pinned(KeyKind::VQueueEntryStatus, keys, true)
                .map_err(|error| StorageError::Generic(error.into()))?
                .into_owned(),
        );
        self.counters.fetched += self.ids.len() as u64;
        Ok(())
    }

    fn check(&mut self, index: usize) -> Result<CursorStep> {
        let reference = &self.window[index];
        let slot = self
            .ids
            .binary_search(&reference.id)
            .map_err(|_| StorageError::DataIntegrityError)?;
        let Some(mut value) = self.value(slot)? else {
            self.counters.missing += 1;
            return Ok(CursorStep::Skipped);
        };
        let header = RawStatusHeaderRef::decode_stored(&mut value)?;
        if reference.matches(&header) {
            self.current = Some(slot);
            self.counters.emitted += 1;
            Ok(CursorStep::Record)
        } else {
            self.counters.mismatched += 1;
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

    pub(super) fn current(&self) -> Result<(BaseEntryId, &[u8])> {
        self.snapshot.check_active()?;
        let slot = self.current.ok_or(StorageError::DataIntegrityError)?;
        let value = self.value(slot)?.ok_or(StorageError::DataIntegrityError)?;
        Ok((self.ids[slot], value))
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use restate_clock::UniqueTimestamp;
    use restate_clock::time::MillisSinceEpoch;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::stats::EntryStatistics;
    use restate_storage_api::vqueue_table::{
        EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryValue, Status, WriteVQueueTable,
    };
    use restate_storage_query_api::selection::ValuePredicate;
    use restate_storage_query_api::tables::index::EntryByQueueClause;
    use restate_types::identifiers::PartitionId;
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef};

    use crate::PartitionStoreManager;
    use crate::vqueue_table::EntryStatusScan;

    use super::*;

    #[restate_core::test]
    async fn stage_tables_navigate_and_check_entry_statuses() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let at = UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000))
            .unwrap();
        // Queues `a` and `b` share partition key 7, so their keys are neighbours.
        let (a, b) = (VQueueId::custom(7, "a"), VQueueId::custom(7, "b"));
        let id = |n: u16| {
            let mut remainder = [0; EntryId::REMAINDER_LEN];
            remainder[..2].copy_from_slice(&n.to_be_bytes());
            EntryId::new(EntryKind::Invocation, remainder)
        };
        let target = EntryTargetRef::Service {
            scope: None,
            service: "svc",
            handler: "run",
        };
        let mut tx = store.transaction();
        // Writes a status and, unless `stage_seq` says otherwise, its stage key.
        let mut put = |qid: &VQueueId, stage: Stage, n: u16, seq: u64, status: bool| {
            let entry_key = EntryKey::new(false, at.to_unix_millis(), seq, id(n));
            let stats = EntryStatistics::new(at, entry_key.run_at());
            if status {
                tx.create_vqueue_entry_status(
                    &EntryContext {
                        qid,
                        target: &target,
                    },
                    EntryStateRef {
                        stage,
                        status: Status::New,
                        entry_key: &entry_key,
                        metadata: &EntryMetadata::default(),
                        stats: &stats,
                    },
                );
            }
            tx.put_vqueue_inbox(
                qid,
                stage,
                &entry_key,
                &EntryValue {
                    status: Status::New,
                    metadata: EntryMetadata::default(),
                    stats,
                },
            );
        };
        // Queue `a`: more inbox entries than one window, two running entries, and
        // a stage key without a status.
        for n in 0..600 {
            put(&a, Stage::Inbox, n, 1, true);
        }
        put(&a, Stage::Running, 600, 1, true);
        put(&a, Stage::Running, 601, 1, true);
        put(&a, Stage::Running, 602, 1, false);
        // Queue `b`: entry 700 is in the inbox at seq 2, and a stale paused key of
        // its earlier incarnation (seq 1) shares the window with the valid key.
        put(&b, Stage::Inbox, 700, 2, true);
        put(&b, Stage::Inbox, 701, 1, true);
        let stale = EntryKey::new(false, at.to_unix_millis(), 1, id(700));
        tx.put_vqueue_inbox(
            &b,
            Stage::Paused,
            &stale,
            &EntryValue {
                status: Status::New,
                metadata: EntryMetadata::default(),
                stats: EntryStatistics::new(at, stale.run_at()),
            },
        );
        tx.commit().await.unwrap();
        drop(tx);

        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let read = |selection: Selection<EntryByQueue>| {
            let reads = Arc::new(StageReads::prepare(&selection).unwrap());
            let mut cursor = snapshot
                .entry_status_cursor(EntryStatusScan::Stages(reads))
                .unwrap();
            let mut ids = Vec::new();
            while cursor.advance().unwrap() != CursorStep::Done {
                if let Ok((id, header)) = cursor.current() {
                    ids.push((id, header.stage, header.seq.as_u64()));
                }
            }
            (ids, cursor.stage_counters().unwrap())
        };
        let queue = |qid: &VQueueId| {
            Selection::All.and(EntryByQueueClause::VqueueId(ValuePredicate::Equal(
                qid.clone(),
            )))
        };
        let base = |n: u16| id(n).to_base_id(7);

        let (rows, counters) = read(queue(&b));
        assert_eq!(
            rows,
            [(base(700), Stage::Inbox, 2), (base(701), Stage::Inbox, 1)],
            "stale_keys_of_one_base_id_do_not_hide_or_duplicate_its_status"
        );
        assert_eq!(
            counters,
            StageReadCounters {
                visited: 3,
                references: 3,
                fetched: 2,
                missing: 0,
                mismatched: 1,
                emitted: 2,
            },
            "a_queue_reads_only_its_own_keys"
        );

        let (rows, counters) = read(queue(&a));
        assert_eq!(rows.len(), 602, "windows_cover_every_reference");
        assert_eq!(
            (counters.references, counters.fetched, counters.missing),
            (603, 603, 1),
            "missing_statuses_are_skipped"
        );

        let (rows, counters) = read(queue(&a).and(EntryByQueueClause::Stage(
            ValuePredicate::Equal(Stage::Running),
        )));
        assert_eq!(
            rows,
            [
                (base(600), Stage::Running, 1),
                (base(601), Stage::Running, 1)
            ],
            "stage_restrictions_skip_other_stages"
        );
        assert_eq!(counters.visited, 3);

        assert_eq!(
            read(Selection::Empty),
            (vec![], StageReadCounters::default()),
            "empty_selections_read_nothing"
        );
    }
}
