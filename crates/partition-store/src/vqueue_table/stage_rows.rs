// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Entry statuses read from the scheduler's stage tables alone, for a query
//! that needs only the columns a stage record holds.
//!
//! A stage key holds an entry's queue, lock, run time, sequence number and ID;
//! its value holds the entry's status, metadata and statistics as of the
//! scheduler's last write of the key. The scheduler keeps exactly one stage key
//! per current entry status, with that status's tuple, status and statistics
//! (the stage/status correspondence), so those columns of the record are the
//! status's. Its metadata isn't: the scheduler doesn't rewrite the record when
//! only the metadata changes, such as when an invocation is pinned to a
//! deployment. A read that needs the metadata fetches the statuses instead (see
//! `stage_reads`).
//!
//! ```text
//! entry status   inv_1 → Running, has_lock, run 10:00, seq 4, Started, 3 attempts
//! running stage  Running | q | has_lock | 10:00 | 4 | inv_1 → Started, 3 attempts
//!                └──────────── the same columns, no status read ─────────────┘
//! ```
//!
//! The read navigates the stage tables with a [`StageReads`] selection, stage
//! after stage, each in key order, and decodes the value only if the query
//! needs a column of it. Rows aren't checked against their entry statuses: a
//! missing or different status, which the correspondence rules out, goes
//! unnoticed, unlike in the reads that fetch them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bilrost::BorrowedMessage;

use restate_clock::RoughTimestamp;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryValueRef, Stage};
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor, Table};
use restate_types::identifiers::BaseEntryId;
use restate_types::vqueues::{Seq, VQueueIdRef};

use crate::{ReadSnapshot, Result};

use super::stage_reads::{Reference, StageCursor, StageReads};

/// A prepared read of stage records.
#[derive(Clone)]
pub struct StageRowScan {
    pub reads: Arc<StageReads>,
    /// Whether the read needs a column of the stage value. Without, rows carry
    /// only the stage key's columns.
    pub values: bool,
}

/// Stage records, each with the base entry ID of its key.
pub enum StageRowRecords {}

impl Table for StageRowRecords {
    type Record<'a> = (BaseEntryId, StageRowRef<'a>);
    type Scan = StageRowScan;
}

/// One stage record: its key's columns, and its value if the read needs it.
pub struct StageRowRef<'a> {
    pub stage: Stage,
    pub qid: VQueueIdRef<'a>,
    pub has_lock: bool,
    pub run_at: RoughTimestamp,
    pub seq: Seq,
    /// Status, metadata and statistics as of the scheduler's last write of the
    /// key; `None` if the read needs no value column.
    pub value: Option<EntryValueRef<'a>>,
}

/// Work done so far by a stage-record read, for diagnostics and bounded-work
/// assertions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StageRowCounters {
    /// Stage keys evaluated by navigation, including keys its field predicates
    /// reject and a final key proving a stage exhausted.
    pub visited: u64,
    /// Rows produced: stage keys accepted by navigation.
    pub rows: u64,
    /// Stage values decoded, by each successful decode of a current row's
    /// value.
    pub values: u64,
}

/// Steps through stage records one key at a time; a step is bounded work.
pub struct StageRowsCursor {
    reads: Arc<StageReads>,
    values: bool,
    /// Index of the next stage to open in [`StageCursor::STAGES`].
    next_stage: usize,
    keys: Option<StageCursor>,
    /// The stage key the cursor is positioned on.
    row: Option<Reference>,
    counters: StageRowCounters,
    /// [`StageRowCounters::values`], counted by [`ReadCursor::current`].
    decoded: AtomicU64,
    /// Last: the stage cursor above is dropped before the query snapshot, so
    /// the read's completion is signalled only once it released its storage,
    /// however the cursor ends.
    snapshot: ReadSnapshot,
}

impl StageRowsCursor {
    pub fn counters(&self) -> StageRowCounters {
        StageRowCounters {
            visited: self.counters.visited + self.keys.as_ref().map_or(0, StageCursor::evaluated),
            values: self.decoded.load(Ordering::Relaxed),
            ..self.counters
        }
    }
}

impl CursorScan<StageRowRecords> for ReadSnapshot {
    type Cursor = StageRowsCursor;

    fn scan_cursor(&self, request: Arc<StageRowScan>) -> Result<StageRowsCursor> {
        self.check_active()?;
        let StageRowScan { reads, values } = Arc::unwrap_or_clone(request);
        Ok(StageRowsCursor {
            reads,
            values,
            next_stage: 0,
            keys: None,
            row: None,
            counters: StageRowCounters::default(),
            decoded: AtomicU64::new(0),
            snapshot: self.clone(),
        })
    }
}

impl ReadCursor<StageRowRecords> for StageRowsCursor {
    fn advance(&mut self) -> Result<CursorStep> {
        self.snapshot.check_active()?;
        self.row = None;
        loop {
            let Some(keys) = &mut self.keys else {
                let Some(&stage) = StageCursor::STAGES.get(self.next_stage) else {
                    return Ok(CursorStep::Done);
                };
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
                    self.row = Some(keys.reference()?);
                    self.counters.rows += 1;
                    return Ok(CursorStep::Record);
                }
            }
        }
    }

    fn current(&self) -> Result<(BaseEntryId, StageRowRef<'_>)> {
        self.snapshot.check_active()?;
        let row = self.row.as_ref().ok_or(StorageError::DataIntegrityError)?;
        let value = if self.values {
            let keys = self.keys.as_ref().ok_or(StorageError::DataIntegrityError)?;
            let value = EntryValueRef::decode_borrowed(keys.value()?)
                .map_err(StorageError::BilrostDecode)?;
            self.decoded.fetch_add(1, Ordering::Relaxed);
            Some(value)
        } else {
            None
        };
        Ok((
            row.id,
            StageRowRef {
                stage: row.stage,
                qid: VQueueIdRef::from(&row.qid),
                has_lock: row.has_lock,
                run_at: row.run_at,
                seq: row.seq,
                value,
            },
        ))
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
    use restate_storage_query_api::selection::Selection;
    use restate_types::identifiers::PartitionId;
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, VQueueId};

    use crate::PartitionStoreManager;

    use super::*;

    /// A stage read counts the keys it visits and the values it decodes, which
    /// it does only for a query that needs them, whether or not it goes on.
    #[restate_core::test]
    async fn stage_reads_count_visited_keys_and_decoded_values() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let at = UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000))
            .unwrap();
        let qid = VQueueId::custom(7, "q");
        let target = EntryTargetRef::Service {
            scope: None,
            service: "svc",
            handler: "run",
        };
        let id = |n: u8| EntryId::new(EntryKind::Invocation, [n; 16]);
        let mut tx = store.transaction();
        for n in 1..=2 {
            let key = EntryKey::new(false, at.to_unix_millis(), u64::from(n), id(n));
            let stats = EntryStatistics::new(at, key.run_at());
            let metadata = EntryMetadata::default();
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target,
                },
                EntryStateRef {
                    stage: Stage::Inbox,
                    status: Status::New,
                    entry_key: &key,
                    metadata: &metadata,
                    stats: &stats,
                },
            );
            tx.put_vqueue_inbox(
                &qid,
                Stage::Inbox,
                &key,
                &EntryValue {
                    status: Status::New,
                    metadata,
                    stats,
                },
            );
        }
        tx.commit().await.unwrap();
        drop(tx);

        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        let reads = Arc::new(StageReads::prepare(&Selection::All).unwrap());
        let first = id(1).to_base_id(7);
        let first_row = |values: bool| {
            let mut cursor = <ReadSnapshot as CursorScan<StageRowRecords>>::scan_cursor(
                &snapshot,
                Arc::new(StageRowScan {
                    reads: Arc::clone(&reads),
                    values,
                }),
            )
            .unwrap();
            while cursor.advance().unwrap() != CursorStep::Record {}
            let (id, row) = cursor.current().unwrap();
            assert_eq!((id, row.stage, row.seq.as_u64()), (first, Stage::Inbox, 1));
            assert_eq!(
                row.value.map(|value| value.status),
                values.then_some(Status::New)
            );
            cursor.counters()
        };
        assert_eq!(
            first_row(true),
            StageRowCounters {
                visited: 1,
                rows: 1,
                values: 1,
            },
            "a_stage_read_counts_decoded_values"
        );
        assert_eq!(
            first_row(false),
            StageRowCounters {
                visited: 1,
                rows: 1,
                values: 0,
            },
            "a_key_only_read_decodes_no_value"
        );
    }
}
