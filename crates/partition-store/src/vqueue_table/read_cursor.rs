// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Entry statuses read for queries: a cursor over the statuses a selection
//! names, on the caller's thread, through one admitted snapshot.
//!
//! A read takes one of three shapes ([`EntryStatusScan`]):
//!
//! - **An ID set**: the statuses of the listed entry IDs, fetched with multi-gets
//!   in bounded batches, in key order.
//! - **A key range**: the entry-status keys of some partition keys, navigated
//!   with seeks ([`EntryStatusKeys`]); an ID restriction positions the iterator
//!   at each requested entry instead of reading every status of the partition key.
//! - **The stage tables**: for a selection on queues or stages, the scheduler's
//!   stage keys are navigated instead, and the statuses they reference are
//!   fetched in windows (see `stage_reads`).
//!
//! The cursor ([`EntryStatusCursor`]) does bounded work per step and hands out
//! the current status's base ID and decoded header, so a query task can
//! interleave the read with other work.
//! `ReadSnapshot::for_each_vqueue_entry_status_blocking` runs such a read to the
//! end on the caller's thread.

use std::num::NonZeroUsize;
use std::sync::Arc;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::RawStatusHeaderRef;
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor, Table};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::vqueue_entry_status::{
    VqueueEntryStatus, VqueueEntryStatusClause, VqueueEntryStatusColumn,
};
use restate_types::identifiers::{BaseEntryId, PartitionKey};
use restate_types::sharding::KeyRange;

use crate::keys::codecs::EntryIdCodec;
use crate::keys::header::{KeySchema, PartitionKeyHeader};
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, KeyDecode, KeyDecoder, KeyKind};
use crate::query_read::SnapshotCursor;
use crate::scan::{PhysicalScan, TableScan};
use crate::table::{MultiGetCursor, MultiGetTable, PhysicalTable, TableCursor};
use crate::{ReadSnapshot, Result};

use super::stage_reads::{StageReadCounters, StageReads, StagesCursor};
use super::{ENTRY_STATUS_MULTI_GET_BATCH_SIZE, EntryStatusKey};

/// Stored entry-status keys: `kind | partition_key | entry_id`, where the base
/// entry ID's bytes after its partition key are the raw entry kind and remainder.
/// That suffix is fixed width, so it is declared as an ordered field: an ID
/// restriction positions the iterator at each requested entry instead of reading
/// every entry that shares its partition key.
pub enum EntryStatusKeys {}

impl Table for EntryStatusKeys {
    type Record<'a> = (KeyDecoder<'a, EntryStatusKey>, &'a [u8]);
    type Scan = KeySelection<EntryStatusKey>;
}

impl PhysicalTable for EntryStatusKeys {
    type Key = EntryStatusKey;
}

impl MultiGetTable for EntryStatusKeys {
    type Id = BaseEntryId;
    const BATCH_SIZE: NonZeroUsize = ENTRY_STATUS_MULTI_GET_BATCH_SIZE;

    fn partition_key(id: &BaseEntryId) -> PartitionKey {
        id.partition_key()
    }

    fn key(id: &BaseEntryId) -> EntryStatusKey {
        EntryStatusKey::from(*id)
    }
}

impl EntryStatusKeys {
    /// Prepares a key iteration over the `header` partition keys, which the caller
    /// derives from the selection's sharding rules. Selected entry IDs, and the
    /// base IDs of selected canonical IDs, position the iterator at each entry
    /// under each header partition key. Callers retain the ID filters as
    /// residuals: they reject other partition keys' entries with a requested
    /// suffix and check canonical incarnations.
    pub fn prepare(
        selection: &Selection<VqueueEntryStatus>,
        header: Option<KeyRange>,
    ) -> Result<KeySelection<EntryStatusKey>> {
        let Some(header) = header else {
            return Ok(KeySelection::empty());
        };
        let columns = match selection {
            Selection::All => return Ok(KeySelection::all().restrict_scope(header)),
            Selection::Empty => return Ok(KeySelection::empty()),
            Selection::Predicates(columns) => columns,
        };
        let ids = [
            VqueueEntryStatusColumn::EntryId,
            VqueueEntryStatusColumn::CanonicalId,
        ]
        .into_iter()
        .flat_map(|column| columns.for_column(column))
        .filter_map(|clause| match clause {
            VqueueEntryStatusClause::EntryId(predicate) => Some(Domain::from(predicate)),
            VqueueEntryStatusClause::CanonicalId(predicate) => {
                Some(Domain::from(predicate).map_values(|id| id.to_base_entry_id()))
            }
            _ => None,
        })
        .fold(Domain::all(), Domain::intersect);
        let selection = match ids.into_values() {
            Some(ids) => {
                Selection::All.and(VqueueEntryStatusClause::EntryId(ValuePredicate::In(ids)))
            }
            None => Selection::<VqueueEntryStatus>::All,
        };
        let request = KeySelection::for_key_fields(&selection, |clause, literals| match clause {
            VqueueEntryStatusClause::EntryId(predicate) => {
                EntryIdCodec::try_prepare_value(predicate, literals)
            }
            _ => Ok(None),
        })?;
        Ok(request.restrict_scope(header))
    }
}

impl KeySchema for EntryStatusKey {
    type Header = PartitionKeyHeader<Self>;

    fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
        // The base entry ID includes the partition key. Retain the full key.
        Ok(KeyDecoder::from_payload(key))
    }
}

impl KeyDecode for EntryStatusKey {
    fn decode(source: &mut &[u8]) -> Result<Self> {
        Self::deserialize_from(source)
    }
}

impl OrderedKeySchema for EntryStatusKey {
    const KEY_KIND: KeyKind = KeyKind::VQueueEntryStatus;
    const FIELDS: &'static [OrderedKeyField] = &[OrderedKeyField::new::<EntryIdCodec>("entry_id")];
}

/// A prepared entry-status read.
#[derive(Clone)]
pub enum EntryStatusScan {
    /// A storage filter: a finite ID set uses multi-get, ranges iterate.
    Filter(ScanEntryIdFilter),
    /// Iteration over stored keys, navigated by the prepared selection.
    Keys(Arc<KeySelection<EntryStatusKey>>),
    /// Statuses referenced by the scheduler's stage tables, navigated by the
    /// prepared stage reads. See [`StageReads`].
    Stages(Arc<StageReads>),
}

impl From<ScanEntryIdFilter> for EntryStatusScan {
    fn from(filter: ScanEntryIdFilter) -> Self {
        Self::Filter(filter)
    }
}

/// Entry-status records, each with the base entry ID it was looked up by. A
/// caller can step through them with a cursor, or a query can scan them on the
/// query IO pool through [`CursorScan`]. Both read the same keys in the same way.
pub enum EntryStatusRecords {}

impl Table for EntryStatusRecords {
    type Record<'a> = (BaseEntryId, RawStatusHeaderRef<'a>);
    type Scan = EntryStatusScan;
}

/// A resumable scan or finite-key lookup over one admitted snapshot.
pub struct EntryStatusCursor {
    mode: Mode,
    first: bool,
}

enum Mode {
    Empty,
    Scan(SnapshotCursor),
    Navigate(Box<TableCursor<EntryStatusKeys>>),
    Keys(MultiGetCursor<EntryStatusKeys>),
    Stages(Box<StagesCursor>),
}

impl ReadSnapshot {
    pub fn entry_status_cursor(
        &self,
        scan: impl Into<EntryStatusScan>,
    ) -> Result<EntryStatusCursor> {
        self.check_active()?;
        let filter = match scan.into() {
            EntryStatusScan::Filter(filter) => filter,
            EntryStatusScan::Keys(request) => {
                let cursor = <Self as CursorScan<EntryStatusKeys>>::scan_cursor(self, request)?;
                return Ok(EntryStatusCursor {
                    mode: Mode::Navigate(Box::new(cursor)),
                    first: true,
                });
            }
            EntryStatusScan::Stages(reads) => {
                return Ok(EntryStatusCursor {
                    mode: Mode::Stages(Box::new(StagesCursor::new(self.clone(), reads))),
                    first: true,
                });
            }
        };
        let scan = match filter {
            ScanEntryIdFilter::EntryIdSet(ids) => {
                return Ok(EntryStatusCursor {
                    mode: Mode::Keys(MultiGetCursor::new(self.clone(), ids)),
                    first: true,
                });
            }
            ScanEntryIdFilter::PartitionKey(range) => {
                TableScan::ScanPartitionKeyRange::<EntryStatusKey>(range)
            }
            ScanEntryIdFilter::EntryIdRange(range) => TableScan::RangeInclusive(
                EntryStatusKey::from(range.start),
                EntryStatusKey::from(range.last),
            ),
        };
        let scan: PhysicalScan<_> = scan.into();
        let owned =
            TableScan::ScanPartitionKeyRange::<EntryStatusKey>(self.partition_key_range()).into();
        let mode = match scan.intersect(&owned) {
            Some(scan) => Mode::Scan(self.physical_cursor(scan)?),
            None => Mode::Empty,
        };
        Ok(EntryStatusCursor { mode, first: true })
    }
}

impl CursorScan<EntryStatusRecords> for ReadSnapshot {
    type Cursor = EntryStatusCursor;

    fn scan_cursor(&self, request: Arc<EntryStatusScan>) -> Result<Self::Cursor> {
        // If the caller hands over the last reference to the request, the cursor
        // takes its ID list; otherwise it copies the list. Either way, cursors
        // opened from one request don't affect each other.
        self.entry_status_cursor(Arc::unwrap_or_clone(request))
    }
}

impl EntryStatusCursor {
    /// Work done so far by a stage-table read; `None` for other reads.
    pub fn stage_counters(&self) -> Option<StageReadCounters> {
        match &self.mode {
            Mode::Stages(cursor) => Some(cursor.counters()),
            _ => None,
        }
    }
}

impl ReadCursor<EntryStatusRecords> for EntryStatusCursor {
    fn advance(&mut self) -> Result<CursorStep> {
        let step = match &mut self.mode {
            Mode::Empty => CursorStep::Done,
            Mode::Scan(cursor) => {
                if !self.first {
                    cursor.advance()?;
                }
                if cursor.peek()?.is_some() {
                    CursorStep::Record
                } else {
                    CursorStep::Done
                }
            }
            Mode::Navigate(cursor) => cursor.advance()?,
            Mode::Keys(cursor) => cursor.advance()?,
            Mode::Stages(cursor) => cursor.advance()?,
        };
        self.first = false;
        // Stage reads keep their counters after exhaustion.
        if step == CursorStep::Done && !matches!(self.mode, Mode::Stages(_)) {
            self.mode = Mode::Empty;
        }
        Ok(step)
    }

    fn current(&self) -> Result<(BaseEntryId, RawStatusHeaderRef<'_>)> {
        let (id, mut value) = match &self.mode {
            Mode::Scan(cursor) => {
                let (mut key, value) = cursor.peek()?.ok_or(StorageError::DataIntegrityError)?;
                let (id,) = EntryStatusKey::deserialize_from(&mut key)?.split();
                (id, value)
            }
            Mode::Navigate(cursor) => {
                let (key, value) = cursor.current()?;
                let (id,) = key.decode_all()?.split();
                (id, value)
            }
            Mode::Keys(cursor) => cursor.current()?,
            Mode::Stages(cursor) => cursor.current()?,
            Mode::Empty => return Err(StorageError::DataIntegrityError),
        };
        let header = RawStatusHeaderRef::decode_stored(&mut value)?;
        Ok((id, header))
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use restate_clock::{UniqueTimestamp, time::MillisSinceEpoch};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::stats::EntryStatistics;
    use restate_storage_api::vqueue_table::{
        EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
    };
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::partitions::Partition;
    use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, Seq, VQueueId};

    use crate::PartitionStoreManager;

    use super::*;

    #[restate_core::test]
    async fn ids_position_entry_status_scans() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        // 128 entries under each partition key, as for one queue. Equal entry IDs
        // across keys model the candidates a header interval admits.
        let id = |key: PartitionKey, n: u8| {
            EntryId::new(EntryKind::Invocation, [n; EntryId::REMAINDER_LEN]).to_base_id(key)
        };
        let at = UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000))
            .unwrap();
        let mut tx = store.transaction();
        for key in [7, 8] {
            let qid = VQueueId::custom(key, "queue");
            for n in 1..=128 {
                let entry_key =
                    EntryKey::new(false, at.to_unix_millis(), 1, *id(key, n).as_entry_id());
                tx.create_vqueue_entry_status(
                    &EntryContext {
                        qid: &qid,
                        target: &EntryTargetRef::Service {
                            scope: None,
                            service: "svc",
                            handler: "run",
                        },
                    },
                    EntryStateRef {
                        stage: Stage::Inbox,
                        status: Status::New,
                        entry_key: &entry_key,
                        metadata: &EntryMetadata::default(),
                        stats: &EntryStatistics::new(at, entry_key.run_at()),
                    },
                );
            }
        }
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        // Returns the records read and the cursor steps taken to read them.
        let read = |selection: Selection<VqueueEntryStatus>, header: KeyRange| {
            let request = EntryStatusKeys::prepare(&selection, Some(header)).unwrap();
            let mut cursor = <ReadSnapshot as CursorScan<EntryStatusRecords>>::scan_cursor(
                &snapshot,
                Arc::new(EntryStatusScan::Keys(Arc::new(request))),
            )
            .unwrap();
            let (mut ids, mut steps) = (Vec::new(), 0);
            loop {
                match cursor.advance().unwrap() {
                    CursorStep::Done => return (ids, steps),
                    CursorStep::Skipped => steps += 1,
                    CursorStep::Record => {
                        steps += 1;
                        ids.push(cursor.current().unwrap().0);
                    }
                }
            }
        };
        let entry_ids = |ids: &[BaseEntryId]| {
            VqueueEntryStatusClause::EntryId(ValuePredicate::In(ids.to_vec()))
        };
        let canonical = |id: BaseEntryId, seq: u64| id.canonicalize(Seq::new(seq));

        assert_eq!(
            read(
                Selection::All.and(entry_ids(&[id(7, 64)])),
                KeyRange::new(7, 7)
            ),
            (vec![id(7, 64)], 1),
            "an_id_reads_one_of_its_neighbors"
        );
        // Canonical IDs select their base entries; the incarnation stays residual.
        let selection = Selection::All.and(VqueueEntryStatusClause::CanonicalId(
            ValuePredicate::In(vec![canonical(id(7, 5), 1), canonical(id(7, 90), 9)]),
        ));
        assert_eq!(
            read(selection, KeyRange::new(7, 7)),
            (vec![id(7, 5), id(7, 90)], 3),
            "canonical_ids_seek_to_their_base_entries"
        );
        let selection =
            Selection::All
                .and(entry_ids(&[id(7, 5)]))
                .and(VqueueEntryStatusClause::CanonicalId(ValuePredicate::Equal(
                    canonical(id(7, 90), 1),
                )));
        assert_eq!(
            read(selection, KeyRange::new(7, 7)),
            (vec![], 0),
            "conflicting_ids_read_nothing"
        );
        let (found, steps) = read(
            Selection::All.and(entry_ids(&[id(7, 5), id(8, 90)])),
            KeyRange::new(7, 8),
        );
        assert_eq!(found, [id(7, 5), id(7, 90), id(8, 5), id(8, 90)]);
        assert_eq!(steps, 9, "cross_header_ids_seek_within_and_between_headers");
        assert_eq!(
            read(Selection::All, KeyRange::new(7, 7)).1,
            128,
            "header_only_reads_every_neighbor"
        );
    }
}
