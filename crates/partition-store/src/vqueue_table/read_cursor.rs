// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::btree_set;
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::BytesMut;
use restate_rocksdb::reads::{OwnedPinnedBatch, ReadSnapshot as RocksReadSnapshot};

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::RawStatusHeaderRef;
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor, Table};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::vqueue_entry_status::{
    VqueueEntryStatus, VqueueEntryStatusClause, VqueueEntryStatusColumn,
};
use restate_types::identifiers::BaseEntryId;
use restate_types::sharding::KeyRange;

use crate::keys::codecs::EntryIdCodec;
use crate::keys::header::{KeySchema, PartitionKeyHeader};
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyDecode, KeyDecoder, KeyKind};
use crate::partition_db::PhysicalPartition;
use crate::query_read::SnapshotCursor;
use crate::scan::{PhysicalScan, TableScan};
use crate::table::{PhysicalTable, TableCursor};
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

/// Borrowed entry-status records, including the base identity used for lookup.
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
    Keys(KeysCursor),
    Stages(Box<StagesCursor>),
}

struct KeysCursor {
    current: Option<usize>,
    results: Option<OwnedPinnedBatch<RocksReadSnapshot<PhysicalPartition>>>,
    next: usize,
    ids: btree_set::IntoIter<BaseEntryId>,
    key_buf: BytesMut,
    batch_ids: Vec<BaseEntryId>,
    snapshot: ReadSnapshot,
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
                let capacity = ids.len().min(ENTRY_STATUS_MULTI_GET_BATCH_SIZE.get());
                return Ok(EntryStatusCursor {
                    mode: Mode::Keys(KeysCursor {
                        current: None,
                        results: None,
                        next: 0,
                        ids: ids.into_iter(),
                        key_buf: BytesMut::with_capacity(
                            capacity * EntryStatusKey::serialized_length_fixed(),
                        ),
                        batch_ids: Vec::with_capacity(capacity),
                        snapshot: self.clone(),
                    }),
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
            Mode::Keys(cursor) => {
                cursor.snapshot.check_active()?;
                let index = cursor.current.ok_or(StorageError::DataIntegrityError)?;
                let value = cursor
                    .results
                    .as_ref()
                    .ok_or(StorageError::DataIntegrityError)?
                    .get(index)
                    .map_err(|error| StorageError::Generic(error.into()))?
                    .ok_or(StorageError::DataIntegrityError)?;
                (cursor.batch_ids[index], value)
            }
            Mode::Stages(cursor) => cursor.current()?,
            Mode::Empty => return Err(StorageError::DataIntegrityError),
        };
        let header = RawStatusHeaderRef::decode_stored(&mut value)?;
        Ok((id, header))
    }
}

impl KeysCursor {
    fn advance(&mut self) -> Result<CursorStep> {
        self.snapshot.check_active()?;
        self.current = None;
        if self.next == self.batch_ids.len() {
            if self.ids.len() == 0 {
                return Ok(CursorStep::Done);
            }
            self.refill()?;
        }
        if self.next == self.batch_ids.len() {
            return Ok(CursorStep::Skipped);
        }
        let index = self.next;
        self.next += 1;
        let present = self
            .results
            .as_ref()
            .ok_or(StorageError::DataIntegrityError)?
            .get(index)
            .map_err(|error| StorageError::Generic(error.into()))?
            .is_some();
        self.current = present.then_some(index);
        Ok(if present {
            CursorStep::Record
        } else {
            CursorStep::Skipped
        })
    }

    fn refill(&mut self) -> Result<()> {
        const KEY_LEN: usize = EntryStatusKey::serialized_length_fixed();
        self.key_buf.clear();
        self.batch_ids.clear();
        self.results = None;
        self.next = 0;
        for id in self
            .ids
            .by_ref()
            .take(ENTRY_STATUS_MULTI_GET_BATCH_SIZE.get())
        {
            if self
                .snapshot
                .partition_key_range()
                .contains(&id.partition_key())
            {
                EntryStatusKey::from(id).serialize_to(&mut self.key_buf);
                self.batch_ids.push(id);
            }
        }
        if self.batch_ids.is_empty() {
            return Ok(());
        }
        let (keys, remainder) = self.key_buf.as_chunks::<KEY_LEN>();
        debug_assert!(remainder.is_empty());
        let reader = self
            .snapshot
            .storage_snapshot()
            .reader(KeyKind::VQueueEntryStatus);
        self.results = Some(
            reader
                .multi_get_pinned(keys, true)
                .map_err(|error| StorageError::Generic(error.into()))?
                .into_owned(),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
    use tokio_util::sync::CancellationToken;

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
            let mut cursor = snapshot
                .entry_status_cursor(EntryStatusScan::Keys(Arc::new(request)))
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
