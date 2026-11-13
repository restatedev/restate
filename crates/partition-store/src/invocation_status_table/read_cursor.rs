// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Invocation statuses read for queries: a cursor over the statuses a selection
//! names, on the caller's thread, through one admitted snapshot.
//!
//! A read is either an **ID set**, fetched with multi-gets in bounded batches in
//! key order, or a **key range** navigated with seeks ([`InvocationStatusKeys`]),
//! where an ID restriction positions the iterator at each requested invocation
//! instead of reading every status of the partition key ([`InvocationStatusScan`]).
//! The cursor ([`InvocationStatusCursor`]) does bounded work per step and hands
//! out the current invocation's ID and its status, decoded lazily.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use restate_storage_api::StorageError;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor, Table};
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::invocation_status::{
    InvocationStatus, InvocationStatusClause,
};
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey};
use restate_types::sharding::KeyRange;

use crate::keys::codecs::InvocationUuidCodec;
use crate::keys::header::{KeySchema, PartitionKeyHeader};
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, KeyDecode, KeyDecoder, KeyKind};
use crate::table::{MultiGetCursor, MultiGetTable, PhysicalTable, TableCursor};
use crate::{ReadSnapshot, Result};

use super::{
    INVOCATION_STATUS_MULTI_GET_BATCH_SIZE, InvocationStatusKey, create_invocation_status_key,
};

/// Stored invocation-status keys: `kind | partition_key | invocation_uuid`. The UUID
/// is raw, fixed-width bytes, so it is declared as an ordered field: an ID
/// restriction positions the iterator at each requested invocation instead of
/// reading every invocation that shares its partition key (e.g. all invocations of
/// one scope or virtual-object key).
pub enum InvocationStatusKeys {}

impl Table for InvocationStatusKeys {
    type Record<'a> = (KeyDecoder<'a, InvocationStatusKey>, &'a [u8]);
    type Scan = KeySelection<InvocationStatusKey>;
}

impl PhysicalTable for InvocationStatusKeys {
    type Key = InvocationStatusKey;
}

impl MultiGetTable for InvocationStatusKeys {
    type Id = InvocationId;
    const BATCH_SIZE: NonZeroUsize = INVOCATION_STATUS_MULTI_GET_BATCH_SIZE;

    fn partition_key(id: &InvocationId) -> PartitionKey {
        id.partition_key()
    }

    fn key(id: &InvocationId) -> InvocationStatusKey {
        create_invocation_status_key(id)
    }
}

impl InvocationStatusKeys {
    /// Prepares a key iteration over the `header` partition keys, which the caller
    /// derives from the selection's sharding rules. Selected IDs position the
    /// iterator at their UUIDs under each header partition key: another partition
    /// key's invocation with a requested UUID is a candidate, so callers retain the
    /// ID filter as a residual, like every unsupported predicate.
    pub fn prepare(
        selection: &Selection<InvocationStatus>,
        header: Option<KeyRange>,
    ) -> Result<KeySelection<InvocationStatusKey>> {
        let request = KeySelection::for_key_fields(selection, |clause, literals| match clause {
            InvocationStatusClause::Id(predicate) => {
                InvocationUuidCodec::try_prepare_value(predicate, literals)
            }
            _ => Ok(None),
        })?;
        Ok(match header {
            Some(range) => request.restrict_scope(range),
            None => KeySelection::empty(),
        })
    }
}

impl KeySchema for InvocationStatusKey {
    type Header = PartitionKeyHeader<Self>;

    fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
        // The invocation ID needs the partition key. Retain the full key.
        Ok(KeyDecoder::from_payload(key))
    }
}

impl KeyDecode for InvocationStatusKey {
    fn decode(source: &mut &[u8]) -> Result<Self> {
        Self::deserialize_from(source)
    }
}

impl OrderedKeySchema for InvocationStatusKey {
    const KEY_KIND: KeyKind = KeyKind::InvocationStatus;
    const FIELDS: &'static [OrderedKeyField] = &[OrderedKeyField::new::<InvocationUuidCodec>("id")];
}

/// A prepared invocation-status read.
#[derive(Clone)]
pub enum InvocationStatusScan {
    /// Point reads of a finite ID set, batched through multi-get.
    Ids(BTreeSet<InvocationId>),
    /// Iteration over stored keys, navigated by the prepared selection.
    Keys(Arc<KeySelection<InvocationStatusKey>>),
}

/// Invocation-status records. A caller can step through them with a cursor, or
/// a query can scan them on the query IO pool through [`CursorScan`]. Both read
/// the same keys in the same way.
pub enum InvocationStatusRecords {}

impl Table for InvocationStatusRecords {
    type Record<'a> = (InvocationId, InvocationStatusV2Lazy<'a>);
    type Scan = InvocationStatusScan;
}

/// A resumable scan or batched lookup of invocation statuses over one admitted
/// snapshot.
pub struct InvocationStatusCursor {
    mode: Mode,
}

enum Mode {
    Empty,
    Scan(Box<TableCursor<InvocationStatusKeys>>),
    Keys(MultiGetCursor<InvocationStatusKeys>),
}

impl ReadSnapshot {
    pub fn invocation_status_cursor(
        &self,
        scan: InvocationStatusScan,
    ) -> Result<InvocationStatusCursor> {
        self.check_active()?;
        let mode = match scan {
            InvocationStatusScan::Ids(ids) => Mode::Keys(MultiGetCursor::new(self.clone(), ids)),
            InvocationStatusScan::Keys(request) => {
                Mode::Scan(Box::new(
                    <Self as CursorScan<InvocationStatusKeys>>::scan_cursor(self, request)?,
                ))
            }
        };
        Ok(InvocationStatusCursor { mode })
    }
}

impl CursorScan<InvocationStatusRecords> for ReadSnapshot {
    type Cursor = InvocationStatusCursor;

    fn scan_cursor(&self, request: Arc<InvocationStatusScan>) -> Result<Self::Cursor> {
        // A request scanned more than once gives each cursor its own copy of the
        // ID list, so cursors don't affect each other. A request scanned once
        // hands its list over without copying.
        self.invocation_status_cursor(Arc::unwrap_or_clone(request))
    }
}

impl ReadCursor<InvocationStatusRecords> for InvocationStatusCursor {
    fn advance(&mut self) -> Result<CursorStep> {
        let step = match &mut self.mode {
            Mode::Empty => CursorStep::Done,
            Mode::Scan(cursor) => cursor.advance()?,
            Mode::Keys(cursor) => cursor.advance()?,
        };
        if step == CursorStep::Done {
            self.mode = Mode::Empty;
        }
        Ok(step)
    }

    fn current(&self) -> Result<(InvocationId, InvocationStatusV2Lazy<'_>)> {
        let (id, value) = match &self.mode {
            Mode::Scan(cursor) => {
                let (key, value) = cursor.current()?;
                let (partition, uuid) = key.decode_all()?.split();
                (InvocationId::from_parts(partition, uuid), value)
            }
            Mode::Keys(cursor) => cursor.current()?,
            Mode::Empty => return Err(StorageError::DataIntegrityError),
        };
        Ok((id, super::decode_invocation_status_lazy(value)?))
    }
}

#[cfg(test)]
mod tests {
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InvocationStatus as StoredStatus, WriteInvocationStatusTable,
    };
    use restate_storage_query_api::selection::ValuePredicate;
    use restate_types::identifiers::{InvocationUuid, PartitionId, PartitionKey};
    use restate_types::partition_table::Partition;
    use tokio_util::sync::CancellationToken;

    use crate::PartitionStoreManager;

    use super::*;

    #[restate_core::test]
    async fn ids_position_invocation_status_scans() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        // 128 invocations under each partition key, as for one scope or object key.
        // Equal UUIDs across keys model the candidates a header interval admits.
        let id = |key: PartitionKey, n: u128| {
            InvocationId::from_parts(key, InvocationUuid::from_u128(n))
        };
        let mut tx = store.transaction();
        for key in [0, 7, 8, PartitionKey::MAX] {
            for n in 1..=128 {
                let status = StoredStatus::Completed(CompletedInvocation::mock_neo());
                tx.put_invocation_status(&id(key, n), &status).unwrap();
            }
        }
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
        // Returns the records read and the cursor steps taken to read them.
        let read = |selection: Selection<InvocationStatus>, header: KeyRange| {
            let request = InvocationStatusKeys::prepare(&selection, Some(header)).unwrap();
            let mut cursor = <ReadSnapshot as CursorScan<InvocationStatusRecords>>::scan_cursor(
                &snapshot,
                Arc::new(InvocationStatusScan::Keys(Arc::new(request))),
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
        let ids = |ids: &[InvocationId]| {
            Selection::All.and(InvocationStatusClause::Id(ValuePredicate::In(ids.to_vec())))
        };

        assert_eq!(
            read(ids(&[id(7, 64)]), KeyRange::new(7, 7)),
            (vec![id(7, 64)], 1),
            "an_id_reads_one_of_its_neighbors"
        );
        assert_eq!(
            read(ids(&[id(7, 5), id(7, 90)]), KeyRange::new(7, 7)),
            (vec![id(7, 5), id(7, 90)], 3),
            "same_header_ids_seek_between_each_other"
        );
        assert_eq!(
            read(ids(&[id(7, 1000)]), KeyRange::new(7, 7)),
            (vec![], 0),
            "a_missing_id_reads_nothing"
        );
        let (found, steps) = read(ids(&[id(7, 5), id(8, 90)]), KeyRange::new(7, 8));
        assert_eq!(found, [id(7, 5), id(7, 90), id(8, 5), id(8, 90)]);
        assert_eq!(steps, 9, "cross_header_ids_seek_within_and_between_headers");
        let (found, steps) = read(ids(&[id(0, 1), id(PartitionKey::MAX, 128)]), KeyRange::FULL);
        assert_eq!(found.len(), 8, "boundary_ids_cross_every_header: {found:?}");
        assert_eq!(steps, 12, "boundary_ids_seek_within_and_between_headers");
        assert_eq!(
            read(Selection::All, KeyRange::new(7, 7)).1,
            128,
            "header_only_reads_every_neighbor"
        );

        // Two cursors from one ID request must not affect each other: open both,
        // read one to the end, then check that the other still returns every ID.
        let expected = [id(7, 5), id(8, 90)];
        let request = Arc::new(InvocationStatusScan::Ids(expected.into()));
        let cursors = [Arc::clone(&request), request].map(|request| {
            <ReadSnapshot as CursorScan<InvocationStatusRecords>>::scan_cursor(&snapshot, request)
                .unwrap()
        });
        for mut cursor in cursors {
            let mut found = Vec::new();
            loop {
                match cursor.advance().unwrap() {
                    CursorStep::Record => found.push(cursor.current().unwrap().0),
                    CursorStep::Skipped => {}
                    CursorStep::Done => break,
                }
            }
            assert_eq!(found, expected, "shared_requests_have_independent_cursors");
        }
    }
}
