// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, btree_set};
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::{Buf, BytesMut};

use restate_rocksdb::reads::{OwnedPinnedBatch, ReadSnapshot as RocksReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor, Table};
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::invocation_status::{
    InvocationStatus, InvocationStatusClause,
};
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::storage::{StorageCodecKind, StorageDecodeError};

use crate::keys::codecs::InvocationUuidCodec;
use crate::keys::header::{KeySchema, PartitionKeyHeader};
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyDecode, KeyDecoder, KeyKind};
use crate::partition_db::PhysicalPartition;
use crate::table::{PhysicalTable, TableCursor};
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
pub enum InvocationStatusScan {
    /// Point reads of a finite ID set, batched through multi-get.
    Ids(BTreeSet<InvocationId>),
    /// Iteration over stored keys, navigated by the prepared selection.
    Keys(Arc<KeySelection<InvocationStatusKey>>),
}

pub enum InvocationStatusRecords {}

impl Table for InvocationStatusRecords {
    type Record<'a> = (InvocationId, InvocationStatusV2Lazy<'a>);
    type Scan = InvocationStatusScan;
}

pub struct InvocationStatusCursor {
    mode: Mode,
}

enum Mode {
    Empty,
    Scan(Box<TableCursor<InvocationStatusKeys>>),
    Keys(KeysCursor),
}

struct KeysCursor {
    current: Option<usize>,
    results: Option<OwnedPinnedBatch<RocksReadSnapshot<PhysicalPartition>>>,
    next: usize,
    ids: btree_set::IntoIter<InvocationId>,
    key_buf: BytesMut,
    batch_ids: Vec<InvocationId>,
    snapshot: ReadSnapshot,
}

impl ReadSnapshot {
    pub fn invocation_status_cursor(
        &self,
        scan: InvocationStatusScan,
    ) -> Result<InvocationStatusCursor> {
        self.check_active()?;
        let mode =
            match scan {
                InvocationStatusScan::Ids(ids) => {
                    let capacity = ids.len().min(INVOCATION_STATUS_MULTI_GET_BATCH_SIZE);
                    Mode::Keys(KeysCursor {
                        current: None,
                        results: None,
                        next: 0,
                        ids: ids.into_iter(),
                        key_buf: BytesMut::with_capacity(
                            capacity * InvocationStatusKey::serialized_length_fixed(),
                        ),
                        batch_ids: Vec::with_capacity(capacity),
                        snapshot: self.clone(),
                    })
                }
                InvocationStatusScan::Keys(request) => Mode::Scan(Box::new(
                    <Self as CursorScan<InvocationStatusKeys>>::scan_cursor(self, request)?,
                )),
            };
        Ok(InvocationStatusCursor { mode })
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
        let (id, mut value) = match &self.mode {
            Mode::Scan(cursor) => {
                let (key, value) = cursor.current()?;
                let (partition, uuid) = key.decode_all()?.split();
                (InvocationId::from_parts(partition, uuid), value)
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
            Mode::Empty => return Err(StorageError::DataIntegrityError),
        };
        if value.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }
        let codec = StorageCodecKind::try_from(value.get_u8())
            .map_err(|error| StorageError::Conversion(error.into()))?;
        if codec != StorageCodecKind::Protobuf {
            return Err(StorageError::Conversion(
                StorageDecodeError::UnsupportedCodecKind(codec).into(),
            ));
        }
        let mut status = InvocationStatusV2Lazy::default();
        status
            .merge(value)
            .map_err(|error| StorageError::Conversion(error.into()))?;
        Ok((id, status))
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
        const KEY_LEN: usize = InvocationStatusKey::serialized_length_fixed();
        self.key_buf.clear();
        self.batch_ids.clear();
        self.results = None;
        self.next = 0;
        for id in self
            .ids
            .by_ref()
            .take(INVOCATION_STATUS_MULTI_GET_BATCH_SIZE)
        {
            if self
                .snapshot
                .partition_key_range()
                .contains(&id.partition_key())
            {
                create_invocation_status_key(&id).serialize_to(&mut self.key_buf);
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
            .reader(KeyKind::InvocationStatus);
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
            let mut cursor = snapshot
                .invocation_status_cursor(InvocationStatusScan::Keys(Arc::new(request)))
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
    }
}
