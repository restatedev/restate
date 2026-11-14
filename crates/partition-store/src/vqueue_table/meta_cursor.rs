// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Queue metadata as a table that queries can scan.
//!
//! A query asks either for a set of queue IDs or for a partition-key range:
//! - a set of IDs is read with RocksDB multi-gets, at most 500 keys per call;
//! - a range is read with an iterator, one key per step.
//!
//! Both read only keys in the partition's owned range.

use std::num::NonZeroUsize;
use std::sync::Arc;

use bilrost::BorrowedMessage;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::filters::ScanMetaFilter;
use restate_storage_api::vqueue_table::metadata::VQueueMetaRef;
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor, Table};
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::VQueueId;

use crate::keys::DecodeTableKey;
use crate::query_read::SnapshotCursor;
use crate::scan::{PhysicalScan, TableScan};
use crate::table::{MultiGetCursor, MultiGetTable};
use crate::{ReadSnapshot, Result};

use super::{MetaKey, VQUEUE_META_MULTI_GET_BATCH_SIZE, VQueueMetaTable};

impl MultiGetTable for VQueueMetaTable {
    type Id = VQueueId;
    const BATCH_SIZE: NonZeroUsize = VQUEUE_META_MULTI_GET_BATCH_SIZE;

    fn partition_key(id: &VQueueId) -> PartitionKey {
        id.partition_key()
    }

    fn key(id: &VQueueId) -> MetaKey {
        MetaKey::from(id)
    }
}

/// Queue metadata records, each with its queue ID. For example, a request for
/// 501 IDs fetches 500 keys first and the last one when the cursor reaches it,
/// skipping IDs that are missing or outside the owned range.
pub enum VQueueMetaRecords {}

impl Table for VQueueMetaRecords {
    type Record<'a> = (VQueueId, VQueueMetaRef<'a>);
    type Scan = ScanMetaFilter;
}

/// A cursor over queue metadata. The values it lends point into the read
/// snapshot, so it keeps that snapshot alive until it is dropped.
pub struct VQueueMetaCursor {
    mode: Mode,
    /// The next `advance` stays on the first candidate of a range scan.
    first: bool,
}

enum Mode {
    /// No owned candidates, or an exhausted scan.
    Empty,
    /// A bounded range iterator.
    Scan(SnapshotCursor),
    /// A finite queue-ID set fetched by bounded multi-gets.
    Keys(MultiGetCursor<VQueueMetaTable>),
}

impl CursorScan<VQueueMetaRecords> for ReadSnapshot {
    type Cursor = VQueueMetaCursor;

    fn scan_cursor(&self, request: Arc<ScanMetaFilter>) -> Result<Self::Cursor> {
        self.check_active()?;
        let scan = match Arc::unwrap_or_clone(request) {
            ScanMetaFilter::MetaIdSet(ids) => {
                return Ok(VQueueMetaCursor {
                    mode: Mode::Keys(MultiGetCursor::new(self.clone(), ids)),
                    first: true,
                });
            }
            ScanMetaFilter::PartitionKey(range) => {
                TableScan::ScanPartitionKeyRange::<MetaKey>(range)
            }
            ScanMetaFilter::MetaIdRange(range) => {
                TableScan::RangeInclusive(MetaKey::from(&range.start), MetaKey::from(&range.last))
            }
        };
        let scan: PhysicalScan<_> = scan.into();
        let owner = TableScan::ScanPartitionKeyRange::<MetaKey>(self.partition_key_range()).into();
        let mode = match scan.intersect(&owner) {
            Some(scan) => Mode::Scan(self.physical_cursor(scan)?),
            None => Mode::Empty,
        };
        Ok(VQueueMetaCursor { mode, first: true })
    }
}

impl ReadCursor<VQueueMetaRecords> for VQueueMetaCursor {
    fn advance(&mut self) -> Result<CursorStep> {
        let step = match &mut self.mode {
            Mode::Empty => CursorStep::Done,
            Mode::Keys(cursor) => cursor.advance()?,
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
        };
        self.first = false;
        if step == CursorStep::Done {
            self.mode = Mode::Empty;
        }
        Ok(step)
    }

    fn current(&self) -> Result<(VQueueId, VQueueMetaRef<'_>)> {
        let (id, value) = match &self.mode {
            Mode::Empty => return Err(StorageError::DataIntegrityError),
            Mode::Keys(cursor) => cursor.current()?,
            Mode::Scan(cursor) => {
                let (mut key, value) = cursor.peek()?.ok_or(StorageError::DataIntegrityError)?;
                let (id,) = MetaKey::deserialize_from(&mut key)?.split();
                (id, value)
            }
        };
        let metadata =
            VQueueMetaRef::decode_borrowed(value).map_err(StorageError::BilrostDecode)?;
        Ok((id, metadata))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ops::RangeBounds;

    use bilrost::Message;
    use tokio_util::sync::CancellationToken;

    use restate_clock::{UniqueTimestamp, time::MillisSinceEpoch};
    use restate_limiter::LimitKey;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
    use restate_types::identifiers::PartitionId;
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;

    use crate::PartitionStoreManager;
    use crate::keys::{EncodeTableKey, KeyKind};

    use super::*;

    #[restate_core::test]
    async fn metadata_reads_keep_batches_ownership_and_snapshot() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let owner = KeyRange::new(20, 30);
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, owner), None)
            .await
            .unwrap();
        let created =
            UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000))
                .unwrap();
        let meta = VQueueMeta::new(created, None, LimitKey::None, VQueueLink::None).encode_to_vec();
        // More than one batch, including out-of-owner IDs and an absent ID.
        let ids: BTreeSet<_> = [19, 20, 30, 31]
            .into_iter()
            .flat_map(|key| (0..256).map(move |n| VQueueId::custom(key, format!("q{n}"))))
            .collect();
        let missing = ids
            .iter()
            .find(|id| id.partition_key() == 20)
            .unwrap()
            .clone();
        let put = |tx: &mut crate::PartitionStoreTransaction<'_>, id: &VQueueId, value: &[u8]| {
            let mut key = Vec::new();
            MetaKey::from(id).serialize_to(&mut key);
            tx.raw_put_cf(KeyKind::VQueueMeta, key, value);
        };
        let mut tx = store.transaction();
        for id in &ids {
            if id != &missing {
                // An ownership bug would decode this invalid out-of-owner value.
                put(
                    &mut tx,
                    id,
                    if owner.contains(&id.partition_key()) {
                        &meta
                    } else {
                        b"\xff"
                    },
                );
            }
        }
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();
        let mut tx = store.transaction();
        put(&mut tx, &missing, &meta);
        tx.commit().await.unwrap();
        drop(tx);

        let expected: Vec<_> = ids
            .iter()
            .filter(|id| owner.contains(&id.partition_key()) && *id != &missing)
            .cloned()
            .collect();
        let request = Arc::new(ScanMetaFilter::MetaIdSet(ids));
        let mut first = <ReadSnapshot as CursorScan<VQueueMetaRecords>>::scan_cursor(
            &snapshot,
            Arc::clone(&request),
        )
        .unwrap();
        // Dropping one read halfway must not affect a later read of the request.
        first.advance().unwrap();
        drop(first);
        for request in [
            request,
            Arc::new(ScanMetaFilter::PartitionKey(KeyRange::FULL)),
        ] {
            let mut cursor =
                <ReadSnapshot as CursorScan<VQueueMetaRecords>>::scan_cursor(&snapshot, request)
                    .unwrap();
            let mut found = Vec::new();
            loop {
                match cursor.advance().unwrap() {
                    CursorStep::Record => found.push(cursor.current().unwrap().0),
                    CursorStep::Skipped => {}
                    CursorStep::Done => break,
                }
            }
            assert_eq!(found, expected);
        }
        drop(snapshot);
        closed.await;
    }
}
