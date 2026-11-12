// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use bilrost::Message;
use bytes::{BufMut, BytesMut};
use futures::FutureExt;
use rocksdb::ReadOptions;

use restate_core::io::IoExecutor;
use restate_rocksdb::{Priority, RocksDbReadPerfGuard, StorageTaskKind};
use restate_storage_api::blob_table::{
    Blob, BlobDecoder, BlobEncode, BlobEntry, Header, ReadBlobTable, ScanBlobTable,
    ScanBlobTableRange, WriteBlobTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::blob::BlobId;
use restate_types::clock::{self, UniqueTimestamp};
use restate_types::identifiers::ResourceId;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::Seq;

use crate::TableKind;
use crate::error::break_on_err;
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind, define_table_key};
use crate::scan::TableScan;
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess};

mod header;
pub use header::*;

define_table_key!(
    TableKind::Blob,
    KeyKind::Blob,
    BlobKey(blob_id: BlobId)
);

impl BlobKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH + BlobId::RAW_BYTES_LEN
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::serialized_length_fixed()] {
        let mut buf = [0u8; Self::serialized_length_fixed()];
        self.serialize_to(&mut buf.as_mut());
        buf
    }
}

const BLOB_MULTI_GET_BATCH_SIZE: usize = 25;

fn get_blob<S: StorageAccess, D: BlobDecoder>(
    storage: &mut S,
    blob_id: BlobId,
) -> Result<Option<D::Type>> {
    let _x = RocksDbReadPerfGuard::new("get-blob");
    let key = BlobKey { blob_id };

    storage.get_kv_raw(key, |_, value| {
        let Some(value) = value else {
            return Ok(None);
        };

        let (header, payload) = BlobHeader::decode(value)?;

        if header.ref_count() == 0 {
            // marked for deletion
            return Ok(None);
        }
        D::decode_blob(header, payload).map(Some)
    })
}

fn decode_blob_entry(id: BlobId, value: &[u8]) -> Result<BlobEntry<'_>> {
    let (header, payload) = BlobHeader::decode(value)?;
    Ok(BlobEntry {
        id,
        kind: header.kind(),
        ref_count: header.ref_count(),
        payload,
    })
}

fn multi_get_blob<F>(
    store: &PartitionStore,
    ids: BTreeSet<BlobId>,
    mut f: F,
) -> impl Future<Output = Result<()>> + Send
where
    F: for<'a> FnMut(BlobEntry<'a>) -> ControlFlow<()> + Send + Sync + 'static,
{
    const KEY_LEN: usize = BlobKey::serialized_length_fixed();

    let rocksdb = store.partition_db().clone();

    async move {
        let read = rocksdb.prepare_read(
            "df-blob",
            StorageTaskKind::MultiGet,
            move |raw_db, cfs| -> Result<()> {
                let cf = cfs.get(KeyKind::Blob);

                let batch_capacity = ids.len().min(BLOB_MULTI_GET_BATCH_SIZE);
                let mut key_buf = BytesMut::with_capacity(batch_capacity * KEY_LEN);
                let mut batch_ids = Vec::with_capacity(batch_capacity);

                let mut readopts = ReadOptions::default();
                readopts.set_async_io(true);
                readopts.set_optimize_multiget_for_io(true);

                let mut ids = ids.into_iter();
                loop {
                    key_buf.clear();
                    batch_ids.clear();

                    for id in ids.by_ref().take(BLOB_MULTI_GET_BATCH_SIZE) {
                        key_buf.put_slice(&BlobKey { blob_id: id }.to_bytes());
                        batch_ids.push(id);
                    }

                    if batch_ids.is_empty() {
                        break;
                    }

                    let (keys, remainder) = key_buf.as_chunks::<KEY_LEN>();
                    debug_assert!(
                        remainder.is_empty(),
                        "Each serialized BlobKey should have KEY_LEN"
                    );

                    let results = raw_db.batched_multi_get_cf_opt(cf, keys, true, &readopts);

                    for (id, result) in batch_ids.iter().zip(results) {
                        let Some(value) = result.map_err(|e| StorageError::Generic(e.into()))?
                        else {
                            continue;
                        };

                        if f(decode_blob_entry(*id, &value)?).is_break() {
                            return Ok(());
                        }
                    }

                    if batch_ids.len() < BLOB_MULTI_GET_BATCH_SIZE {
                        break;
                    }
                }

                Ok(())
            },
        );
        rocksdb
            .rocksdb()
            .executor()
            .low_pri()
            .spawn(read)
            .map_err(|_| StorageError::OperationalError)?
            .await
            .map_err(|_| StorageError::OperationalError)?
    }
}

impl ScanBlobTable for PartitionStore {
    fn for_each_blob<F: for<'a> FnMut(BlobEntry<'a>) -> ControlFlow<()> + Send + Sync + 'static>(
        &self,
        range: ScanBlobTableRange,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        let created_at = match range {
            ScanBlobTableRange::BlobIdSet(ids) => {
                return Ok(multi_get_blob(self, ids, f).boxed());
            }
            ScanBlobTableRange::CreatedAt(created_at) => created_at,
        };
        if created_at.is_empty() {
            return Ok(std::future::ready(Ok(())).boxed());
        }

        // Blob ids are prefixed by their creation timestamp, so the range bounds the seek. The
        // end bound is the first id of the next millisecond and is skipped below if it exists.
        let lower_bound = |millis: MillisSinceEpoch| {
            let ts = match UniqueTimestamp::try_from_unix_millis(millis) {
                Ok(ts) => ts,
                Err(clock::Error::UnixTimestampBelowMin(_)) => UniqueTimestamp::MIN,
                Err(_) => UniqueTimestamp::MAX,
            };
            BlobKey::builder().blob_id(BlobId::new(ts, Seq::MIN, 0, [0; 16]))
        };
        let start = lower_bound(*created_at.start());
        let end = lower_bound(MillisSinceEpoch::new(
            created_at.end().as_u64().saturating_add(1),
        ));

        let scan_fut = self
            .iterator_for_each(
                "df-blob",
                Priority::Low,
                TableScan::RangeInclusive(start, end),
                move |(mut key, value)| {
                    let blob_id = break_on_err(BlobKey::deserialize_from(&mut key))?.blob_id;
                    if !created_at.contains(&blob_id.timestamp().to_unix_millis()) {
                        return ControlFlow::Continue(());
                    }
                    let entry = break_on_err(decode_blob_entry(blob_id, value))?;

                    f(entry).map_break(Ok)
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(scan_fut.boxed())
    }
}

impl ReadBlobTable for PartitionStore {
    async fn get_blob<D: BlobDecoder>(&mut self, blob_id: BlobId) -> Result<Option<D::Type>> {
        get_blob::<_, D>(self, blob_id)
    }
}

impl ReadBlobTable for PartitionStoreTransaction<'_> {
    async fn get_blob<D: BlobDecoder>(&mut self, blob_id: BlobId) -> Result<Option<D::Type>> {
        get_blob::<_, D>(self, blob_id)
    }
}

impl WriteBlobTable for PartitionStoreTransaction<'_> {
    fn put_blob<B: BlobEncode>(&mut self, blob_id: BlobId, blob: Blob<B>) -> Result<()> {
        let key = BlobKey { blob_id };

        let mut buf = BytesMut::new();

        BlobHeader::new(blob.kind(), blob.ref_count()).encode(&mut buf);

        blob.inner().encode_blob(&mut buf)?;

        self.put_kv_raw(key, &buf)
    }

    fn decrement_ref_count(&mut self, blob_id: BlobId, count: u16) {
        if count == 0 {
            return;
        }

        let key_buf = BlobKey { blob_id }.to_bytes();
        let value = HeaderUpdate::decrement(count);
        self.raw_merge_cf(KeyKind::Blob, key_buf, value.encode_to_bytes());
    }

    fn increment_ref_count(&mut self, blob_id: BlobId, count: u16) {
        if count == 0 {
            return;
        }

        let key_buf = BlobKey { blob_id }.to_bytes();
        let value = HeaderUpdate::increment(count);
        self.raw_merge_cf(KeyKind::Blob, key_buf, value.encode_to_bytes());
    }
}

#[cfg(test)]
mod tests {

    use restate_types::blob::BlobKind;

    use super::*;

    #[test]
    fn blob_header_roundtrip() {
        let ref_count = 300;
        let header = BlobHeader::new(BlobKind::Output, ref_count);
        let mut value = Vec::new();
        header.encode(&mut value);
        assert_eq!(header.encoded_len(), value.len());
        let v = BlobHeader::VERSION;
        assert_eq!(value, [v, 0, 3, BlobKind::Output as u8, 1, 44]);
        value.extend_from_slice(b"payload");

        let (header, payload) = BlobHeader::decode(&value).unwrap();
        assert_eq!(header.kind(), BlobKind::Output);
        assert_eq!(header.ref_count(), ref_count);
        assert_eq!(payload, b"payload");

        // empty, unknown version, truncated prelude, fields_len past the end,
        // fields_len shorter than the known fields, unknown kind
        assert!(BlobHeader::decode(&[]).is_err());
        assert!(BlobHeader::decode(&[v + 1, 0, 3, 1, 0, 1]).is_err());
        assert!(BlobHeader::decode(&[v, 0]).is_err());
        assert!(BlobHeader::decode(&[v, 0, 4, 1, 0, 1]).is_err());
        assert!(BlobHeader::decode(&[v, 0, 2, 1, 0, 1]).is_err());
        assert!(BlobHeader::decode(&[v, 0, 3, 42, 0, 1]).is_err());
    }
}
