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
use zerocopy::IntoBytes;

use restate_rocksdb::{Priority, RocksDbReadPerfGuard, StorageTaskKind};
use restate_storage_api::blob_table::{
    Blob, BlobDecoder, BlobEntry, Header, ReadBlobTable, ScanBlobTable, ScanBlobTableRange,
    WriteBlobTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::blob::{BaseBlobId, BlobId};
use restate_types::sharding::{PartitionKey, WithPartitionKey};

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
    BlobKey(
        partition_key: PartitionKey,
        blob_id: BlobId,
    )
);

impl BlobKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>() + BlobId::RAW_BYTES_LEN
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::serialized_length_fixed()] {
        let mut buf = [0u8; Self::serialized_length_fixed()];
        self.serialize_to(&mut buf.as_mut());
        buf
    }
}

const BLOB_MULTI_GET_BATCH_SIZE: usize = 25;

#[inline]
fn create_blob_key(blob_id: BaseBlobId) -> BlobKey {
    BlobKey {
        partition_key: blob_id.partition_key(),
        blob_id: blob_id.into_blob_id(),
    }
}

fn get_blob<S: StorageAccess, D: BlobDecoder>(
    storage: &mut S,
    blob_id: BaseBlobId,
) -> Result<Option<D::Type>> {
    let _x = RocksDbReadPerfGuard::new("get-blob");
    let key = create_blob_key(blob_id);

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

fn decode_blob_entry(id: BaseBlobId, value: &[u8]) -> Result<BlobEntry<'_>> {
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
    ids: BTreeSet<BaseBlobId>,
    mut f: F,
) -> impl Future<Output = Result<()>> + Send
where
    F: for<'a> FnMut(BlobEntry<'a>) -> ControlFlow<()> + Send + Sync + 'static,
{
    const KEY_LEN: usize = BlobKey::serialized_length_fixed();

    let rocksdb = store.partition_db().rocksdb().clone();
    let cf_name: restate_rocksdb::CfName = store.partition_db().partition().cf_name().into();

    async move {
        rocksdb
            .run_background_read_op(
                "df-blob",
                StorageTaskKind::MultiGet,
                Priority::Low,
                move |raw_db| -> Result<()> {
                    let Some(cf) = raw_db.cf_handle(cf_name.as_str()) else {
                        return Err(StorageError::Generic(anyhow::anyhow!(
                            "column family {cf_name} not found for blob multi-get"
                        )));
                    };

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
                            key_buf.put_slice(&create_blob_key(id).to_bytes());
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

                        let results = raw_db.batched_multi_get_cf_opt(&cf, keys, true, &readopts);

                        for (id, result) in batch_ids.iter().zip(results) {
                            let Some(value) =
                                result.map_err(|e| StorageError::Generic(e.into()))?
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
            )
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
        let partition_keys = match range {
            ScanBlobTableRange::BlobIdSet(ids) => {
                return Ok(multi_get_blob(self, ids, f).boxed());
            }
            ScanBlobTableRange::PartitionKey(partition_keys) => partition_keys,
        };

        let scan_fut = self
            .iterator_for_each(
                "df-blob",
                Priority::Low,
                TableScan::ScanPartitionKeyRange::<BlobKeyBuilder>(partition_keys),
                move |(mut key, value)| {
                    let (partition_key, blob_id) =
                        break_on_err(BlobKey::deserialize_from(&mut key))?.split();
                    let entry = break_on_err(decode_blob_entry(
                        BaseBlobId::new(partition_key, blob_id),
                        value,
                    ))?;

                    f(entry).map_break(Ok)
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(scan_fut.boxed())
    }
}

impl ReadBlobTable for PartitionStore {
    async fn get_blob<D: BlobDecoder>(&mut self, blob_id: BaseBlobId) -> Result<Option<D::Type>> {
        get_blob::<_, D>(self, blob_id)
    }
}

impl ReadBlobTable for PartitionStoreTransaction<'_> {
    async fn get_blob<D: BlobDecoder>(&mut self, blob_id: BaseBlobId) -> Result<Option<D::Type>> {
        get_blob::<_, D>(self, blob_id)
    }
}

impl WriteBlobTable for PartitionStoreTransaction<'_> {
    fn put_blob(&mut self, blob_id: BaseBlobId, blob: impl Blob) -> Result<()> {
        let key = create_blob_key(blob_id);

        let mut buf = BytesMut::new();
        let header = blob.header();

        buf.put_slice(BlobHeader::new(header.kind(), header.ref_count()).as_bytes());

        blob.encode(&mut buf)?;

        self.put_kv_raw(key, &buf)
    }

    fn decrement_ref_count(&mut self, blob_id: BaseBlobId, count: u16) {
        if count == 0 {
            return;
        }

        let key_buf = create_blob_key(blob_id).to_bytes();
        let value = HeaderUpdate::decrement(count);
        self.raw_merge_cf(KeyKind::Blob, key_buf, value.encode_to_bytes());
    }

    fn increment_ref_count(&mut self, blob_id: BaseBlobId, count: u16) {
        if count == 0 {
            return;
        }

        let key_buf = create_blob_key(blob_id).to_bytes();
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
        let mut value = BlobHeader::new(BlobKind::Output, ref_count)
            .as_bytes()
            .to_vec();
        assert_eq!(value, [BlobHeader::VERSION, BlobKind::Output as u8, 1, 44]);
        value.extend_from_slice(b"payload");

        let (header, payload) = BlobHeader::decode(&value).unwrap();
        assert_eq!(header.kind(), BlobKind::Output);
        assert_eq!(header.ref_count(), ref_count);
        assert_eq!(payload, b"payload");

        // empty, unknown version, truncated header, unknown kind
        assert!(BlobHeader::decode(&[]).is_err());
        assert!(BlobHeader::decode(&[BlobHeader::VERSION + 1, 1, 0, 1]).is_err());
        assert!(BlobHeader::decode(&[BlobHeader::VERSION, 1, 0]).is_err());
        assert!(BlobHeader::decode(&[BlobHeader::VERSION, 42, 0, 1]).is_err());
    }
}
