// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::Message;
use bytes::{Bytes, BytesMut};

use restate_core::io::IoExecutor;
use restate_rocksdb::RocksDbReadPerfGuard;
use restate_storage_api::blob_table::{
    Blob, BlobDecoder, BlobEncode, Header, ReadBlobTable, WriteBlobTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::blob::BlobId;
use restate_types::identifiers::ResourceId;

use crate::TableKind;
use crate::keys::{FixedLengthTableKey, KeyKind, define_table_key};
use crate::offloaded_reads::OffloadedRead;
use crate::{PartitionStore, PartitionStoreTransaction, ReadSnapshot, StorageAccess, StorageRead};

mod header;
pub use header::*;

define_table_key!(TableKind::Blob, KeyKind::Blob, BlobKey(blob_id: BlobId));

impl BlobKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH + BlobId::RAW_BYTES_LEN
    }
}

impl FixedLengthTableKey<{ BlobKey::serialized_length_fixed() }> for BlobKey {}

/// Bytes a blob costs on top of its payload: its key and header. Smaller content isn't worth
/// storing in a blob.
pub(crate) const BLOB_OVERHEAD: usize =
    BlobKey::serialized_length_fixed() + BlobHeader::BASE_ENCODED_LEN;

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

impl ReadSnapshot {
    /// Lends the payload of a blob to `f`, reading it from the admitted snapshot on the caller's
    /// thread. The payload is `None` if the blob doesn't exist or is marked for deletion.
    pub fn map_blob_payload_blocking<R>(
        &self,
        blob_id: BlobId,
        f: impl FnOnce(Option<&[u8]>) -> R,
    ) -> Result<R> {
        self.check_active()?;
        let _x = RocksDbReadPerfGuard::new("get-blob");
        let reader = self.storage_snapshot().reader();
        let value = reader
            .get_pinned(KeyKind::Blob, &BlobKey { blob_id }.to_bytes())
            .map_err(|error| StorageError::Generic(error.into()))?;
        Ok(f(live_value_payload(value.as_deref())?))
    }
}

/// Copies the payload of a blob. `None` if the blob doesn't exist or is marked for deletion.
/// A transaction sees the blobs it wrote.
pub(crate) fn get_blob_payload<S: StorageRead>(
    storage: &S,
    blob_id: BlobId,
) -> Result<Option<Bytes>> {
    let _x = RocksDbReadPerfGuard::new("get-blob");
    let value = storage.get(KeyKind::Blob, BlobKey { blob_id }.to_bytes())?;
    Ok(value
        .as_deref()
        .map(live_payload)
        .transpose()?
        .flatten()
        .map(Bytes::copy_from_slice))
}

/// Like [`get_blob_payload`], but if the blob isn't in memory, a worker of `executor` reads it, so
/// that the calling task doesn't wait for the disk.
///
/// Blob payloads never change once written, but a blob can be released after the referencing
/// entry was read. Read from the live store only blobs whose references can't be released
/// concurrently (e.g. journals of in-flight invocations); otherwise read from the same snapshot
/// as the referencing entry.
pub(crate) async fn get_blob_payload_cached_or_offload<S: OffloadedRead>(
    storage: &S,
    blob_id: BlobId,
    executor: &(impl IoExecutor + Sync),
) -> Result<Option<Bytes>> {
    let key = BlobKey { blob_id }.to_bytes();
    let value = storage
        .get_cached_or_offload("get-blob", KeyKind::Blob, &key, executor)?
        .await?;
    let value = value.as_ref().map(AsRef::as_ref);
    Ok(live_value_payload(value)?.map(Bytes::copy_from_slice))
}

/// The payload of an encoded blob, if there is one and it isn't marked for deletion.
fn live_value_payload(value: Option<&[u8]>) -> Result<Option<&[u8]>> {
    Ok(value.map(live_payload).transpose()?.flatten())
}

/// The payload of an encoded blob, unless it's marked for deletion.
fn live_payload(value: &[u8]) -> Result<Option<&[u8]>> {
    let (header, payload) = BlobHeader::decode(value)?;
    Ok((header.ref_count() > 0).then_some(payload))
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
    fn put_blob<B: BlobEncode>(&mut self, blob_id: BlobId, blob: Blob<B>) -> Result<u64> {
        let key = BlobKey { blob_id };

        let mut buf = BytesMut::new();

        BlobHeader::new(blob.kind(), blob.ref_count()).encode(&mut buf);
        let header_len = buf.len();

        blob.inner().encode_blob(&mut buf)?;
        let payload_len = (buf.len() - header_len) as u64;

        self.put_kv_raw(key, &buf)?;
        Ok(payload_len)
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
        let header = BlobHeader::new(BlobKind::Journal, ref_count);
        let mut value = Vec::new();
        header.encode(&mut value);
        assert_eq!(header.encoded_len(), value.len());
        let v = BlobHeader::VERSION;
        let journal = BlobKind::Journal as u8;
        assert_eq!(value, [v, 0, 9, journal, 0, 0, 0, 0, 0, 0, 1, 44]);
        value.extend_from_slice(b"payload");

        let (header, payload) = BlobHeader::decode(&value).unwrap();
        assert_eq!(header.kind(), BlobKind::Journal);
        assert_eq!(header.ref_count(), ref_count);
        assert_eq!(payload, b"payload");

        // empty, unknown version, truncated prelude, fields_len past the end,
        // fields_len shorter than the known fields, unknown kind
        let rc = [0, 0, 0, 0, 0, 0, 0, 1];
        let encoded =
            |version, fields_len, kind| [&[version, 0, fields_len, kind][..], &rc].concat();
        assert!(BlobHeader::decode(&encoded(v, 9, journal)).is_ok());
        assert!(BlobHeader::decode(&[]).is_err());
        assert!(BlobHeader::decode(&encoded(v + 1, 9, journal)).is_err());
        assert!(BlobHeader::decode(&[v, 0]).is_err());
        assert!(BlobHeader::decode(&encoded(v, 10, journal)).is_err());
        assert!(BlobHeader::decode(&encoded(v, 8, journal)).is_err());
        assert!(BlobHeader::decode(&encoded(v, 9, 42)).is_err());
    }
}
