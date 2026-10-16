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

use bytes::BytesMut;
use futures::FutureExt;
use rocksdb::ReadOptions;

use restate_rocksdb::{Priority, RocksDbReadPerfGuard, StorageTaskKind};
use restate_storage_api::blob_table::{
    ReadBlobTable, ScanBlobTable, ScanBlobTableRange, WriteBlobTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::invocation::ResponseResult;
use restate_types::sharding::{PartitionKey, WithPartitionKey};

use crate::TableKind::Blob;
use crate::error::break_on_err;
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind, define_table_key};
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan};

define_table_key!(
    Blob,
    KeyKind::Blob,
    BlobKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

impl BlobKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            + InvocationUuid::RAW_BYTES_LEN
    }
}

/// Maximum number of blob keys passed to one RocksDB multi-get call.
/// Kept low since each value holds a (potentially large) invocation output payload.
const BLOB_MULTI_GET_BATCH_SIZE: usize = 25;

#[inline]
fn create_blob_key(invocation_id: &InvocationId) -> BlobKey {
    BlobKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
    }
}

fn put_blob<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    value: &ResponseResult,
) -> Result<()> {
    storage.put_kv_proto(create_blob_key(invocation_id), value)
}

fn delete_blob<S: StorageAccess>(storage: &mut S, invocation_id: &InvocationId) -> Result<()> {
    storage.delete_key(&create_blob_key(invocation_id))
}

fn get_blob<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<Option<ResponseResult>> {
    let _x = RocksDbReadPerfGuard::new("get-blob");
    storage.get_value_proto(create_blob_key(invocation_id))
}

fn multi_get_blob<F>(
    store: &PartitionStore,
    ids: BTreeSet<InvocationId>,
    mut f: F,
) -> impl Future<Output = Result<()>> + Send
where
    F: FnMut((InvocationId, ResponseResult)) -> ControlFlow<()> + Send + Sync + 'static,
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
                            EncodeTableKey::serialize_to(&create_blob_key(&id), &mut key_buf);
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
                            let output = ResponseResult::decode(&mut value.as_ref())?;

                            if f((*id, output)).is_break() {
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

impl ReadBlobTable for PartitionStore {
    async fn get_blob(&mut self, invocation_id: &InvocationId) -> Result<Option<ResponseResult>> {
        get_blob(self, invocation_id)
    }
}

impl ScanBlobTable for PartitionStore {
    fn for_each_blob<
        F: FnMut((InvocationId, ResponseResult)) -> ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: ScanBlobTableRange,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        if let ScanBlobTableRange::InvocationIdSet(ids) = range {
            return Ok(multi_get_blob(self, ids, f).boxed());
        }

        let scan = match range {
            ScanBlobTableRange::PartitionKey(partition_key) => {
                TableScan::ScanPartitionKeyRange::<BlobKeyBuilder>(partition_key)
            }
            ScanBlobTableRange::InvocationId(invocation_id) => {
                let start = BlobKey::builder()
                    .partition_key(invocation_id.start().partition_key())
                    .invocation_uuid(invocation_id.start().invocation_uuid());

                let end = BlobKey::builder()
                    .partition_key(invocation_id.end().partition_key())
                    .invocation_uuid(invocation_id.end().invocation_uuid());

                TableScan::RangeInclusive(start, end)
            }
            ScanBlobTableRange::InvocationIdSet(_) => unreachable!("handled above"),
        };

        let scan_fut = self
            .iterator_for_each(
                "df-blob",
                Priority::Low,
                scan,
                move |(mut key, mut value)| {
                    let output_key = break_on_err(BlobKey::deserialize_from(&mut key))?;
                    let (partition_key, invocation_uuid) = output_key.split();
                    let output = break_on_err(ResponseResult::decode(&mut value))?;

                    f((
                        InvocationId::from_parts(partition_key, invocation_uuid),
                        output,
                    ))
                    .map_break(Ok)
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(scan_fut.boxed())
    }
}

impl ReadBlobTable for PartitionStoreTransaction<'_> {
    async fn get_blob(&mut self, invocation_id: &InvocationId) -> Result<Option<ResponseResult>> {
        get_blob(self, invocation_id)
    }
}

impl WriteBlobTable for PartitionStoreTransaction<'_> {
    fn put_blob(&mut self, invocation_id: &InvocationId, result: &ResponseResult) -> Result<()> {
        put_blob(self, invocation_id, result)
    }

    fn delete_blob(&mut self, invocation_id: &InvocationId) -> Result<()> {
        delete_blob(self, invocation_id)
    }
}
