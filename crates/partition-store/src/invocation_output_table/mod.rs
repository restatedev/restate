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
use restate_storage_api::output_table::{
    ReadInvocationOutputTable, ScanInvocationOutputTable, ScanOutputTableRange,
    WriteInvocationOutputTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::invocation::ResponseResult;
use restate_types::sharding::{PartitionKey, WithPartitionKey};

use crate::TableKind::InvocationOutput;
use crate::error::break_on_err;
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind, define_table_key};
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan};

define_table_key!(
    InvocationOutput,
    KeyKind::InvocationOutput,
    InvocationOutputKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

impl InvocationOutputKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            + InvocationUuid::RAW_BYTES_LEN
    }
}

/// Maximum number of invocation-output keys passed to one RocksDB multi-get call.
/// Kept low since each value holds a (potentially large) invocation output payload.
const INVOCATION_OUTPUT_MULTI_GET_BATCH_SIZE: usize = 25;

#[inline]
fn create_invocation_output_key(invocation_id: &InvocationId) -> InvocationOutputKey {
    InvocationOutputKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
    }
}

fn put_output<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    output_message: &ResponseResult,
) -> Result<()> {
    storage.put_kv_proto(create_invocation_output_key(invocation_id), output_message)
}

fn delete_output<S: StorageAccess>(storage: &mut S, invocation_id: &InvocationId) -> Result<()> {
    storage.delete_key(&create_invocation_output_key(invocation_id))
}

fn get_invocation_output<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<Option<ResponseResult>> {
    let _x = RocksDbReadPerfGuard::new("get-output");
    storage.get_value_proto(create_invocation_output_key(invocation_id))
}

fn multi_get_invocation_output<F>(
    store: &PartitionStore,
    ids: BTreeSet<InvocationId>,
    mut f: F,
) -> impl Future<Output = Result<()>> + Send
where
    F: FnMut((InvocationId, ResponseResult)) -> ControlFlow<()> + Send + Sync + 'static,
{
    const KEY_LEN: usize = InvocationOutputKey::serialized_length_fixed();

    let rocksdb = store.partition_db().rocksdb().clone();
    let cf_name: restate_rocksdb::CfName = store.partition_db().partition().cf_name().into();

    async move {
        rocksdb
            .run_background_read_op(
                "df-invocation-output",
                StorageTaskKind::MultiGet,
                Priority::Low,
                move |raw_db| -> Result<()> {
                    let Some(cf) = raw_db.cf_handle(cf_name.as_str()) else {
                        return Err(StorageError::Generic(anyhow::anyhow!(
                            "column family {cf_name} not found for invocation-output multi-get"
                        )));
                    };

                    let batch_capacity = ids.len().min(INVOCATION_OUTPUT_MULTI_GET_BATCH_SIZE);
                    let mut key_buf = BytesMut::with_capacity(batch_capacity * KEY_LEN);
                    let mut batch_ids = Vec::with_capacity(batch_capacity);

                    let mut readopts = ReadOptions::default();
                    readopts.set_async_io(true);
                    readopts.set_optimize_multiget_for_io(true);

                    let mut ids = ids.into_iter();
                    loop {
                        key_buf.clear();
                        batch_ids.clear();

                        for id in ids.by_ref().take(INVOCATION_OUTPUT_MULTI_GET_BATCH_SIZE) {
                            EncodeTableKey::serialize_to(
                                &create_invocation_output_key(&id),
                                &mut key_buf,
                            );
                            batch_ids.push(id);
                        }

                        if batch_ids.is_empty() {
                            break;
                        }

                        let (keys, remainder) = key_buf.as_chunks::<KEY_LEN>();
                        debug_assert!(
                            remainder.is_empty(),
                            "Each serialized InvocationOutputKey should have KEY_LEN"
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

                        if batch_ids.len() < INVOCATION_OUTPUT_MULTI_GET_BATCH_SIZE {
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

impl ReadInvocationOutputTable for PartitionStore {
    async fn get_invocation_output(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<ResponseResult>> {
        get_invocation_output(self, invocation_id)
    }
}

impl ScanInvocationOutputTable for PartitionStore {
    fn for_each_output<
        F: FnMut((InvocationId, ResponseResult)) -> ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: ScanOutputTableRange,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        if let ScanOutputTableRange::InvocationIdSet(ids) = range {
            return Ok(multi_get_invocation_output(self, ids, f).boxed());
        }

        let scan = match range {
            ScanOutputTableRange::PartitionKey(partition_key) => {
                TableScan::ScanPartitionKeyRange::<InvocationOutputKeyBuilder>(partition_key)
            }
            ScanOutputTableRange::InvocationId(invocation_id) => {
                let start = InvocationOutputKey::builder()
                    .partition_key(invocation_id.start().partition_key())
                    .invocation_uuid(invocation_id.start().invocation_uuid());

                let end = InvocationOutputKey::builder()
                    .partition_key(invocation_id.end().partition_key())
                    .invocation_uuid(invocation_id.end().invocation_uuid());

                TableScan::RangeInclusive(start, end)
            }
            ScanOutputTableRange::InvocationIdSet(_) => unreachable!("handled above"),
        };

        let scan_fut = self
            .iterator_for_each(
                "df-invocation-output",
                Priority::Low,
                scan,
                move |(mut key, mut value)| {
                    let output_key = break_on_err(InvocationOutputKey::deserialize_from(&mut key))?;
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

impl ReadInvocationOutputTable for PartitionStoreTransaction<'_> {
    async fn get_invocation_output(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<ResponseResult>> {
        get_invocation_output(self, invocation_id)
    }
}

impl WriteInvocationOutputTable for PartitionStoreTransaction<'_> {
    fn put_invocation_output(
        &mut self,
        invocation_id: &InvocationId,
        result: &ResponseResult,
    ) -> Result<()> {
        put_output(self, invocation_id, result)
    }

    fn delete_invocation_output(&mut self, invocation_id: &InvocationId) -> Result<()> {
        delete_output(self, invocation_id)
    }
}
